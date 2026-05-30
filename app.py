"""
app/streamlit_app/app.py

Streamlit UI scaffold for CallSage:
- Sidebar: config and environment controls
- Main: Start/Stop streaming, live transcript, recommendations, sentiment
- Background thread: connects to backend WebSocket ingest endpoint and updates session_state
Notes:
- Replace WS_ENDPOINT with your FastAPI WebSocket ingest endpoint.
- Backend must send JSON messages with keys: type, payload.
  Example messages:
    {"type":"partial_transcript","payload":{"text":"...","speaker":"rep"}}
    {"type":"final_transcript","payload":{"text":"...","speaker":"rep"}}
    {"type":"recommendation","payload":{"items":[{"id":"p1","title":"Phone X","score":0.92}]}}
    {"type":"sentiment","payload":{"score":0.12,"label":"neutral"}}
"""

import streamlit as st
import threading
import asyncio
import json
import time
from typing import Any, Dict, List

# Optional: websockets client. Ensure it's in requirements for real use.
# pip install websockets
try:
    import websockets
except Exception:
    websockets = None  # Placeholder: backend streaming requires websockets package

# ---------- Config / Defaults ----------
WS_ENDPOINT = st.secrets.get("WS_ENDPOINT", "ws://localhost:8000/ingest")
RECONNECT_DELAY = 2.0  # seconds

# ---------- Session state initialization ----------
if "streaming" not in st.session_state:
    st.session_state.streaming = False
if "transcript" not in st.session_state:
    st.session_state.transcript = []  # list of dicts: {"text":..., "speaker":..., "final":bool}
if "recommendations" not in st.session_state:
    st.session_state.recommendations = []  # list of dicts
if "sentiment" not in st.session_state:
    st.session_state.sentiment = {"score": None, "label": None}
if "ws_thread" not in st.session_state:
    st.session_state.ws_thread = None
if "stop_event" not in st.session_state:
    st.session_state.stop_event = threading.Event()


# ---------- Helper functions ----------
def append_transcript(text: str, speaker: str = "unknown", final: bool = False) -> None:
    st.session_state.transcript.append({"text": text, "speaker": speaker, "final": final})


def set_recommendations(items: List[Dict[str, Any]]) -> None:
    st.session_state.recommendations = items


def set_sentiment(score: float, label: str) -> None:
    st.session_state.sentiment = {"score": score, "label": label}


# Background WebSocket listener (runs in separate thread)
def _ws_listener_loop(ws_endpoint: str, stop_event: threading.Event):
    """
    Connects to backend WebSocket and listens for JSON messages.
    This function runs in a dedicated thread and uses asyncio event loop for websockets.
    Replace message handling with your backend message schema.
    """
    if websockets is None:
        # If websockets package is not installed, simulate messages for demo
        _simulate_stream(stop_event)
        return

    async def _run():
        while not stop_event.is_set():
            try:
                async with websockets.connect(ws_endpoint) as ws:
                    # Optionally send an init message
                    await ws.send(json.dumps({"type": "init", "payload": {"client": "streamlit"}}))
                    while not stop_event.is_set():
                        msg = await ws.recv()
                        _handle_ws_message(msg)
            except Exception as e:
                # Log and retry
                print(f"[ws_listener] connection error: {e}. Reconnecting in {RECONNECT_DELAY}s")
                await asyncio.sleep(RECONNECT_DELAY)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_run())
    finally:
        loop.close()


def _handle_ws_message(raw_msg: str) -> None:
    """
    Parse backend message and update session_state.
    Expected JSON structure: {"type": "...", "payload": {...}}
    """
    try:
        msg = json.loads(raw_msg)
    except Exception:
        print("[ws_listener] invalid json:", raw_msg)
        return

    mtype = msg.get("type")
    payload = msg.get("payload", {})

    if mtype == "partial_transcript":
        text = payload.get("text", "")
        speaker = payload.get("speaker", "unknown")
        # For partial transcripts we append as non-final
        append_transcript(text, speaker, final=False)
    elif mtype == "final_transcript":
        text = payload.get("text", "")
        speaker = payload.get("speaker", "unknown")
        append_transcript(text, speaker, final=True)
    elif mtype == "recommendation":
        items = payload.get("items", [])
        set_recommendations(items)
    elif mtype == "sentiment":
        score = payload.get("score")
        label = payload.get("label")
        set_sentiment(score, label)
    else:
        # Unknown message type: store as debug transcript
        append_transcript(f"[debug] {raw_msg}", "system", final=True)


def _simulate_stream(stop_event: threading.Event):
    """
    Demo fallback when websockets package is not available.
    Emits simulated messages to session_state for UI testing.
    """
    demo_texts = [
        ("Hello, thanks for calling. How can I help you today?", "rep"),
        ("I'm looking for a phone with long battery life and good camera.", "customer"),
        ("We have Phone X with 48h battery and 108MP camera.", "rep"),
        ("That sounds good. What's the price?", "customer"),
        ("Phone X is $499. We also have Phone Y at $399.", "rep"),
    ]
    idx = 0
    while not stop_event.is_set() and idx < len(demo_texts):
        text, speaker = demo_texts[idx]
        append_transcript(text, speaker, final=True)
        # Simulate recommendation after second customer utterance
        if idx == 1:
            set_recommendations([
                {"id": "phone_x", "title": "Phone X", "price": "$499", "score": 0.95},
                {"id": "phone_y", "title": "Phone Y", "price": "$399", "score": 0.78},
            ])
            set_sentiment(0.1, "neutral")
        idx += 1
        time.sleep(1.5)


def start_streaming():
    if st.session_state.streaming:
        return
    st.session_state.stop_event.clear()
    st.session_state.streaming = True
    thread = threading.Thread(target=_ws_listener_loop, args=(WS_ENDPOINT, st.session_state.stop_event), daemon=True)
    st.session_state.ws_thread = thread
    thread.start()


def stop_streaming():
    if not st.session_state.streaming:
        return
    st.session_state.stop_event.set()
    st.session_state.streaming = False
    # Allow thread to exit gracefully
    time.sleep(0.2)


# ---------- Streamlit UI ----------
st.set_page_config(page_title="CallSage — Live Assistant", layout="wide")
st.title("CallSage — Real‑time Sales Call Assistant")

# Sidebar controls
with st.sidebar:
    st.header("Controls")
    env = st.selectbox("Environment", ["dev", "staging", "prod"], index=0)
    st.markdown("**Streaming endpoint**")
    st.text_input("WebSocket endpoint", value=WS_ENDPOINT, key="ws_endpoint_input")
    st.markdown("---")
    st.button("Start Streaming", on_click=start_streaming)
    st.button("Stop Streaming", on_click=stop_streaming)
    st.markdown("---")
    st.header("Status")
    st.write("**Streaming:**", "✅" if st.session_state.streaming else "⛔")
    st.write("WS Endpoint:", st.session_state.get("ws_endpoint_input", WS_ENDPOINT))
    st.write("Transcript lines:", len(st.session_state.transcript))
    st.write("Recommendations:", len(st.session_state.recommendations))
    st.write("Sentiment:", st.session_state.sentiment.get("label"))

# Main layout: transcript (left), recommendations (right)
left_col, right_col = st.columns([2, 1])

with left_col:
    st.subheader("Live Transcript")
    transcript_container = st.container()
    # Render transcript with most recent at bottom
    for idx, item in enumerate(st.session_state.transcript):
        speaker = item.get("speaker", "unknown")
        text = item.get("text", "")
        final = item.get("final", False)
        style = "font-weight:600;" if final else "opacity:0.7;"
        transcript_container.markdown(f"**{speaker}**: <span style='{style}'>{text}</span>", unsafe_allow_html=True)

    st.markdown("---")
    st.subheader("Sentiment Timeline")
    sentiment = st.session_state.sentiment
    st.write(f"**Label:** {sentiment.get('label')}  •  **Score:** {sentiment.get('score')}")

with right_col:
    st.subheader("Recommendations")
    rec_container = st.container()
    if st.session_state.recommendations:
        for rec in st.session_state.recommendations:
            title = rec.get("title", "Unknown")
            price = rec.get("price", "")
            score = rec.get("score", 0.0)
            rec_container.markdown(f"**{title}**  \nPrice: {price}  \nScore: {score:.2f}")
            # Action buttons (non-functional placeholders)
            cols = rec_container.columns([1, 1, 1])
            if cols[0].button("Add to Quote", key=f"quote_{title}"):
                st.toast(f"Added {title} to quote (placeholder)")
            if cols[1].button("Send Link", key=f"link_{title}"):
                st.toast(f"Sent product link for {title} (placeholder)")
            if cols[2].button("More Info", key=f"info_{title}"):
                st.toast(f"Showing details for {title} (placeholder)")
            rec_container.markdown("---")
    else:
        rec_container.info("No recommendations yet. Start streaming or upload a sample call.")

# Footer / utilities
st.markdown("---")
st.caption("CallSage demo UI — replace placeholders with your backend endpoints and auth.")

# Graceful shutdown on page unload (best-effort)
def _on_exit():
    stop_streaming()

# Note: Streamlit does not provide a reliable page-unload hook; this is a best-effort call.
st.button("Shutdown (stop streaming)", on_click=_on_exit)
