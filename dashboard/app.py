from __future__ import annotations

import os
from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st
from streamlit_autorefresh import st_autorefresh

API_DEFAULT = os.getenv("MACHINOCARE_API_URL", "http://localhost:8000")
THINGSPEAK_CHANNEL_DEFAULT = os.getenv("MACHINOCARE_THINGSPEAK_CHANNEL", "3336916")

st.set_page_config(
    page_title="MachinoCare Live Control Room",
    page_icon="MM",
    layout="wide",
)

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;700&family=IBM+Plex+Mono:wght@400;600&display=swap');
    :root {
        --mc-bg: radial-gradient(circle at 15% 10%, #e9f6ff 0%, #f8f2dc 45%, #f3f9f0 100%);
        --mc-card: rgba(255, 255, 255, 0.82);
        --mc-border: rgba(16, 44, 61, 0.2);
        --mc-healthy: #1b8a5a;
        --mc-alert: #d91e18;
        --mc-ink: #0b1f2a;
        --mc-subtle: #445b68;
    }

    html, body, [class*="css"] {
        font-family: 'Space Grotesk', sans-serif;
        color: var(--mc-ink);
    }

    .stApp {
        background: var(--mc-bg);
    }

    .mc-glass {
        background: var(--mc-card);
        border: 1px solid var(--mc-border);
        border-radius: 18px;
        padding: 0.9rem 1rem;
        box-shadow: 0 12px 35px rgba(11, 31, 42, 0.08);
    }

    .mc-title {
        letter-spacing: 0.08em;
        text-transform: uppercase;
        font-size: 0.78rem;
        color: var(--mc-subtle);
        margin-bottom: 0.25rem;
    }

    .mc-value {
        font-size: 1.7rem;
        font-weight: 700;
        line-height: 1.1;
    }

    .mc-status {
        font-size: 1.05rem;
        font-weight: 700;
        border-radius: 999px;
        display: inline-block;
        padding: 0.45rem 0.9rem;
        letter-spacing: 0.05em;
        text-transform: uppercase;
    }

    .mc-healthy {
        background: rgba(27, 138, 90, 0.14);
        color: var(--mc-healthy);
        border: 1px solid rgba(27, 138, 90, 0.32);
    }

    .mc-anomaly {
        background: rgba(217, 30, 24, 0.13);
        color: var(--mc-alert);
        border: 1px solid rgba(217, 30, 24, 0.32);
    }

    .mc-job {
        border-radius: 14px;
        border: 1px solid rgba(16, 44, 61, 0.18);
        padding: 0.8rem;
        background: rgba(255, 255, 255, 0.7);
    }

    code, pre {
        font-family: 'IBM Plex Mono', monospace;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def request_json(url: str, method: str = "GET", payload: dict | None = None) -> tuple[dict | None, str | None]:
    try:
        if method == "POST":
            response = requests.post(url, json=payload, timeout=5)
        else:
            response = requests.get(url, timeout=5)
        response.raise_for_status()
        return response.json(), None
    except Exception as exc:  # noqa: BLE001
        return None, str(exc)


def thingspeak_history(channel_id: str, results: int = 60) -> tuple[pd.DataFrame | None, str | None]:
    url = f"https://api.thingspeak.com/channels/{channel_id}/feeds.json?results={results}"
    data, error = request_json(url)
    if error:
        return None, error
    feeds = data.get("feeds", []) if data else []
    if not feeds:
        return pd.DataFrame(), None

    frame = pd.DataFrame(feeds)
    if "created_at" in frame.columns:
        frame["created_at"] = pd.to_datetime(frame["created_at"], errors="coerce")
    if "field1" in frame.columns:
        frame["field1"] = pd.to_numeric(frame["field1"], errors="coerce")
    return frame, None


def status_badge(is_anomaly: bool, status_label: str) -> str:
    css_class = "mc-anomaly" if is_anomaly else "mc-healthy"
    return f"<span class='mc-status {css_class}'>{status_label}</span>"


st_autorefresh(interval=1000, key="machinocare_refresh")

if "active_job_id" not in st.session_state:
    st.session_state.active_job_id = None
if "active_job_machine" not in st.session_state:
    st.session_state.active_job_machine = None
if "active_job_device" not in st.session_state:
    st.session_state.active_job_device = None

st.title("MachinoCare - AI Predictive Maintenance")
st.caption("Live vibration intelligence with backend-driven calibration and edge-safe inference.")

with st.sidebar:
    st.header("Controls")
    api_base = st.text_input("FastAPI Base URL", API_DEFAULT)

    machines_data, _ = request_json(f"{api_base}/api/v1/machines")
    machine_options = machines_data.get("machines", []) if machines_data else []

    default_machine = "Fan_1"
    if machine_options:
        selected_machine = st.selectbox("Machine", machine_options)
    else:
        selected_machine = st.text_input("Machine", default_machine)

    device_data, _ = request_json(f"{api_base}/api/v1/devices/{selected_machine}")
    device_options = device_data.get("devices", []) if device_data else []
    default_device = "esp32_fan_1"
    if device_options:
        selected_device = st.selectbox("Device", device_options)
    else:
        selected_device = st.text_input("Device", default_device)

    lookback_seconds = st.slider("Live lookback (seconds)", min_value=30, max_value=600, value=120, step=10)

    st.subheader("Calibration")
    sample_rate_hz = st.number_input("Sample rate (Hz)", min_value=1, max_value=500, value=10, step=1)
    fallback_seconds = st.number_input("Fallback window (s)", min_value=30, max_value=3600, value=300, step=30)
    contamination = st.slider("Isolation Forest contamination", min_value=0.01, max_value=0.40, value=0.05, step=0.01)
    new_device_setup = st.toggle("New device setup", value=True)

    if st.button("Start calibration training", use_container_width=True):
        payload = {
            "machine_id": selected_machine,
            "device_id": selected_device,
            "sample_rate_hz": int(sample_rate_hz),
            "fallback_seconds": int(fallback_seconds),
            "window_seconds": 1,
            "contamination": float(contamination),
            "min_consecutive_windows": 3,
            "new_device_setup": bool(new_device_setup),
            "trigger_source": "dashboard_ui",
        }
        start_data, start_error = request_json(
            f"{api_base}/api/v1/calibrate/start",
            method="POST",
            payload=payload,
        )
        if start_error:
            st.error(f"Calibration start failed: {start_error}")
        else:
            st.session_state.active_job_id = start_data.get("job_id")
            st.session_state.active_job_machine = selected_machine
            st.session_state.active_job_device = selected_device
            st.success(f"Calibration job started: {st.session_state.active_job_id}")

active_job_data = None
if st.session_state.active_job_id:
    active_job_data, _ = request_json(f"{api_base}/api/v1/calibrate/status/{st.session_state.active_job_id}")
    if active_job_data and active_job_data.get("status") in {"completed", "failed"}:
        # Keep the final result visible for one cycle, then clear to avoid stale polling.
        st.session_state.completed_job = dict(active_job_data)
        st.session_state.active_job_id = None

status_data, status_error = request_json(
    f"{api_base}/api/v1/status/{selected_machine}/{selected_device}"
)
recent_data, recent_error = request_json(
    f"{api_base}/api/v1/stream/{selected_machine}/recent?seconds={lookback_seconds}&limit=5000&device_id={selected_device}"
)

if status_error and recent_error:
    st.error("Unable to reach backend. Confirm FastAPI is running and URL is correct.")
    st.stop()

status_data = status_data or {}
current = status_data.get("current", {})
calibration = status_data.get("calibration", {})
model_summary = status_data.get("model_summary", {})

is_anomaly = bool(status_data.get("is_anomaly", False))
status_label = status_data.get("status_label", "UNKNOWN")

metric_cols = st.columns(4)
metric_html = [
    ("Current Acc Magnitude", current.get("acc_mag")),
    ("Anomaly Score", current.get("score")),
    ("Decision Threshold", current.get("decision_threshold") or model_summary.get("decision_threshold")),
    ("Model Version", calibration.get("model_version")),
]
for col, (label, value) in zip(metric_cols, metric_html):
    with col:
        st.markdown(
            (
                "<div class='mc-glass'>"
                f"<div class='mc-title'>{label}</div>"
                f"<div class='mc-value'>{value if value is not None else 'n/a'}</div>"
                "</div>"
            ),
            unsafe_allow_html=True,
        )

cal_progress = int(calibration.get("progress") or 0)
cal_stage = calibration.get("stage") or "idle"
cal_message = calibration.get("message") or "Calibration idle"
cal_job_id = calibration.get("job_id")

if active_job_data:
    st.markdown("<div class='mc-glass'>", unsafe_allow_html=True)
    st.markdown("<div class='mc-title'>Live Calibration Training</div>", unsafe_allow_html=True)
    st.write(
        {
            "job_id": active_job_data.get("job_id"),
            "status": active_job_data.get("status"),
            "stage": active_job_data.get("stage"),
            "trigger_source": active_job_data.get("trigger_source"),
            "new_device_setup": active_job_data.get("new_device_setup"),
        }
    )
    st.progress(int(active_job_data.get("progress", 0)))
    st.caption(active_job_data.get("message") or "Training in progress")
    st.markdown("</div>", unsafe_allow_html=True)

elif st.session_state.get("completed_job"):
    completed = st.session_state.completed_job
    st.markdown("<div class='mc-glass'>", unsafe_allow_html=True)
    st.markdown("<div class='mc-title'>Last Calibration Job</div>", unsafe_allow_html=True)
    st.write(
        {
            "job_id": completed.get("job_id"),
            "status": completed.get("status"),
            "stage": completed.get("stage"),
            "progress": completed.get("progress"),
        }
    )
    if completed.get("status") == "completed":
        result = completed.get("result", {})
        pkg = result.get("model_package", {})
        st.success("Model package generated and ready for device")
        st.write(
            {
                "model_version": pkg.get("model_version"),
                "checksum": pkg.get("checksum"),
                "target_device_id": pkg.get("target_device_id"),
            }
        )
    else:
        st.error(completed.get("error") or "Calibration failed")
    st.markdown("</div>", unsafe_allow_html=True)

left, right = st.columns([2.8, 1.2])
with left:
    samples = (recent_data or {}).get("samples", [])
    if not samples:
        st.warning("No live samples available yet. Start streaming to /api/v1/stream.")
    else:
        frame = pd.DataFrame(samples)
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], errors="coerce")
        frame = frame.dropna(subset=["timestamp"])

        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=frame["timestamp"],
                y=frame["acc_mag"],
                mode="lines",
                name="acc_mag",
                line={"color": "#1177cc", "width": 3},
            )
        )

        threshold_value = current.get("decision_threshold") or model_summary.get("decision_threshold")
        if threshold_value is not None:
            fig.add_hline(
                y=float(threshold_value),
                line_dash="dash",
                line_color="#d91e18",
                line_width=2,
                annotation_text="AI threshold",
                annotation_position="top right",
            )

        fig.update_layout(
            title="Live Vibration Waveform",
            template="plotly_white",
            margin={"l": 20, "r": 20, "t": 45, "b": 20},
            xaxis_title="Timestamp",
            yaxis_title="Acceleration Magnitude",
            hovermode="x unified",
            legend={"orientation": "h", "y": 1.1, "x": 0},
        )
        st.plotly_chart(fig, use_container_width=True)

with right:
    st.markdown(
        (
            "<div class='mc-glass'>"
            "<div class='mc-title'>Machine Status</div>"
            f"{status_badge(is_anomaly, status_label)}"
            "<div style='margin-top:0.8rem; font-size:0.95rem;'>"
            f"Machine: {selected_machine}<br/>"
            f"Device: {selected_device}<br/>"
            f"Last update: {current.get('last_update', 'n/a')}<br/>"
            f"Consecutive anomaly windows: {current.get('consecutive_windows', 0)}<br/>"
            f"Checksum: {calibration.get('model_checksum', 'n/a')}"
            "</div>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )

    st.markdown("<div style='height: 0.8rem;'></div>", unsafe_allow_html=True)

    st.markdown("<div class='mc-glass'>", unsafe_allow_html=True)
    st.markdown("<div class='mc-title'>Calibration Runtime</div>", unsafe_allow_html=True)
    st.progress(cal_progress)
    st.write(
        {
            "job_id": cal_job_id,
            "progress": cal_progress,
            "stage": cal_stage,
            "message": cal_message,
            "last_calibration_at": calibration.get("last_calibration_at"),
            "model_version": calibration.get("model_version"),
            "window_size": model_summary.get("window_size"),
            "quality_correlation": model_summary.get("quality_correlation"),
        }
    )
    st.markdown("</div>", unsafe_allow_html=True)

st.subheader("ThingSpeak Historical Trend")
use_thingspeak = st.toggle("Load ThingSpeak history", value=False)
if use_thingspeak:
    channel_id = st.text_input("ThingSpeak Channel ID", THINGSPEAK_CHANNEL_DEFAULT)
    history, history_error = thingspeak_history(channel_id=channel_id, results=80)
    if history_error:
        st.error(f"ThingSpeak fetch failed: {history_error}")
    elif history is None or history.empty:
        st.info("No ThingSpeak history available.")
    else:
        hist_fig = go.Figure()
        hist_fig.add_trace(
            go.Scatter(
                x=history["created_at"],
                y=history["field1"],
                mode="lines+markers",
                name="ThingSpeak field1 (acc)",
                line={"color": "#7e5bef", "width": 2},
                marker={"size": 6},
            )
        )
        hist_fig.update_layout(
            template="plotly_white",
            margin={"l": 20, "r": 20, "t": 30, "b": 20},
            xaxis_title="Timestamp",
            yaxis_title="ThingSpeak Acc Magnitude",
        )
        st.plotly_chart(hist_fig, use_container_width=True)

footer_stamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
st.caption(f"Dashboard refresh timestamp: {footer_stamp}")
