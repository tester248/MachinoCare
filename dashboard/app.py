from __future__ import annotations

import os
from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st

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
        --mc-card: rgba(255, 255, 255, 0.94);
        --mc-border: rgba(16, 44, 61, 0.2);
        --mc-healthy: #1b8a5a;
        --mc-alert: #d91e18;
        --mc-ink: #0b1f2a;
        --mc-subtle: #445b68;
        --mc-sidebar-bg: linear-gradient(180deg, #1f2430 0%, #212634 100%);
        --mc-sidebar-ink: #f3f6ff;
        --mc-sidebar-input-bg: #0a1020;
    }

    html, body {
        font-family: 'Space Grotesk', sans-serif;
    }

    [data-testid="stStatusWidget"] {
        visibility: hidden;
        height: 0;
    }

    [data-testid="stAppViewContainer"] {
        background: var(--mc-bg);
    }

    [data-testid="stSidebar"] {
        background: var(--mc-sidebar-bg) !important;
    }

    [data-testid="stSidebar"] * {
        color: var(--mc-sidebar-ink) !important;
    }

    [data-testid="stSidebar"] input,
    [data-testid="stSidebar"] textarea,
    [data-testid="stSidebar"] [data-baseweb="select"] > div,
    [data-testid="stSidebar"] [data-baseweb="input"] > div {
        background: var(--mc-sidebar-input-bg) !important;
        border-color: rgba(173, 188, 255, 0.22) !important;
    }

    .mc-glass {
        background: var(--mc-card);
        border: 1px solid var(--mc-border);
        border-radius: 18px;
        padding: 0.9rem 1rem;
        box-shadow: 0 12px 35px rgba(11, 31, 42, 0.08);
        color: var(--mc-ink) !important;
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
        color: var(--mc-ink) !important;
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
        color: var(--mc-healthy) !important;
        border: 1px solid rgba(27, 138, 90, 0.32);
    }

    .mc-anomaly {
        background: rgba(217, 30, 24, 0.13);
        color: var(--mc-alert) !important;
        border: 1px solid rgba(217, 30, 24, 0.32);
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
    for col in ["field1", "field2", "field3", "field4", "field5", "field6"]:
        if col in frame.columns:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")
    return frame, None


def status_badge(is_anomaly: bool, status_label: str) -> str:
    css_class = "mc-anomaly" if is_anomaly else "mc-healthy"
    return f"<span class='mc-status {css_class}'>{status_label}</span>"


def normalize_recent_samples(samples: list[dict]) -> pd.DataFrame:
    if not samples:
        return pd.DataFrame()

    frame = pd.DataFrame(samples)
    rename_map = {
        "accMag": "acc_mag",
        "gyroMag": "gyro_mag",
        "isAnomaly": "is_anomaly",
        "decisionThreshold": "decision_threshold",
        "windowIndex": "window_index",
    }
    frame = frame.rename(columns=rename_map)

    if "timestamp" in frame.columns:
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], errors="coerce")
        frame = frame.dropna(subset=["timestamp"])

    for col in ["acc_mag", "gyro_mag", "gx", "gy", "gz", "sw420", "score", "decision_threshold"]:
        if col in frame.columns:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")

    if "is_anomaly" in frame.columns:
        frame["is_anomaly"] = frame["is_anomaly"].astype(str).str.lower().isin(["1", "true", "yes"])

    return frame


if "active_job_id" not in st.session_state:
    st.session_state.active_job_id = None
if "active_job_machine" not in st.session_state:
    st.session_state.active_job_machine = None
if "active_job_device" not in st.session_state:
    st.session_state.active_job_device = None
if "completed_job" not in st.session_state:
    st.session_state.completed_job = None

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
    refresh_seconds = st.slider("Live refresh (seconds)", min_value=1, max_value=10, value=1, step=1)

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


def render_live_ui() -> None:
    active_job_data = None
    if st.session_state.active_job_id:
        active_job_data, _ = request_json(f"{api_base}/api/v1/calibrate/status/{st.session_state.active_job_id}")
        if active_job_data and active_job_data.get("status") in {"completed", "failed"}:
            st.session_state.completed_job = dict(active_job_data)
            st.session_state.active_job_id = None

    status_data, status_error = request_json(f"{api_base}/api/v1/status/{selected_machine}/{selected_device}")
    recent_data, recent_error = request_json(
        f"{api_base}/api/v1/stream/{selected_machine}/recent?seconds={lookback_seconds}&limit=5000&device_id={selected_device}"
    )

    if status_error and recent_error:
        st.error("Unable to reach backend. Confirm FastAPI is running and URL is correct.")
        return

    status_data = status_data or {}
    current = status_data.get("current", {})
    calibration = status_data.get("calibration", {})
    model_summary = status_data.get("model_summary", {})

    is_anomaly = bool(status_data.get("is_anomaly", False))
    status_label = status_data.get("status_label", "UNKNOWN")

    metric_cols = st.columns(4)
    metric_html = [
        ("Current Acc Magnitude", current.get("acc_mag")),
        ("Current Gyro Magnitude", current.get("gyro_mag")),
        ("Anomaly Score", current.get("score")),
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

    if active_job_data:
        st.markdown("<div class='mc-glass'>", unsafe_allow_html=True)
        st.markdown("<div class='mc-title'>Live Calibration Training</div>", unsafe_allow_html=True)
        st.json(active_job_data)
        st.progress(int(active_job_data.get("progress", 0)))
        st.caption(active_job_data.get("message") or "Training in progress")
        st.markdown("</div>", unsafe_allow_html=True)
    elif st.session_state.get("completed_job"):
        st.markdown("<div class='mc-glass'>", unsafe_allow_html=True)
        st.markdown("<div class='mc-title'>Last Calibration Job</div>", unsafe_allow_html=True)
        st.json(st.session_state.completed_job)
        st.markdown("</div>", unsafe_allow_html=True)

    left, right = st.columns([2.8, 1.2])
    with left:
        samples = (recent_data or {}).get("samples", [])
        frame = normalize_recent_samples(samples)

        if frame.empty:
            st.warning("No live samples available yet. Start streaming to /api/v1/stream.")
        else:
            fig_primary = go.Figure()
            for col, color in [("acc_mag", "#1177cc"), ("gyro_mag", "#059669"), ("score", "#d91e18")]:
                if col in frame.columns:
                    fig_primary.add_trace(
                        go.Scatter(
                            x=frame["timestamp"],
                            y=frame[col],
                            mode="lines",
                            name=col,
                            line={"color": color, "width": 2},
                        )
                    )

            threshold_value = current.get("decision_threshold") or model_summary.get("decision_threshold")
            if threshold_value is not None and "score" in frame.columns:
                fig_primary.add_hline(
                    y=float(threshold_value),
                    line_dash="dash",
                    line_color="#7f1d1d",
                    line_width=2,
                    annotation_text="decision threshold",
                    annotation_position="top right",
                )

            fig_primary.update_layout(
                title="Live Magnitude + Score",
                template="plotly_white",
                margin={"l": 20, "r": 20, "t": 45, "b": 20},
                xaxis_title="Timestamp",
                yaxis_title="Value",
                hovermode="x unified",
                legend={"orientation": "h", "y": 1.1, "x": 0},
            )
            st.plotly_chart(fig_primary, use_container_width=True)

            fig_axes = go.Figure()
            for col, color in [("gx", "#5b21b6"), ("gy", "#0f766e"), ("gz", "#b45309")]:
                if col in frame.columns:
                    fig_axes.add_trace(
                        go.Scatter(
                            x=frame["timestamp"],
                            y=frame[col],
                            mode="lines",
                            name=col,
                            line={"color": color, "width": 2},
                        )
                    )
            if "sw420" in frame.columns:
                fig_axes.add_trace(
                    go.Scatter(
                        x=frame["timestamp"],
                        y=frame["sw420"],
                        mode="lines",
                        name="sw420",
                        line={"color": "#111827", "width": 2, "dash": "dot"},
                        yaxis="y2",
                    )
                )
                fig_axes.update_layout(
                    yaxis2={"overlaying": "y", "side": "right", "title": "sw420"},
                )

            fig_axes.update_layout(
                title="Axis Vibration + SW420",
                template="plotly_white",
                margin={"l": 20, "r": 20, "t": 45, "b": 20},
                xaxis_title="Timestamp",
                yaxis_title="Gyro Axis",
                hovermode="x unified",
                legend={"orientation": "h", "y": 1.1, "x": 0},
            )
            st.plotly_chart(fig_axes, use_container_width=True)

            latest = frame.tail(1).copy()
            if not latest.empty:
                st.subheader("Latest Sample - All Values")
                latest_t = latest.T
                latest_t.columns = ["value"]
                st.dataframe(latest_t, use_container_width=True, height=420)

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
        st.progress(int(calibration.get("progress") or 0))
        st.json(
            {
                "job_id": calibration.get("job_id"),
                "progress": calibration.get("progress"),
                "stage": calibration.get("stage"),
                "message": calibration.get("message"),
                "last_calibration_at": calibration.get("last_calibration_at"),
                "model_version": calibration.get("model_version"),
                "model_checksum": calibration.get("model_checksum"),
                "window_size": model_summary.get("window_size"),
                "quality_correlation": model_summary.get("quality_correlation"),
                "decision_threshold": model_summary.get("decision_threshold"),
                "fallback_threshold": model_summary.get("fallback_threshold"),
            }
        )
        st.markdown("</div>", unsafe_allow_html=True)

    with st.expander("Backend Payload - Full Status JSON", expanded=False):
        st.json(status_data)

    with st.expander("Backend Payload - Full Recent Stream JSON", expanded=False):
        st.json(recent_data or {})


@st.fragment(run_every=f"{refresh_seconds}s")
def live_fragment() -> None:
    render_live_ui()


live_fragment()

st.subheader("ThingSpeak Historical Trend")
use_thingspeak = st.toggle("Load ThingSpeak history", value=False)
if use_thingspeak:
    channel_id = st.text_input("ThingSpeak Channel ID", THINGSPEAK_CHANNEL_DEFAULT)
    history, history_error = thingspeak_history(channel_id=channel_id, results=120)
    if history_error:
        st.error(f"ThingSpeak fetch failed: {history_error}")
    elif history is None or history.empty:
        st.info("No ThingSpeak history available.")
    else:
        hist_fig = go.Figure()
        for field, color in [
            ("field1", "#1d4ed8"),
            ("field2", "#0f766e"),
            ("field3", "#7e22ce"),
            ("field4", "#b45309"),
            ("field5", "#be123c"),
            ("field6", "#111827"),
        ]:
            if field in history.columns:
                hist_fig.add_trace(
                    go.Scatter(
                        x=history["created_at"],
                        y=history[field],
                        mode="lines+markers",
                        name=field,
                        line={"color": color, "width": 2},
                        marker={"size": 5},
                    )
                )
        hist_fig.update_layout(
            title="ThingSpeak Fields 1-6",
            template="plotly_white",
            margin={"l": 20, "r": 20, "t": 30, "b": 20},
            xaxis_title="Timestamp",
            yaxis_title="Field Value",
            hovermode="x unified",
        )
        st.plotly_chart(hist_fig, use_container_width=True)

footer_stamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
st.caption(f"Dashboard loaded at: {footer_stamp}")
