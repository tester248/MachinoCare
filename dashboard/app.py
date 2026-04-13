from __future__ import annotations

import os
import re
import time
from datetime import datetime, timezone

import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st

API_DEFAULT = os.getenv("MACHINOCARE_API_URL", "http://localhost:8000")
THINGSPEAK_CHANNEL_DEFAULT = os.getenv("MACHINOCARE_THINGSPEAK_CHANNEL", "3336916")
UNASSIGNED_MACHINE_ID = os.getenv("MACHINOCARE_UNASSIGNED_MACHINE_ID", "unassigned_machine")
UNASSIGNED_DEVICE_ID = os.getenv("MACHINOCARE_UNASSIGNED_DEVICE_ID", "unassigned_device")

LIVE_FIELD_META: dict[str, tuple[str, str]] = {
    "acc_mag": ("Acceleration Magnitude", "#1177cc"),
    "gyro_mag": ("Gyroscope Magnitude", "#059669"),
    "score": ("Anomaly Score", "#d91e18"),
    "gx": ("Gyro X", "#5b21b6"),
    "gy": ("Gyro Y", "#0f766e"),
    "gz": ("Gyro Z", "#b45309"),
    "sw420": ("SW420", "#111827"),
}
LIVE_PRIMARY_FIELDS = ["acc_mag", "gyro_mag", "score"]
LIVE_AXIS_FIELDS = ["gx", "gy", "gz", "sw420"]

THINGSPEAK_FIELD_META: dict[str, str] = {
    "field1": "#1d4ed8",
    "field2": "#0f766e",
    "field3": "#7e22ce",
    "field4": "#b45309",
    "field5": "#be123c",
    "field6": "#111827",
}

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
        --mc-bg: radial-gradient(circle at 15% 10%, #e7f2ff 0%, #f8f2dc 45%, #edf9f2 100%);
        --mc-card: rgba(255, 255, 255, 0.96);
        --mc-border: rgba(16, 44, 61, 0.24);
        --mc-healthy: #1b8a5a;
        --mc-alert: #d91e18;
        --mc-ink: #0f1f2d;
        --mc-subtle: #2d5168;
        --mc-sidebar-bg: linear-gradient(180deg, #111a31 0%, #1b2540 100%);
        --mc-sidebar-ink: #f3f6ff;
        --mc-sidebar-input-bg: #0c1528;
        --mc-live: #0ea5e9;
    }

    @media (prefers-color-scheme: dark) {
        :root {
            --mc-bg: radial-gradient(circle at 12% 8%, #0f1a30 0%, #172239 40%, #1a2f2c 100%);
            --mc-card: rgba(16, 28, 46, 0.9);
            --mc-border: rgba(148, 189, 216, 0.34);
            --mc-healthy: #5de2a3;
            --mc-alert: #ff8b8b;
            --mc-ink: #eff8ff;
            --mc-subtle: #c2dbe9;
            --mc-sidebar-bg: linear-gradient(180deg, #0b1222 0%, #111933 100%);
            --mc-sidebar-input-bg: #060d1c;
            --mc-live: #67e8f9;
        }
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

    [data-testid="stAppViewContainer"] .main,
    [data-testid="stAppViewContainer"] .main p,
    [data-testid="stAppViewContainer"] .main span,
    [data-testid="stAppViewContainer"] .main label,
    [data-testid="stAppViewContainer"] .main h1,
    [data-testid="stAppViewContainer"] .main h2,
    [data-testid="stAppViewContainer"] .main h3,
    [data-testid="stAppViewContainer"] .main h4,
    [data-testid="stAppViewContainer"] .main h5,
    [data-testid="stAppViewContainer"] .main h6,
    [data-testid="stAppViewContainer"] .main li,
    [data-testid="stAppViewContainer"] .main div {
        color: var(--mc-ink);
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
        border-color: rgba(173, 188, 255, 0.34) !important;
    }

    [data-testid="stSidebar"] button {
        border: 1px solid rgba(198, 217, 255, 0.45) !important;
    }

    [data-testid="stSidebar"] button:disabled {
        opacity: 0.45;
    }

    [data-testid="stSkeleton"] {
        display: none !important;
    }

    [data-testid="stElementOverlay"] {
        background: transparent !important;
    }

    [data-testid="stAppViewContainer"] [data-stale='true'] {
        opacity: 1 !important;
        filter: none !important;
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

    .mc-live-pill {
        display: inline-flex;
        align-items: center;
        gap: 0.45rem;
        border: 1px solid rgba(8, 145, 178, 0.38);
        background: rgba(8, 145, 178, 0.12);
        border-radius: 999px;
        padding: 0.3rem 0.75rem;
        font-size: 0.8rem;
        font-weight: 600;
        margin-bottom: 0.65rem;
        color: var(--mc-ink) !important;
    }

    .mc-live-dot {
        width: 0.6rem;
        height: 0.6rem;
        border-radius: 50%;
        background: var(--mc-live);
        box-shadow: 0 0 0 0 rgba(14, 165, 233, 0.7);
        animation: mc-pulse 1.4s infinite;
    }

    @keyframes mc-pulse {
        0% { box-shadow: 0 0 0 0 rgba(14, 165, 233, 0.7); }
        70% { box-shadow: 0 0 0 10px rgba(14, 165, 233, 0); }
        100% { box-shadow: 0 0 0 0 rgba(14, 165, 233, 0); }
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def request_json(url: str, method: str = "GET", payload: dict | None = None) -> tuple[dict | None, str | None]:
    try:
        method = method.upper()
        if method == "POST":
            response = requests.post(url, json=payload, timeout=5)
        elif method == "DELETE":
            response = requests.delete(url, timeout=5)
        else:
            response = requests.get(url, timeout=5)
        response.raise_for_status()
        return response.json(), None
    except Exception as exc:  # noqa: BLE001
        return None, str(exc)


def thingspeak_history(channel_id: str, results: int = 60) -> tuple[pd.DataFrame | None, dict[str, str], str | None]:
    url = f"https://api.thingspeak.com/channels/{channel_id}/feeds.json?results={results}"
    data, error = request_json(url)
    if error:
        return None, {}, error

    field_labels: dict[str, str] = {}
    channel_data = data.get("channel", {}) if data else {}
    for field_key in THINGSPEAK_FIELD_META.keys():
        declared = str(channel_data.get(field_key) or "").strip()
        field_labels[field_key] = declared or field_key

    feeds = data.get("feeds", []) if data else []
    if not feeds:
        return pd.DataFrame(), field_labels, None

    frame = pd.DataFrame(feeds)
    if "created_at" in frame.columns:
        frame["created_at"] = pd.to_datetime(frame["created_at"], errors="coerce")
    for col in ["field1", "field2", "field3", "field4", "field5", "field6"]:
        if col in frame.columns:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")
    return frame, field_labels, None


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


def profile_key(machine_id: str, device_id: str) -> str:
    return f"{machine_id}::{device_id}"


def profile_display_name(profile: dict, index: int) -> str:
    name = str(profile.get("display_name") or "").strip()
    if name:
        return name
    return f"Unnamed profile {index + 1}"


def slugify_name(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return normalized or "profile"


def build_unique_profile_ids(display_name: str, existing_profiles: list[dict]) -> tuple[str, str]:
    taken = {
        profile_key(str(item.get("machine_id") or ""), str(item.get("device_id") or ""))
        for item in existing_profiles
    }
    base = slugify_name(display_name)

    suffix = 1
    while True:
        machine_id = base if suffix == 1 else f"{base}_{suffix}"
        device_id = f"{machine_id}_device"
        key = profile_key(machine_id, device_id)
        if key not in taken:
            return machine_id, device_id
        suffix += 1


if "active_job_id" not in st.session_state:
    st.session_state.active_job_id = None
if "active_job_machine" not in st.session_state:
    st.session_state.active_job_machine = None
if "active_job_device" not in st.session_state:
    st.session_state.active_job_device = None
if "completed_job" not in st.session_state:
    st.session_state.completed_job = None
if "selected_profile_key" not in st.session_state:
    st.session_state.selected_profile_key = None
if "profile_form_loaded_for" not in st.session_state:
    st.session_state.profile_form_loaded_for = None
if "live_tick" not in st.session_state:
    st.session_state.live_tick = 0
if "session_started_at" not in st.session_state:
    st.session_state.session_started_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

st.title("MachinoCare - AI Predictive Maintenance")
st.caption("Live vibration intelligence with backend-driven calibration and edge-safe inference.")

with st.sidebar:
    st.header("Controls")
    api_base = st.text_input("FastAPI Base URL", API_DEFAULT)

    profiles_data, profiles_error = request_json(f"{api_base}/api/v1/device-profiles?limit=500")
    profile_rows = profiles_data.get("profiles", []) if profiles_data else []

    normalized_profiles: list[dict] = []
    display_name_counts: dict[str, int] = {}
    for idx, profile in enumerate(profile_rows):
        machine = str(profile.get("machine_id") or "").strip()
        device = str(profile.get("device_id") or "").strip()
        if not machine or not device:
            continue

        base_name = profile_display_name(profile, idx)
        occurrence = display_name_counts.get(base_name, 0) + 1
        display_name_counts[base_name] = occurrence
        display_label = base_name if occurrence == 1 else f"{base_name} #{occurrence}"

        normalized_profiles.append(
            {
                **profile,
                "machine_id": machine,
                "device_id": device,
                "display_name_raw": str(profile.get("display_name") or "").strip(),
                "display_label": display_label,
                "key": profile_key(machine, device),
            }
        )

    profile_by_key = {item["key"]: item for item in normalized_profiles}
    profile_options = [item["key"] for item in normalized_profiles]

    binding_data, binding_error = request_json(f"{api_base}/api/v1/stream-binding")
    active_binding = binding_data or {}
    active_binding_machine = active_binding.get("machine_id") if active_binding.get("is_active") else None
    active_binding_device = active_binding.get("device_id") if active_binding.get("is_active") else None
    active_binding_key = (
        profile_key(str(active_binding_machine), str(active_binding_device))
        if active_binding_machine and active_binding_device
        else None
    )

    selected_profile: dict | None = None
    selected_profile_label = "No profile selected"
    if profile_options:
        preferred_key = st.session_state.selected_profile_key
        if active_binding_key in profile_by_key:
            preferred_key = active_binding_key
        if preferred_key not in profile_by_key:
            preferred_key = profile_options[0]

        selected_profile_key = st.selectbox(
            "Profile",
            options=profile_options,
            index=profile_options.index(preferred_key),
            format_func=lambda key: profile_by_key[key]["display_label"],
        )

        if st.session_state.selected_profile_key != selected_profile_key:
            with st.spinner("Switching profile..."):
                time.sleep(0.25)

        st.session_state.selected_profile_key = selected_profile_key
        selected_profile = profile_by_key[selected_profile_key]
        selected_profile_label = selected_profile["display_label"]
    else:
        st.info("Create your first profile to start monitoring and calibration.")
        st.session_state.selected_profile_key = None

    selected_machine = selected_profile["machine_id"] if selected_profile else UNASSIGNED_MACHINE_ID
    selected_device = selected_profile["device_id"] if selected_profile else UNASSIGNED_DEVICE_ID

    form_scope_key = selected_profile["key"] if selected_profile else "__new_profile__"
    if st.session_state.profile_form_loaded_for != form_scope_key:
        st.session_state.form_display_name = (
            selected_profile.get("display_name_raw")
            if selected_profile
            else ""
        )
        st.session_state.form_sample_rate_hz = int((selected_profile or {}).get("sample_rate_hz") or 10)
        st.session_state.form_window_seconds = int((selected_profile or {}).get("window_seconds") or 1)
        st.session_state.form_fallback_seconds = int((selected_profile or {}).get("fallback_seconds") or 300)
        st.session_state.form_contamination = float((selected_profile or {}).get("contamination") or 0.05)
        st.session_state.form_min_windows = int((selected_profile or {}).get("min_consecutive_windows") or 3)
        st.session_state.form_notes = str((selected_profile or {}).get("notes") or "")
        st.session_state.profile_form_loaded_for = form_scope_key

    st.subheader("Incoming Stream Association")
    if active_binding_key and active_binding_key in profile_by_key:
        st.success(f"Incoming stream -> {profile_by_key[active_binding_key]['display_label']}")
    elif active_binding_machine and active_binding_device:
        st.warning("Incoming stream is associated to a profile not currently listed.")
    else:
        st.warning("No active association. Incoming stream routes to unassigned target.")

    association_col, clear_col = st.columns(2)
    if association_col.button("Associate stream", use_container_width=True, disabled=selected_profile is None):
        bind_data, bind_error = request_json(
            f"{api_base}/api/v1/stream-binding",
            method="POST",
            payload={
                "machine_id": selected_machine,
                "device_id": selected_device,
                "source": "streamlit_dashboard",
            },
        )
        if bind_error:
            st.error(f"Stream association failed: {bind_error}")
        else:
            _ = bind_data
            st.success(f"Incoming stream now targets {selected_profile_label}")

    if clear_col.button("Clear association", use_container_width=True):
        _, clear_error = request_json(
            f"{api_base}/api/v1/stream-binding?source=streamlit_dashboard",
            method="DELETE",
        )
        if clear_error:
            st.error(f"Failed clearing association: {clear_error}")
        else:
            st.success("Incoming stream association cleared.")

    if profiles_error:
        st.caption(f"Profile list warning: {profiles_error}")
    if binding_error:
        st.caption(f"Binding lookup warning: {binding_error}")

    lookback_seconds = st.slider("Live lookback (seconds)", min_value=30, max_value=600, value=120, step=10)
    refresh_seconds = st.slider("Live refresh (seconds)", min_value=1, max_value=10, value=1, step=1)

    st.subheader("Profile Settings")
    st.text_input("Display name", key="form_display_name")
    sample_rate_hz = st.number_input(
        "Sample rate (Hz)",
        min_value=1,
        max_value=500,
        step=1,
        key="form_sample_rate_hz",
    )
    window_seconds = st.number_input(
        "Window size (s)",
        min_value=1,
        max_value=10,
        step=1,
        key="form_window_seconds",
    )
    fallback_seconds = st.number_input(
        "Fallback window (s)",
        min_value=30,
        max_value=3600,
        step=30,
        key="form_fallback_seconds",
    )
    contamination = st.slider(
        "Isolation Forest contamination",
        min_value=0.01,
        max_value=0.40,
        step=0.01,
        key="form_contamination",
    )
    min_consecutive_windows = st.number_input(
        "Min consecutive windows",
        min_value=1,
        max_value=10,
        step=1,
        key="form_min_windows",
    )
    st.text_area("Profile notes", key="form_notes")

    if st.button("Save selected profile", use_container_width=True, disabled=selected_profile is None):
        save_data, save_error = request_json(
            f"{api_base}/api/v1/device-profiles",
            method="POST",
            payload={
                "machine_id": selected_machine,
                "device_id": selected_device,
                "display_name": st.session_state.form_display_name.strip() or None,
                "sample_rate_hz": int(sample_rate_hz),
                "window_seconds": int(window_seconds),
                "fallback_seconds": int(fallback_seconds),
                "contamination": float(contamination),
                "min_consecutive_windows": int(min_consecutive_windows),
                "notes": st.session_state.form_notes.strip() or None,
            },
        )
        if save_error:
            st.error(f"Profile save failed: {save_error}")
        else:
            st.success(f"Saved profile {save_data.get('display_name') or selected_profile_label}")
            st.rerun()

    new_profile_display_name = st.text_input("Create profile (display name)", key="new_profile_display_name")
    if st.button("Create profile", use_container_width=True):
        create_name = new_profile_display_name.strip()
        if not create_name:
            st.error("Enter a display name to create a profile.")
        else:
            existing_names = {
                str(item.get("display_name_raw") or "").strip().lower()
                for item in normalized_profiles
                if str(item.get("display_name_raw") or "").strip()
            }
            if create_name.lower() in existing_names:
                st.error("A profile with this display name already exists.")
            else:
                new_machine_id, new_device_id = build_unique_profile_ids(create_name, normalized_profiles)
                create_data, create_error = request_json(
                    f"{api_base}/api/v1/device-profiles",
                    method="POST",
                    payload={
                        "machine_id": new_machine_id,
                        "device_id": new_device_id,
                        "display_name": create_name,
                        "sample_rate_hz": int(sample_rate_hz),
                        "window_seconds": int(window_seconds),
                        "fallback_seconds": int(fallback_seconds),
                        "contamination": float(contamination),
                        "min_consecutive_windows": int(min_consecutive_windows),
                        "notes": st.session_state.form_notes.strip() or None,
                    },
                )
                if create_error:
                    st.error(f"Profile creation failed: {create_error}")
                else:
                    st.session_state.selected_profile_key = profile_key(
                        str(create_data.get("machine_id") or ""),
                        str(create_data.get("device_id") or ""),
                    )
                    st.session_state.new_profile_display_name = ""
                    st.success(f"Created profile {create_data.get('display_name') or create_name}")
                    st.rerun()

    st.subheader("Calibration")
    use_profile_defaults = st.toggle("Use saved profile settings", value=True)
    new_device_setup = st.toggle("New device setup", value=True)

    if st.button("Start calibration training", use_container_width=True):
        if selected_profile is None:
            st.error("Create and select a profile before starting calibration.")
            start_data = None
            start_error = "No profile selected"
        else:
            start_data = None
            start_error = None

            if use_profile_defaults:
                query = "true" if bool(new_device_setup) else "false"
                start_data, start_error = request_json(
                    (
                        f"{api_base}/api/v1/calibrate/start/profile/{selected_machine}/{selected_device}"
                        f"?new_device_setup={query}&trigger_source=dashboard_ui"
                    ),
                    method="POST",
                )

            if (not use_profile_defaults) or start_error:
                payload = {
                    "machine_id": selected_machine,
                    "device_id": selected_device,
                    "sample_rate_hz": int(sample_rate_hz),
                    "fallback_seconds": int(fallback_seconds),
                    "window_seconds": int(window_seconds),
                    "contamination": float(contamination),
                    "min_consecutive_windows": int(min_consecutive_windows),
                    "new_device_setup": bool(new_device_setup),
                    "trigger_source": "dashboard_ui",
                }
                if use_profile_defaults and start_error:
                    st.warning(f"Profile-based start failed. Using manual values instead: {start_error}")
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

    st.subheader("Profile Lifecycle")
    confirm_profile_delete = st.toggle("Confirm profile delete", value=False)
    if st.button("Delete selected profile", use_container_width=True, disabled=selected_profile is None):
        if not confirm_profile_delete:
            st.error("Enable profile delete confirmation before removing a profile.")
        else:
            _, delete_error = request_json(
                f"{api_base}/api/v1/device-profiles/{selected_machine}/{selected_device}",
                method="DELETE",
            )
            if delete_error:
                st.error(f"Profile delete failed: {delete_error}")
            else:
                st.session_state.selected_profile_key = None
                st.session_state.profile_form_loaded_for = None
                st.success(f"Deleted profile {selected_profile_label}")
                st.rerun()


def render_live_ui() -> None:
    if st.session_state.selected_profile_key is None:
        st.info("Select or create a profile to start live monitoring.")
        return

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
    debug_logs_data, debug_logs_error = request_json(
        (
            f"{api_base}/api/v1/debug/logs"
            f"?machine_id={selected_machine}&device_id={selected_device}&limit=120"
        )
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
        primary_labels = [LIVE_FIELD_META[field][0] for field in LIVE_PRIMARY_FIELDS]
        axis_labels = [LIVE_FIELD_META[field][0] for field in LIVE_AXIS_FIELDS]
        selected_primary_labels = st.multiselect(
            "Live primary fields",
            options=primary_labels,
            default=primary_labels,
            key="live_primary_selected_labels",
            help="Toggle which primary live metrics appear in the first chart.",
        )
        selected_axis_labels = st.multiselect(
            "Live axis fields",
            options=axis_labels,
            default=axis_labels,
            key="live_axis_selected_labels",
            help="Toggle which axis/vibration fields appear in the second chart.",
        )

        selected_primary_fields = [
            field
            for field in LIVE_PRIMARY_FIELDS
            if LIVE_FIELD_META[field][0] in selected_primary_labels
        ]
        selected_axis_fields = [
            field
            for field in LIVE_AXIS_FIELDS
            if LIVE_FIELD_META[field][0] in selected_axis_labels
        ]

        samples = (recent_data or {}).get("samples", [])
        frame = normalize_recent_samples(samples)

        if frame.empty:
            st.warning("No live samples available yet. Start streaming to /api/v1/stream.")
        else:
            fig_primary = go.Figure()
            for col in selected_primary_fields:
                if col in frame.columns:
                    label, color = LIVE_FIELD_META[col]
                    fig_primary.add_trace(
                        go.Scatter(
                            x=frame["timestamp"],
                            y=frame[col],
                            mode="lines",
                            name=label,
                            line={"color": color, "width": 2},
                        )
                    )

            threshold_value = current.get("decision_threshold") or model_summary.get("decision_threshold")
            if threshold_value is not None and "score" in frame.columns and "score" in selected_primary_fields:
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
            if fig_primary.data:
                st.plotly_chart(fig_primary, use_container_width=True)
            else:
                st.info("Select at least one primary field to display the first live chart.")

            fig_axes = go.Figure()
            for col in [field for field in selected_axis_fields if field != "sw420"]:
                if col in frame.columns:
                    label, color = LIVE_FIELD_META[col]
                    fig_axes.add_trace(
                        go.Scatter(
                            x=frame["timestamp"],
                            y=frame[col],
                            mode="lines",
                            name=label,
                            line={"color": color, "width": 2},
                        )
                    )
            if "sw420" in frame.columns and "sw420" in selected_axis_fields:
                sw420_label, sw420_color = LIVE_FIELD_META["sw420"]
                fig_axes.add_trace(
                    go.Scatter(
                        x=frame["timestamp"],
                        y=frame["sw420"],
                        mode="lines",
                        name=sw420_label,
                        line={"color": sw420_color, "width": 2, "dash": "dot"},
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
            if fig_axes.data:
                st.plotly_chart(fig_axes, use_container_width=True)
            else:
                st.info("Select at least one axis field to display the second live chart.")

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
                f"Profile: {selected_profile_label}<br/>"
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

    st.subheader("Live API Logs (ESP/backend)")
    if debug_logs_error:
        st.warning(f"Live logs unavailable: {debug_logs_error}")
    else:
        logs = (debug_logs_data or {}).get("logs", [])
        if not logs:
            st.info("No recent API logs for this profile yet.")
        else:
            summary_rows = [
                {
                    "time": item.get("created_at"),
                    "method": item.get("method"),
                    "endpoint": item.get("endpoint"),
                    "status": item.get("status_code"),
                    "latency_ms": item.get("latency_ms"),
                    "machine": item.get("machine_id"),
                    "device": item.get("device_id"),
                }
                for item in logs
            ]
            st.dataframe(pd.DataFrame(summary_rows), use_container_width=True, height=280)
            with st.expander("Latest API log payload details", expanded=False):
                st.json(logs[:20])

    with st.expander("Backend Payload - Full Recent Stream JSON", expanded=False):
        st.json(recent_data or {})


@st.fragment(run_every=f"{refresh_seconds}s")
def live_fragment() -> None:
    st.session_state.live_tick += 1
    tick_stamp = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    st.markdown(
        (
            "<div class='mc-live-pill'>"
            "<span class='mc-live-dot'></span>"
            f"Live updates every {refresh_seconds}s | tick #{st.session_state.live_tick} | {tick_stamp}"
            "</div>"
        ),
        unsafe_allow_html=True,
    )
    render_live_ui()


live_fragment()

st.subheader("ThingSpeak Historical Trend")
use_thingspeak = st.toggle("Load ThingSpeak history", value=False)
if use_thingspeak:
    channel_id = st.text_input("ThingSpeak Channel ID", THINGSPEAK_CHANNEL_DEFAULT)
    history, field_labels, history_error = thingspeak_history(channel_id=channel_id, results=120)
    if history_error:
        st.error(f"ThingSpeak fetch failed: {history_error}")
    elif history is None or history.empty:
        st.info("No ThingSpeak history available.")
    else:
        available_fields = [field for field in THINGSPEAK_FIELD_META.keys() if field in history.columns]
        label_to_field = {
            f"{field_labels.get(field, field)} ({field})": field
            for field in available_fields
        }
        selected_thingspeak_labels = st.multiselect(
            "ThingSpeak fields",
            options=list(label_to_field.keys()),
            default=list(label_to_field.keys()),
            key=f"thingspeak_selected_fields_{channel_id}",
            help="Toggle which ThingSpeak fields are visible in the history chart.",
        )
        selected_thingspeak_fields = [label_to_field[label] for label in selected_thingspeak_labels]

        hist_fig = go.Figure()
        for field in selected_thingspeak_fields:
            color = THINGSPEAK_FIELD_META[field]
            if field in history.columns:
                hist_fig.add_trace(
                    go.Scatter(
                        x=history["created_at"],
                        y=history[field],
                        mode="lines+markers",
                        name=field_labels.get(field, field),
                        line={"color": color, "width": 2},
                        marker={"size": 5},
                    )
                )
        hist_fig.update_layout(
            title="ThingSpeak Field History",
            template="plotly_white",
            margin={"l": 20, "r": 20, "t": 30, "b": 20},
            xaxis_title="Timestamp",
            yaxis_title="Field Value",
            hovermode="x unified",
        )
        if hist_fig.data:
            st.plotly_chart(hist_fig, use_container_width=True)
        else:
            st.info("Select at least one ThingSpeak field to display history.")

st.caption(f"Dashboard session started at: {st.session_state.session_started_at}")
