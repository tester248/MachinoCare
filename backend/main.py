from __future__ import annotations

import asyncio
import json
import os
import random
import re
import statistics
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv
from fastapi import APIRouter, FastAPI, HTTPException, Query, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse

from backend.ml_engine import (
    FEATURE_ORDER,
    acc_threshold_stats,
    build_checksum,
    build_feature_matrix,
    latest_feature_vector,
    score_feature_vector,
    train_isolation_forest_distilled,
    train_oneclass_svm_distilled,
)
from backend.models import (
    ApiDebugLogEntry,
    CalibrationJobStatus,
    CalibrationRequest,
    CalibrationResponse,
    CalibrationStartResponse,
    DeviceProfileResponse,
    DeviceProfileUpsertRequest,
    StreamBindingResponse,
    StreamBindingUpsertRequest,
    StreamIngestRequest,
)
from backend.debug_dashboard import get_debug_dashboard_html
from backend.storage import DataStore

ROOT_DIR = Path(__file__).resolve().parent.parent
load_dotenv(ROOT_DIR / ".env")
load_dotenv()


def _get_env_var(*names: str, default: str = "") -> str:
    for name in names:
        value = os.getenv(name)
        if value is not None and str(value).strip():
            return str(value).strip()
    return default

DB_PATH = os.getenv("MACHINOCARE_DB", "data/machinocare.db")
DATABASE_URL = os.getenv("DATABASE_URL")
BUFFER_SIZE = int(os.getenv("MACHINOCARE_BUFFER_SIZE", "12000"))
DEBUG_SAMPLE_RATE = max(0.0, min(1.0, float(os.getenv("MACHINOCARE_DEBUG_SAMPLE_RATE", "0.10"))))
DEBUG_RETENTION_DAYS = int(os.getenv("MACHINOCARE_DEBUG_RETENTION_DAYS", "30"))
DEBUG_MAX_BODY_BYTES = int(os.getenv("MACHINOCARE_DEBUG_MAX_BODY_BYTES", "20000"))
LIVE_PUSH_INTERVAL_SECONDS = max(0.2, float(os.getenv("MACHINOCARE_LIVE_PUSH_INTERVAL_SECONDS", "0.75")))
UNASSIGNED_MACHINE_ID = os.getenv("MACHINOCARE_UNASSIGNED_MACHINE_ID", "unassigned_machine")
UNASSIGNED_DEVICE_ID = os.getenv("MACHINOCARE_UNASSIGNED_DEVICE_ID", "unassigned_device")
GROQ_API_KEY = _get_env_var("GROQ_API_KEY", "MACHINOCARE_GROQ_API_KEY")
GROQ_MODEL = _get_env_var("MACHINOCARE_GROQ_MODEL", "GROQ_MODEL", default="llama-3.3-70b-versatile")
GROQ_API_URL = _get_env_var(
    "MACHINOCARE_GROQ_API_URL",
    "GROQ_API_URL",
    default="https://api.groq.com/openai/v1/chat/completions",
)
GROQ_TIMEOUT_SECONDS = max(5, int(float(os.getenv("MACHINOCARE_GROQ_TIMEOUT_SECONDS", "20"))))
INSIGHT_MAX_REPORT_CHARS = max(200, int(os.getenv("MACHINOCARE_INSIGHT_MAX_REPORT_CHARS", "1000")))

store = DataStore(db_path=DB_PATH, max_buffer_size=BUFFER_SIZE, database_url=DATABASE_URL)

app = FastAPI(
    title="MachinoCare Backend",
    version="0.2.0",
    description=(
        "FastAPI backend for vibration streaming, device-specific calibration jobs, "
        "and model package delivery."
    ),
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

router = APIRouter()

calibration_jobs: dict[str, dict[str, Any]] = {}
calibration_jobs_lock = threading.Lock()
insight_cache: dict[str, dict[str, Any]] = {}
insight_cache_lock = threading.Lock()
last_debug_purge_monotonic = 0.0


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse(url="/docs", status_code=307)


@app.get("/debug-dashboard", include_in_schema=False)
def debug_dashboard() -> HTMLResponse:
    return HTMLResponse(content=get_debug_dashboard_html())


def utc_iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def maybe_decode_json(raw: bytes | None) -> Any | None:
    if not raw:
        return None

    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return None

    if not text.strip():
        return None

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def truncate_payload(payload: Any) -> Any:
    if payload is None:
        return None

    if isinstance(payload, str):
        if len(payload) <= DEBUG_MAX_BODY_BYTES:
            return payload
        return payload[:DEBUG_MAX_BODY_BYTES] + "...<truncated>"

    serialized = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    if len(serialized) <= DEBUG_MAX_BODY_BYTES:
        return payload

    return {
        "truncated": True,
        "preview": serialized[:DEBUG_MAX_BODY_BYTES],
    }


def extract_machine_device(path: str, payload: Any, query_params: dict[str, str]) -> tuple[str | None, str | None]:
    machine_id = None
    device_id = None
    device_name = None

    if isinstance(payload, dict):
        machine_id = payload.get("machine_id")
        device_id = payload.get("device_id")
        device_name = payload.get("device_name")

    machine_id = machine_id or query_params.get("machine_id")
    device_id = device_id or query_params.get("device_id")
    device_name = device_name or query_params.get("device_name")

    if device_name and (not machine_id or not device_id):
        profile = store.get_device_profile_by_name(str(device_name))
        if profile:
            machine_id = machine_id or profile.get("machine_id")
            device_id = device_id or profile.get("device_id")

    parts = [part for part in path.split("/") if part]
    if len(parts) >= 4 and parts[0] == "api" and parts[1] == "v1":
        if parts[2] in {"status", "model", "insights", "anomaly-log"}:
            device_name_from_path = parts[3]
            profile = store.get_device_profile_by_name(device_name_from_path)
            if profile:
                machine_id = machine_id or profile.get("machine_id")
                device_id = device_id or profile.get("device_id")

        if parts[2] == "stream" and len(parts) >= 5 and parts[3] == "recent":
            device_name_from_path = parts[4]
            profile = store.get_device_profile_by_name(device_name_from_path)
            if profile:
                machine_id = machine_id or profile.get("machine_id")
                device_id = device_id or profile.get("device_id")

        if parts[2] == "calibrate" and len(parts) >= 6 and parts[3] == "start" and parts[4] == "profile":
            device_name_from_path = parts[5]
            profile = store.get_device_profile_by_name(device_name_from_path)
            if profile:
                machine_id = machine_id or profile.get("machine_id")
                device_id = device_id or profile.get("device_id")

    return machine_id, device_id


def persist_debug_log(entry: dict[str, Any]) -> None:
    global last_debug_purge_monotonic

    try:
        store.save_api_debug_log(entry)
        now_mono = time.monotonic()
        if now_mono - last_debug_purge_monotonic >= 3600:
            store.purge_api_debug_logs_older_than(DEBUG_RETENTION_DAYS)
            last_debug_purge_monotonic = now_mono
    except Exception:  # noqa: BLE001
        # Logging must never interrupt the main API flow.
        return


@app.middleware("http")
async def api_debug_log_middleware(request: Request, call_next: Any) -> Response:
    if not request.url.path.startswith("/api/v1"):
        return await call_next(request)

    correlation_id = request.headers.get("x-correlation-id", str(uuid.uuid4()))

    raw_request_body = await request.body()

    async def _receive() -> dict[str, Any]:
        return {
            "type": "http.request",
            "body": raw_request_body,
            "more_body": False,
        }

    rebuilt_request = Request(request.scope, _receive)
    parsed_request = maybe_decode_json(raw_request_body)

    machine_id, device_id = extract_machine_device(
        path=request.url.path,
        payload=parsed_request,
        query_params=dict(request.query_params),
    )

    started = time.perf_counter()
    response: Response | None = None
    caught_error = ""

    try:
        response = await call_next(rebuilt_request)
    except Exception as exc:  # noqa: BLE001
        caught_error = str(exc)
        persist_debug_log(
            {
                "created_at": utc_iso_now(),
                "endpoint": request.url.path,
                "method": request.method,
                "machine_id": machine_id,
                "device_id": device_id,
                "status_code": 500,
                "latency_ms": int((time.perf_counter() - started) * 1000),
                "request_size": len(raw_request_body),
                "response_size": 0,
                "correlation_id": correlation_id,
                "is_error": True,
                "payload_sampled": True,
                "request_payload": truncate_payload(parsed_request),
                "response_payload": None,
                "error_text": caught_error,
            }
        )
        raise

    response.headers["x-correlation-id"] = correlation_id

    response_status = response.status_code
    is_error = response_status >= 400
    payload_sampled = is_error or (random.random() <= DEBUG_SAMPLE_RATE)

    raw_response_body = getattr(response, "body", b"") or b""
    parsed_response = maybe_decode_json(raw_response_body)

    persist_debug_log(
        {
            "created_at": utc_iso_now(),
            "endpoint": request.url.path,
            "method": request.method,
            "machine_id": machine_id,
            "device_id": device_id,
            "status_code": response_status,
            "latency_ms": int((time.perf_counter() - started) * 1000),
            "request_size": len(raw_request_body),
            "response_size": len(raw_response_body),
            "correlation_id": correlation_id,
            "is_error": is_error,
            "payload_sampled": payload_sampled,
            "request_payload": truncate_payload(parsed_request) if payload_sampled else None,
            "response_payload": truncate_payload(parsed_response) if payload_sampled else None,
            "error_text": caught_error or None,
        }
    )

    return response


def round_list(values: list[float], ndigits: int = 6) -> list[float]:
    return [round(float(v), ndigits) for v in values]


def _bounded_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(maximum, parsed))


def _normalize_device_name(value: str | None) -> str:
    return str(value or "").strip()


def _slugify_device_name(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return normalized or "device"


def _build_profile_ids_for_name(device_name: str) -> tuple[str, str]:
    base = _slugify_device_name(device_name)
    suffix = 1
    while True:
        machine_id = base if suffix == 1 else f"{base}_{suffix}"
        device_id = f"{machine_id}_device"
        if not store.get_device_profile(machine_id, device_id):
            return machine_id, device_id
        suffix += 1


def _resolve_profile_or_404(device_name: str) -> dict[str, Any]:
    normalized = _normalize_device_name(device_name)
    if not normalized:
        raise HTTPException(status_code=400, detail="device_name is required")

    profile = store.get_device_profile_by_name(normalized)
    if not profile:
        raise HTTPException(status_code=404, detail=f"Device profile '{normalized}' not found.")
    return profile


def _resolve_machine_device_from_name(device_name: str) -> tuple[str, str]:
    profile = _resolve_profile_or_404(device_name)
    return str(profile["machine_id"]), str(profile["device_id"])


def _profile_response(profile: dict[str, Any]) -> DeviceProfileResponse:
    row = dict(profile)
    row["device_name"] = str(row.get("display_name") or "")
    return DeviceProfileResponse(**row)


def _binding_with_device_name(binding: dict[str, Any] | None) -> dict[str, Any] | None:
    if not binding:
        return None

    item = dict(binding)
    machine_id = item.get("machine_id")
    device_id = item.get("device_id")
    if machine_id and device_id:
        profile = store.get_device_profile(str(machine_id), str(device_id))
        item["device_name"] = (profile or {}).get("display_name")
    else:
        item["device_name"] = None
    return item


def _device_name_for_ids(machine_id: str, device_id: str) -> str | None:
    profile = store.get_device_profile(machine_id, device_id)
    name = str((profile or {}).get("display_name") or "").strip()
    return name or None


def _resolve_calibration_payload(payload: CalibrationRequest) -> CalibrationRequest:
    machine_id = str(payload.machine_id or "").strip()
    device_id = str(payload.device_id or "").strip()

    if machine_id and device_id:
        device_name = _normalize_device_name(payload.device_name) or _device_name_for_ids(machine_id, device_id)
        return payload.model_copy(
            update={
                "machine_id": machine_id,
                "device_id": device_id,
                "device_name": device_name,
            }
        )

    normalized_name = _normalize_device_name(payload.device_name)
    if not normalized_name:
        raise HTTPException(
            status_code=400,
            detail="Provide 'device_name' (or both 'machine_id' and 'device_id').",
        )

    profile = _resolve_profile_or_404(normalized_name)
    return payload.model_copy(
        update={
            "device_name": normalized_name,
            "machine_id": str(profile["machine_id"]),
            "device_id": str(profile["device_id"]),
        }
    )


def _calibration_collection_seconds(payload: CalibrationRequest) -> int:
    seconds = payload.calibration_duration_seconds or payload.fallback_seconds
    return max(10, int(seconds))


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _insight_cache_key(machine_id: str, device_id: str) -> str:
    return f"{machine_id}::{device_id}"


def _health_band(score: float) -> str:
    if score >= 85:
        return "Excellent"
    if score >= 70:
        return "Good"
    if score >= 50:
        return "Watchlist"
    return "Critical"


def _compute_health_snapshot(status_payload: dict[str, Any], anomaly_count_24h: int) -> dict[str, Any]:
    current = status_payload.get("current") or {}

    score = _safe_float(current.get("score"))
    threshold = _safe_float(current.get("decision_threshold"))
    consecutive_windows = max(0, int(current.get("consecutive_windows") or 0))
    is_anomaly = bool(status_payload.get("is_anomaly"))

    if score is not None and threshold is not None and threshold > 0:
        score_ratio = score / threshold
        score_penalty = 0.0
        if score_ratio > 0.60:
            score_penalty = min(45.0, (score_ratio - 0.60) * 35.0)
    else:
        score_ratio = None
        score_penalty = 10.0 if is_anomaly else 4.0

    consecutive_penalty = min(20.0, consecutive_windows * 4.0)
    anomaly_penalty = min(20.0, max(0, anomaly_count_24h) * 2.0)
    active_alert_penalty = 15.0 if is_anomaly else 0.0

    health_score = 100.0 - score_penalty - consecutive_penalty - anomaly_penalty - active_alert_penalty
    health_score = max(0.0, min(100.0, health_score))
    health_score = round(health_score, 2)

    return {
        "health_score_percent": health_score,
        "health_band": _health_band(health_score),
        "score_ratio": round(score_ratio, 6) if score_ratio is not None else None,
        "components": {
            "score_penalty": round(score_penalty, 2),
            "consecutive_penalty": round(consecutive_penalty, 2),
            "anomaly_history_penalty": round(anomaly_penalty, 2),
            "active_alert_penalty": round(active_alert_penalty, 2),
            "anomaly_count_24h": int(max(0, anomaly_count_24h)),
            "consecutive_windows": consecutive_windows,
        },
    }


def _compute_vibration_statistics(machine_id: str, device_id: str) -> dict[str, Any]:
    """Compute vibration statistics from recent samples for fault diagnosis."""
    # Fetch recent samples (last 500 samples ~2 minutes at 4Hz)
    recent_samples = store.get_recent_samples(machine_id, device_id=device_id, seconds=300, limit=500)
    
    if not recent_samples:
        return {
            "acc_mags": [],
            "gyro_mags": [],
            "acc_current": None,
            "gyro_current": None,
            "acc_mean": None,
            "acc_max": None,
            "acc_stddev": None,
            "gyro_mean": None,
            "gyro_max": None,
            "gyro_stddev": None,
            "vibration_level": "unknown",
            "trend": "unknown",
        }
    
    acc_mags = [float(s.get("acc_mag", 0)) for s in recent_samples if s.get("acc_mag") is not None]
    gyro_mags = [float(s.get("gyro_mag", 0)) for s in recent_samples if s.get("gyro_mag") is not None]
    
    def safe_stats(values: list[float]) -> tuple[float | None, float | None, float | None]:
        if not values:
            return None, None, None
        mean_val = statistics.mean(values)
        max_val = max(values)
        try:
            stddev_val = statistics.stdev(values) if len(values) > 1 else 0.0
        except statistics.StatisticsError:
            stddev_val = 0.0
        return mean_val, max_val, stddev_val
    
    acc_mean, acc_max, acc_stddev = safe_stats(acc_mags)
    gyro_mean, gyro_max, gyro_stddev = safe_stats(gyro_mags)
    
    # Classify vibration level
    vibration_level = "normal"
    if acc_max and acc_max > 30000:
        vibration_level = "high"
    elif acc_max and acc_max > 20000:
        vibration_level = "elevated"
    
    # Compute trend (comparing first half vs second half)
    trend = "stable"
    if len(acc_mags) >= 10:
        mid = len(acc_mags) // 2
        first_half_mean = statistics.mean(acc_mags[:mid]) if mid > 0 else 0
        second_half_mean = statistics.mean(acc_mags[mid:]) if len(acc_mags) > mid else 0
        if second_half_mean > first_half_mean * 1.15:
            trend = "increasing"
        elif second_half_mean < first_half_mean * 0.85:
            trend = "decreasing"
    
    return {
        "acc_mags": acc_mags,
        "gyro_mags": gyro_mags,
        "acc_current": acc_mags[-1] if acc_mags else None,
        "gyro_current": gyro_mags[-1] if gyro_mags else None,
        "acc_mean": round(acc_mean, 2) if acc_mean is not None else None,
        "acc_max": round(acc_max, 2) if acc_max is not None else None,
        "acc_stddev": round(acc_stddev, 2) if acc_stddev is not None else None,
        "gyro_mean": round(gyro_mean, 2) if gyro_mean is not None else None,
        "gyro_max": round(gyro_max, 2) if gyro_max is not None else None,
        "gyro_stddev": round(gyro_stddev, 2) if gyro_stddev is not None else None,
        "vibration_level": vibration_level,
        "trend": trend,
        "sample_count": len(acc_mags),
    }


def _build_report_prompt(
    machine_id: str,
    device_id: str,
    status_payload: dict[str, Any],
    snapshot: dict[str, Any],
    vibration_stats: dict[str, Any] | None = None,
) -> str:
    current = status_payload.get("current") or {}
    calibration = status_payload.get("calibration") or {}
    model_summary = status_payload.get("model_summary") or {}
    
    # Build vibration data section
    vib_section = ""
    if vibration_stats:
        vib_section = (
            f"Vibration level: {vibration_stats.get('vibration_level')}\n"
            f"Vibration trend (5min): {vibration_stats.get('trend')}\n"
            f"Current acceleration magnitude: {vibration_stats.get('acc_current')}\n"
            f"Accel mean (5min): {vibration_stats.get('acc_mean')}, "
            f"max: {vibration_stats.get('acc_max')}, "
            f"stddev: {vibration_stats.get('acc_stddev')}\n"
            f"Gyro magnitude mean (5min): {vibration_stats.get('gyro_mean')}, "
            f"max: {vibration_stats.get('gyro_max')}, "
            f"stddev: {vibration_stats.get('gyro_stddev')}\n"
        )

    return (
        "You are an expert vibration analyst for predictive maintenance on rotating machinery. "
        "Analyze the telemetry and provide a diagnostic report that includes: "
        "(1) whether vibrations are normal/elevated/high, "
        "(2) the current health status, "
        "(3) a suggested fault type (if anomalies detected), and "
        "(4) recommended action.\n"
        "Output format:\n"
        "- Exactly 3 bullet points (vibration status, health diagnosis, fault/action).\n"
        "- Maximum 110 words total.\n"
        "- Use technical but accessible language.\n"
        "- No markdown formatting.\n\n"
        f"Machine: {machine_id} | Device: {device_id}\n"
        f"Health score: {snapshot.get('health_score_percent')}% ({snapshot.get('health_band')})\n"
        f"Anomaly status: {status_payload.get('is_anomaly')}\n"
        f"Status label: {status_payload.get('status_label')}\n"
        f"Anomaly score: {current.get('score')} / threshold: {current.get('decision_threshold')}\n"
        f"Consecutive anomaly windows: {current.get('consecutive_windows')}\n"
        f"24-hour anomaly events: {(snapshot.get('components') or {}).get('anomaly_count_24h')}\n"
        f"Current model type: {model_summary.get('model_type')}\n\n"
        f"{vib_section}"
    )


def _truncate_report(text: str) -> str:
    if len(text) <= INSIGHT_MAX_REPORT_CHARS:
        return text
    return text[:INSIGHT_MAX_REPORT_CHARS].rstrip() + "..."


def _generate_report_with_groq(
    machine_id: str,
    device_id: str,
    status_payload: dict[str, Any],
    snapshot: dict[str, Any],
    vibration_stats: dict[str, Any] | None = None,
) -> str:
    if not GROQ_API_KEY:
        raise RuntimeError("GROQ_API_KEY is not configured")

    payload = {
        "model": GROQ_MODEL,
        "temperature": 0.2,
        "max_tokens": 280,
        "messages": [
            {
                "role": "system",
                "content": "You are a vibration diagnostics expert. Provide insightful, actionable machine health reports.",
            },
            {
                "role": "user",
                "content": _build_report_prompt(
                    machine_id=machine_id,
                    device_id=device_id,
                    status_payload=status_payload,
                    snapshot=snapshot,
                    vibration_stats=vibration_stats,
                ),
            },
        ],
    }

    response = requests.post(
        GROQ_API_URL,
        headers={
            "Authorization": f"Bearer {GROQ_API_KEY}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=GROQ_TIMEOUT_SECONDS,
    )
    response.raise_for_status()

    body = response.json()
    choices = body.get("choices") or []
    message = ((choices[0] if choices else {}).get("message") or {}).get("content")
    text = str(message or "").strip()
    if not text:
        raise RuntimeError("Groq response did not include report content")
    return _truncate_report(text)


def _fallback_report(snapshot: dict[str, Any], status_payload: dict[str, Any]) -> str:
    current = status_payload.get("current") or {}
    score = snapshot.get("health_score_percent")
    band = snapshot.get("health_band")
    anomaly_count = (snapshot.get("components") or {}).get("anomaly_count_24h", 0)
    consecutive = (snapshot.get("components") or {}).get("consecutive_windows", 0)
    anomaly_flag = bool(status_payload.get("is_anomaly"))

    if anomaly_flag:
        action = "Investigate vibration source and inspect motor/fan mount before next cycle."
    elif band in {"Watchlist", "Critical"}:
        action = "Schedule preventive inspection and recalibrate model with fresh baseline data."
    else:
        action = "Continue monitoring and keep calibration schedule unchanged."

    return (
        f"- Health score is {score}% ({band}).\n"
        f"- Current anomaly score is {current.get('score')} with threshold {current.get('decision_threshold')}.\n"
        f"- Last 24h anomalies: {anomaly_count}; consecutive anomaly windows: {consecutive}.\n"
        f"{action}"
    )


def _build_machine_insight(machine_id: str, device_id: str, *, force_regenerate: bool) -> dict[str, Any]:
    status_payload = get_status_for_device(machine_id, device_id)
    anomaly_count_24h = len(store.get_anomalies(machine_id, device_id=device_id, hours=24, limit=200))
    snapshot = _compute_health_snapshot(status_payload, anomaly_count_24h)
    vibration_stats = _compute_vibration_statistics(machine_id, device_id)

    cache_key = _insight_cache_key(machine_id, device_id)
    with insight_cache_lock:
        cached = dict(insight_cache.get(cache_key, {}))

    llm_report = str(cached.get("llm_report") or "").strip()
    llm_meta = dict(cached.get("llm") or {})

    should_generate = force_regenerate or not llm_report
    if should_generate:
        generated_at = utc_iso_now()
        report_source = "groq"
        report_error = None

        try:
            llm_report = _generate_report_with_groq(machine_id, device_id, status_payload, snapshot, vibration_stats)
        except Exception as exc:  # noqa: BLE001
            llm_report = _fallback_report(snapshot, status_payload)
            report_source = "fallback"
            report_error = str(exc)

        llm_meta = {
            "provider": "groq",
            "model": GROQ_MODEL,
            "source": report_source,
            "generated_at": generated_at,
            "cached": False,
            "api_key_configured": bool(GROQ_API_KEY),
            "error": report_error,
        }
        with insight_cache_lock:
            insight_cache[cache_key] = {
                "llm_report": llm_report,
                "llm": llm_meta,
            }
    else:
        llm_meta.setdefault("provider", "groq")
        llm_meta.setdefault("model", GROQ_MODEL)
        llm_meta.setdefault("source", "cache")
        llm_meta.setdefault("generated_at", None)
        llm_meta["cached"] = True
        llm_meta.setdefault("api_key_configured", bool(GROQ_API_KEY))
        llm_meta.setdefault("error", None)

    return {
        "machine_id": machine_id,
        "device_id": device_id,
        "server_timestamp": utc_iso_now(),
        "status_label": status_payload.get("status_label"),
        "is_anomaly": bool(status_payload.get("is_anomaly")),
        "health_score_percent": snapshot["health_score_percent"],
        "health_band": snapshot["health_band"],
        "health_components": snapshot["components"],
        "score_ratio": snapshot.get("score_ratio"),
        "llm_report": llm_report,
        "llm": llm_meta,
    }


def get_active_stream_binding() -> dict[str, Any] | None:
    binding = store.get_stream_binding()
    if not binding or not binding.get("is_active"):
        return None

    machine_id = binding.get("machine_id")
    device_id = binding.get("device_id")
    if not machine_id or not device_id:
        return None

    return binding


def resolve_stream_target(_: StreamIngestRequest) -> tuple[str, str, dict[str, Any] | None]:
    binding = get_active_stream_binding()
    if binding:
        return str(binding["machine_id"]), str(binding["device_id"]), binding
    return UNASSIGNED_MACHINE_ID, UNASSIGNED_DEVICE_ID, None


def sample_to_record(sample: Any) -> dict[str, Any]:
    return {
        "timestamp": sample.timestamp.astimezone(timezone.utc).isoformat(),
        "acc_mag": float(sample.acc_mag),
        "gyro_mag": float(sample.gyro_mag),
        "gx": float(sample.gx),
        "gy": float(sample.gy),
        "gz": float(sample.gz),
        "sw420": sample.sw420,
        "sequence": sample.sequence,
    }


def _sanitize_training_sample(record: dict[str, Any]) -> dict[str, float]:
    """Whitelist only accel/gyro features for model training."""
    return {
        "acc_mag": float(record.get("acc_mag", 0.0)),
        "gyro_mag": float(record.get("gyro_mag", 0.0)),
        "gx": float(record.get("gx", 0.0)),
        "gy": float(record.get("gy", 0.0)),
        "gz": float(record.get("gz", 0.0)),
    }


def _assess_calibration_quality(
    samples: list[dict[str, float]],
    *,
    sample_rate_hz: int,
    collection_seconds: int,
) -> dict[str, Any]:
    expected_count = max(1, int(sample_rate_hz * collection_seconds))
    sample_count = len(samples)
    coverage_ratio = float(sample_count / expected_count) if expected_count else 1.0

    timestamps = []
    for sample in samples:
        raw = sample.get("timestamp")
        if not raw:
            continue
        try:
            timestamps.append(datetime.fromisoformat(str(raw)))
        except ValueError:
            continue

    timestamps.sort()
    deltas: list[float] = []
    for idx in range(1, len(timestamps)):
        deltas.append((timestamps[idx] - timestamps[idx - 1]).total_seconds())

    expected_interval = 1.0 / max(1, sample_rate_hz)
    max_allowed_gap = max(1.5, expected_interval * 5.0)
    gap_ratio = (
        float(sum(1 for delta in deltas if delta > max_allowed_gap) / len(deltas))
        if deltas
        else 0.0
    )

    acc_values = [float(sample.get("acc_mag", 0.0)) for sample in samples]
    gyro_values = [float(sample.get("gyro_mag", 0.0)) for sample in samples]
    acc_mean = sum(acc_values) / max(1, len(acc_values))
    gyro_mean = sum(gyro_values) / max(1, len(gyro_values))
    acc_var = sum((value - acc_mean) ** 2 for value in acc_values) / max(1, len(acc_values))
    gyro_var = sum((value - gyro_mean) ** 2 for value in gyro_values) / max(1, len(gyro_values))
    acc_std = acc_var**0.5
    gyro_std = gyro_var**0.5

    n = len(samples)
    segment = max(1, n // 3)
    first = samples[:segment]
    last = samples[-segment:]
    first_acc = sum(float(item.get("acc_mag", 0.0)) for item in first) / max(1, len(first))
    last_acc = sum(float(item.get("acc_mag", 0.0)) for item in last) / max(1, len(last))
    first_gyro = sum(float(item.get("gyro_mag", 0.0)) for item in first) / max(1, len(first))
    last_gyro = sum(float(item.get("gyro_mag", 0.0)) for item in last) / max(1, len(last))

    acc_drift_ratio = abs(last_acc - first_acc) / max(1.0, abs(acc_mean))
    gyro_drift_ratio = abs(last_gyro - first_gyro) / max(1.0, abs(gyro_mean))

    checks = {
        "coverage_ok": coverage_ratio >= 0.70,
        "continuity_ok": gap_ratio <= 0.15,
        "variance_ok": acc_std >= 0.01 and gyro_std >= 0.005,
        "drift_ok": acc_drift_ratio <= 0.35 and gyro_drift_ratio <= 0.50,
    }
    passed = all(checks.values())
    reasons = [name for name, ok in checks.items() if not ok]

    return {
        "passed": passed,
        "failed_checks": reasons,
        "checks": checks,
        "metrics": {
            "sample_count": sample_count,
            "expected_count": expected_count,
            "coverage_ratio": round(coverage_ratio, 4),
            "timestamp_gap_ratio": round(gap_ratio, 4),
            "expected_interval_seconds": round(expected_interval, 5),
            "max_allowed_gap_seconds": round(max_allowed_gap, 5),
            "acc_std": round(acc_std, 6),
            "gyro_std": round(gyro_std, 6),
            "acc_drift_ratio": round(acc_drift_ratio, 6),
            "gyro_drift_ratio": round(gyro_drift_ratio, 6),
        },
    }


def resolve_calibration_samples(request: CalibrationRequest) -> tuple[list[dict[str, Any]], str]:
    if request.baseline_samples:
        return [sample_to_record(sample) for sample in request.baseline_samples], "payload_baseline_samples"

    if request.magnitudes:
        now = datetime.now(timezone.utc)
        generated = []
        for idx, magnitude in enumerate(request.magnitudes):
            generated.append(
                {
                    "timestamp": (now + timedelta(seconds=idx)).isoformat(),
                    "acc_mag": float(magnitude),
                    "gyro_mag": 0.0,
                    "gx": 0.0,
                    "gy": 0.0,
                    "gz": 0.0,
                    "sw420": None,
                    "sequence": None,
                }
            )
        return generated, "payload_magnitudes"

    machine_id = str(request.machine_id or "").strip()
    device_id = str(request.device_id or "").strip()
    if not machine_id or not device_id:
        raise HTTPException(
            status_code=400,
            detail="Calibration target not resolved. Provide device_name or valid machine/device IDs.",
        )

    collection_seconds = _calibration_collection_seconds(request)

    fallback = store.get_recent_samples(
        machine_id,
        device_id=device_id,
        seconds=collection_seconds,
        limit=max(1000, request.sample_rate_hz * collection_seconds),
    )
    return fallback, "fallback_recent_stream"


def _set_job(job_id: str, **updates: Any) -> dict[str, Any]:
    with calibration_jobs_lock:
        if job_id not in calibration_jobs:
            raise KeyError(job_id)
        calibration_jobs[job_id].update(updates)
        calibration_jobs[job_id]["updated_at"] = utc_iso_now()
        return dict(calibration_jobs[job_id])


def _get_job(job_id: str) -> dict[str, Any] | None:
    with calibration_jobs_lock:
        job = calibration_jobs.get(job_id)
        return dict(job) if job else None


def _active_calibration_for_device(machine_id: str, device_id: str) -> dict[str, Any] | None:
    with calibration_jobs_lock:
        for job in reversed(list(calibration_jobs.values())):
            if job["machine_id"] != machine_id or job["device_id"] != device_id:
                continue
            if job["status"] in {"queued", "running"}:
                return dict(job)
    return None


def _to_job_status(job: dict[str, Any]) -> CalibrationJobStatus:
    machine_id = str(job.get("machine_id") or "").strip()
    device_id = str(job.get("device_id") or "").strip()
    device_name = _normalize_device_name(job.get("device_name")) or _device_name_for_ids(machine_id, device_id)

    return CalibrationJobStatus(
        job_id=job["job_id"],
        status=job["status"],
        stage=job["stage"],
        progress=int(job["progress"]),
        device_name=device_name,
        machine_id=machine_id,
        device_id=device_id,
        calibration_duration_seconds=job.get("calibration_duration_seconds"),
        trigger_source=job["trigger_source"],
        new_device_setup=bool(job["new_device_setup"]),
        started_at=job["started_at"],
        updated_at=job["updated_at"],
        message=job.get("message"),
        error=job.get("error"),
        result=job.get("result"),
    )


def perform_calibration(payload: CalibrationRequest) -> CalibrationResponse:
    machine_id = str(payload.machine_id or "").strip()
    device_id = str(payload.device_id or "").strip()
    if not machine_id or not device_id:
        raise HTTPException(
            status_code=400,
            detail="Calibration target not resolved. Provide device_name or valid machine/device IDs.",
        )

    device_name = _normalize_device_name(payload.device_name) or _device_name_for_ids(machine_id, device_id)
    collection_seconds = _calibration_collection_seconds(payload)

    samples, source = resolve_calibration_samples(payload)
    if len(samples) < 20 and not payload.force_train_on_low_quality:
        sample_count = len(samples)
        required_min = 20
        expected_count = max(1, int(payload.sample_rate_hz * collection_seconds))
        raise HTTPException(
            status_code=400,
            detail=(
                "Insufficient samples for calibration. Provide at least 20 samples, "
                "or increase fallback_seconds. "
                f"Metrics: sample_count={sample_count}, required_min={required_min}, "
                f"expected_count={expected_count}, sample_rate_hz={payload.sample_rate_hz}, "
                f"collection_seconds={collection_seconds}."
            ),
        )

    training_samples = [_sanitize_training_sample(sample) for sample in samples]
    quality = _assess_calibration_quality(
        samples=samples,
        sample_rate_hz=payload.sample_rate_hz,
        collection_seconds=collection_seconds,
    )
    if not quality["passed"] and not payload.force_train_on_low_quality:
        failed_checks = ", ".join(quality["failed_checks"]) or "unknown_quality_issue"
        metrics = quality.get("metrics", {})
        sample_count = int(metrics.get("sample_count", len(samples)))
        expected_count = int(metrics.get("expected_count", max(1, payload.sample_rate_hz * collection_seconds)))
        coverage_ratio = float(metrics.get("coverage_ratio", 0.0))
        gap_ratio = float(metrics.get("timestamp_gap_ratio", 0.0))
        max_gap = float(metrics.get("max_allowed_gap_seconds", 0.0))
        raise HTTPException(
            status_code=400,
            detail=(
                "Calibration data quality check failed. "
                f"Failed checks: {failed_checks}. "
                f"Metrics: sample_count={sample_count}, expected_count={expected_count}, "
                f"coverage_ratio={coverage_ratio:.3f}, gap_ratio={gap_ratio:.3f}, "
                f"max_allowed_gap_s={max_gap:.2f}. "
                "Recalibrate with longer stable baseline capture."
            ),
        )

    if payload.force_train_on_low_quality:
      effective_window_size = max(1, min(max(2, payload.sample_rate_hz * payload.window_seconds), len(training_samples)))
    else:
      effective_window_size = max(8, payload.sample_rate_hz * payload.window_seconds)

    feature_matrix = build_feature_matrix(training_samples, window_size=effective_window_size)

    if feature_matrix.shape[0] < 8 and not payload.force_train_on_low_quality:
        raise HTTPException(
            status_code=400,
            detail=(
                "Not enough windows for model calibration. "
                "Increase sample count or reduce window size."
            ),
        )

    if payload.force_train_on_low_quality and feature_matrix.shape[0] <= 0:
        raise HTTPException(
            status_code=400,
            detail="Force-train requested but zero usable windows were produced.",
        )

    if payload.force_train_on_low_quality and feature_matrix.shape[0] < 8:
        reps = int(np.ceil(8 / feature_matrix.shape[0]))
        feature_matrix = np.tile(feature_matrix, (reps, 1))[:8, :]

    model_variant = str(payload.model_variant or "ocsvm_distilled").strip().lower()
    if model_variant == "if_distilled":
        distilled = train_isolation_forest_distilled(
            feature_matrix=feature_matrix,
            contamination=payload.contamination,
        )
        model_type = "isolation_forest_distilled_linear"
    else:
        distilled = train_oneclass_svm_distilled(
            feature_matrix=feature_matrix,
            contamination=payload.contamination,
        )
        model_type = "oneclass_svm_distilled_linear"

    baseline_stats = acc_threshold_stats(training_samples)

    existing = store.get_model_package(machine_id, device_id)
    new_version = int(existing["model_version"]) + 1 if existing else 1

    package = {
        "model_type": model_type,
        "model_variant": model_variant,
        "model_version": new_version,
        "feature_order": FEATURE_ORDER,
        "feature_means": round_list(distilled["feature_means"]),
        "feature_stds": round_list(distilled["feature_stds"]),
        "weights": round_list(distilled["weights"]),
        "bias": round(float(distilled["bias"]), 6),
        "decision_threshold": round(float(distilled["decision_threshold"]), 6),
        "hysteresis_high": round(float(distilled["hysteresis_high"]), 6),
        "hysteresis_low": round(float(distilled["hysteresis_low"]), 6),
        "min_consecutive_windows": payload.min_consecutive_windows,
        "effective_window_size": effective_window_size,
        "sample_rate_hz": payload.sample_rate_hz,
        "window_seconds": payload.window_seconds,
        "fallback_acc_threshold": round(float(baseline_stats["threshold_mean_3sigma"]), 6),
        "quality_correlation": round(float(distilled["quality_correlation"]), 6),
        "trained_on_windows": int(distilled["window_count"]),
        "trained_on_samples": len(samples),
        "calibration_quality": quality,
        "quality_override_enabled": bool(payload.force_train_on_low_quality),
        "quality_checks_passed": bool(quality.get("passed", False)),
        "created_at": utc_iso_now(),
        "target_machine_id": machine_id,
        "target_device_id": device_id,
        "calibration_duration_seconds": collection_seconds,
        "new_device_setup": payload.new_device_setup,
        "trigger_source": payload.trigger_source,
    }
    package["checksum"] = build_checksum(package)

    store.save_model_package(machine_id, device_id, package, baseline_stats)

    state = store.get_machine_state(machine_id, device_id)
    state.update(
        {
            "machine_id": machine_id,
            "device_id": device_id,
            "last_calibration_at": package["created_at"],
            "model_version": new_version,
            "model_checksum": package["checksum"],
            "fallback_acc_threshold": package["fallback_acc_threshold"],
            "decision_threshold": package["decision_threshold"],
            "calibration_in_progress": False,
            "calibration_progress": 100,
            "calibration_stage": "completed",
            "calibration_message": "Model package ready for device",
        }
    )
    store.set_machine_state(machine_id, device_id, state)

    return CalibrationResponse(
        status="success",
        device_name=device_name,
        machine_id=machine_id,
        device_id=device_id,
        calibration_duration_seconds=collection_seconds,
        calibration_source=source,
        sample_count=len(samples),
        window_count=int(distilled["window_count"]),
        statistics={
            "mean_acc": round(float(baseline_stats["mean_acc"]), 6),
            "std_acc": round(float(baseline_stats["std_acc"]), 6),
            "threshold_mean_3sigma": round(float(baseline_stats["threshold_mean_3sigma"]), 6),
            "quality_correlation": round(float(distilled["quality_correlation"]), 6),
            "calibration_quality": quality,
        },
        model_package=package,
    )


def _run_calibration_job(job_id: str, payload_data: dict[str, Any]) -> None:
    payload = _resolve_calibration_payload(CalibrationRequest(**payload_data))
    machine_id = str(payload.machine_id or "")
    device_id = str(payload.device_id or "")
    collection_seconds = _calibration_collection_seconds(payload)

    try:
        _set_job(
            job_id,
            status="running",
            stage="collecting_data",
            progress=15,
            message=f"Collecting stream data for {collection_seconds}s",
        )
        state = store.get_machine_state(machine_id, device_id)
        state.update(
            {
                "machine_id": machine_id,
                "device_id": device_id,
                "calibration_in_progress": True,
                "calibration_progress": 15,
                "calibration_stage": "collecting_data",
                "calibration_message": f"Collecting stream data for {collection_seconds}s",
                "active_calibration_job_id": job_id,
            }
        )
        store.set_machine_state(machine_id, device_id, state)

        start_time = time.monotonic()
        while True:
            elapsed = time.monotonic() - start_time
            if elapsed >= collection_seconds:
                break

            progress = min(30, 15 + int((elapsed / max(1, collection_seconds)) * 15))
            message = f"Collecting stream data for {collection_seconds}s ({int(elapsed)}s elapsed)"
            _set_job(
                job_id,
                stage="collecting_data",
                progress=progress,
                message=message,
            )
            state = store.get_machine_state(machine_id, device_id)
            state.update(
                {
                    "calibration_progress": progress,
                    "calibration_stage": "collecting_data",
                    "calibration_message": message,
                }
            )
            store.set_machine_state(machine_id, device_id, state)
            time.sleep(1.0)

        for target_progress, target_stage, target_message in [
            (45, "extracting_features", "Building vibration feature windows"),
            (75, "training_model", "Training Isolation Forest and distilling edge weights"),
        ]:
            _set_job(
                job_id,
                stage=target_stage,
                progress=target_progress,
                message=target_message,
            )
            state = store.get_machine_state(machine_id, device_id)
            state.update(
                {
                    "calibration_progress": target_progress,
                    "calibration_stage": target_stage,
                    "calibration_message": target_message,
                }
            )
            store.set_machine_state(machine_id, device_id, state)
            time.sleep(0.25)

        result = perform_calibration(payload)

        _set_job(
            job_id,
            status="completed",
            stage="completed",
            progress=100,
            message="Calibration completed and model package generated",
            result=result.model_dump(),
        )
        state = store.get_machine_state(machine_id, device_id)
        state.update(
            {
                "calibration_in_progress": False,
                "calibration_progress": 100,
                "calibration_stage": "completed",
                "calibration_message": "Calibration completed and model package generated",
                "active_calibration_job_id": None,
            }
        )
        store.set_machine_state(machine_id, device_id, state)

    except HTTPException as exc:
        _set_job(
            job_id,
            status="failed",
            stage="failed",
            progress=100,
            error=str(exc.detail),
            message="Calibration failed",
        )
        state = store.get_machine_state(machine_id, device_id)
        state.update(
            {
                "calibration_in_progress": False,
                "calibration_progress": 100,
                "calibration_stage": "failed",
                "calibration_message": str(exc.detail),
                "active_calibration_job_id": None,
            }
        )
        store.set_machine_state(machine_id, device_id, state)

    except Exception as exc:  # noqa: BLE001
        _set_job(
            job_id,
            status="failed",
            stage="failed",
            progress=100,
            error=str(exc),
            message="Calibration failed due to unexpected error",
        )
        state = store.get_machine_state(machine_id, device_id)
        state.update(
            {
                "calibration_in_progress": False,
                "calibration_progress": 100,
                "calibration_stage": "failed",
                "calibration_message": str(exc),
                "active_calibration_job_id": None,
            }
        )
        store.set_machine_state(machine_id, device_id, state)


@router.get("/health")
def health() -> dict[str, str]:
    return {
        "status": "ok",
        "service": "machinocare-backend",
        "time": utc_iso_now(),
        "db_backend": store.backend,
    }


def _ingest_stream_payload(payload: StreamIngestRequest) -> dict[str, Any]:
    samples = [sample_to_record(sample) for sample in payload.expanded_samples()]
    if not samples:
        raise HTTPException(status_code=400, detail="No samples provided.")

    if len(samples) > 4000:
        raise HTTPException(status_code=413, detail="Payload too large. Reduce batch size.")

    machine_id, device_id, binding = resolve_stream_target(payload)
    profile = store.get_device_profile(machine_id, device_id)
    device_name = (profile or {}).get("display_name")

    store.add_samples(machine_id, device_id, samples)

    state = store.get_machine_state(machine_id, device_id)
    model_package = store.get_model_package(machine_id, device_id)

    last_sample = samples[-1]
    score: float | None = None
    threshold: float | None = None
    consecutive = int(state.get("consecutive_anomaly_windows", 0))
    is_anomaly = bool(state.get("is_anomaly", False))

    if model_package:
        window_size = int(model_package.get("effective_window_size", 25))
        recent = store.get_recent_samples(
            machine_id,
            device_id=device_id,
            limit=max(window_size * 2, window_size),
        )
        feature_vector = latest_feature_vector(recent, window_size)
        if feature_vector is not None:
            score = score_feature_vector(feature_vector, model_package)
            high = float(model_package.get("hysteresis_high", model_package["decision_threshold"]))
            low = float(model_package.get("hysteresis_low", high))
            min_windows = int(model_package.get("min_consecutive_windows", 3))

            if score >= high:
                consecutive += 1
            elif score < low:
                consecutive = 0

            threshold = high
            prev_anomaly = bool(state.get("is_anomaly", False))
            is_anomaly = consecutive >= min_windows
            if is_anomaly and not prev_anomaly:
                store.record_anomaly(
                    machine_id=machine_id,
                    device_id=device_id,
                    timestamp=last_sample["timestamp"],
                    acc_mag=last_sample["acc_mag"],
                    score=score,
                    threshold=threshold,
                    reason="distilled_score",
                )
    else:
        fallback = state.get("fallback_acc_threshold")
        if fallback is not None:
            threshold = float(fallback)
            prev_anomaly = bool(state.get("is_anomaly", False))
            is_anomaly = last_sample["acc_mag"] >= threshold
            consecutive = 1 if is_anomaly else 0
            if is_anomaly and not prev_anomaly:
                store.record_anomaly(
                    machine_id=machine_id,
                    device_id=device_id,
                    timestamp=last_sample["timestamp"],
                    acc_mag=last_sample["acc_mag"],
                    score=None,
                    threshold=threshold,
                    reason="fallback_acc_threshold",
                )

    state_threshold = threshold
    if state_threshold is None and model_package:
        state_threshold = float(model_package.get("decision_threshold"))

    active_job = _active_calibration_for_device(machine_id, device_id)

    new_state = {
        **state,
        "machine_id": machine_id,
        "device_id": device_id,
        "esp_model_version": payload.esp_model_version,
        "esp_model_checksum": payload.esp_model_checksum,
        "last_update": last_sample["timestamp"],
        "current_acc_mag": round(last_sample["acc_mag"], 6),
        "current_gyro_mag": round(last_sample.get("gyro_mag", 0.0), 6),
        "current_score": round(score, 6) if score is not None else None,
        "decision_threshold": round(state_threshold, 6) if state_threshold is not None else None,
        "consecutive_anomaly_windows": consecutive,
        "is_anomaly": is_anomaly,
        "status_label": "ANOMALY DETECTED" if is_anomaly else "HEALTHY",
        "calibration_in_progress": bool(active_job),
        "calibration_progress": active_job["progress"] if active_job else state.get("calibration_progress"),
        "calibration_stage": active_job["stage"] if active_job else state.get("calibration_stage"),
        "calibration_message": active_job["message"] if active_job else state.get("calibration_message"),
        "active_calibration_job_id": active_job["job_id"] if active_job else state.get("active_calibration_job_id"),
    }
    store.set_machine_state(machine_id, device_id, new_state)

    binding_with_name = _binding_with_device_name(binding)
    binding_view = StreamBindingResponse(**binding_with_name).model_dump() if binding_with_name else None

    return {
        "status": "queued",
        "device_name": device_name,
        "received_samples": len(samples),
        "server_timestamp": utc_iso_now(),
        "is_anomaly": is_anomaly,
        "score": round(score, 6) if score is not None else None,
        "decision_threshold": round(state_threshold, 6) if state_threshold is not None else None,
        "stream_binding": binding_view,
        "calibration": {
            "in_progress": bool(active_job),
            "job_id": active_job["job_id"] if active_job else None,
            "progress": active_job["progress"] if active_job else None,
            "stage": active_job["stage"] if active_job else None,
        },
        "esp_model_version": state.get("esp_model_version"),
        "esp_model_checksum": state.get("esp_model_checksum"),
    }


@router.post("/stream", status_code=202)
def ingest_stream(payload: StreamIngestRequest) -> dict[str, Any]:
    return _ingest_stream_payload(payload)


@router.post("/calibrate/start", response_model=CalibrationStartResponse)
def calibrate_start(payload: CalibrationRequest) -> CalibrationStartResponse:
    payload = _resolve_calibration_payload(payload)
    machine_id = str(payload.machine_id or "")
    device_id = str(payload.device_id or "")
    device_name = _normalize_device_name(payload.device_name) or _device_name_for_ids(machine_id, device_id)

    active = _active_calibration_for_device(machine_id, device_id)
    if active:
        return CalibrationStartResponse(
            status="already_running",
            job_id=active["job_id"],
            device_name=device_name,
            machine_id=machine_id,
            device_id=device_id,
            trigger_source=payload.trigger_source,
            new_device_setup=payload.new_device_setup,
        )

    job_id = str(uuid.uuid4())
    now = utc_iso_now()
    job = {
        "job_id": job_id,
        "status": "queued",
        "stage": "queued",
        "progress": 0,
        "device_name": device_name,
        "machine_id": machine_id,
        "device_id": device_id,
        "calibration_duration_seconds": payload.calibration_duration_seconds or payload.fallback_seconds,
        "trigger_source": payload.trigger_source,
        "new_device_setup": payload.new_device_setup,
        "started_at": now,
        "updated_at": now,
        "message": "Calibration queued",
        "error": None,
        "result": None,
    }
    with calibration_jobs_lock:
        calibration_jobs[job_id] = job

    state = store.get_machine_state(machine_id, device_id)
    state.update(
        {
            "machine_id": machine_id,
            "device_id": device_id,
            "calibration_in_progress": True,
            "calibration_progress": 0,
            "calibration_stage": "queued",
            "calibration_message": f"Calibration queued for {payload.calibration_duration_seconds or payload.fallback_seconds}s",
            "active_calibration_job_id": job_id,
        }
    )
    store.set_machine_state(machine_id, device_id, state)

    thread = threading.Thread(
        target=_run_calibration_job,
        args=(job_id, payload.model_dump()),
        daemon=True,
    )
    thread.start()

    return CalibrationStartResponse(
        status="queued",
        job_id=job_id,
        device_name=device_name,
        machine_id=machine_id,
        device_id=device_id,
        calibration_duration_seconds=payload.calibration_duration_seconds or payload.fallback_seconds,
        trigger_source=payload.trigger_source,
        new_device_setup=payload.new_device_setup,
    )


@router.post("/calibrate/start/profile/{device_name}", response_model=CalibrationStartResponse)
def calibrate_start_from_profile(
    device_name: str,
    new_device_setup: bool = Query(default=True),
    trigger_source: str = Query(default="dashboard_profile"),
    force_train_on_low_quality: bool = Query(default=False),
    calibration_duration_seconds: int | None = Query(default=None, ge=10, le=86400),
) -> CalibrationStartResponse:
    profile = _resolve_profile_or_404(device_name)
    machine_id = str(profile["machine_id"])
    device_id = str(profile["device_id"])

    payload = CalibrationRequest(
        device_name=_normalize_device_name(device_name),
        machine_id=machine_id,
        device_id=device_id,
        sample_rate_hz=int(profile.get("sample_rate_hz") or 25),
        window_seconds=int(profile.get("window_seconds") or 1),
        fallback_seconds=int(profile.get("fallback_seconds") or 300),
        calibration_duration_seconds=int(calibration_duration_seconds or profile.get("fallback_seconds") or 300),
        contamination=float(profile.get("contamination") or 0.05),
        min_consecutive_windows=int(profile.get("min_consecutive_windows") or 3),
        force_train_on_low_quality=bool(force_train_on_low_quality),
        new_device_setup=new_device_setup,
        trigger_source=trigger_source,
    )
    return calibrate_start(payload)


@router.get("/calibrate/status/{job_id}", response_model=CalibrationJobStatus)
def calibrate_status(job_id: str) -> CalibrationJobStatus:
    job = _get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Calibration job '{job_id}' not found.")
    return _to_job_status(job)


@router.post("/calibrate", response_model=CalibrationResponse)
def calibrate(payload: CalibrationRequest) -> CalibrationResponse:
    payload = _resolve_calibration_payload(payload)
    return perform_calibration(payload)


@router.get("/model/{device_name}")
def get_model_for_device(device_name: str) -> dict[str, Any]:
    machine_id, device_id = _resolve_machine_device_from_name(device_name)
    package = store.get_model_package(machine_id, device_id)
    if not package:
        raise HTTPException(
            status_code=404,
            detail=f"No model package found for device '{device_name}'.",
        )
    return {
        "status": "success",
        "device_name": _normalize_device_name(device_name),
        "model_package": package,
    }


@router.get("/model/{machine_id}/{device_id}")
def get_model_for_machine_device(machine_id: str, device_id: str) -> dict[str, Any]:
    """Serve model package by explicit machine_id and device_id.
    
    This endpoint is used by ESP32 firmware to pull the model after calibration.
    """
    package = store.get_model_package(machine_id, device_id)
    if not package:
        raise HTTPException(
            status_code=404,
            detail=f"No model package found for machine '{machine_id}' device '{device_id}'.",
        )
    return {
        "status": "success",
        "machine_id": machine_id,
        "device_id": device_id,
        "model_package": package,
    }


def get_status_for_device(machine_id: str, device_id: str) -> dict[str, Any]:
    state = store.get_machine_state(machine_id, device_id)
    latest_sample = store.get_latest_sample_for_device(machine_id=machine_id, device_id=device_id)
    package = store.get_model_package(machine_id, device_id)
    active_job = _active_calibration_for_device(machine_id, device_id)

    if not state and not latest_sample and not package:
        raise HTTPException(
            status_code=404,
            detail=f"Machine '{machine_id}' with device '{device_id}' has no data yet.",
        )

    calibration_view = {
        "last_calibration_at": state.get("last_calibration_at"),
        "model_version": state.get("model_version"),
        "model_checksum": state.get("model_checksum"),
        "fallback_acc_threshold": state.get("fallback_acc_threshold"),
        "in_progress": bool(active_job) or bool(state.get("calibration_in_progress")),
        "progress": active_job["progress"] if active_job else state.get("calibration_progress"),
        "stage": active_job["stage"] if active_job else state.get("calibration_stage"),
        "message": active_job["message"] if active_job else state.get("calibration_message"),
        "job_id": active_job["job_id"] if active_job else state.get("active_calibration_job_id"),
    }

    return {
        "machine_id": machine_id,
        "device_id": device_id,
        "server_timestamp": utc_iso_now(),
        "esp_model_version": state.get("esp_model_version"),
        "esp_model_checksum": state.get("esp_model_checksum"),
        "status_label": state.get("status_label", "UNKNOWN"),
        "is_anomaly": state.get("is_anomaly", False),
        "current": {
            "acc_mag": state.get("current_acc_mag"),
            "gyro_mag": state.get("current_gyro_mag"),
            "score": state.get("current_score"),
            "decision_threshold": state.get("decision_threshold") or (package or {}).get("decision_threshold"),
            "consecutive_windows": state.get("consecutive_anomaly_windows", 0),
            "last_update": state.get("last_update") or (latest_sample or {}).get("timestamp"),
        },
        "calibration": calibration_view,
        "model_summary": {
            "model_type": (package or {}).get("model_type"),
            "model_version": (package or {}).get("model_version"),
            "window_size": (package or {}).get("effective_window_size"),
            "decision_threshold": (package or {}).get("decision_threshold"),
            "hysteresis_low": (package or {}).get("hysteresis_low"),
            "quality_correlation": (package or {}).get("quality_correlation"),
            "esp_model_version": state.get("esp_model_version"),
            "esp_model_checksum": state.get("esp_model_checksum"),
            "target_device_id": (package or {}).get("target_device_id"),
        },
    }


@router.get("/status/{device_name}")
def get_status(device_name: str) -> dict[str, Any]:
    machine_id, device_id = _resolve_machine_device_from_name(device_name)
    payload = get_status_for_device(machine_id, device_id)
    payload["device_name"] = _normalize_device_name(device_name)
    payload.pop("machine_id", None)
    payload.pop("device_id", None)
    return payload


@router.get("/insights/{device_name}")
def machine_insights(
    device_name: str,
    regenerate: bool = Query(default=False),
) -> dict[str, Any]:
    normalized_name = _normalize_device_name(device_name)
    machine_id, device_id = _resolve_machine_device_from_name(normalized_name)
    insight = _build_machine_insight(machine_id, device_id, force_regenerate=bool(regenerate))
    insight["device_name"] = normalized_name
    insight.pop("machine_id", None)
    insight.pop("device_id", None)
    return insight


@router.post("/insights/{device_name}/regenerate")
def regenerate_machine_insights(device_name: str) -> dict[str, Any]:
    return machine_insights(device_name, regenerate=True)


@router.get("/blynk/insights/{device_name}")
def blynk_insights(device_name: str) -> dict[str, Any]:
    normalized_name = _normalize_device_name(device_name)
    machine_id, device_id = _resolve_machine_device_from_name(normalized_name)
    insight = _build_machine_insight(machine_id, device_id, force_regenerate=False)
    return {
        "device_name": normalized_name,
        "server_timestamp": insight.get("server_timestamp"),
        "status_label": insight.get("status_label"),
        "is_anomaly": insight.get("is_anomaly"),
        "health_score_percent": insight.get("health_score_percent"),
        "llm_report": insight.get("llm_report"),
        "report_generated_at": (insight.get("llm") or {}).get("generated_at"),
        "report_source": (insight.get("llm") or {}).get("source"),
    }


@router.get("/stream/recent/{device_name}")
def recent_stream(
    device_name: str,
    seconds: int = Query(default=120, ge=5, le=86400),
    limit: int = Query(default=500, ge=1, le=10000),
) -> dict[str, Any]:
    machine_id, device_id = _resolve_machine_device_from_name(device_name)
    samples = store.get_recent_samples(machine_id, device_id=device_id, seconds=seconds, limit=limit)
    return {
        "device_name": _normalize_device_name(device_name),
        "seconds": seconds,
        "count": len(samples),
        "samples": samples,
    }


@router.get("/anomaly-log/{device_name}")
def anomaly_log(
    device_name: str,
    hours: int = Query(default=24, ge=1, le=720),
    limit: int = Query(default=200, ge=1, le=1000),
) -> dict[str, Any]:
    machine_id, device_id = _resolve_machine_device_from_name(device_name)
    anomalies = store.get_anomalies(machine_id, device_id=device_id, hours=hours, limit=limit)
    return {
        "device_name": _normalize_device_name(device_name),
        "hours": hours,
        "count": len(anomalies),
        "anomalies": anomalies,
    }


@router.get("/device-names")
def device_names(limit: int = Query(default=500, ge=1, le=2000)) -> dict[str, Any]:
    names = store.list_device_names(limit=limit)
    return {"count": len(names), "device_names": names}


@router.get("/stream-binding", response_model=StreamBindingResponse)
def get_stream_binding() -> StreamBindingResponse:
    binding = _binding_with_device_name(store.get_stream_binding())
    if not binding:
        return StreamBindingResponse(binding_name="primary", is_active=False)
    return StreamBindingResponse(**binding)


@router.post("/stream-binding", response_model=StreamBindingResponse)
def upsert_stream_binding(payload: StreamBindingUpsertRequest) -> StreamBindingResponse:
    profile = None
    if payload.device_name:
        profile = store.get_device_profile_by_name(payload.device_name)
    elif payload.machine_id and payload.device_id:
        profile = store.get_device_profile(payload.machine_id, payload.device_id)

    if not profile:
        raise HTTPException(
            status_code=404,
            detail="Cannot bind stream: profile not found for provided device_name.",
        )

    binding = store.set_stream_binding(
        machine_id=str(profile["machine_id"]),
        device_id=str(profile["device_id"]),
        source=payload.source,
    )
    return StreamBindingResponse(**(_binding_with_device_name(binding) or binding))


@router.delete("/stream-binding", response_model=StreamBindingResponse)
def clear_stream_binding(source: str = Query(default="dashboard_manual")) -> StreamBindingResponse:
    binding = store.clear_stream_binding(source=source)
    return StreamBindingResponse(**(_binding_with_device_name(binding) or binding))


@router.post("/device-profiles", response_model=DeviceProfileResponse)
def upsert_device_profile(payload: DeviceProfileUpsertRequest) -> DeviceProfileResponse:
    device_name = _normalize_device_name(payload.device_name or payload.display_name)
    if not device_name:
        raise HTTPException(status_code=400, detail="device_name is required")

    existing = store.get_device_profile_by_name(device_name)
    if existing:
        machine_id = str(existing["machine_id"])
        device_id = str(existing["device_id"])
    elif payload.machine_id and payload.device_id:
        machine_id = payload.machine_id
        device_id = payload.device_id
    else:
        machine_id, device_id = _build_profile_ids_for_name(device_name)

    upsert_payload = payload.model_dump()
    upsert_payload["machine_id"] = machine_id
    upsert_payload["device_id"] = device_id
    upsert_payload["display_name"] = device_name

    profile = store.upsert_device_profile(upsert_payload)
    if not profile:
        raise HTTPException(status_code=500, detail="Failed to save device profile.")
    return _profile_response(profile)


@router.get("/device-profiles/{device_name}", response_model=DeviceProfileResponse)
def get_device_profile(device_name: str) -> DeviceProfileResponse:
    profile = store.get_device_profile_by_name(device_name)
    if not profile:
        raise HTTPException(
            status_code=404,
            detail=f"Device profile '{device_name}' not found.",
        )
    return _profile_response(profile)


@router.get("/device-profiles")
def list_device_profiles(
    limit: int = Query(default=200, ge=1, le=1000),
) -> dict[str, Any]:
    profiles = store.list_device_profiles(machine_id=None, limit=limit)
    return {
        "count": len(profiles),
        "profiles": [_profile_response(profile).model_dump() for profile in profiles],
    }


@router.delete("/device-profiles/{device_name}")
def delete_device_profile(device_name: str) -> dict[str, Any]:
    deleted_profile = store.delete_device_profile_by_name(device_name)
    if not deleted_profile:
        raise HTTPException(
            status_code=404,
            detail=f"Device profile '{device_name}' not found.",
        )

    return {
        "status": "deleted",
        "device_name": _normalize_device_name(device_name),
    }


@router.get("/debug/logs")
def debug_logs(
    device_name: str | None = Query(default=None),
    endpoint: str | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=1000),
) -> dict[str, Any]:
    machine_id = None
    device_id = None
    normalized_name = None
    if device_name:
        normalized_name = _normalize_device_name(device_name)
        machine_id, device_id = _resolve_machine_device_from_name(normalized_name)

    logs = store.list_api_debug_logs(
        machine_id=machine_id,
        device_id=device_id,
        endpoint=endpoint,
        limit=limit,
    )
    return {
        "device_name": normalized_name,
        "endpoint": endpoint,
        "count": len(logs),
        "logs": [ApiDebugLogEntry(**item).model_dump() for item in logs],
    }


@router.websocket("/ws/stream")
async def ws_stream_ingest(websocket: WebSocket) -> None:
    await websocket.accept()
    await websocket.send_json(
        {
            "type": "connected",
            "message": "WebSocket stream ingest ready",
            "server_timestamp": utc_iso_now(),
        }
    )

    while True:
        try:
            raw_text = await websocket.receive_text()
        except WebSocketDisconnect:
            break

        try:
            decoded = json.loads(raw_text)
        except json.JSONDecodeError:
            await websocket.send_json(
                {
                    "type": "error",
                    "status_code": 400,
                    "detail": "Invalid JSON payload.",
                    "server_timestamp": utc_iso_now(),
                }
            )
            continue

        if isinstance(decoded, dict) and decoded.get("type") == "ping":
            await websocket.send_json({"type": "pong", "server_timestamp": utc_iso_now()})
            continue

        try:
            payload = StreamIngestRequest.model_validate(decoded)
            last_sequence = None
            expanded = payload.expanded_samples()
            if expanded:
                last_sequence = expanded[-1].sequence

            result = _ingest_stream_payload(payload)
            await websocket.send_json(
                {
                    "type": "ack",
                    "ack_sequence": last_sequence,
                    **result,
                }
            )
        except HTTPException as exc:
            await websocket.send_json(
                {
                    "type": "error",
                    "status_code": exc.status_code,
                    "detail": exc.detail,
                    "server_timestamp": utc_iso_now(),
                }
            )
        except Exception as exc:  # noqa: BLE001
            await websocket.send_json(
                {
                    "type": "error",
                    "status_code": 500,
                    "detail": str(exc),
                    "server_timestamp": utc_iso_now(),
                }
            )


@router.websocket("/ws/live")
async def ws_live(websocket: WebSocket) -> None:
    await websocket.accept()

    initial_binding = _binding_with_device_name(get_active_stream_binding())
    default_device_name = str((initial_binding or {}).get("device_name") or "").strip() or None

    device_name = websocket.query_params.get("device_name") or default_device_name
    machine_id = None
    device_id = None
    if device_name:
        profile = store.get_device_profile_by_name(device_name)
        if profile:
            machine_id = str(profile["machine_id"])
            device_id = str(profile["device_id"])

    lookback_seconds = _bounded_int(
        websocket.query_params.get("lookback_seconds"),
        default=120,
        minimum=10,
        maximum=86400,
    )
    last_log_id = _bounded_int(
        websocket.query_params.get("last_log_id"),
        default=0,
        minimum=0,
        maximum=2_000_000_000,
    )
    await websocket.send_json(
        {
            "type": "connected",
            "device_name": device_name,
            "active_stream_binding": StreamBindingResponse(**initial_binding).model_dump() if initial_binding else None,
            "server_timestamp": utc_iso_now(),
        }
    )

    while True:
        try:
            message = await asyncio.wait_for(websocket.receive_text(), timeout=LIVE_PUSH_INTERVAL_SECONDS)
            if message:
                try:
                    payload = json.loads(message)
                except json.JSONDecodeError:
                    payload = {}

                if payload.get("type") == "subscribe":
                    device_name = payload.get("device_name") or device_name
                    if device_name:
                        profile = store.get_device_profile_by_name(device_name)
                        if profile:
                            machine_id = str(profile["machine_id"])
                            device_id = str(profile["device_id"])
                    lookback_seconds = _bounded_int(
                        payload.get("lookback_seconds"),
                        default=lookback_seconds,
                        minimum=10,
                        maximum=86400,
                    )
                    last_log_id = _bounded_int(
                        payload.get("last_log_id"),
                        default=last_log_id,
                        minimum=0,
                        maximum=2_000_000_000,
                    )
                if payload.get("type") == "ping":
                    await websocket.send_json({"type": "pong", "server_timestamp": utc_iso_now()})

        except asyncio.TimeoutError:
            pass
        except WebSocketDisconnect:
            break

        new_logs = store.list_api_debug_logs_since(
            after_id=last_log_id,
            machine_id=machine_id,
            device_id=device_id,
            limit=120,
        )
        if new_logs:
            last_log_id = max(last_log_id, int(new_logs[-1].get("id", last_log_id)))

        latest_sample = None
        if machine_id and device_id:
            samples = store.get_recent_samples(
                machine_id,
                device_id=device_id,
                seconds=lookback_seconds,
                limit=500,
            )
            if samples:
                latest_sample = samples[-1]

        status_payload = None
        try:
            if device_name:
                status_payload = get_status(device_name)
        except HTTPException:
            status_payload = None

        active_binding = _binding_with_device_name(get_active_stream_binding())

        await websocket.send_json(
            {
                "type": "snapshot",
                "device_name": device_name,
                "server_timestamp": utc_iso_now(),
                "latest_sample": latest_sample,
                "status": status_payload,
                "active_stream_binding": (
                    StreamBindingResponse(**active_binding).model_dump() if active_binding else None
                ),
                "new_logs": [ApiDebugLogEntry(**item).model_dump() for item in new_logs],
            }
        )


app.include_router(router, prefix="/api/v1")
