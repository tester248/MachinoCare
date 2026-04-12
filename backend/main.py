from __future__ import annotations

import asyncio
import json
import os
import random
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

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

DB_PATH = os.getenv("MACHINOCARE_DB", "data/machinocare.db")
DATABASE_URL = os.getenv("DATABASE_URL")
BUFFER_SIZE = int(os.getenv("MACHINOCARE_BUFFER_SIZE", "12000"))
DEBUG_SAMPLE_RATE = max(0.0, min(1.0, float(os.getenv("MACHINOCARE_DEBUG_SAMPLE_RATE", "0.10"))))
DEBUG_RETENTION_DAYS = int(os.getenv("MACHINOCARE_DEBUG_RETENTION_DAYS", "30"))
DEBUG_MAX_BODY_BYTES = int(os.getenv("MACHINOCARE_DEBUG_MAX_BODY_BYTES", "20000"))
LIVE_PUSH_INTERVAL_SECONDS = max(0.2, float(os.getenv("MACHINOCARE_LIVE_PUSH_INTERVAL_SECONDS", "0.75")))
UNASSIGNED_MACHINE_ID = os.getenv("MACHINOCARE_UNASSIGNED_MACHINE_ID", "unassigned_machine")
UNASSIGNED_DEVICE_ID = os.getenv("MACHINOCARE_UNASSIGNED_DEVICE_ID", "unassigned_device")

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

    if isinstance(payload, dict):
        machine_id = payload.get("machine_id")
        device_id = payload.get("device_id")

    machine_id = machine_id or query_params.get("machine_id")
    device_id = device_id or query_params.get("device_id")

    parts = [part for part in path.split("/") if part]
    if len(parts) >= 4 and parts[0] == "api" and parts[1] == "v1":
        if parts[2] in {"status", "stream", "anomaly-log", "devices", "model"}:
            machine_id = machine_id or parts[3]
        if len(parts) >= 5 and parts[2] in {"status", "model"}:
            device_id = device_id or parts[4]

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

    fallback = store.get_recent_samples(
        request.machine_id,
        device_id=request.device_id,
        seconds=request.fallback_seconds,
        limit=max(1000, request.sample_rate_hz * request.fallback_seconds),
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
    return CalibrationJobStatus(
        job_id=job["job_id"],
        status=job["status"],
        stage=job["stage"],
        progress=int(job["progress"]),
        machine_id=job["machine_id"],
        device_id=job["device_id"],
        trigger_source=job["trigger_source"],
        new_device_setup=bool(job["new_device_setup"]),
        started_at=job["started_at"],
        updated_at=job["updated_at"],
        message=job.get("message"),
        error=job.get("error"),
        result=job.get("result"),
    )


def perform_calibration(payload: CalibrationRequest) -> CalibrationResponse:
    samples, source = resolve_calibration_samples(payload)
    if len(samples) < 20:
        raise HTTPException(
            status_code=400,
            detail=(
                "Insufficient samples for calibration. Provide at least 20 samples, "
                "or increase fallback_seconds."
            ),
        )

    effective_window_size = max(8, payload.sample_rate_hz * payload.window_seconds)
    feature_matrix = build_feature_matrix(samples, window_size=effective_window_size)

    if feature_matrix.shape[0] < 8:
        raise HTTPException(
            status_code=400,
            detail=(
                "Not enough windows for model calibration. "
                "Increase sample count or reduce window size."
            ),
        )

    distilled = train_isolation_forest_distilled(
        feature_matrix=feature_matrix,
        contamination=payload.contamination,
    )
    baseline_stats = acc_threshold_stats(samples)

    existing = store.get_model_package(payload.machine_id, payload.device_id)
    new_version = int(existing["model_version"]) + 1 if existing else 1

    package = {
        "model_type": "isolation_forest_distilled_linear",
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
        "created_at": utc_iso_now(),
        "target_machine_id": payload.machine_id,
        "target_device_id": payload.device_id,
        "new_device_setup": payload.new_device_setup,
        "trigger_source": payload.trigger_source,
    }
    package["checksum"] = build_checksum(package)

    store.save_model_package(payload.machine_id, payload.device_id, package, baseline_stats)

    state = store.get_machine_state(payload.machine_id, payload.device_id)
    state.update(
        {
            "machine_id": payload.machine_id,
            "device_id": payload.device_id,
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
    store.set_machine_state(payload.machine_id, payload.device_id, state)

    return CalibrationResponse(
        status="success",
        machine_id=payload.machine_id,
        device_id=payload.device_id,
        calibration_source=source,
        sample_count=len(samples),
        window_count=int(distilled["window_count"]),
        statistics={
            "mean_acc": round(float(baseline_stats["mean_acc"]), 6),
            "std_acc": round(float(baseline_stats["std_acc"]), 6),
            "threshold_mean_3sigma": round(float(baseline_stats["threshold_mean_3sigma"]), 6),
            "quality_correlation": round(float(distilled["quality_correlation"]), 6),
        },
        model_package=package,
    )


def _run_calibration_job(job_id: str, payload_data: dict[str, Any]) -> None:
    payload = CalibrationRequest(**payload_data)

    try:
        _set_job(
            job_id,
            status="running",
            stage="collecting_data",
            progress=15,
            message="Collecting baseline window data",
        )
        state = store.get_machine_state(payload.machine_id, payload.device_id)
        state.update(
            {
                "machine_id": payload.machine_id,
                "device_id": payload.device_id,
                "calibration_in_progress": True,
                "calibration_progress": 15,
                "calibration_stage": "collecting_data",
                "calibration_message": "Collecting baseline window data",
                "active_calibration_job_id": job_id,
            }
        )
        store.set_machine_state(payload.machine_id, payload.device_id, state)

        time.sleep(0.30)

        _set_job(
            job_id,
            stage="extracting_features",
            progress=45,
            message="Building vibration feature windows",
        )
        state = store.get_machine_state(payload.machine_id, payload.device_id)
        state.update(
            {
                "calibration_progress": 45,
                "calibration_stage": "extracting_features",
                "calibration_message": "Building vibration feature windows",
            }
        )
        store.set_machine_state(payload.machine_id, payload.device_id, state)

        time.sleep(0.30)

        _set_job(
            job_id,
            stage="training_model",
            progress=75,
            message="Training Isolation Forest and distilling edge weights",
        )
        state = store.get_machine_state(payload.machine_id, payload.device_id)
        state.update(
            {
                "calibration_progress": 75,
                "calibration_stage": "training_model",
                "calibration_message": "Training Isolation Forest and distilling edge weights",
            }
        )
        store.set_machine_state(payload.machine_id, payload.device_id, state)

        time.sleep(0.30)

        result = perform_calibration(payload)

        _set_job(
            job_id,
            status="completed",
            stage="completed",
            progress=100,
            message="Calibration completed and model package generated",
            result=result.model_dump(),
        )
        state = store.get_machine_state(payload.machine_id, payload.device_id)
        state.update(
            {
                "calibration_in_progress": False,
                "calibration_progress": 100,
                "calibration_stage": "completed",
                "calibration_message": "Calibration completed and model package generated",
                "active_calibration_job_id": None,
            }
        )
        store.set_machine_state(payload.machine_id, payload.device_id, state)

    except HTTPException as exc:
        _set_job(
            job_id,
            status="failed",
            stage="failed",
            progress=100,
            error=str(exc.detail),
            message="Calibration failed",
        )
        state = store.get_machine_state(payload.machine_id, payload.device_id)
        state.update(
            {
                "calibration_in_progress": False,
                "calibration_progress": 100,
                "calibration_stage": "failed",
                "calibration_message": str(exc.detail),
                "active_calibration_job_id": None,
            }
        )
        store.set_machine_state(payload.machine_id, payload.device_id, state)

    except Exception as exc:  # noqa: BLE001
        _set_job(
            job_id,
            status="failed",
            stage="failed",
            progress=100,
            error=str(exc),
            message="Calibration failed due to unexpected error",
        )
        state = store.get_machine_state(payload.machine_id, payload.device_id)
        state.update(
            {
                "calibration_in_progress": False,
                "calibration_progress": 100,
                "calibration_stage": "failed",
                "calibration_message": str(exc),
                "active_calibration_job_id": None,
            }
        )
        store.set_machine_state(payload.machine_id, payload.device_id, state)


@router.get("/health")
def health() -> dict[str, str]:
    return {
        "status": "ok",
        "service": "machinocare-backend",
        "time": utc_iso_now(),
        "db_backend": store.backend,
    }


@router.post("/stream", status_code=202)
def ingest_stream(payload: StreamIngestRequest) -> dict[str, Any]:
    samples = [sample_to_record(sample) for sample in payload.expanded_samples()]
    if not samples:
        raise HTTPException(status_code=400, detail="No samples provided.")

    if len(samples) > 4000:
        raise HTTPException(status_code=413, detail="Payload too large. Reduce batch size.")

    reported_machine_id = payload.machine_id
    reported_device_id = payload.device_id
    machine_id, device_id, binding = resolve_stream_target(payload)

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
        "reported_machine_id": reported_machine_id,
        "reported_device_id": reported_device_id,
    }
    store.set_machine_state(machine_id, device_id, new_state)

    binding_view = StreamBindingResponse(**binding).model_dump() if binding else None

    return {
        "status": "queued",
        "machine_id": machine_id,
        "device_id": device_id,
        "reported_machine_id": reported_machine_id,
        "reported_device_id": reported_device_id,
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
    }


@router.post("/calibrate/start", response_model=CalibrationStartResponse)
def calibrate_start(payload: CalibrationRequest) -> CalibrationStartResponse:
    active = _active_calibration_for_device(payload.machine_id, payload.device_id)
    if active:
        return CalibrationStartResponse(
            status="already_running",
            job_id=active["job_id"],
            machine_id=payload.machine_id,
            device_id=payload.device_id,
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
        "machine_id": payload.machine_id,
        "device_id": payload.device_id,
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

    state = store.get_machine_state(payload.machine_id, payload.device_id)
    state.update(
        {
            "machine_id": payload.machine_id,
            "device_id": payload.device_id,
            "calibration_in_progress": True,
            "calibration_progress": 0,
            "calibration_stage": "queued",
            "calibration_message": "Calibration queued",
            "active_calibration_job_id": job_id,
        }
    )
    store.set_machine_state(payload.machine_id, payload.device_id, state)

    thread = threading.Thread(
        target=_run_calibration_job,
        args=(job_id, payload.model_dump()),
        daemon=True,
    )
    thread.start()

    return CalibrationStartResponse(
        status="queued",
        job_id=job_id,
        machine_id=payload.machine_id,
        device_id=payload.device_id,
        trigger_source=payload.trigger_source,
        new_device_setup=payload.new_device_setup,
    )


@router.post("/calibrate/start/profile/{machine_id}/{device_id}", response_model=CalibrationStartResponse)
def calibrate_start_from_profile(
    machine_id: str,
    device_id: str,
    new_device_setup: bool = Query(default=True),
    trigger_source: str = Query(default="dashboard_profile"),
) -> CalibrationStartResponse:
    profile = store.get_device_profile(machine_id, device_id)
    if not profile:
        raise HTTPException(
            status_code=404,
            detail=f"No device profile found for machine '{machine_id}', device '{device_id}'.",
        )

    payload = CalibrationRequest(
        machine_id=machine_id,
        device_id=device_id,
        sample_rate_hz=int(profile.get("sample_rate_hz") or 25),
        window_seconds=int(profile.get("window_seconds") or 1),
        fallback_seconds=int(profile.get("fallback_seconds") or 300),
        contamination=float(profile.get("contamination") or 0.05),
        min_consecutive_windows=int(profile.get("min_consecutive_windows") or 3),
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
    return perform_calibration(payload)


@router.get("/model/{machine_id}/{device_id}")
def get_model_for_device(machine_id: str, device_id: str) -> dict[str, Any]:
    package = store.get_model_package(machine_id, device_id)
    if not package:
        raise HTTPException(
            status_code=404,
            detail=f"No model package found for machine '{machine_id}', device '{device_id}'.",
        )
    return {
        "status": "success",
        "machine_id": machine_id,
        "device_id": device_id,
        "model_package": package,
    }


@router.get("/model/{machine_id}")
def get_model(machine_id: str) -> dict[str, Any]:
    package = store.get_model_package(machine_id)
    if not package:
        raise HTTPException(status_code=404, detail=f"No model package found for machine '{machine_id}'.")

    return {
        "status": "success",
        "machine_id": machine_id,
        "device_id": package.get("target_device_id"),
        "model_package": package,
    }


@router.get("/status/{machine_id}/{device_id}")
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
            "window_size": (package or {}).get("effective_window_size"),
            "decision_threshold": (package or {}).get("decision_threshold"),
            "hysteresis_low": (package or {}).get("hysteresis_low"),
            "quality_correlation": (package or {}).get("quality_correlation"),
            "target_device_id": (package or {}).get("target_device_id"),
        },
    }


@router.get("/status/{machine_id}")
def get_status(machine_id: str) -> dict[str, Any]:
    latest_device = store.latest_device_for_machine(machine_id)
    if not latest_device:
        raise HTTPException(status_code=404, detail=f"Machine '{machine_id}' has no data yet.")
    return get_status_for_device(machine_id, latest_device)


@router.get("/stream/{machine_id}/recent")
def recent_stream(
    machine_id: str,
    seconds: int = Query(default=120, ge=5, le=86400),
    limit: int = Query(default=500, ge=1, le=10000),
    device_id: str | None = Query(default=None),
) -> dict[str, Any]:
    samples = store.get_recent_samples(machine_id, device_id=device_id, seconds=seconds, limit=limit)
    return {
        "machine_id": machine_id,
        "device_id": device_id,
        "seconds": seconds,
        "count": len(samples),
        "samples": samples,
    }


@router.get("/anomaly-log/{machine_id}")
def anomaly_log(
    machine_id: str,
    hours: int = Query(default=24, ge=1, le=720),
    limit: int = Query(default=200, ge=1, le=1000),
    device_id: str | None = Query(default=None),
) -> dict[str, Any]:
    anomalies = store.get_anomalies(machine_id, device_id=device_id, hours=hours, limit=limit)
    return {
        "machine_id": machine_id,
        "device_id": device_id,
        "hours": hours,
        "count": len(anomalies),
        "anomalies": anomalies,
    }


@router.get("/machines")
def machines() -> dict[str, list[str]]:
    return {"machines": store.list_machine_ids()}


@router.get("/devices/{machine_id}")
def devices(machine_id: str) -> dict[str, Any]:
    return {
        "machine_id": machine_id,
        "devices": store.list_devices(machine_id),
    }


@router.get("/stream-binding", response_model=StreamBindingResponse)
def get_stream_binding() -> StreamBindingResponse:
    binding = store.get_stream_binding()
    if not binding:
        return StreamBindingResponse(binding_name="primary", is_active=False)
    return StreamBindingResponse(**binding)


@router.post("/stream-binding", response_model=StreamBindingResponse)
def upsert_stream_binding(payload: StreamBindingUpsertRequest) -> StreamBindingResponse:
    profile = store.get_device_profile(payload.machine_id, payload.device_id)
    if not profile:
        raise HTTPException(
            status_code=404,
            detail=(
                "Cannot bind stream: profile not found for "
                f"machine '{payload.machine_id}', device '{payload.device_id}'."
            ),
        )

    binding = store.set_stream_binding(
        machine_id=payload.machine_id,
        device_id=payload.device_id,
        source=payload.source,
    )
    return StreamBindingResponse(**binding)


@router.delete("/stream-binding", response_model=StreamBindingResponse)
def clear_stream_binding(source: str = Query(default="dashboard_manual")) -> StreamBindingResponse:
    binding = store.clear_stream_binding(source=source)
    return StreamBindingResponse(**binding)


@router.post("/device-profiles", response_model=DeviceProfileResponse)
def upsert_device_profile(payload: DeviceProfileUpsertRequest) -> DeviceProfileResponse:
    profile = store.upsert_device_profile(payload.model_dump())
    if not profile:
        raise HTTPException(status_code=500, detail="Failed to save device profile.")
    return DeviceProfileResponse(**profile)


@router.get("/device-profiles/{machine_id}/{device_id}", response_model=DeviceProfileResponse)
def get_device_profile(machine_id: str, device_id: str) -> DeviceProfileResponse:
    profile = store.get_device_profile(machine_id, device_id)
    if not profile:
        raise HTTPException(
            status_code=404,
            detail=f"Device profile not found for machine '{machine_id}', device '{device_id}'.",
        )
    return DeviceProfileResponse(**profile)


@router.get("/device-profiles")
def list_device_profiles(
    machine_id: str | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=1000),
) -> dict[str, Any]:
    profiles = store.list_device_profiles(machine_id=machine_id, limit=limit)
    return {
        "machine_id": machine_id,
        "count": len(profiles),
        "profiles": [DeviceProfileResponse(**profile).model_dump() for profile in profiles],
    }


@router.delete("/device-profiles/{machine_id}/{device_id}")
def delete_device_profile(machine_id: str, device_id: str) -> dict[str, Any]:
    deleted = store.delete_device_profile(machine_id, device_id)
    if not deleted:
        raise HTTPException(
            status_code=404,
            detail=f"Device profile not found for machine '{machine_id}', device '{device_id}'.",
        )

    return {
        "status": "deleted",
        "machine_id": machine_id,
        "device_id": device_id,
    }


@router.get("/debug/logs")
def debug_logs(
    machine_id: str | None = Query(default=None),
    device_id: str | None = Query(default=None),
    endpoint: str | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=1000),
) -> dict[str, Any]:
    logs = store.list_api_debug_logs(
        machine_id=machine_id,
        device_id=device_id,
        endpoint=endpoint,
        limit=limit,
    )
    return {
        "machine_id": machine_id,
        "device_id": device_id,
        "endpoint": endpoint,
        "count": len(logs),
        "logs": [ApiDebugLogEntry(**item).model_dump() for item in logs],
    }


@router.websocket("/ws/live")
async def ws_live(websocket: WebSocket) -> None:
    await websocket.accept()

    initial_binding = get_active_stream_binding()
    default_machine_id = str(initial_binding["machine_id"]) if initial_binding else UNASSIGNED_MACHINE_ID
    default_device_id = str(initial_binding["device_id"]) if initial_binding else None

    machine_id = websocket.query_params.get("machine_id") or default_machine_id
    device_id = websocket.query_params.get("device_id") or default_device_id
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
    last_sample_timestamp = ""

    await websocket.send_json(
        {
            "type": "connected",
            "machine_id": machine_id,
            "device_id": device_id,
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
                    machine_id = payload.get("machine_id") or machine_id
                    device_id = payload.get("device_id") or device_id
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
                    last_sample_timestamp = ""

                if payload.get("type") == "ping":
                    await websocket.send_json({"type": "pong", "server_timestamp": utc_iso_now()})

        except asyncio.TimeoutError:
            pass
        except WebSocketDisconnect:
            break

        if machine_id and not device_id:
            device_id = store.latest_device_for_machine(machine_id)

        new_logs = store.list_api_debug_logs_since(
            after_id=last_log_id,
            machine_id=machine_id,
            device_id=device_id,
            limit=120,
        )
        if new_logs:
            last_log_id = max(last_log_id, int(new_logs[-1].get("id", last_log_id)))

        latest_sample = None
        if machine_id:
            samples = store.get_recent_samples(
                machine_id,
                device_id=device_id,
                seconds=lookback_seconds,
                limit=500,
            )
            if samples:
                candidate = samples[-1]
                candidate_ts = str(candidate.get("timestamp") or "")
                if candidate_ts and candidate_ts != last_sample_timestamp:
                    latest_sample = candidate
                    last_sample_timestamp = candidate_ts

        status_payload = None
        try:
            if machine_id and device_id:
                status_payload = get_status_for_device(machine_id, device_id)
            elif machine_id:
                status_payload = get_status(machine_id)
                device_id = status_payload.get("device_id") or device_id
        except HTTPException:
            status_payload = None

        active_binding = get_active_stream_binding()

        await websocket.send_json(
            {
                "type": "snapshot",
                "machine_id": machine_id,
                "device_id": device_id,
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
