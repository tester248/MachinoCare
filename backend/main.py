from __future__ import annotations

import os
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse

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
    CalibrationJobStatus,
    CalibrationRequest,
    CalibrationResponse,
    CalibrationStartResponse,
    StreamIngestRequest,
)
from backend.storage import DataStore

DB_PATH = os.getenv("MACHINOCARE_DB", "data/machinocare.db")
BUFFER_SIZE = int(os.getenv("MACHINOCARE_BUFFER_SIZE", "12000"))

store = DataStore(db_path=DB_PATH, max_buffer_size=BUFFER_SIZE)

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


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse(url="/docs", status_code=307)


def utc_iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def round_list(values: list[float], ndigits: int = 6) -> list[float]:
    return [round(float(v), ndigits) for v in values]


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
    return {"status": "ok", "service": "machinocare-backend", "time": utc_iso_now()}


@router.post("/stream", status_code=202)
def ingest_stream(payload: StreamIngestRequest) -> dict[str, Any]:
    samples = [sample_to_record(sample) for sample in payload.expanded_samples()]
    if not samples:
        raise HTTPException(status_code=400, detail="No samples provided.")

    if len(samples) > 4000:
        raise HTTPException(status_code=413, detail="Payload too large. Reduce batch size.")

    store.add_samples(payload.machine_id, payload.device_id, samples)

    state = store.get_machine_state(payload.machine_id, payload.device_id)
    model_package = store.get_model_package(payload.machine_id, payload.device_id)

    last_sample = samples[-1]
    score: float | None = None
    threshold: float | None = None
    consecutive = int(state.get("consecutive_anomaly_windows", 0))
    is_anomaly = bool(state.get("is_anomaly", False))

    if model_package:
        window_size = int(model_package.get("effective_window_size", 25))
        recent = store.get_recent_samples(
            payload.machine_id,
            device_id=payload.device_id,
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
                    machine_id=payload.machine_id,
                    device_id=payload.device_id,
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
                    machine_id=payload.machine_id,
                    device_id=payload.device_id,
                    timestamp=last_sample["timestamp"],
                    acc_mag=last_sample["acc_mag"],
                    score=None,
                    threshold=threshold,
                    reason="fallback_acc_threshold",
                )

    state_threshold = threshold
    if state_threshold is None and model_package:
        state_threshold = float(model_package.get("decision_threshold"))

    active_job = _active_calibration_for_device(payload.machine_id, payload.device_id)

    new_state = {
        **state,
        "machine_id": payload.machine_id,
        "device_id": payload.device_id,
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
    store.set_machine_state(payload.machine_id, payload.device_id, new_state)

    return {
        "status": "queued",
        "machine_id": payload.machine_id,
        "device_id": payload.device_id,
        "received_samples": len(samples),
        "server_timestamp": utc_iso_now(),
        "is_anomaly": is_anomaly,
        "score": round(score, 6) if score is not None else None,
        "decision_threshold": round(state_threshold, 6) if state_threshold is not None else None,
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


app.include_router(router, prefix="/api/v1")
