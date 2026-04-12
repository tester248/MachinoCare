from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class StreamSample(BaseModel):
    """Single sensor sample sent by ESP32."""

    model_config = ConfigDict(populate_by_name=True)

    timestamp: datetime = Field(default_factory=utc_now)
    acc_mag: float = Field(alias="accMag", ge=0)
    gyro_mag: float = Field(default=0.0, alias="gyroMag", ge=0)
    gx: float = 0.0
    gy: float = 0.0
    gz: float = 0.0
    sw420: Optional[int] = Field(default=None, ge=0, le=1)
    sequence: Optional[int] = None

    @field_validator("timestamp", mode="before")
    @classmethod
    def ensure_timestamp(cls, value: Optional[datetime]) -> datetime:
        if value is None:
            return utc_now()
        return value

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)


class StreamIngestRequest(BaseModel):
    """Supports both single-sample and batched ingest."""

    model_config = ConfigDict(populate_by_name=True)

    machine_id: str = Field(min_length=1, max_length=64)
    device_id: str = Field(default="esp32_unknown", min_length=1, max_length=64)
    sample: Optional[StreamSample] = None
    samples: Optional[List[StreamSample]] = None

    @model_validator(mode="after")
    def validate_shape(self) -> "StreamIngestRequest":
        if self.sample is None and not self.samples:
            raise ValueError("Provide either 'sample' or 'samples'.")
        return self

    def expanded_samples(self) -> List[StreamSample]:
        if self.samples:
            return self.samples
        if self.sample:
            return [self.sample]
        return []


class CalibrationRequest(BaseModel):
    """Calibration request from ESP32 or dashboard."""

    model_config = ConfigDict(populate_by_name=True)

    machine_id: str = Field(min_length=1, max_length=64)
    device_id: str = Field(default="esp32_unknown", min_length=1, max_length=64)
    baseline_samples: Optional[List[StreamSample]] = None
    magnitudes: Optional[List[float]] = None
    sample_rate_hz: int = Field(default=25, ge=1, le=500)
    window_seconds: int = Field(default=1, ge=1, le=10)
    fallback_seconds: int = Field(default=300, ge=10, le=86400)
    contamination: float = Field(default=0.05, ge=0.01, le=0.40)
    min_consecutive_windows: int = Field(default=3, ge=1, le=10)
    new_device_setup: bool = False
    trigger_source: str = Field(default="manual", min_length=1, max_length=64)

    @field_validator("magnitudes")
    @classmethod
    def validate_magnitudes(cls, values: Optional[List[float]]) -> Optional[List[float]]:
        if values is None:
            return values
        if not values:
            raise ValueError("'magnitudes' cannot be empty when provided.")
        if any(v < 0 for v in values):
            raise ValueError("All magnitudes must be non-negative.")
        return values


class CalibrationResponse(BaseModel):
    status: str
    machine_id: str
    device_id: str
    calibration_source: str
    sample_count: int
    window_count: int
    statistics: dict
    model_package: dict


class CalibrationStartResponse(BaseModel):
    status: str
    job_id: str
    machine_id: str
    device_id: str
    trigger_source: str
    new_device_setup: bool


class CalibrationJobStatus(BaseModel):
    job_id: str
    status: str
    stage: str
    progress: int
    machine_id: str
    device_id: str
    trigger_source: str
    new_device_setup: bool
    started_at: str
    updated_at: str
    message: str | None = None
    error: str | None = None
    result: dict | None = None


class DeviceProfileUpsertRequest(BaseModel):
    machine_id: str = Field(min_length=1, max_length=64)
    device_id: str = Field(min_length=1, max_length=64)
    display_name: str | None = Field(default=None, max_length=128)
    sample_rate_hz: int | None = Field(default=None, ge=1, le=500)
    window_seconds: int | None = Field(default=None, ge=1, le=10)
    fallback_seconds: int | None = Field(default=None, ge=10, le=86400)
    contamination: float | None = Field(default=None, ge=0.01, le=0.40)
    min_consecutive_windows: int | None = Field(default=None, ge=1, le=10)
    notes: str | None = Field(default=None, max_length=1000)


class DeviceProfileResponse(BaseModel):
    machine_id: str
    device_id: str
    display_name: str | None = None
    sample_rate_hz: int | None = None
    window_seconds: int | None = None
    fallback_seconds: int | None = None
    contamination: float | None = None
    min_consecutive_windows: int | None = None
    notes: str | None = None
    created_at: str
    updated_at: str


class ApiDebugLogEntry(BaseModel):
    id: int
    created_at: str
    endpoint: str
    method: str
    machine_id: str | None = None
    device_id: str | None = None
    status_code: int | None = None
    latency_ms: int | None = None
    request_size: int | None = None
    response_size: int | None = None
    correlation_id: str | None = None
    is_error: bool = False
    payload_sampled: bool = False
    request_payload: Any | None = None
    response_payload: Any | None = None
    error_text: str | None = None
