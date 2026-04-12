# MachinoCare

MachinoCare is an end-to-end predictive maintenance system for vibration-based machine monitoring.

It combines:
- ESP32 edge firmware (MPU6050 + SW420 + relays + buttons)
- FastAPI backend for streaming, calibration, and model management
- Streamlit dashboard (fallback) + FastAPI realtime debug dashboard

## What This Repository Contains

- `backend/` FastAPI service and ML pipeline
- `dashboard/` Streamlit live control room
- `firmware/MachinoCare_ESP32/` ESP32 firmware
- `docs/` supporting documentation (including Blynk setup)
- `run_all.py` helper to start backend + dashboard together
- `Procfile` deploy entrypoint (runs `run_all.py`)

## System Overview

1. ESP32 samples vibration data and publishes stream packets to the backend.
2. Backend stores recent samples (ring buffer + SQLite), computes status, and serves APIs.
3. Calibration jobs train an Isolation Forest-based model and produce a distilled edge package.
4. ESP32 pulls model package updates and performs local inference with hysteresis.
5. Dashboard visualizes live state, calibration progress, and recent/historical telemetry.

## Key Features

### Backend
- Real-time ingest endpoint for single or batched stream payloads
- Synchronous calibration (`/api/v1/calibrate`) and async calibration jobs (`/api/v1/calibrate/start`)
- Device-specific status and model package endpoints
- Recent stream retrieval, anomaly logs, machine/device discovery
- Realtime WebSocket debug feed (`/api/v1/ws/live`)
- SQLite or PostgreSQL persistence (`DATABASE_URL` auto-detected)

### Firmware (ESP32)
- 2-relay control (motor + fan)
- 3 physical buttons (motor toggle, fan toggle, calibration trigger)
- SW420 interrupt-based emergency behavior
- Blynk and ThingSpeak integration
- Backend stream telemetry counters (success/fail/last HTTP code/result)
- Model package persistence in NVS (`Preferences`)

### Dashboard
- Fragment-based live auto-refresh (smooth partial rerender)
- Live plots for `acc_mag`, `gyro_mag`, `score`, `gx`, `gy`, `gz`, and `sw420`
- Calibration control and progress monitoring
- Full backend payload inspectors for troubleshooting
- ThingSpeak multi-field historical chart
- New backend-served realtime debug dashboard (`/debug-dashboard`) with no Streamlit rerender greying

## Quick Start (Local)

### 1. Create and activate a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Run backend and dashboard

Option A: run separately

```bash
uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000
```

```bash
streamlit run dashboard/app.py --server.port 8501
```

Option B: run together

```bash
python run_all.py
```

### 4. Open interfaces

- Backend docs: `http://localhost:8000/docs`
- Dashboard: `http://localhost:8501`
- Realtime debug dashboard: `http://localhost:8000/debug-dashboard`
- Health: `http://localhost:8000/api/v1/health`

## API Surface (Current)

### Health and root
- `GET /` redirects to `/docs`
- `GET /api/v1/health`

### Streaming and status
- `POST /api/v1/stream`
- `GET /api/v1/stream/{machine_id}/recent`
- `GET /api/v1/status/{machine_id}`
- `GET /api/v1/status/{machine_id}/{device_id}`
- `GET /api/v1/anomaly-log/{machine_id}`

`POST /api/v1/stream` now routes incoming samples to the active dashboard-selected stream binding. ESP payload `machine_id` and `device_id` are accepted for compatibility and logged, but ignored for routing.

### Calibration and model
- `POST /api/v1/calibrate`
- `POST /api/v1/calibrate/start`
- `POST /api/v1/calibrate/start/profile/{machine_id}/{device_id}`
- `GET /api/v1/calibrate/status/{job_id}`
- `GET /api/v1/model/{machine_id}`
- `GET /api/v1/model/{machine_id}/{device_id}`

### Discovery
- `GET /api/v1/machines`
- `GET /api/v1/devices/{machine_id}`

### Debug and profiles
- `POST /api/v1/device-profiles`
- `GET /api/v1/device-profiles`
- `GET /api/v1/device-profiles/{machine_id}/{device_id}`
- `DELETE /api/v1/device-profiles/{machine_id}/{device_id}`
- `GET /api/v1/stream-binding`
- `POST /api/v1/stream-binding`
- `DELETE /api/v1/stream-binding`
- `GET /api/v1/debug/logs`
- `WS /api/v1/ws/live`

These endpoints are additive and do not require ESP32 firmware changes.

## Example Requests

### Stream ingest

```bash
curl -X POST http://localhost:8000/api/v1/stream \
    -H "Content-Type: application/json" \
    -d '{
        "machine_id":"Fan_1",
        "device_id":"esp32_fan_1",
        "sample":{
            "timestamp":"2026-04-12T12:00:00Z",
            "accMag":16000,
            "gyroMag":1000,
            "gx":10,
            "gy":12,
            "gz":8,
            "sw420":0
        }
    }'
```

### Start async calibration

```bash
curl -X POST http://localhost:8000/api/v1/calibrate/start \
    -H "Content-Type: application/json" \
    -d '{
        "machine_id":"Fan_1",
        "device_id":"esp32_fan_1",
        "sample_rate_hz":10,
        "window_seconds":1,
        "fallback_seconds":300,
        "contamination":0.05,
        "min_consecutive_windows":3,
        "new_device_setup":true,
        "trigger_source":"dashboard_ui"
    }'
```

### Associate incoming stream to a profile

```bash
curl -X POST http://localhost:8000/api/v1/stream-binding \
    -H "Content-Type: application/json" \
    -d '{
        "machine_id":"Fan_1",
        "device_id":"esp32_fan_1",
        "source":"dashboard_manual"
    }'
```

## Firmware Setup

Firmware file:
- `firmware/MachinoCare_ESP32/MachinoCare_ESP32.ino`

Before flashing, set these placeholders in the firmware:
- `BLYNK_AUTH_TOKEN`
- `ssid`
- `password`
- `TS_WRITE_KEY`
- `BACKEND_BASE_URL`

Current hardware pin mapping in firmware:
- `SW420_PIN = 34`
- `RELAY_MOTOR_PIN = 25`
- `RELAY_FAN_PIN = 26`
- `BTN_MOTOR_PIN = 18`
- `BTN_FAN_PIN = 19`
- `BTN_CALIB_PIN = 23`

Blynk virtual pin usage (current):
- `V0..V7` live sensor and status signals
- `V8` start calibration
- `V9` model version
- `V10..V12` calibration stage/progress/active
- `V13` new-device setup mode
- `V14`, `V15` relay controls/state
- `V16..V19` stream telemetry (success/fail/http/result)

Detailed Blynk setup:
- `docs/BLYNK_FINAL_DEMO_SETUP.md`

## Deployment Notes

This repo includes:
- `Procfile` (`web: python run_all.py`)
- `railway.toml` (automatic Railway build/start/healthcheck config)

For Railway (or similar platforms):
1. Deploy from GitHub repository.
2. Railway reads `railway.toml` on push and starts with `python run_all.py`.
3. `run_all.py` starts FastAPI on Railway's `PORT` and Streamlit on `DASHBOARD_PORT` (default `8501`) in parallel.
4. If using Railway proxying for dashboard, route proxy traffic to `DASHBOARD_PORT`.
5. Add a Railway PostgreSQL service and set `DATABASE_URL` for managed DB persistence (auto-used by backend).
6. Set persistent SQLite path only if not using PostgreSQL:
     - `MACHINOCARE_DB=/data/machinocare.db`
7. Verify after deploy:
     - `/api/v1/health`
     - `/docs`
    - `/debug-dashboard`

Railway CLI bootstrap (automated):

```bash
railway login
RAILWAY_PROJECT_ID=<project-id> RAILWAY_BACKEND_SERVICE=<backend-service-name> ./scripts/railway_setup_postgres.sh
railway up
```

The script creates or reuses a PostgreSQL service, wires `DATABASE_URL` into your backend service, and sets debug dashboard runtime variables.

If neither PostgreSQL nor persistent volume is attached, SQLite data resets on redeploy/restart.

## Development Notes

- Python dependencies are listed in `requirements.txt`.
- Backend logic lives mainly in:
    - `backend/main.py`
    - `backend/storage.py`
    - `backend/ml_engine.py`
- Dashboard entrypoint:
    - `dashboard/app.py`

## Safety Note

Start with firmware `debugMode = true` during bench testing. Switch to `debugMode = false` only when you are ready to enable hard shutdown behavior for emergency events.