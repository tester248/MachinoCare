# Project Context: MachinoCare – AI-Based Predictive Maintenance

## 1. Project Overview
**MachinoCare** is an Industrial IoT (IIoT) predictive maintenance system. It detects early machine faults (like imbalance or looseness) using edge vibration analysis. 
* **Edge Hardware:** ESP32, MPU6050 (Accelerometer), SW-420 (Vibration/Failsafe), 5V Relay, 12V PC Fan (Mock Machine).
* **Current State:** The ESP32 C++ firmware is complete. It uses non-blocking timers to stream data to Blynk and ThingSpeak, and includes a hardwired interrupt kill-switch via the SW-420.
* **Current Task:** Build the Python Machine Learning Backend and a real-time Streamlit visualization dashboard.

## 2. Architecture & Data Flow
Because ThingSpeak has a 15-second rate limit, the real-time visualization and AI calibration will be handled by a custom Python backend (Flask or FastAPI) running alongside a Streamlit dashboard.

The ESP32 will interact with this backend in two ways:
1. **Real-time Streaming (For Streamlit):** The ESP32 will send high-frequency vibration data to the Python server to drive the live dashboard.
2. **AI Calibration (For Edge Updates):** When a user triggers "Calibration Mode", the ESP32 sends a batch of baseline data. The Python backend calculates the mathematical anomaly threshold and sends it back as a JSON response.

## 3. The Backend Requirements (Python)

### A. The REST API (Flask or FastAPI)
The server needs to expose the following endpoints to communicate with the ESP32:

**Endpoint 1: `/stream` (POST)**
* **Purpose:** Receives high-frequency data from the ESP32 to feed the Streamlit dashboard.
* **Incoming Payload:** `{"machine_id": "Fan_1", "accMag": 16500, "timestamp": "2026-04-11 14:00:00"}`
* **Action:** Store this data temporarily (e.g., in a local SQLite DB, a Pandas DataFrame in memory, or via a WebSocket) so Streamlit can read it instantly.

**Endpoint 2: `/calibrate` (POST)**
* **Purpose:** Calculates the "Normal" baseline threshold using statistical math or an ML model. 
* **Incoming Payload:** `{"machine_id": "Fan_1", "current_magnitude": 15000}` *(Note: The ESP32 might send an array of recent magnitudes, or the backend can pull the last N seconds of data from the `/stream` database).*
* **Action:** 1. Retrieve the most recent batch of baseline vibration data.
    2. Calculate the Mean and Standard Deviation (or use an Isolation Forest / SVM if applicable).
    3. Calculate the threshold: `Threshold = Mean + (3 * Standard Deviation)`.
* **Required JSON Response:** ```json
    {
      "status": "success",
      "new_threshold": 18500.50
    }
    ```

### B. The Streamlit Dashboard
The Streamlit app needs to read the incoming real-time data and visualize the predictive maintenance pipeline. It should include:

1. **Live Vibration Graph:** A real-time line chart plotting the `accMag` (Acceleration Magnitude) over time. This needs to update fast (every 1 second) to show the machine's mechanical waveform.
2. **Dynamic Threshold Line:** A horizontal red line on the graph showing the current AI-calculated `vibrationThreshold`. 
3. **Machine Status Indicator:** A prominent UI element showing the current state:
    * **HEALTHY (Green):** Current `accMag` is below the threshold.
    * **ANOMALY DETECTED (Red):** Current `accMag` has breached the threshold.
4. **Historical Data Integration (Optional but preferred):** A secondary view that pulls the long-term 15-second interval data from the ThingSpeak API (Channel ID: 3336916) to show historical trends.

## 4. Notes for the Coding Agent
* Keep the API lightweight to ensure minimal latency for the ESP32.
* Use `threading` or `multiprocessing` if running both the Flask/FastAPI server and Streamlit app from the same `main.py` execution script, or provide clear instructions on how to run them simultaneously.
* Focus on the visual "wow factor" for the Streamlit dashboard, as this will be used for a live hackathon/project demonstration to visualize the AI anomaly detection.

## 5. Current Implementation (Started)

The repository now includes a working first-cut implementation:

- `backend/main.py`: FastAPI app with live ingest, calibration, status, model, anomaly log, and recent stream endpoints.
- `backend/storage.py`: Hybrid data layer (in-memory ring buffer + SQLite persistence).
- `backend/ml_engine.py`: Feature extraction, Isolation Forest training, and distilled linear edge model package generation.
- `dashboard/app.py`: Streamlit dashboard with live waveform, dynamic threshold line, health/anomaly indicator, and optional ThingSpeak trend.
- `run_all.py`: Starts backend and dashboard together.

## 6. AI Design Used in This Implementation

### Backend model (specific ML model)
- Isolation Forest trained on 1-second feature windows.

### Edge model (ESP32-friendly)
- Distilled linear scorer generated from backend outputs.
- Backend sends compact parameters instead of heavy model artifacts:
    - `feature_means[8]`
    - `feature_stds[8]`
    - `weights[8]`
    - `bias`
    - `decision_threshold`
    - `hysteresis_high`, `hysteresis_low`
    - `min_consecutive_windows`
    - `model_version`, `checksum`

### Feature vector (per window)
1. `mean_acc_mag`
2. `std_acc_mag`
3. `max_acc_mag`
4. `mean_gyro_mag`
5. `std_gx`
6. `std_gy`
7. `std_gz`
8. `axis_imbalance_ratio`

## 7. Setup and Run

### Install
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Run backend + dashboard in separate terminals
```bash
uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000
```

```bash
streamlit run dashboard/app.py --server.port 8501
```

### Or run both together
```bash
python run_all.py
```

## 8. API Contract (Implemented)

### `POST /api/v1/stream`
Accepts either a single sample (`sample`) or batch (`samples`):

```json
{
    "machine_id": "Fan_1",
    "device_id": "esp32_fan_1",
    "sample": {
        "timestamp": "2026-04-11T14:00:00Z",
        "accMag": 16500,
        "gyroMag": 1200,
        "gx": 20,
        "gy": -14,
        "gz": 8,
        "sw420": 0,
        "sequence": 101
    }
}
```

### `POST /api/v1/calibrate`
Supports:
- payload baseline (`baseline_samples`)
- magnitude-only payload (`magnitudes`)
- fallback to recent streamed data (`fallback_seconds`)

Example fallback calibration request:

```json
{
    "machine_id": "Fan_1",
    "device_id": "esp32_fan_1",
    "sample_rate_hz": 25,
    "window_seconds": 1,
    "fallback_seconds": 300,
    "contamination": 0.05,
    "min_consecutive_windows": 3
}
```

### `POST /api/v1/calibrate/start`
Starts an asynchronous training job for a specific machine and device.

Request example:

```json
{
    "machine_id": "Fan_1",
    "device_id": "esp32_fan_1",
    "sample_rate_hz": 10,
    "window_seconds": 1,
    "fallback_seconds": 300,
    "contamination": 0.05,
    "min_consecutive_windows": 3,
    "new_device_setup": true,
    "trigger_source": "blynk_button"
}
```

### `GET /api/v1/calibrate/status/{job_id}`
Returns real-time job progress, stage, and final model package result when completed.

### `GET /api/v1/status/{machine_id}`
Returns current machine status, latest score, threshold, and calibration metadata.

### `GET /api/v1/status/{machine_id}/{device_id}`
Returns status for the exact target device.

### `GET /api/v1/model/{machine_id}`
Returns latest distilled model package for ESP32.

### `GET /api/v1/model/{machine_id}/{device_id}`
Returns model package for a specific device.

### `GET /api/v1/stream/{machine_id}/recent`
Returns recent stream points for live charting.

### `GET /api/v1/anomaly-log/{machine_id}`
Returns anomaly events recorded by the backend.

### `GET /api/v1/machines`
Returns known machine IDs.

### `GET /api/v1/devices/{machine_id}`
Returns known device IDs under that machine.

## 9. ESP32 Integration Notes

Firmware now supports button-triggered asynchronous calibration:
1. Blynk button `V8` calls `/api/v1/calibrate/start` with `new_device_setup` mode.
2. ESP32 polls `/api/v1/calibrate/status/{job_id}` in real time.
3. On completion, ESP32 applies returned model weights for that exact device.

Recommended ESP32-side persistence for received model package:
- store parameter arrays and thresholds in `Preferences` (NVS)
- validate `checksum` before activation
- keep previous package as rollback fallback

## 10. Quick Smoke Test

```bash
curl -X POST http://localhost:8000/api/v1/stream \
    -H "Content-Type: application/json" \
    -d '{"machine_id":"Fan_1","device_id":"esp32_fan_1","sample":{"accMag":16000,"gyroMag":1000,"gx":10,"gy":12,"gz":8}}'
```

```bash
curl -X POST http://localhost:8000/api/v1/calibrate \
    -H "Content-Type: application/json" \
    -d '{"machine_id":"Fan_1","device_id":"esp32_fan_1","fallback_seconds":300,"sample_rate_hz":25}'
```

## 11. ESP32 Firmware Added to Repo

Firmware path:
- `firmware/MachinoCare_ESP32/MachinoCare_ESP32.ino`

What this firmware includes:
1. Existing failsafe behavior with SW-420 interrupt and relay cut logic.
2. Existing Blynk and ThingSpeak integration.
3. Backend stream push every 1 second to `/api/v1/stream`.
4. Device-specific model pull from `/api/v1/model/{machine_id}/{device_id}`.
5. Async calibration start from `/api/v1/calibrate/start`.
6. Real-time calibration polling from `/api/v1/calibrate/status/{job_id}`.
6. Lightweight local inference using distilled model parameters from backend.
7. NVS persistence of model package (version/checksum/weights/stats).

Required Arduino libraries:
1. MPU6050
2. Blynk
3. ThingSpeak
4. ArduinoJson
5. Preferences (built-in on ESP32 core)

Before flashing:
1. Set secrets/placeholders in firmware:
    - `BLYNK_AUTH_TOKEN`
    - `ssid`, `password`
    - `TS_WRITE_KEY`
    - `BACKEND_BASE_URL` (Railway public URL)
2. Keep `debugMode = true` for safe bench testing first.
3. Set `debugMode = false` only when you are ready for hard relay shutdown behavior.

Blynk pin notes:
1. `V8` triggers calibration training job.
2. `V9` shows model version loaded on device.
3. `V10` shows training stage text.
4. `V11` shows training progress.
5. `V12` shows training-active LED status.
6. `V13` toggles new-device setup mode.

Detailed Blynk setup guide:
- `docs/BLYNK_FINAL_DEMO_SETUP.md`

## 12. Deploy FastAPI Backend to Railway

Deployment files added:
1. `Procfile`
2. `railway.json`

### Option A: Deploy from GitHub in Railway UI
1. Push this repository to GitHub.
2. In Railway, create New Project -> Deploy from GitHub Repo.
3. Select this repo.
4. Railway will install dependencies from `requirements.txt` and start using `Procfile`/`railway.json`.
5. After deploy, open the generated domain and verify:
    - `https://<your-domain>/api/v1/health`

### Option B: Deploy via Railway CLI
```bash
railway login
railway init
railway up
```

### Recommended Railway environment variables
1. `MACHINOCARE_DB=/data/machinocare.db`
2. `MACHINOCARE_BUFFER_SIZE=12000`

If you do not attach a persistent volume in Railway, SQLite data will reset on restart/redeploy.

## 13. End-to-End Bring-up Order

1. Deploy backend to Railway and confirm `/api/v1/health` works.
2. Update `BACKEND_BASE_URL` in firmware with Railway URL.
3. Flash firmware in debug mode.
4. Verify backend receives stream samples.
5. Trigger calibration from Blynk (`V8`) and confirm model version increments (`V9`).
6. Switch to production mode (`debugMode = false`) after validation.