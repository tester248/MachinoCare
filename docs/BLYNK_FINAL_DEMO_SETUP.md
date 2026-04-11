# Blynk Final Demo Setup - MachinoCare

This document defines the exact Blynk configuration required for the final demo, including calibration controls and real-time model training visibility.

## 1. Create Template

1. Open Blynk Console.
2. Create a new Template named MachinoCare.
3. Hardware: ESP32.
4. Connection type: WiFi.
5. Copy the generated `BLYNK_TEMPLATE_ID`, `BLYNK_TEMPLATE_NAME`, and Auth Token into firmware.

## 2. Datastreams to Create

Create these Virtual Pin datastreams in the template:

1. V0
- Name: Acc Magnitude
- Type: Double
- Min: 0
- Max: 60000

2. V1
- Name: Gyro Magnitude
- Type: Double
- Min: 0
- Max: 40000

3. V2
- Name: Gyro X
- Type: Integer
- Min: -40000
- Max: 40000

4. V3
- Name: Gyro Y
- Type: Integer
- Min: -40000
- Max: 40000

5. V4
- Name: Gyro Z
- Type: Integer
- Min: -40000
- Max: 40000

6. V5
- Name: SW420 Status
- Type: Integer
- Min: 0
- Max: 1

7. V6
- Name: Timestamp
- Type: String

8. V7
- Name: Machine Health LED
- Type: Integer
- Min: 0
- Max: 255

9. V8
- Name: Start Calibration Button
- Type: Integer
- Min: 0
- Max: 1

10. V9
- Name: Model Version
- Type: Integer
- Min: 0
- Max: 10000

11. V10
- Name: Calibration Stage Text
- Type: String

12. V11
- Name: Calibration Progress
- Type: Integer
- Min: 0
- Max: 100

13. V12
- Name: Training Active LED
- Type: Integer
- Min: 0
- Max: 255

14. V13
- Name: New Device Setup Mode
- Type: Integer
- Min: 0
- Max: 1

## 3. Web Dashboard Widgets

Add these widgets to Blynk Web Dashboard:

1. Gauge -> V0 (Acc Magnitude)
2. Gauge -> V1 (Gyro Magnitude)
3. Labeled Value -> V2 (Gyro X)
4. Labeled Value -> V3 (Gyro Y)
5. Labeled Value -> V4 (Gyro Z)
6. LED -> V5 (SW420 Status)
7. Labeled Value -> V6 (Timestamp)
8. LED -> V7 (Machine Health LED)
- ON color: red
- OFF color: green
9. Button (switch mode OFF by default) -> V8 (Start Calibration Button)
10. Labeled Value -> V9 (Model Version)
11. Labeled Value or Text Panel -> V10 (Calibration Stage Text)
12. Gauge or Progress widget -> V11 (Calibration Progress)
13. LED -> V12 (Training Active LED)
- ON color: blue
- OFF color: gray
14. Switch -> V13 (New Device Setup Mode)
- ON means calibration request is sent as `new_device_setup=true`

## 4. Events to Configure

Create these events in Blynk Template Events:

1. machine_alert
- Triggered for anomaly alerts and calibration lifecycle messages.
- Enable push notification and email if needed.

2. critical_failure
- Triggered when failsafe kill switch locks the system in production mode.
- Set highest priority notification.

## 5. Firmware Behavior Mapping

The firmware uses these Blynk behaviors:

1. Pressing V8 starts backend calibration job via `/api/v1/calibrate/start`.
2. V13 controls whether this is marked as a new device setup.
3. During backend training:
- V10 shows stage and message.
- V11 increments from 0 to 100.
- V12 turns ON while training is active.
4. On completion:
- ESP32 reads returned model package and applies weights.
- V9 updates to the new model version.
- V12 turns OFF.

## 6. Backend Requirements for This Flow

Ensure backend is reachable from ESP32 and deployed URL is set in firmware:

1. `BACKEND_BASE_URL` must point to Railway service domain.
2. Backend endpoints used:
- `POST /api/v1/stream`
- `POST /api/v1/calibrate/start`
- `GET /api/v1/calibrate/status/{job_id}`
- `GET /api/v1/model/{machine_id}/{device_id}`

## 7. Final Demo Runbook

1. Start backend on Railway and verify health endpoint.
2. Open Streamlit dashboard and Blynk dashboard side by side.
3. Set V13 to ON (new device mode).
4. Press V8 to start calibration.
5. Show real-time calibration progress:
- Streamlit shows job stage/progress in real time.
- Blynk shows stage text (V10), progress (V11), and active LED (V12).
6. When done, confirm:
- V9 increments model version.
- Streamlit status shows calibration completed for that device.
7. Demonstrate live stream and anomaly behavior by perturbing machine vibration.

## 8. Troubleshooting Checklist

1. V8 pressed but no training starts:
- Check ESP32 WiFi.
- Check `BACKEND_BASE_URL`.
- Check Railway logs.

2. Progress stuck at 0:
- Confirm backend endpoint `/api/v1/calibrate/start` responds.
- Confirm firmware timer for status polling is running.

3. Model version not updating:
- Confirm backend status returns `completed` with `result.model_package`.
- Confirm JSON size does not exceed available memory on ESP32.

4. Calibration fails with insufficient windows:
- Increase fallback window seconds.
- Lower sample rate parameter to match effective stream density.
