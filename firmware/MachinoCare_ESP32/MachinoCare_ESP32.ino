// ============================================================
// MachinoCare - FINAL COMBINED (NO SECRETS)
// AI + Failsafe + Cloud + Backend + ThingSpeak + Blynk Relay/LED/Buzzer Control
// (No physical buttons)
// LED normal ON, anomaly => LED OFF + buzzer pulse for 5s
// ============================================================

// -------------------- BLYNK CREDENTIALS --------------------
#define BLYNK_TEMPLATE_ID   "YOUR_BLYNK_TEMPLATE_ID"
#define BLYNK_TEMPLATE_NAME "YOUR_BLYNK_TEMPLATE_NAME"
#define BLYNK_AUTH_TOKEN    "YOUR_BLYNK_AUTH_TOKEN"

#include <Wire.h>
#include <MPU6050.h>
#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <HTTPClient.h>
#include <ArduinoJson.h>
#include <Preferences.h>  
#include <BlynkSimpleEsp32.h>
#include <ThingSpeak.h>
#include <freertos/FreeRTOS.h>
#include <freertos/semphr.h>
#include "esp_system.h"
#include "time.h"

// WiFi and cloud credentials
const char* ssid     = "YOUR_WIFI_SSID";
const char* password = "YOUR_WIFI_PASSWORD";

unsigned long TS_CHANNEL_ID = 0; // e.g. 1234567
const char* TS_WRITE_KEY = "YOUR_THINGSPEAK_WRITE_KEY";
WiFiClient tsClient;

// Backend settings
const char* BACKEND_BASE_URL = "https://YOUR_BACKEND_URL";
bool backendEnabled = true;
String activeBindingMachineId = "";
String activeBindingDeviceId = "";

// NTP settings
const char* ntpServer = "pool.ntp.org";
const long gmtOffset_sec = 19800;
const int daylightOffset_sec = 0;


// Hardware pins
const int SW420_PIN = 34;
const int RELAY_MOTOR_PIN = 25;
const int RELAY_FAN_PIN = 26;
const int LED_PIN = 27;
const int BUZZER_PIN = 33;

// Relay polarity (active LOW module)
const int RELAY_ON = LOW;
const int RELAY_OFF = HIGH;

const int BUZZER_ON = HIGH;
const int BUZZER_OFF = LOW;
const int LED_ON = HIGH;
const int LED_OFF = LOW;

// true = don't hard-cut outputs on SW420 trigger, false = production kill
volatile bool debugMode = true;

MPU6050 mpu;
BlynkTimer timer;
Preferences prefs;
WiFiClientSecure apiClient;

float accMag = 0;
float gyroMag = 0;
int gx = 0, gy = 0, gz = 0;

// Fallback threshold
float aiThreshold = 25000.0;
const float AI_THRESHOLD_SCALE = 1.0;
const int AI_MIN_CONSECUTIVE = 3;

volatile bool emergencyTriggered = false;
bool isMachineFailing = false;
bool sw420FaultLatched = false;
bool sw420DebugCooldownActive = false;
bool sw420FaultAnnounced = false;

volatile bool sw420InterruptPending = false;
unsigned long sw420LastConfirmedTriggerMs = 0;
unsigned long sw420DebugCooldownUntilMs = 0;

const unsigned long SW420_DEBOUNCE_MS = 150;
const unsigned long SW420_DEBUG_COOLDOWN_MS = 4000;
const int SW420_FILTER_WINDOW = 7;
const int SW420_FILTER_ACTIVE_MIN = 5;
const bool SW420_USE_INTERRUPT = false;

int sw420IdleLevel = HIGH;
bool sw420PolarityCalibrated = false;
uint8_t sw420RecentActive[SW420_FILTER_WINDOW] = {0};
int sw420RecentPos = 0;
int sw420RecentCount = 0;

// NEW SW420 frame-threshold logic state
unsigned long sw420HighAccumulatedMs = 0;
unsigned long sw420FrameStartMs = 0;
unsigned long sw420LastSampleMs = 0;
bool sw420FrameFail = false;

// Slider-configurable values (Blynk)
int sw420ThresholdSec = 20;   // V22
int sw420FrameSec = 40;       // V23

// SW420 fail buzzer behavior
unsigned long sw420FailBuzzerStartMs = 0;
bool sw420FailBuzzerActive = false;
const unsigned long SW420_FAIL_BUZZ_MS = 5000;

// Indicator control state
bool motorOn = false;
bool fanOn = false;
bool buzzerManualOn = false;
bool ledManualOn = false;
bool productionRelayCutoffApplied = false;

// Anomaly alert behavior: LED OFF + buzzer pulse for 5s
bool anomalyAlertActive = false;
unsigned long anomalyAlertStartMs = 0;
const unsigned long ANOMALY_ALERT_DURATION_MS = 5000;
const unsigned long ANOMALY_BUZZER_PULSE_MS = 220;

// Alert source mode (Blynk V28): 0 = SW420 basic, 1 = MPU deviation, 2 = backend ML
const int ALERT_MODE_SW420 = 0;
const int ALERT_MODE_MPU = 1;
const int ALERT_MODE_BACKEND = 2;
int alertMode = ALERT_MODE_SW420;

// Mode 2 (MPU deviation) parameters
const float MPU_BASELINE_ACC_MAG = 16500.0;
const float MPU_BASELINE_GYRO_MAG = 300.0;
float mpuAccDeviationThreshold = 1200.0;   // V24
float mpuGyroDeviationThreshold = 120.0;   // V25
const int MPU_DEVIATION_MIN_CONSECUTIVE = 3;
int mpuDeviationStreak = 0;
bool mpuDeviationActive = false;
float mpuLastAccDeviation = 0.0;
float mpuLastGyroDeviation = 0.0;

// Mode 3 (backend ML) state
bool backendAnomalyActive = false;
float backendLastScore = 0.0;
float backendLastThreshold = 0.0;
bool backendHasScore = false;
bool backendHasThreshold = false;

// ThingSpeak window stats
float accSum = 0;
float accPeak = 0;
int sampleCount = 0;

// Distilled model
const int FEATURE_DIM = 8;
float featureMeans[FEATURE_DIM] = {0};
float featureStds[FEATURE_DIM] = {1,1,1,1,1,1,1,1};
float modelWeights[FEATURE_DIM] = {0};
float modelBias = 0.0;
float modelDecisionThreshold = 0.55;
float modelHysteresisHigh = 0.55;
float modelHysteresisLow = 0.48;
int modelMinConsecutiveWindows = 3;
int modelVersion = 0;
String modelChecksum = "";
bool modelReady = false;
int anomalyStreak = 0;

// Calibration job tracking
String calibrationJobId = "";
bool calibrationInProgress = false;
int calibrationProgress = 0;
String calibrationStage = "idle";
String calibrationMessage = "Idle";
bool calibrationAsNewDevice = true;

// Backend stream telemetry
unsigned long streamAttemptCount = 0;
unsigned long streamSuccessCount = 0;
unsigned long streamFailCount = 0;
int lastStreamHttpCode = 0;
String lastStreamResult = "INIT";

unsigned long lastWiFiReconnectAttemptMs = 0;
unsigned long lastBlynkConnectAttemptMs = 0;
unsigned long lastBindingRefreshMs = 0;

const unsigned long WIFI_RECONNECT_INTERVAL_MS = 5000;
const unsigned long BLYNK_RECONNECT_INTERVAL_MS = 5000;
const unsigned long BINDING_REFRESH_INTERVAL_MS = 30000;
const int STREAM_TARGET_SAMPLE_HZ = 4;
const unsigned long STREAM_SAMPLE_INTERVAL_MS = 250;
const unsigned long STREAM_FLUSH_INTERVAL_MS = 8000;
const int STREAM_HIGH_WATERMARK = 80;
const unsigned long STREAM_TASK_SLEEP_MS = 20;

const int STREAM_BATCH_SIZE = 32;
const int STREAM_QUEUE_CAPACITY = 120;

// Feature window
const int WINDOW_SIZE = 10;
float accWindow[WINDOW_SIZE] = {0};
float gyroWindow[WINDOW_SIZE] = {0};
float gxWindow[WINDOW_SIZE] = {0};
float gyWindow[WINDOW_SIZE] = {0};
float gzWindow[WINDOW_SIZE] = {0};
int windowPos = 0;
int windowCount = 0;

const int STREAM_HTTP_TIMEOUT_MS = 1500;
const int MODEL_HTTP_TIMEOUT_MS = 5000;

struct StreamQueuedSample {
  char timestamp[40];
  float accMag;
  float gyroMag;
  int gx;
  int gy;
  int gz;
  int sw420;
  int sequence;
};

StreamQueuedSample streamQueue[STREAM_QUEUE_CAPACITY];
int streamQueueHead = 0;
int streamQueueCount = 0;
int streamNextSequence = 1;
unsigned long streamDroppedCount = 0;
SemaphoreHandle_t streamStateMutex = nullptr;
TaskHandle_t streamUploaderTaskHandle = nullptr;

// -------------------- Helpers --------------------

void updateAlertOutputs();
void calibrateSw420Polarity();
void updateSw420FilterSample();
bool isSw420ActiveFiltered();
bool isSw420AlertMode();
bool isMpuAlertMode();
bool isBackendAlertMode();
const char* alertModeLabel();
bool evaluateMpuDeviationMode();
bool currentModeFailureActive();
String buildModeStatusLine();
void pushModeTelemetryToBlynk();
void streamUploaderTask(void* parameter);
void setStreamStatus(int httpCode, const String& result, bool markSuccess, bool markFail);
void incrementStreamAttemptCount();
void clearLocalModelForProfileChange(const char* reason);

void applyIndicators() {
  updateAlertOutputs();
}

bool isSw420AlertMode() {
  return alertMode == ALERT_MODE_SW420;
}

bool isMpuAlertMode() {
  return alertMode == ALERT_MODE_MPU;
}

bool isBackendAlertMode() {
  return alertMode == ALERT_MODE_BACKEND;
}

const char* alertModeLabel() {
  if (isMpuAlertMode()) return "MPU";
  if (isBackendAlertMode()) return "BACKEND";
  return "SW420";
}

bool currentModeFailureActive() {
  unsigned long now = millis();
  bool sw420Muted = sw420DebugCooldownActive && debugMode && now < sw420DebugCooldownUntilMs;

  if (isSw420AlertMode()) {
    return sw420FaultLatched || sw420Muted;
  }
  if (isMpuAlertMode()) {
    return mpuDeviationActive;
  }
  return backendAnomalyActive;
}

String buildModeStatusLine() {
  bool modeFail = currentModeFailureActive();

  if (isSw420AlertMode()) {
    return "M1 SW420 | fail=" + String(modeFail ? 1 : 0) +
      " high=" + String(sw420HighAccumulatedMs / 1000.0, 1) +
      "s th=" + String(sw420ThresholdSec) +
      "s frame=" + String(sw420FrameSec) + "s";
  }

  if (isMpuAlertMode()) {
    return "M2 MPU | fail=" + String(modeFail ? 1 : 0) +
      " dAcc=" + String(mpuLastAccDeviation, 0) +
      " dGy=" + String(mpuLastGyroDeviation, 0);
  }

  String scoreText = backendHasScore ? String(backendLastScore, 3) : "na";
  String thresholdText = backendHasThreshold ? String(backendLastThreshold, 3) : "na";
  return "M3 BACKEND | fail=" + String(modeFail ? 1 : 0) +
    " score=" + scoreText +
    " th=" + thresholdText;
}

void pushModeTelemetryToBlynk() {
  Blynk.virtualWrite(V29, alertMode);
  if (isSw420AlertMode()) {
    Blynk.virtualWrite(V30, sw420HighAccumulatedMs / 1000.0);
    Blynk.virtualWrite(V31, (float)sw420ThresholdSec);
  } else if (isMpuAlertMode()) {
    Blynk.virtualWrite(V30, mpuLastAccDeviation);
    Blynk.virtualWrite(V31, mpuLastGyroDeviation);
  } else {
    Blynk.virtualWrite(V30, backendHasScore ? backendLastScore : -1.0);
    Blynk.virtualWrite(V31, backendHasThreshold ? backendLastThreshold : -1.0);
  }
  Blynk.virtualWrite(V26, buildModeStatusLine());
}

void updateAlertOutputs() {
  unsigned long now = millis();
  bool modeFail = currentModeFailureActive();

  if (sw420DebugCooldownActive && debugMode && now >= sw420DebugCooldownUntilMs) {
    sw420DebugCooldownActive = false;
    sw420FaultAnnounced = false;
  }

  if (anomalyAlertActive && (now - anomalyAlertStartMs >= ANOMALY_ALERT_DURATION_MS)) {
    anomalyAlertActive = false;
  }

  bool sw420SafetyEnabled = isSw420AlertMode();
  bool advancedAlertEnabled = isMpuAlertMode() || isBackendAlertMode();
  bool sw420Muted = sw420DebugCooldownActive && debugMode && now < sw420DebugCooldownUntilMs;

  // Production mode: apply relay shutdown once per fault episode, never reboot ESP.
  if (!debugMode) {
    bool sw420GraceActive = isSw420AlertMode() && sw420FailBuzzerActive && ((now - sw420FailBuzzerStartMs) < SW420_FAIL_BUZZ_MS);
    if (modeFail && !sw420GraceActive && !productionRelayCutoffApplied) {
      motorOn = false;
      fanOn = false;
      productionRelayCutoffApplied = true;
      Serial.print("RELAY_CUTOFF_APPLIED,mode=");
      Serial.println(alertModeLabel());
    } else if (!modeFail) {
      productionRelayCutoffApplied = false;
    }
  } else {
    productionRelayCutoffApplied = false;
  }

  // Normal state: LED ON, buzzer OFF
  bool ledState = true;
  bool buzzerState = false;

  // Anomaly state: LED OFF + buzzer pulse
  if (advancedAlertEnabled && anomalyAlertActive) {
    ledState = false;
    buzzerState = ((now / ANOMALY_BUZZER_PULSE_MS) % 2) == 0;
  }

  // Manual controls only when no anomaly and no SW420 block
  if ((!anomalyAlertActive || !advancedAlertEnabled) && (!sw420SafetyEnabled || (!sw420FaultLatched && !sw420Muted))) {
    ledState = ledState || ledManualOn;
    buzzerState = buzzerManualOn;
  }

  // SW420 safety highest priority
  if (sw420SafetyEnabled && (sw420FaultLatched || sw420Muted)) {
    ledState = false;

    // Buzzer ON for first 5s after SW420 fail
    if (sw420FailBuzzerActive && (now - sw420FailBuzzerStartMs < SW420_FAIL_BUZZ_MS)) {
      buzzerState = true;
    } else {
      sw420FailBuzzerActive = false;
      buzzerState = false;
    }
  }

  digitalWrite(RELAY_MOTOR_PIN, motorOn ? RELAY_ON : RELAY_OFF);
  digitalWrite(RELAY_FAN_PIN, fanOn ? RELAY_ON : RELAY_OFF);
  digitalWrite(LED_PIN, ledState ? LED_ON : LED_OFF);
  digitalWrite(BUZZER_PIN, buzzerState ? BUZZER_ON : BUZZER_OFF);
}

void ensureWiFiConnection() {
  if (WiFi.status() == WL_CONNECTED) return;

  unsigned long now = millis();
  if (now - lastWiFiReconnectAttemptMs < WIFI_RECONNECT_INTERVAL_MS) return;

  lastWiFiReconnectAttemptMs = now;
  Serial.print("WIFI_RECONNECT,status=");
  Serial.println((int)WiFi.status());
  WiFi.reconnect();
}

void updateCalibrationRuntime(const String& stage, int progress, const String& message, bool inProgress) {
  calibrationStage = stage;
  calibrationProgress = progress;
  calibrationMessage = message;
  calibrationInProgress = inProgress;
}

void resetRuntimeForFreshCalibration() {
  accSum = 0;
  accPeak = 0;
  sampleCount = 0;
  anomalyStreak = 0;
  isMachineFailing = false;
  mpuDeviationStreak = 0;
  mpuDeviationActive = false;
  backendAnomalyActive = false;
  anomalyAlertActive = false;

  for (int i = 0; i < WINDOW_SIZE; i++) {
    accWindow[i] = 0;
    gyroWindow[i] = 0;
    gxWindow[i] = 0;
    gyWindow[i] = 0;
    gzWindow[i] = 0;
  }
  windowPos = 0;
  windowCount = 0;

  updateCalibrationRuntime("queued", 1, "Manual reset: starting new calibration", true);
}

void IRAM_ATTR emergencyKillSwitch() {
  sw420InterruptPending = true;
}

void calibrateSw420Polarity() {
  int highCount = 0;
  int lowCount = 0;
  const int sampleCountForCal = 120;

  for (int i = 0; i < sampleCountForCal; i++) {
    int raw = digitalRead(SW420_PIN);
    if (raw == HIGH) highCount++;
    else lowCount++;
    delay(2);
  }

  sw420IdleLevel = (highCount >= lowCount) ? HIGH : LOW;
  sw420PolarityCalibrated = true;

  for (int i = 0; i < SW420_FILTER_WINDOW; i++) {
    sw420RecentActive[i] = 0;
  }
  sw420RecentPos = 0;
  sw420RecentCount = 0;

  Serial.print("SW420_POLARITY,idle=");
  Serial.print(sw420IdleLevel == HIGH ? "HIGH" : "LOW");
  Serial.print(",high_samples=");
  Serial.print(highCount);
  Serial.print(",low_samples=");
  Serial.println(lowCount);
}

void updateSw420FilterSample() {
  int raw = digitalRead(SW420_PIN);
  uint8_t active = (raw != sw420IdleLevel) ? 1 : 0;
  sw420RecentActive[sw420RecentPos] = active;
  sw420RecentPos = (sw420RecentPos + 1) % SW420_FILTER_WINDOW;
  if (sw420RecentCount < SW420_FILTER_WINDOW) sw420RecentCount++;
}

bool isSw420ActiveFiltered() {
  if (!sw420PolarityCalibrated) return false;
  if (sw420RecentCount <= 0) return false;

  int activeCount = 0;
  for (int i = 0; i < sw420RecentCount; i++) {
    activeCount += sw420RecentActive[i];
  }

  int required = SW420_FILTER_ACTIVE_MIN;
  if (sw420RecentCount < SW420_FILTER_WINDOW) {
    required = (sw420RecentCount * 2 + 2) / 3; // around 67% active while buffer is warming up
    if (required < 1) required = 1;
  }
  return activeCount >= required;
}

bool evaluateMpuDeviationMode() {
  mpuLastAccDeviation = fabs(accMag - MPU_BASELINE_ACC_MAG);
  mpuLastGyroDeviation = fabs(gyroMag - MPU_BASELINE_GYRO_MAG);

  bool instantDeviation =
    (mpuLastAccDeviation >= mpuAccDeviationThreshold) &&
    (mpuLastGyroDeviation >= mpuGyroDeviationThreshold);

  if (instantDeviation) {
    if (mpuDeviationStreak < 10000) mpuDeviationStreak++;
  } else {
    mpuDeviationStreak = 0;
  }

  mpuDeviationActive = (mpuDeviationStreak >= MPU_DEVIATION_MIN_CONSECUTIVE);
  return mpuDeviationActive;
}

void evaluateSw420FrameLogic() {
  unsigned long now = millis();

  if (sw420FrameStartMs == 0) {
    sw420FrameStartMs = now;
    sw420LastSampleMs = now;
    sw420HighAccumulatedMs = 0;
    sw420FrameFail = false;
    return;
  }

  unsigned long dt = now - sw420LastSampleMs;
  sw420LastSampleMs = now;

  // Keep threshold valid: threshold < frame
  if (sw420ThresholdSec >= sw420FrameSec) {
    sw420ThresholdSec = sw420FrameSec - 1;
    if (sw420ThresholdSec < 1) sw420ThresholdSec = 1;
    if (Blynk.connected()) Blynk.virtualWrite(V22, sw420ThresholdSec);
  }

  unsigned long frameMs = (unsigned long)sw420FrameSec * 1000UL;
  unsigned long thresholdMs = (unsigned long)sw420ThresholdSec * 1000UL;

  // Accumulate HIGH duration, capped to frame
  if (isSw420ActiveFiltered()) {
    if (sw420HighAccumulatedMs + dt >= frameMs) sw420HighAccumulatedMs = frameMs;
    else sw420HighAccumulatedMs += dt;
  }

  // Drift-safe frame completion
  while (now - sw420FrameStartMs >= frameMs) {
    if (sw420HighAccumulatedMs > frameMs) sw420HighAccumulatedMs = frameMs;

    sw420FrameFail = (sw420HighAccumulatedMs >= thresholdMs);

    if (sw420FrameFail) {
      if (isSw420AlertMode()) {
        // start 5s buzzer window
        sw420FailBuzzerActive = true;
        sw420FailBuzzerStartMs = millis();

        if (debugMode) {
          sw420DebugCooldownActive = true;
          sw420DebugCooldownUntilMs = now + SW420_DEBUG_COOLDOWN_MS;
          sw420FaultLatched = false;
          emergencyTriggered = false;
          sw420FaultAnnounced = false;
          Serial.println("SW420 FRAME FAIL (debug mute active)");
        } else {
          sw420FaultLatched = true;
          sw420FaultAnnounced = false;
          emergencyTriggered = true;
        }
      } else {
        Serial.print("SW420_FRAME_FAIL_IGNORED,mode=");
        Serial.println(alertModeLabel());
      }

      if (Blynk.connected() && isSw420AlertMode()) {
        Blynk.virtualWrite(V26,
          "FAIL | HIGH=" + String(sw420HighAccumulatedMs / 1000.0, 1) +
          "s / FRAME=" + String(sw420FrameSec) +
          "s | TH=" + String(sw420ThresholdSec) + "s");
      }
    } else {
      if (Blynk.connected() && isSw420AlertMode()) {
        Blynk.virtualWrite(V26,
          "PASS | HIGH=" + String(sw420HighAccumulatedMs / 1000.0, 1) +
          "s / FRAME=" + String(sw420FrameSec) +
          "s | TH=" + String(sw420ThresholdSec) + "s");
      }
    }

    Serial.print("SW420_FRAME,high_s=");
    Serial.print(sw420HighAccumulatedMs / 1000.0, 2);
    Serial.print(",frame_s=");
    Serial.print(sw420FrameSec);
    Serial.print(",th_s=");
    Serial.print(sw420ThresholdSec);
    Serial.print(",result=");
    Serial.println(sw420FrameFail ? "FAIL" : "PASS");

    // Advance exactly one frame
    sw420FrameStartMs += frameMs;

    // Reset accumulators for next frame
    sw420HighAccumulatedMs = 0;
    sw420FrameFail = false;
  }
}

void processSw420Trigger() {
  bool pending = false;

  updateSw420FilterSample();

  if (SW420_USE_INTERRUPT) {
    noInterrupts();
    pending = sw420InterruptPending;
    sw420InterruptPending = false;
    interrupts();

    if (pending) {
      unsigned long now = millis();
      if (now - sw420LastConfirmedTriggerMs >= SW420_DEBOUNCE_MS) {
        sw420LastConfirmedTriggerMs = now;
      }
    }
  }

  evaluateSw420FrameLogic();
}

String getTimeString() {
  struct tm timeinfo;
  if (!getLocalTime(&timeinfo)) return "Time_Error";
  char buff[20];
  strftime(buff, sizeof(buff), "%Y-%m-%d %H:%M:%S", &timeinfo);
  return String(buff);
}

bool getIsoTimestampForApi(String& out) {
  struct tm timeinfo;
  if (!getLocalTime(&timeinfo)) return false;

  char buff[32];
  strftime(buff, sizeof(buff), "%Y-%m-%dT%H:%M:%S%z", &timeinfo);
  String ts(buff);

  // Convert +0530 to +05:30 for strict ISO8601 parsers.
  if (ts.length() >= 5) {
    ts = ts.substring(0, ts.length() - 2) + ":" + ts.substring(ts.length() - 2);
  }

  out = ts;
  return true;
}

void pushWindowSample(float aMag, float gMag, float x, float y, float z) {
  accWindow[windowPos] = aMag;
  gyroWindow[windowPos] = gMag;
  gxWindow[windowPos] = x;
  gyWindow[windowPos] = y;
  gzWindow[windowPos] = z;

  windowPos = (windowPos + 1) % WINDOW_SIZE;
  if (windowCount < WINDOW_SIZE) windowCount++;
}

int orderedIndex(int logicalIdx) {
  if (windowCount < WINDOW_SIZE) return logicalIdx;
  return (windowPos + logicalIdx) % WINDOW_SIZE;
}

float meanOf(float* arr, int n) {
  if (n <= 0) return 0.0;
  float s = 0.0;
  for (int i = 0; i < n; i++) s += arr[orderedIndex(i)];
  return s / n;
}

float stdOf(float* arr, int n, float mu) {
  if (n <= 1) return 0.0;
  float ss = 0.0;
  for (int i = 0; i < n; i++) {
    float d = arr[orderedIndex(i)] - mu;
    ss += d * d;
  }
  return sqrt(ss / n);
}

float maxOf(float* arr, int n) {
  if (n <= 0) return 0.0;
  float m = arr[orderedIndex(0)];
  for (int i = 1; i < n; i++) {
    float v = arr[orderedIndex(i)];
    if (v > m) m = v;
  }
  return m;
}

bool computeFeatureVector(float outFeatures[FEATURE_DIM]) {
  if (windowCount <= 1) return false;

  float meanAcc = meanOf(accWindow, windowCount);
  float stdAcc = stdOf(accWindow, windowCount, meanAcc);
  float maxAcc = maxOf(accWindow, windowCount);
  float meanGyro = meanOf(gyroWindow, windowCount);

  float muGx = meanOf(gxWindow, windowCount);
  float muGy = meanOf(gyWindow, windowCount);
  float muGz = meanOf(gzWindow, windowCount);

  float stdGx = stdOf(gxWindow, windowCount, muGx);
  float stdGy = stdOf(gyWindow, windowCount, muGy);
  float stdGz = stdOf(gzWindow, windowCount, muGz);

  float axisSum = stdGx + stdGy + stdGz;
  float axisMax = stdGx;
  if (stdGy > axisMax) axisMax = stdGy;
  if (stdGz > axisMax) axisMax = stdGz;
  float axisImbalanceRatio = axisMax / (axisSum + 1e-6);

  outFeatures[0] = meanAcc;
  outFeatures[1] = stdAcc;
  outFeatures[2] = maxAcc;
  outFeatures[3] = meanGyro;
  outFeatures[4] = stdGx;
  outFeatures[5] = stdGy;
  outFeatures[6] = stdGz;
  outFeatures[7] = axisImbalanceRatio;
  return true;
}

float scoreDistilledModel(float features[FEATURE_DIM]) {
  float score = modelBias;
  for (int i = 0; i < FEATURE_DIM; i++) {
    float denom = featureStds[i];
    if (fabs(denom) < 1e-6) denom = 1.0;
    float z = (features[i] - featureMeans[i]) / denom;
    score += modelWeights[i] * z;
  }
  return score;
}

bool evaluateLocalAI() {
  if (!modelReady) return accMag > (aiThreshold * AI_THRESHOLD_SCALE);

  float features[FEATURE_DIM] = {0};
  if (!computeFeatureVector(features)) return accMag > (aiThreshold * AI_THRESHOLD_SCALE);

  float score = scoreDistilledModel(features);

  if (score >= modelHysteresisHigh) anomalyStreak++;
  else if (score < modelHysteresisLow) anomalyStreak = 0;

  bool anomaly = anomalyStreak >= AI_MIN_CONSECUTIVE;

  Serial.print("MODEL,");
  Serial.print(score); Serial.print(",");
  Serial.print(modelHysteresisHigh); Serial.print(",");
  Serial.println(anomaly ? 1 : 0);

  return anomaly;
}

void setStreamStatus(int httpCode, const String& result, bool markSuccess, bool markFail) {
  if (streamStateMutex && xSemaphoreTake(streamStateMutex, pdMS_TO_TICKS(40)) == pdTRUE) {
    if (markSuccess) streamSuccessCount++;
    if (markFail) streamFailCount++;
    lastStreamHttpCode = httpCode;
    lastStreamResult = result;
    xSemaphoreGive(streamStateMutex);
    return;
  }

  if (markSuccess) streamSuccessCount++;
  if (markFail) streamFailCount++;
  lastStreamHttpCode = httpCode;
  lastStreamResult = result;
}

void incrementStreamAttemptCount() {
  if (streamStateMutex && xSemaphoreTake(streamStateMutex, pdMS_TO_TICKS(20)) == pdTRUE) {
    streamAttemptCount++;
    xSemaphoreGive(streamStateMutex);
    return;
  }
  streamAttemptCount++;
}

void clearLocalModelForProfileChange(const char* reason) {
  modelReady = false;
  modelVersion = 0;
  modelChecksum = "";
  modelBias = 0.0;
  modelDecisionThreshold = 0.55;
  modelHysteresisHigh = 0.55;
  modelHysteresisLow = 0.48;
  modelMinConsecutiveWindows = 3;
  aiThreshold = 25000.0;
  anomalyStreak = 0;

  for (int i = 0; i < FEATURE_DIM; i++) {
    featureMeans[i] = 0.0;
    featureStds[i] = 1.0;
    modelWeights[i] = 0.0;
  }

  saveModelToNvs();

  Serial.print("MODEL_LOCAL_CLEARED,reason=");
  Serial.println(reason ? reason : "unknown");

  if (Blynk.connected()) {
    Blynk.virtualWrite(V9, modelVersion);
    Blynk.virtualWrite(V26, "Model cleared for profile switch");
  }
}

bool httpPostJson(const String& url, const String& payload, String& responseOut, int timeoutMs, int* statusCodeOut = nullptr, int maxAttempts = 2) {
  if (!backendEnabled) {
    if (statusCodeOut) *statusCodeOut = -1;
    return false;
  }

  if (WiFi.status() != WL_CONNECTED) {
    ensureWiFiConnection();
    if (WiFi.status() != WL_CONNECTED) {
      if (statusCodeOut) *statusCodeOut = -1;
      return false;
    }
  }

  if (maxAttempts < 1) maxAttempts = 1;
  for (int attempt = 0; attempt < maxAttempts; attempt++) {
    HTTPClient http;
    bool beginOk = false;
    if (url.startsWith("https://")) {
      apiClient.setInsecure();
      beginOk = http.begin(apiClient, url);
    } else {
      WiFiClient plainClient;
      beginOk = http.begin(plainClient, url);
    }

    if (!beginOk) {
      if (statusCodeOut) *statusCodeOut = -2;
      return false;
    }

    http.setConnectTimeout(timeoutMs);
    http.setTimeout(timeoutMs);
    http.setReuse(false);
    http.addHeader("Content-Type", "application/json");
    http.addHeader("Accept", "application/json");

    int code = http.POST(payload);
    if (statusCodeOut) *statusCodeOut = code;
    if (code > 0) responseOut = http.getString();

    http.end();

    if (code >= 200 && code < 300) return true;
    if (code != -11 && code != -1) return false;

    ensureWiFiConnection();
    delay(40);
  }

  return false;
}

bool httpGetJson(const String& url, String& responseOut, int timeoutMs, int* statusCodeOut = nullptr) {
  if (!backendEnabled) {
    if (statusCodeOut) *statusCodeOut = -1;
    return false;
  }

  if (WiFi.status() != WL_CONNECTED) {
    ensureWiFiConnection();
    if (WiFi.status() != WL_CONNECTED) {
      if (statusCodeOut) *statusCodeOut = -1;
      return false;
    }
  }

  for (int attempt = 0; attempt < 2; attempt++) {
    HTTPClient http;
    bool beginOk = false;
    if (url.startsWith("https://")) {
      apiClient.setInsecure();
      beginOk = http.begin(apiClient, url);
    } else {
      WiFiClient plainClient;
      beginOk = http.begin(plainClient, url);
    }

    if (!beginOk) {
      if (statusCodeOut) *statusCodeOut = -2;
      return false;
    }

    http.setConnectTimeout(timeoutMs);
    http.setTimeout(timeoutMs);
    http.setReuse(false);

    int code = http.GET();
    if (statusCodeOut) *statusCodeOut = code;
    if (code > 0) responseOut = http.getString();

    http.end();

    if (code >= 200 && code < 300) return true;
    if (code != -11 && code != -1) return false;

    ensureWiFiConnection();
    delay(40);
  }

  return false;
}

int streamQueueIndex(int logicalIdx) {
  return (streamQueueHead + logicalIdx) % STREAM_QUEUE_CAPACITY;
}

void popStreamQueue(int count) {
  if (count <= 0 || streamQueueCount <= 0) return;
  if (count > streamQueueCount) count = streamQueueCount;
  streamQueueHead = (streamQueueHead + count) % STREAM_QUEUE_CAPACITY;
  streamQueueCount -= count;
}

void enqueueCurrentSampleForStream() {
  StreamQueuedSample item;
  memset(&item, 0, sizeof(item));

  String apiTs;
  if (getIsoTimestampForApi(apiTs)) {
    apiTs.toCharArray(item.timestamp, sizeof(item.timestamp));
  } else {
    item.timestamp[0] = '\0';
  }

  item.accMag = accMag;
  item.gyroMag = gyroMag;
  item.gx = gx;
  item.gy = gy;
  item.gz = gz;
  item.sw420 = isSw420ActiveFiltered() ? 1 : 0;
  item.sequence = streamNextSequence++;

  if (streamQueueCount >= STREAM_QUEUE_CAPACITY) {
    popStreamQueue(1);
    streamDroppedCount++;
  }

  int insertAt = streamQueueIndex(streamQueueCount);
  streamQueue[insertAt] = item;
  streamQueueCount++;
}

String buildBatchPayload(int maxSamples, int& outBatchCount) {
  outBatchCount = streamQueueCount;
  if (outBatchCount > maxSamples) outBatchCount = maxSamples;
  if (outBatchCount < 0) outBatchCount = 0;

  StaticJsonDocument<4096> req;
  req["esp_model_version"] = modelVersion;
  req["esp_model_checksum"] = modelChecksum;
  JsonArray samples = req.createNestedArray("samples");

  for (int i = 0; i < outBatchCount; i++) {
    const StreamQueuedSample& item = streamQueue[streamQueueIndex(i)];
    JsonObject s = samples.createNestedObject();
    if (item.timestamp[0] != '\0') {
      s["timestamp"] = item.timestamp;
    }
    s["accMag"] = item.accMag;
    s["gyroMag"] = item.gyroMag;
    s["gx"] = item.gx;
    s["gy"] = item.gy;
    s["gz"] = item.gz;
    s["sw420"] = item.sw420;
    s["sequence"] = item.sequence;
  }

  String payload;
  serializeJson(req, payload);
  return payload;
}

bool sendBatchOverHttpFallback(const String& payload, int payloadCount) {
  if (payloadCount <= 0) return false;

  String response;
  int httpCode = 0;
  String url = String(BACKEND_BASE_URL) + "/api/v1/stream";
  if (!httpPostJson(url, payload, response, STREAM_HTTP_TIMEOUT_MS, &httpCode, 1)) {
    if (httpCode == -1) setStreamStatus(httpCode, "FAIL: WIFI_DOWN", false, true);
    else if (httpCode == -2) setStreamStatus(httpCode, "FAIL: HTTP_BEGIN", false, true);
    else if (httpCode == -11) setStreamStatus(httpCode, "FAIL: HTTP_TIMEOUT", false, true);
    else setStreamStatus(httpCode, "FAIL: HTTP_" + String(httpCode), false, true);
    return false;
  }

  popStreamQueue(payloadCount);
  setStreamStatus(httpCode, "OK:HTTP_BATCH", true, false);

  DynamicJsonDocument doc(2048);
  if (deserializeJson(doc, response) == DeserializationError::Ok) {
    bool previousBackendAnomaly = backendAnomalyActive;

    if (doc.containsKey("is_anomaly")) {
      backendAnomalyActive = doc["is_anomaly"].as<bool>();
    }
    if (doc.containsKey("score") && !doc["score"].isNull()) {
      backendLastScore = doc["score"].as<float>();
      backendHasScore = true;
    }
    if (doc.containsKey("decision_threshold") && !doc["decision_threshold"].isNull()) {
      backendLastThreshold = doc["decision_threshold"].as<float>();
      backendHasThreshold = true;
    }

    if (isBackendAlertMode() && backendAnomalyActive && !previousBackendAnomaly) {
      anomalyAlertActive = true;
      anomalyAlertStartMs = millis();
      Blynk.logEvent("machine_alert", "Backend ML anomaly flagged");
    }
  }
  return true;
}

bool refreshActiveBinding(bool force = false) {
  unsigned long now = millis();
  if (!force && (now - lastBindingRefreshMs) < BINDING_REFRESH_INTERVAL_MS) {
    return activeBindingMachineId.length() > 0 && activeBindingDeviceId.length() > 0;
  }
  lastBindingRefreshMs = now;

  String response;
  int httpCode = 0;
  String url = String(BACKEND_BASE_URL) + "/api/v1/stream-binding";
  if (!httpGetJson(url, response, MODEL_HTTP_TIMEOUT_MS, &httpCode)) {
    if (httpCode > 0) {
      Serial.print("BINDING_FETCH_FAIL,http=");
      Serial.println(httpCode);
    }
    return false;
  }

  DynamicJsonDocument doc(3072);
  if (deserializeJson(doc, response) != DeserializationError::Ok) {
    Serial.println("BINDING_PARSE_FAIL");
    return false;
  }

  bool isActive = doc["is_active"] | false;
  if (!isActive) {
    bool hadActiveBinding = activeBindingMachineId.length() > 0 || activeBindingDeviceId.length() > 0;
    if (hadActiveBinding) {
      clearLocalModelForProfileChange("binding_cleared");
    }
    activeBindingMachineId = "";
    activeBindingDeviceId = "";
    return false;
  }

  String machine = String((const char*)(doc["machine_id"] | ""));
  String device = String((const char*)(doc["device_id"] | ""));
  if (machine.length() == 0 || device.length() == 0) {
    activeBindingMachineId = "";
    activeBindingDeviceId = "";
    return false;
  }

  bool changed = (machine != activeBindingMachineId) || (device != activeBindingDeviceId);
  activeBindingMachineId = machine;
  activeBindingDeviceId = device;

  if (changed) {
    clearLocalModelForProfileChange("binding_changed");

    Serial.print("BINDING_ACTIVE,machine=");
    Serial.print(activeBindingMachineId);
    Serial.print(",device=");
    Serial.println(activeBindingDeviceId);

    // Try pulling backend model immediately for the new profile binding.
    pullModelPackageFromBackend();
  }

  return true;
}

void refreshActiveBindingTask() {
  refreshActiveBinding(false);
}

// -------------------- Model persistence --------------------

void saveModelToNvs() {
  prefs.begin("machinocare", false);
  prefs.putInt("mver", modelVersion);
  prefs.putString("mchk", modelChecksum);
  prefs.putFloat("abias", modelBias);
  prefs.putFloat("athr", aiThreshold);
  prefs.putFloat("dthr", modelDecisionThreshold);
  prefs.putFloat("hhi", modelHysteresisHigh);
  prefs.putFloat("hlo", modelHysteresisLow);
  prefs.putInt("minw", modelMinConsecutiveWindows);
  prefs.putBytes("fmu", featureMeans, sizeof(featureMeans));
  prefs.putBytes("fsd", featureStds, sizeof(featureStds));
  prefs.putBytes("wts", modelWeights, sizeof(modelWeights));
  prefs.end();
}

void loadModelFromNvs() {
  prefs.begin("machinocare", true);

  aiThreshold = prefs.getFloat("athr", aiThreshold);
  modelVersion = prefs.getInt("mver", 0);
  modelChecksum = prefs.getString("mchk", "");
  modelBias = prefs.getFloat("abias", 0.0);
  modelDecisionThreshold = prefs.getFloat("dthr", 0.55);
  modelHysteresisHigh = prefs.getFloat("hhi", modelDecisionThreshold);
  modelHysteresisLow = prefs.getFloat("hlo", modelDecisionThreshold * 0.9);
  modelMinConsecutiveWindows = prefs.getInt("minw", 3);

  bool hasArrays = true;
  hasArrays &= (prefs.getBytesLength("fmu") == sizeof(featureMeans));
  hasArrays &= (prefs.getBytesLength("fsd") == sizeof(featureStds));
  hasArrays &= (prefs.getBytesLength("wts") == sizeof(modelWeights));

  if (hasArrays) {
    prefs.getBytes("fmu", featureMeans, sizeof(featureMeans));
    prefs.getBytes("fsd", featureStds, sizeof(featureStds));
    prefs.getBytes("wts", modelWeights, sizeof(modelWeights));
    modelReady = (modelVersion > 0);
  }

  prefs.end();
}

bool applyModelPackage(JsonObject pkg) {
  if (!pkg.containsKey("feature_means") || !pkg.containsKey("feature_stds") || !pkg.containsKey("weights")) return false;

  JsonArray means = pkg["feature_means"].as<JsonArray>();
  JsonArray stds  = pkg["feature_stds"].as<JsonArray>();
  JsonArray wts   = pkg["weights"].as<JsonArray>();

  if (means.size() != FEATURE_DIM || stds.size() != FEATURE_DIM || wts.size() != FEATURE_DIM) return false;

  for (int i = 0; i < FEATURE_DIM; i++) {
    featureMeans[i] = means[i].as<float>();
    featureStds[i] = stds[i].as<float>();
    if (fabs(featureStds[i]) < 1e-6) featureStds[i] = 1.0;
    modelWeights[i] = wts[i].as<float>();
  }

  modelBias = pkg["bias"] | 0.0;
  modelDecisionThreshold = pkg["decision_threshold"] | 0.55;
  modelHysteresisHigh = pkg["hysteresis_high"] | modelDecisionThreshold;
  modelHysteresisLow  = pkg["hysteresis_low"] | (modelDecisionThreshold * 0.9);
  modelMinConsecutiveWindows = pkg["min_consecutive_windows"] | 3;
  modelVersion = pkg["model_version"] | modelVersion;
  modelChecksum = String((const char*)(pkg["checksum"] | ""));

  if (pkg.containsKey("fallback_acc_threshold")) {
    aiThreshold = pkg["fallback_acc_threshold"].as<float>();
  }

  modelReady = true;
  saveModelToNvs();
  return true;
}

// -------------------- Backend calibration --------------------

void pushCalibrationToBlynk() {
  Blynk.virtualWrite(V10, calibrationStage + " | " + calibrationMessage);
  Blynk.virtualWrite(V11, calibrationProgress);
  Blynk.virtualWrite(V12, calibrationInProgress ? 255 : 0);
  Blynk.virtualWrite(V9, modelVersion);
}

bool startCalibrationJobOnBackend(bool newDeviceSetup, const char* triggerSource) {
  if (!backendEnabled) return false;
  if (WiFi.status() != WL_CONNECTED) {
    ensureWiFiConnection();
    if (WiFi.status() != WL_CONNECTED) return false;
  }
  if (calibrationInProgress) return true;

  if (!refreshActiveBinding(true)) {
    updateCalibrationRuntime("failed", 100, "No active stream binding/profile", false);
    return false;
  }

  StaticJsonDocument<768> req;
  req["machine_id"] = activeBindingMachineId;
  req["device_id"] = activeBindingDeviceId;
  req["sample_rate_hz"] = STREAM_TARGET_SAMPLE_HZ;
  req["window_seconds"] = 1;
  req["fallback_seconds"] = 300;
  req["contamination"] = 0.05;
  req["min_consecutive_windows"] = 3;
  req["new_device_setup"] = newDeviceSetup;
  req["trigger_source"] = triggerSource;

  String payload;
  serializeJson(req, payload);

  String response;
  String url = String(BACKEND_BASE_URL) + "/api/v1/calibrate/start";
  if (!httpPostJson(url, payload, response, MODEL_HTTP_TIMEOUT_MS)) {
    updateCalibrationRuntime("failed", 100, "Calibration start failed", false);
    return false;
  }

  DynamicJsonDocument doc(2048);
  if (deserializeJson(doc, response) != DeserializationError::Ok) {
    updateCalibrationRuntime("failed", 100, "Calibration start parse error", false);
    return false;
  }

  calibrationJobId = String((const char*)(doc["job_id"] | ""));
  if (calibrationJobId.length() == 0) {
    updateCalibrationRuntime("failed", 100, "Missing job id", false);
    return false;
  }

  updateCalibrationRuntime("queued", 1, "New device training queued", true);
  Blynk.logEvent("machine_alert", "Calibration started");
  return true;
}

void pollCalibrationJobStatus() {
  if (!backendEnabled || !calibrationInProgress || calibrationJobId.length() == 0) return;

  String response;
  String url = String(BACKEND_BASE_URL) + "/api/v1/calibrate/status/" + calibrationJobId;
  if (!httpGetJson(url, response, MODEL_HTTP_TIMEOUT_MS)) return;

  DynamicJsonDocument doc(16384);
  if (deserializeJson(doc, response) != DeserializationError::Ok) return;

  String status = String((const char*)(doc["status"] | "unknown"));
  String stage = String((const char*)(doc["stage"] | "unknown"));
  int progress = doc["progress"] | 0;
  String message = String((const char*)(doc["message"] | ""));

  updateCalibrationRuntime(stage, progress, message, status == "queued" || status == "running");

  if (status == "completed") {
    JsonObject result = doc["result"].as<JsonObject>();
    JsonObject pkg = result["model_package"].as<JsonObject>();
    if (!pkg.isNull() && applyModelPackage(pkg)) {
      updateCalibrationRuntime("completed", 100, "Weights applied to device", false);
      Blynk.logEvent("machine_alert", "Calibration completed");
    } else {
      updateCalibrationRuntime("failed", 100, "Model package apply failed", false);
    }
    calibrationJobId = "";
  }

  if (status == "failed") {
    String err = String((const char*)(doc["error"] | "Calibration failed"));
    updateCalibrationRuntime("failed", 100, err, false);
    calibrationJobId = "";
    Blynk.logEvent("machine_alert", "Calibration failed");
  }
}

void pullModelPackageFromBackend() {
  if (!backendEnabled) return;

  if (!refreshActiveBinding(true)) {
    Serial.println("Model pull skipped: no active stream binding");
    return;
  }

  String response;
  String url = String(BACKEND_BASE_URL) + "/api/v1/model/" + activeBindingMachineId + "/" + activeBindingDeviceId;

  if (!httpGetJson(url, response, MODEL_HTTP_TIMEOUT_MS)) {
    Serial.println("Backend model pull failed");
    return;
  }

  DynamicJsonDocument doc(8192);
  if (deserializeJson(doc, response) != DeserializationError::Ok) {
    Serial.println("Model JSON parse error");
    return;
  }

  JsonObject pkg = doc["model_package"].as<JsonObject>();
  if (pkg.isNull()) {
    Serial.println("No model package in response");
    return;
  }

  int incomingVersion = pkg["model_version"] | 0;
  String incomingChecksum = String((const char*)(pkg["checksum"] | ""));
  bool sameChecksum = incomingChecksum.length() > 0 && incomingChecksum == modelChecksum;
  if (incomingVersion < modelVersion && sameChecksum) return;

  if (incomingVersion < modelVersion && !sameChecksum) {
    Serial.print("Model version rollback accepted, local=");
    Serial.print(modelVersion);
    Serial.print(",incoming=");
    Serial.println(incomingVersion);
  }

  if (applyModelPackage(pkg)) {
    Serial.print("Model package applied, version=");
    Serial.println(modelVersion);
  }
}

void requestCalibrationFromBackend() {
  startCalibrationJobOnBackend(false, "scheduled_timer");
}

// -------------------- Stream + cloud --------------------

void streamUploaderTask(void* parameter) {
  unsigned long nextSampleAtMs = millis();
  unsigned long nextFlushAtMs = millis() + STREAM_FLUSH_INTERVAL_MS;

  for (;;) {
    if (!backendEnabled || (emergencyTriggered && !debugMode)) {
      vTaskDelay(pdMS_TO_TICKS(150));
      continue;
    }

    unsigned long now = millis();

    int sampleBursts = 0;
    while ((long)(now - nextSampleAtMs) >= 0 && sampleBursts < 3) {
      enqueueCurrentSampleForStream();
      nextSampleAtMs += STREAM_SAMPLE_INTERVAL_MS;
      sampleBursts++;
    }

    bool flushDue = (long)(now - nextFlushAtMs) >= 0;
    bool queuePressured = streamQueueCount >= STREAM_HIGH_WATERMARK;
    if (flushDue || queuePressured) {
      sendStreamToBackend();
      nextFlushAtMs = now + STREAM_FLUSH_INTERVAL_MS;
    }

    vTaskDelay(pdMS_TO_TICKS(STREAM_TASK_SLEEP_MS));
  }
}

void enqueueStreamSampleTask() {
  if (!backendEnabled) return;
  if (emergencyTriggered && !debugMode) return;
  enqueueCurrentSampleForStream();
}

void sendStreamToBackend() {
  if (!backendEnabled) return;
  if (emergencyTriggered && !debugMode) return;

  if (streamQueueCount <= 0) {
    return;
  }

  incrementStreamAttemptCount();

  int batchCount = 0;
  String payload = buildBatchPayload(STREAM_BATCH_SIZE, batchCount);
  if (batchCount <= 0) {
    return;
  }

  sendBatchOverHttpFallback(payload, batchCount);
}

void reportBackendTelemetry() {
  unsigned long attempts = streamAttemptCount;
  unsigned long success = streamSuccessCount;
  unsigned long fail = streamFailCount;
  int code = lastStreamHttpCode;
  String result = lastStreamResult;
  if (streamStateMutex && xSemaphoreTake(streamStateMutex, pdMS_TO_TICKS(30)) == pdTRUE) {
    attempts = streamAttemptCount;
    success = streamSuccessCount;
    fail = streamFailCount;
    code = lastStreamHttpCode;
    result = lastStreamResult;
    xSemaphoreGive(streamStateMutex);
  }

  Serial.print("STREAM_STATS,attempt=");
  Serial.print(attempts);
  Serial.print(",success=");
  Serial.print(success);
  Serial.print(",fail=");
  Serial.print(fail);
  Serial.print(",lastCode=");
  Serial.print(code);
  Serial.print(",lastResult=");
  Serial.print(result);
  Serial.print(",queued=");
  Serial.print(streamQueueCount);
  Serial.print(",dropped=");
  Serial.print(streamDroppedCount);
  Serial.println(",transport=http");
}

// Sync alert mode change to backend so dashboards reflect it immediately
void syncAlertModeToBackend(int mode) {
  if (!Blynk.connected() || activeBindingMachineId.length() == 0 || activeBindingDeviceId.length() == 0) {
    return;
  }
  
  String syncUrl = String(BACKEND_BASE_URL) + "/api/v1/device/" + 
    activeBindingMachineId + "/" + activeBindingDeviceId + "/alert-mode-sync?alert_mode=" + String(mode);
  
  String response;
  int code = 0;
  
  if (httpPostJson(syncUrl, "{}", response, 1000, &code, 1)) {
    Serial.print("ALERT_MODE_SYNC,code=");
    Serial.println(code);
  } else {
    Serial.print("ALERT_MODE_SYNC_FAILED,code=");
    Serial.println(code);
  }
}

// -------------------- Blynk handlers --------------------

// V8: calibration trigger
BLYNK_WRITE(V8) {
  int trigger = param.asInt();
  if (trigger == 1) {
    resetRuntimeForFreshCalibration();
    startCalibrationJobOnBackend(calibrationAsNewDevice, "blynk_button");
    Blynk.virtualWrite(V8, 0);
  }
}

// V13: new-device calibration mode
BLYNK_WRITE(V13) {
  calibrationAsNewDevice = (param.asInt() == 1);
}

// V14: motor relay
BLYNK_WRITE(V14) {
  motorOn = (param.asInt() == 1);
  applyIndicators();
  Serial.print("BLYNK_MOTOR,");
  Serial.println(motorOn ? "ON" : "OFF");
}

// V15: fan relay
BLYNK_WRITE(V15) {
  fanOn = (param.asInt() == 1);
  applyIndicators();
  Serial.print("BLYNK_FAN,");
  Serial.println(fanOn ? "ON" : "OFF");
}

// V20: buzzer
BLYNK_WRITE(V20) {
  buzzerManualOn = (param.asInt() == 1);
  applyIndicators();
  Serial.print("BLYNK_BUZZER,");
  Serial.println(buzzerManualOn ? "ON" : "OFF");
}

// V21: LED (optional manual override)
BLYNK_WRITE(V21) {
  ledManualOn = (param.asInt() == 1);
  applyIndicators();
  Serial.print("BLYNK_LED,");
  Serial.println(ledManualOn ? "ON" : "OFF");
}

// V22: SW420 threshold seconds
BLYNK_WRITE(V22) {
  int v = param.asInt();
  if (v < 1) v = 1;
  if (v > 300) v = 300;
  sw420ThresholdSec = v;

  if (sw420ThresholdSec >= sw420FrameSec) {
    sw420ThresholdSec = sw420FrameSec - 1;
    if (sw420ThresholdSec < 1) sw420ThresholdSec = 1;
  }

  Blynk.virtualWrite(V22, sw420ThresholdSec);

  Serial.print("SW420_THRESHOLD_SEC=");
  Serial.println(sw420ThresholdSec);
}

// V23: SW420 frame seconds
BLYNK_WRITE(V23) {
  int v = param.asInt();
  if (v < 2) v = 2;
  if (v > 600) v = 600;
  sw420FrameSec = v;

  if (sw420ThresholdSec >= sw420FrameSec) {
    sw420ThresholdSec = sw420FrameSec - 1;
    if (sw420ThresholdSec < 1) sw420ThresholdSec = 1;
    Blynk.virtualWrite(V22, sw420ThresholdSec);
  }

  Blynk.virtualWrite(V23, sw420FrameSec);

  Serial.print("SW420_FRAME_SEC=");
  Serial.println(sw420FrameSec);
}

// V24: MPU accel magnitude deviation threshold
BLYNK_WRITE(V24) {
  int v = param.asInt();
  if (v < 100) v = 100;
  if (v > 8000) v = 8000;
  mpuAccDeviationThreshold = (float)v;
  Blynk.virtualWrite(V24, (int)mpuAccDeviationThreshold);

  Serial.print("MPU_ACC_DEV_TH=");
  Serial.println(mpuAccDeviationThreshold);
}

// V25: MPU gyro magnitude deviation threshold
BLYNK_WRITE(V25) {
  int v = param.asInt();
  if (v < 20) v = 20;
  if (v > 2000) v = 2000;
  mpuGyroDeviationThreshold = (float)v;
  Blynk.virtualWrite(V25, (int)mpuGyroDeviationThreshold);

  Serial.print("MPU_GYRO_DEV_TH=");
  Serial.println(mpuGyroDeviationThreshold);
}

// V27: debug mode switch (1=debug ON, 0=production OFF)
BLYNK_WRITE(V27) {
  debugMode = (param.asInt() == 1);
  Blynk.virtualWrite(V27, debugMode ? 1 : 0);

  if (debugMode) {
    // Leaving production mode: clear latched shutdown state so debug behavior is immediate.
    emergencyTriggered = false;
    sw420FaultLatched = false;
    sw420FaultAnnounced = false;
    productionRelayCutoffApplied = false;
  }

  Serial.print("DEBUG_MODE=");
  Serial.println(debugMode ? "ON" : "OFF");
}

// V28: alert source mode (0=SW420 basic, 1=MPU deviation, 2=backend ML)
BLYNK_WRITE(V28) {
  int v = param.asInt();
  if (v < ALERT_MODE_SW420) v = ALERT_MODE_SW420;
  if (v > ALERT_MODE_BACKEND) v = ALERT_MODE_BACKEND;
  alertMode = v;
  Blynk.virtualWrite(V28, alertMode);

  anomalyAlertActive = false;

  // Keep only selected mode state active.
  if (!isSw420AlertMode()) {
    sw420FaultLatched = false;
    sw420DebugCooldownActive = false;
    sw420FailBuzzerActive = false;
    sw420FaultAnnounced = false;
    emergencyTriggered = false;
  }
  if (!isMpuAlertMode()) {
    mpuDeviationStreak = 0;
    mpuDeviationActive = false;
  }
  if (!isBackendAlertMode()) {
    backendAnomalyActive = false;
    backendHasScore = false;
    backendHasThreshold = false;
  } else if (backendAnomalyActive) {
    anomalyAlertActive = true;
    anomalyAlertStartMs = millis();
  }

  isMachineFailing = currentModeFailureActive();

  Serial.print("ALERT_MODE=");
  Serial.println(alertModeLabel());

  if (Blynk.connected()) {
    Blynk.virtualWrite(V26, "MODE=" + String(alertModeLabel()));
  }

  // Sync the mode change to the backend so dashboards reflect it immediately
  syncAlertModeToBackend(alertMode);
}

// -------------------- Main tasks --------------------

void readSensorsAndPredict() {
  int16_t raw_ax, raw_ay, raw_az, raw_gx, raw_gy, raw_gz;
  mpu.getMotion6(&raw_ax, &raw_ay, &raw_az, &raw_gx, &raw_gy, &raw_gz);

  accMag = sqrt((float)raw_ax * raw_ax + (float)raw_ay * raw_ay + (float)raw_az * raw_az);
  gyroMag = sqrt((float)raw_gx * raw_gx + (float)raw_gy * raw_gy + (float)raw_gz * raw_gz);

  gx = raw_gx;
  gy = raw_gy;
  gz = raw_gz;

  pushWindowSample(accMag, gyroMag, gx, gy, gz);

  accSum += accMag;
  if (accMag > accPeak) accPeak = accMag;
  sampleCount++;

  bool previousMpuDeviation = mpuDeviationActive;

  if (isMpuAlertMode()) {
    evaluateMpuDeviationMode();
    if (mpuDeviationActive && !previousMpuDeviation) {
      anomalyAlertActive = true;
      anomalyAlertStartMs = millis();
      Blynk.logEvent("machine_alert", "MPU deviation anomaly detected");
    }
  } else {
    mpuDeviationStreak = 0;
    mpuDeviationActive = false;
  }

  isMachineFailing = currentModeFailureActive();

  // Serial output
  Serial.print(accMag);      Serial.print(",");
  Serial.print(gyroMag);     Serial.print(",");
  Serial.print(gx);          Serial.print(",");
  Serial.print(gy);          Serial.print(",");
  Serial.print(gz);          Serial.print(",");
  Serial.println(isMachineFailing ? "1" : "0");
}

void updateBlynk() {
  if (!Blynk.connected()) return;
  if (emergencyTriggered && !debugMode) return;

  // ---- cache for change-only writes ----
  static int lastV7 = -1, lastV14 = -1, lastV15 = -1, lastV20 = -1, lastV21 = -1;
  static int lastV22 = -9999, lastV23 = -9999, lastV24 = -9999, lastV25 = -9999;
  static int lastV27 = -1, lastV28 = -1, lastV29 = -1;
  static int lastV16 = -1, lastV17 = -1, lastV18 = -9999;
  static String lastV19 = "", lastV26 = "";
  static float lastV30 = -99999.0, lastV31 = -99999.0;

  // status line + mode telemetry
  String statusLine = buildModeStatusLine();
  if (statusLine != lastV26) { Blynk.virtualWrite(V26, statusLine); lastV26 = statusLine; }

  if (alertMode != lastV29) { Blynk.virtualWrite(V29, alertMode); lastV29 = alertMode; }
  if (alertMode != lastV28) { Blynk.virtualWrite(V28, alertMode); lastV28 = alertMode; }

  float metric1 = 0.0, metric2 = 0.0;
  if (isSw420AlertMode()) {
    metric1 = sw420HighAccumulatedMs / 1000.0f;
    metric2 = (float)sw420ThresholdSec;
  } else if (isMpuAlertMode()) {
    metric1 = mpuLastAccDeviation;
    metric2 = mpuLastGyroDeviation;
  } else {
    metric1 = backendHasScore ? backendLastScore : -1.0f;
    metric2 = backendHasThreshold ? backendLastThreshold : -1.0f;
  }

  if (fabs(metric1 - lastV30) > 0.05f) { Blynk.virtualWrite(V30, metric1); lastV30 = metric1; }
  if (fabs(metric2 - lastV31) > 0.05f) { Blynk.virtualWrite(V31, metric2); lastV31 = metric2; }

  int v7 = isMachineFailing ? 255 : 0;
  if (v7 != lastV7) { Blynk.virtualWrite(V7, v7); lastV7 = v7; }

  int v14 = motorOn ? 1 : 0;
  int v15 = fanOn ? 1 : 0;
  int v20 = buzzerManualOn ? 1 : 0;
  int v21 = ledManualOn ? 1 : 0;
  int v27 = debugMode ? 1 : 0;

  if (v14 != lastV14) { Blynk.virtualWrite(V14, v14); lastV14 = v14; }
  if (v15 != lastV15) { Blynk.virtualWrite(V15, v15); lastV15 = v15; }
  if (v20 != lastV20) { Blynk.virtualWrite(V20, v20); lastV20 = v20; }
  if (v21 != lastV21) { Blynk.virtualWrite(V21, v21); lastV21 = v21; }
  if (v27 != lastV27) { Blynk.virtualWrite(V27, v27); lastV27 = v27; }

  if (sw420ThresholdSec != lastV22) { Blynk.virtualWrite(V22, sw420ThresholdSec); lastV22 = sw420ThresholdSec; }
  if (sw420FrameSec != lastV23) { Blynk.virtualWrite(V23, sw420FrameSec); lastV23 = sw420FrameSec; }

  int v24 = (int)mpuAccDeviationThreshold;
  int v25 = (int)mpuGyroDeviationThreshold;
  if (v24 != lastV24) { Blynk.virtualWrite(V24, v24); lastV24 = v24; }
  if (v25 != lastV25) { Blynk.virtualWrite(V25, v25); lastV25 = v25; }

  // stream/calibration stats only every ~15s (updateBlynk runs every 3s)
  static uint8_t slowTick = 0;
  slowTick++;
  if (slowTick >= 5) {
    slowTick = 0;

    int v16 = (int)streamSuccessCount;
    int v17 = (int)streamFailCount;
    int v18 = lastStreamHttpCode;
    String v19 = lastStreamResult;

    if (v16 != lastV16) { Blynk.virtualWrite(V16, v16); lastV16 = v16; }
    if (v17 != lastV17) { Blynk.virtualWrite(V17, v17); lastV17 = v17; }
    if (v18 != lastV18) { Blynk.virtualWrite(V18, v18); lastV18 = v18; }
    if (v19 != lastV19) { Blynk.virtualWrite(V19, v19); lastV19 = v19; }

    pushCalibrationToBlynk();
  }
}

void updateThingSpeak() {
  if (emergencyTriggered && !debugMode) return;

  float accAvg = 0;
  if (sampleCount > 0) accAvg = accSum / sampleCount;

  int sw420val = isSw420ActiveFiltered() ? 1 : 0;

  ThingSpeak.setField(1, accAvg);
  ThingSpeak.setField(2, accPeak);
  ThingSpeak.setField(3, gx);
  ThingSpeak.setField(4, gy);
  ThingSpeak.setField(5, gz);
  ThingSpeak.setField(6, sw420val);

  int status = ThingSpeak.writeFields(TS_CHANNEL_ID, TS_WRITE_KEY);
  if (status == 200) {
    Serial.println("ThingSpeak update success (avg + peak)");
    accSum = 0;
    accPeak = 0;
    sampleCount = 0;
  } else {
    Serial.print("ThingSpeak error ");
    Serial.println(status);
  }
}

// -------------------- setup/loop --------------------

void setup() {
  Serial.begin(115200);
  delay(1000);
  Serial.print("RESET_REASON=");
  Serial.println((int)esp_reset_reason());
  Serial.println("S1: boot");

  Wire.begin(21, 22);
  Serial.println("S2: wire");
  mpu.initialize();
  Serial.println("S3: mpu");

  pinMode(SW420_PIN, INPUT);
  pinMode(RELAY_MOTOR_PIN, OUTPUT);
  pinMode(RELAY_FAN_PIN, OUTPUT);
  pinMode(BUZZER_PIN, OUTPUT);
  pinMode(LED_PIN, OUTPUT);

  // Keep high-current outputs OFF on boot to avoid brownout/power resets.
  motorOn = false;
  fanOn = false;
  buzzerManualOn = false;
  ledManualOn = false;
  applyIndicators();
  Serial.println("S4: pins");

  calibrateSw420Polarity();
  if (SW420_USE_INTERRUPT) {
    attachInterrupt(digitalPinToInterrupt(SW420_PIN), emergencyKillSwitch, CHANGE);
    Serial.println("S5: interrupt enabled");
  } else {
    Serial.println("S5: interrupt disabled (polling mode)");
  }

  // Stable WiFi setup
  WiFi.mode(WIFI_STA);
  WiFi.setAutoReconnect(true);
  WiFi.setSleep(false);
  WiFi.begin(ssid, password);
  unsigned long wifiWaitStart = millis();
  while (WiFi.status() != WL_CONNECTED && millis() - wifiWaitStart < 8000) {
    delay(200);
    Serial.print(".");
  }
  Serial.println();

  if (WiFi.status() == WL_CONNECTED) {
    Serial.print("S6: wifi connected, ip=");
    Serial.println(WiFi.localIP());
  } else {
    Serial.println("S6: wifi pending, retry in loop");
  }

  Blynk.config(BLYNK_AUTH_TOKEN);
  if (WiFi.status() == WL_CONNECTED) {
    Blynk.connect(1500);
  }
  Serial.println("S6b: blynk configured");

  ThingSpeak.begin(tsClient);
  configTime(gmtOffset_sec, daylightOffset_sec, ntpServer);
  Serial.println("S7: cloud init");

  loadModelFromNvs();
  updateCalibrationRuntime("idle", 0, "Ready", false);
  Serial.println("S8: model loaded");

  streamStateMutex = xSemaphoreCreateMutex();
  if (streamStateMutex == nullptr) {
    Serial.println("S8b: stream mutex init failed");
  }

  // init SW420 frame timers
  sw420FrameStartMs = millis();
  sw420LastSampleMs = millis();

  // push defaults
  if (Blynk.connected()) {
    Blynk.virtualWrite(V22, sw420ThresholdSec);
    Blynk.virtualWrite(V23, sw420FrameSec);
    Blynk.virtualWrite(V24, (int)mpuAccDeviationThreshold);
    Blynk.virtualWrite(V25, (int)mpuGyroDeviationThreshold);
    Blynk.virtualWrite(V27, debugMode ? 1 : 0);
    Blynk.virtualWrite(V28, alertMode);
    Blynk.virtualWrite(V26, "MODE=" + String(alertModeLabel()) + " | INIT");
    Blynk.virtualWrite(V29, alertMode);
    Blynk.virtualWrite(V30, 0);
    Blynk.virtualWrite(V31, 0);
  }

  // Timers
  timer.setInterval(100L, readSensorsAndPredict);
  timer.setInterval(3000L, updateBlynk);     // reduced Blynk writes
  timer.setInterval(1000L, enqueueStreamSampleTask);
  timer.setInterval(5000L, sendStreamToBackend);
  timer.setInterval(10000L, reportBackendTelemetry);
  timer.setInterval(30000L, updateThingSpeak);
  timer.setInterval(BINDING_REFRESH_INTERVAL_MS, refreshActiveBindingTask);

  timer.setTimeout(6000L, refreshActiveBindingTask);
  timer.setTimeout(15000L, pullModelPackageFromBackend);
  timer.setInterval(300000L, pullModelPackageFromBackend);      // 5 min
  timer.setInterval(1800000L, requestCalibrationFromBackend);   // 30 min
  timer.setInterval(2000L, pollCalibrationJobStatus);           // 2 sec

  Serial.println("S8c: stream uploader task disabled (timer mode)");

  Serial.println("S9: setup complete");
}

void loop() {
  ensureWiFiConnection();

  processSw420Trigger();

  if (isSw420AlertMode() && sw420FaultLatched && !sw420FaultAnnounced) {
    if (!debugMode) {
      Blynk.logEvent("critical_failure", "SW420 frame threshold exceeded");
      Serial.println("EMERGENCY WARNING: SW420 frame threshold exceeded");
      Blynk.virtualWrite(V5, 1);
      Blynk.virtualWrite(V26, "FAIL | LATCHED");
    } else {
      Serial.println("DEBUG WARNING: SW420 frame threshold exceeded (shutdown bypassed)");
    }
    sw420FaultAnnounced = true;
  }

  // IMPORTANT: no hard infinite lock here, so 5s buzzer window can run
  // and then outputs are cut in updateAlertOutputs() when debugMode=false.

  if (WiFi.status() == WL_CONNECTED) {
    if (!Blynk.connected() && (millis() - lastBlynkConnectAttemptMs) > BLYNK_RECONNECT_INTERVAL_MS) {
      lastBlynkConnectAttemptMs = millis();
      Blynk.connect(600);
    }
    Blynk.run();
  }

  timer.run();
  updateAlertOutputs();

  static unsigned long t = 0;
  if (millis() - t > 3000) {
    t = millis();
    Serial.print("HB wifi=");
    Serial.print(WiFi.status());
    Serial.print(" blynk=");
    Serial.print(Blynk.connected() ? "1" : "0");
    Serial.print(" acc=");
    Serial.print(accMag);
    Serial.print(" fail=");
    Serial.print(isMachineFailing ? "1" : "0");
    Serial.print(" mode=");
    Serial.print(alertModeLabel());
    Serial.print(" debug=");
    Serial.print(debugMode ? "1" : "0");
    Serial.print(" th=");
    Serial.print(sw420ThresholdSec);
    Serial.print(" frame=");
    Serial.println(sw420FrameSec);
  }
}