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
unsigned long sw420WindowStartMs = 0;
int sw420TriggerCountInWindow = 0;
unsigned long sw420DebugCooldownUntilMs = 0;

const unsigned long SW420_DEBOUNCE_MS = 150;
const unsigned long SW420_WINDOW_MS = 5000;
const int SW420_MAX_TRIGGERS = 3;
const unsigned long SW420_DEBUG_COOLDOWN_MS = 4000;

// Indicator control state
bool motorOn = true;
bool fanOn = true;
bool buzzerManualOn = false;
bool ledManualOn = false;

// Anomaly alert behavior: LED OFF + buzzer pulse for 5s
bool anomalyAlertActive = false;
unsigned long anomalyAlertStartMs = 0;
const unsigned long ANOMALY_ALERT_DURATION_MS = 5000;
const unsigned long ANOMALY_BUZZER_PULSE_MS = 220;

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

// Feature window
const int WINDOW_SIZE = 10;
float accWindow[WINDOW_SIZE] = {0};
float gyroWindow[WINDOW_SIZE] = {0};
float gxWindow[WINDOW_SIZE] = {0};
float gyWindow[WINDOW_SIZE] = {0};
float gzWindow[WINDOW_SIZE] = {0};
int windowPos = 0;
int windowCount = 0;

const int STREAM_HTTP_TIMEOUT_MS = 3500;
const int MODEL_HTTP_TIMEOUT_MS = 5000;

// -------------------- Helpers --------------------

void applyIndicators() {
  updateAlertOutputs();
}

void updateAlertOutputs() {
  unsigned long now = millis();

  if (sw420DebugCooldownActive && debugMode && now >= sw420DebugCooldownUntilMs) {
    sw420DebugCooldownActive = false;
    sw420FaultAnnounced = false;
  }

  if (anomalyAlertActive && (now - anomalyAlertStartMs >= ANOMALY_ALERT_DURATION_MS)) {
    anomalyAlertActive = false;
  }

  bool sw420Muted = sw420DebugCooldownActive && debugMode && now < sw420DebugCooldownUntilMs;

  // Normal state: LED ON, buzzer OFF
  bool ledState = true;
  bool buzzerState = false;

  // Anomaly state: LED OFF + buzzer pulse
  if (anomalyAlertActive) {
    ledState = false;
    buzzerState = ((now / ANOMALY_BUZZER_PULSE_MS) % 2) == 0;
  }

  // Manual controls only when no anomaly and no SW420 block
  if (!anomalyAlertActive && !sw420FaultLatched && !sw420Muted) {
    ledState = ledState || ledManualOn;
    buzzerState = buzzerManualOn;
  }

  // SW420 safety highest priority
  if (sw420FaultLatched || sw420Muted) {
    ledState = false;
    buzzerState = false;
    if (!debugMode) {
      motorOn = false;
      fanOn = false;
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

void processSw420Trigger() {
  bool pending = false;

  noInterrupts();
  pending = sw420InterruptPending;
  sw420InterruptPending = false;
  interrupts();

  if (!pending) return;
  if (digitalRead(SW420_PIN) != HIGH) return;

  unsigned long now = millis();
  if (now - sw420LastConfirmedTriggerMs < SW420_DEBOUNCE_MS) return;
  sw420LastConfirmedTriggerMs = now;

  if (sw420WindowStartMs == 0 || now - sw420WindowStartMs > SW420_WINDOW_MS) {
    sw420WindowStartMs = now;
    sw420TriggerCountInWindow = 0;
    sw420FaultAnnounced = false;
  }

  sw420TriggerCountInWindow++;

  if (sw420TriggerCountInWindow >= SW420_MAX_TRIGGERS) {
    if (debugMode) {
      sw420DebugCooldownActive = true;
      sw420DebugCooldownUntilMs = now + SW420_DEBUG_COOLDOWN_MS;
      sw420FaultLatched = false;
      emergencyTriggered = false;
      sw420FaultAnnounced = false;
      Serial.println("DEBUG WARNING: SW-420 trigger window exceeded (temporary mute active)");
    } else {
      sw420FaultLatched = true;
      sw420FaultAnnounced = false;
      emergencyTriggered = true;
    }
  } else {
    Serial.print("DEBUG WARNING: SW-420 trigger detected (debounced, count=");
    Serial.print(sw420TriggerCountInWindow);
    Serial.print(", window_ms=");
    Serial.print(SW420_WINDOW_MS);
    Serial.println(")");
  }

  emergencyTriggered = sw420FaultLatched;
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

bool httpPostJson(const String& url, const String& payload, String& responseOut, int timeoutMs, int* statusCodeOut = nullptr) {
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
    http.addHeader("Content-Type", "application/json");
    http.addHeader("Accept", "application/json");

    int code = http.POST(payload);
    if (statusCodeOut) *statusCodeOut = code;
    if (code > 0) responseOut = http.getString();

    http.end();

    if (code >= 200 && code < 300) {
      return true;
    }

    if (code != -11 && code != -1) {
      return false;
    }

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

    if (code >= 200 && code < 300) {
      return true;
    }

    if (code != -11 && code != -1) {
      return false;
    }

    ensureWiFiConnection();
    delay(40);
  }

  return false;
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
    Serial.print("BINDING_ACTIVE,machine=");
    Serial.print(activeBindingMachineId);
    Serial.print(",device=");
    Serial.println(activeBindingDeviceId);
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
  req["sample_rate_hz"] = 10;
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
  if (incomingVersion < modelVersion) return;

  if (applyModelPackage(pkg)) {
    Serial.print("Model package applied, version=");
    Serial.println(modelVersion);
  }
}

void requestCalibrationFromBackend() {
  startCalibrationJobOnBackend(false, "scheduled_timer");
}

// -------------------- Stream + cloud --------------------

void sendStreamToBackend() {
  if (!backendEnabled) return;
  if (emergencyTriggered && !debugMode) return;

  streamAttemptCount++;

  StaticJsonDocument<512> req;

  JsonObject sample = req.createNestedObject("sample");
  String apiTs;
  if (getIsoTimestampForApi(apiTs)) {
    sample["timestamp"] = apiTs;
  }
  req["esp_model_version"] = modelVersion;
  req["esp_model_checksum"] = modelChecksum;
  sample["accMag"] = accMag;
  sample["gyroMag"] = gyroMag;
  sample["gx"] = gx;
  sample["gy"] = gy;
  sample["gz"] = gz;
  sample["sw420"] = digitalRead(SW420_PIN);

  String payload;
  serializeJson(req, payload);

  String response;
  String url = String(BACKEND_BASE_URL) + "/api/v1/stream";
  int httpCode = 0;

  if (!httpPostJson(url, payload, response, STREAM_HTTP_TIMEOUT_MS, &httpCode)) {
    streamFailCount++;
    lastStreamHttpCode = httpCode;
    if (httpCode == -1) lastStreamResult = "FAIL: WIFI_DOWN";
    else if (httpCode == -2) lastStreamResult = "FAIL: HTTP_BEGIN";
    else if (httpCode == -11) lastStreamResult = "FAIL: HTTP_TIMEOUT";
    else lastStreamResult = "FAIL: HTTP_" + String(httpCode);

    Serial.print("STREAM_FAIL,code=");
    Serial.print(httpCode);
    Serial.print(",wifi=");
    Serial.println(WiFi.status());
    Serial.print("STREAM_FAIL_BODY=");
    Serial.println(response);
    return;
  }

  streamSuccessCount++;
  lastStreamHttpCode = httpCode;
  lastStreamResult = "OK";

  Serial.print("STREAM_OK,code=");
  Serial.print(httpCode);
  Serial.print(",attempt=");
  Serial.print(streamAttemptCount);
  Serial.print(",success=");
  Serial.println(streamSuccessCount);

  DynamicJsonDocument doc(2048);
  if (deserializeJson(doc, response) == DeserializationError::Ok) {
    if (doc.containsKey("is_anomaly") && doc["is_anomaly"].as<bool>()) {
      isMachineFailing = true;
    }
  }
}

void reportBackendTelemetry() {
  Serial.print("STREAM_STATS,attempt=");
  Serial.print(streamAttemptCount);
  Serial.print(",success=");
  Serial.print(streamSuccessCount);
  Serial.print(",fail=");
  Serial.print(streamFailCount);
  Serial.print(",lastCode=");
  Serial.print(lastStreamHttpCode);
  Serial.print(",lastResult=");
  Serial.println(lastStreamResult);
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

  bool wasFailing = isMachineFailing;
  isMachineFailing = evaluateLocalAI();

  if (isMachineFailing && !wasFailing) {
    anomalyAlertActive = true;
    anomalyAlertStartMs = millis();
    Blynk.logEvent("machine_alert", "AI anomaly detected by local edge inference");
  }

  // Serial output restored (for plotting/IA pipeline)
  Serial.print(accMag);      Serial.print(",");
  Serial.print(gyroMag);     Serial.print(",");
  Serial.print(gx);          Serial.print(",");
  Serial.print(gy);          Serial.print(",");
  Serial.print(gz);          Serial.print(",");
  Serial.println(isMachineFailing ? "1" : "0");
}

void updateBlynk() {
  if (emergencyTriggered && !debugMode) return;

  int sw420val = digitalRead(SW420_PIN);

  Blynk.virtualWrite(V0, accMag);
  Blynk.virtualWrite(V1, gyroMag);
  Blynk.virtualWrite(V2, gx);
  Blynk.virtualWrite(V3, gy);
  Blynk.virtualWrite(V4, gz);
  Blynk.virtualWrite(V5, sw420val);
  Blynk.virtualWrite(V6, getTimeString());
  Blynk.virtualWrite(V7, isMachineFailing ? 255 : 0);

  Blynk.virtualWrite(V14, motorOn ? 1 : 0);
  Blynk.virtualWrite(V15, fanOn ? 1 : 0);
  Blynk.virtualWrite(V20, buzzerManualOn ? 1 : 0);
  Blynk.virtualWrite(V21, ledManualOn ? 1 : 0);

  Blynk.virtualWrite(V16, (int)streamSuccessCount);
  Blynk.virtualWrite(V17, (int)streamFailCount);
  Blynk.virtualWrite(V18, lastStreamHttpCode);
  Blynk.virtualWrite(V19, lastStreamResult);

  pushCalibrationToBlynk();
}

void updateThingSpeak() {
  if (emergencyTriggered && !debugMode) return;

  float accAvg = 0;
  if (sampleCount > 0) accAvg = accSum / sampleCount;

  int sw420val = digitalRead(SW420_PIN);

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
  motorOn = true;
  fanOn = true;
  buzzerManualOn = false;
  ledManualOn = false;
  applyIndicators();
  Serial.println("S4: pins");

  attachInterrupt(digitalPinToInterrupt(SW420_PIN), emergencyKillSwitch, RISING);
  Serial.println("S5: interrupt");

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

  // Full timers restored
  timer.setInterval(100L, readSensorsAndPredict);
  timer.setInterval(1000L, updateBlynk);
  timer.setInterval(1000L, sendStreamToBackend);
  timer.setInterval(5000L, reportBackendTelemetry);
  timer.setInterval(16000L, updateThingSpeak);
  timer.setInterval(BINDING_REFRESH_INTERVAL_MS, refreshActiveBindingTask);

  timer.setTimeout(6000L, refreshActiveBindingTask);
  timer.setTimeout(15000L, pullModelPackageFromBackend);
  timer.setInterval(300000L, pullModelPackageFromBackend);      // 5 min
  timer.setInterval(1800000L, requestCalibrationFromBackend);   // 30 min
  timer.setInterval(2000L, pollCalibrationJobStatus);           // 2 sec

  Serial.println("S9: setup complete");
}

void loop() {
  ensureWiFiConnection();

  processSw420Trigger();

  if (sw420FaultLatched && !sw420FaultAnnounced) {
    if (!debugMode) {
      Blynk.logEvent("critical_failure", "SW-420 hardware kill switch activated");
      Serial.println("EMERGENCY SHUTDOWN: SW-420 trigger window exceeded");
      Blynk.virtualWrite(V5, 1);
    } else {
      Serial.println("DEBUG WARNING: SW-420 trigger window exceeded (shutdown bypassed)");
    }
    sw420FaultAnnounced = true;
  }

  if (sw420FaultLatched && !debugMode) {
    motorOn = false;
    fanOn = false;
    buzzerManualOn = false;
    ledManualOn = false;

    updateAlertOutputs();

    while (true) {
      digitalWrite(RELAY_MOTOR_PIN, RELAY_OFF);
      digitalWrite(RELAY_FAN_PIN, RELAY_OFF);
      digitalWrite(LED_PIN, LED_OFF);
      digitalWrite(BUZZER_PIN, BUZZER_OFF);
      delay(150);
    }
  }

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
    Serial.println(isMachineFailing ? "1" : "0");
  }
}
