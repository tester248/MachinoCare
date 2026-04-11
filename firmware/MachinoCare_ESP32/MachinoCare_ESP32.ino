// ============================================================
// MachinoCare - Final Firmware (AI + Failsafe + Cloud + Backend)
// ============================================================

#define BLYNK_TEMPLATE_ID   "TMPL3LJfoU1on"
#define BLYNK_TEMPLATE_NAME "Machinocare"
#define BLYNK_AUTH_TOKEN    "REPLACE_WITH_BLYNK_AUTH_TOKEN"

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
const char* ssid     = "REPLACE_WITH_WIFI_SSID";
const char* password = "REPLACE_WITH_WIFI_PASSWORD";
unsigned long TS_CHANNEL_ID = 3336916;
const char* TS_WRITE_KEY = "REPLACE_WITH_THINGSPEAK_WRITE_KEY";
WiFiClient tsClient;

// Backend settings (Railway URL)
const char* BACKEND_BASE_URL = "https://REPLACE_WITH_RAILWAY_DOMAIN.up.railway.app";
const char* MACHINE_ID = "Fan_1";
const char* DEVICE_ID = "esp32_fan_1";
bool backendEnabled = true;

// NTP settings
const char* ntpServer = "pool.ntp.org";
const long gmtOffset_sec = 19800;
const int daylightOffset_sec = 0;

// Hardware
const int SW420_PIN = 34;
const int RELAY_PIN = 5;

// true = no relay cut (safe debugging), false = hard failsafe
bool debugMode = true;

MPU6050 mpu;
BlynkTimer timer;
Preferences prefs;
WiFiClientSecure apiClient;

float accMag = 0;
float gyroMag = 0;
int gx = 0, gy = 0, gz = 0;

// Fallback threshold on raw acceleration magnitude
float aiThreshold = 25000.0;

volatile bool emergencyTriggered = false;
bool isMachineFailing = false;

// ThingSpeak window stats
float accSum = 0;
float accPeak = 0;
int sampleCount = 0;

// Lightweight edge model package (distilled from backend)
const int FEATURE_DIM = 8;
float featureMeans[FEATURE_DIM] = {0};
float featureStds[FEATURE_DIM] = {1, 1, 1, 1, 1, 1, 1, 1};
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

// Calibration job tracking (from backend)
String calibrationJobId = "";
bool calibrationInProgress = false;
int calibrationProgress = 0;
String calibrationStage = "idle";
String calibrationMessage = "Idle";
bool calibrationAsNewDevice = true;

// Backend delivery telemetry
unsigned long streamAttemptCount = 0;
unsigned long streamSuccessCount = 0;
unsigned long streamFailCount = 0;
int lastStreamHttpCode = 0;
String lastStreamResult = "INIT";

// 1 second feature window from 100 ms sensor ticks
const int WINDOW_SIZE = 10;
float accWindow[WINDOW_SIZE] = {0};
float gyroWindow[WINDOW_SIZE] = {0};
float gxWindow[WINDOW_SIZE] = {0};
float gyWindow[WINDOW_SIZE] = {0};
float gzWindow[WINDOW_SIZE] = {0};
int windowPos = 0;
int windowCount = 0;

const int STREAM_HTTP_TIMEOUT_MS = 1400;
const int MODEL_HTTP_TIMEOUT_MS = 2500;

void IRAM_ATTR emergencyKillSwitch() {
  if (!debugMode) {
    digitalWrite(RELAY_PIN, LOW);
  }
  emergencyTriggered = true;
}

String getTimeString() {
  struct tm timeinfo;
  if (!getLocalTime(&timeinfo)) return "Time_Error";
  char timeStringBuff[20];
  strftime(timeStringBuff, sizeof(timeStringBuff), "%Y-%m-%d %H:%M:%S", &timeinfo);
  return String(timeStringBuff);
}

void pushWindowSample(float aMag, float gMag, float x, float y, float z) {
  accWindow[windowPos] = aMag;
  gyroWindow[windowPos] = gMag;
  gxWindow[windowPos] = x;
  gyWindow[windowPos] = y;
  gzWindow[windowPos] = z;

  windowPos = (windowPos + 1) % WINDOW_SIZE;
  if (windowCount < WINDOW_SIZE) {
    windowCount++;
  }
}

int orderedIndex(int logicalIdx) {
  if (windowCount < WINDOW_SIZE) {
    return logicalIdx;
  }
  return (windowPos + logicalIdx) % WINDOW_SIZE;
}

float meanOf(float* arr, int n) {
  if (n <= 0) return 0.0;
  float s = 0.0;
  for (int i = 0; i < n; i++) {
    s += arr[orderedIndex(i)];
  }
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
  if (windowCount <= 1) {
    return false;
  }

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
  if (!modelReady) {
    return accMag > aiThreshold;
  }

  float features[FEATURE_DIM] = {0};
  if (!computeFeatureVector(features)) {
    return accMag > aiThreshold;
  }

  float score = scoreDistilledModel(features);

  if (score >= modelHysteresisHigh) {
    anomalyStreak++;
  } else if (score < modelHysteresisLow) {
    anomalyStreak = 0;
  }

  bool anomaly = anomalyStreak >= modelMinConsecutiveWindows;

  Serial.print("MODEL,");
  Serial.print(score); Serial.print(",");
  Serial.print(modelHysteresisHigh); Serial.print(",");
  Serial.println(anomaly ? 1 : 0);

  return anomaly;
}

bool httpPostJson(
  const String& url,
  const String& payload,
  String& responseOut,
  int timeoutMs,
  int* statusCodeOut = nullptr
) {
  if (!backendEnabled || WiFi.status() != WL_CONNECTED) {
    if (statusCodeOut != nullptr) {
      *statusCodeOut = -1;
    }
    return false;
  }

  HTTPClient http;
  apiClient.setInsecure();

  if (!http.begin(apiClient, url)) {
    if (statusCodeOut != nullptr) {
      *statusCodeOut = -2;
    }
    return false;
  }

  http.setConnectTimeout(timeoutMs);
  http.setTimeout(timeoutMs);
  http.addHeader("Content-Type", "application/json");

  int code = http.POST(payload);
  if (statusCodeOut != nullptr) {
    *statusCodeOut = code;
  }
  if (code > 0) {
    responseOut = http.getString();
  }

  http.end();
  return (code >= 200 && code < 300);
}

bool httpGetJson(const String& url, String& responseOut, int timeoutMs, int* statusCodeOut = nullptr) {
  if (!backendEnabled || WiFi.status() != WL_CONNECTED) {
    if (statusCodeOut != nullptr) {
      *statusCodeOut = -1;
    }
    return false;
  }

  HTTPClient http;
  apiClient.setInsecure();

  if (!http.begin(apiClient, url)) {
    if (statusCodeOut != nullptr) {
      *statusCodeOut = -2;
    }
    return false;
  }

  http.setConnectTimeout(timeoutMs);
  http.setTimeout(timeoutMs);

  int code = http.GET();
  if (statusCodeOut != nullptr) {
    *statusCodeOut = code;
  }
  if (code > 0) {
    responseOut = http.getString();
  }

  http.end();
  return (code >= 200 && code < 300);
}

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
  if (!pkg.containsKey("feature_means") || !pkg.containsKey("feature_stds") || !pkg.containsKey("weights")) {
    return false;
  }

  JsonArray means = pkg["feature_means"].as<JsonArray>();
  JsonArray stds = pkg["feature_stds"].as<JsonArray>();
  JsonArray wts = pkg["weights"].as<JsonArray>();

  if (means.size() != FEATURE_DIM || stds.size() != FEATURE_DIM || wts.size() != FEATURE_DIM) {
    return false;
  }

  for (int i = 0; i < FEATURE_DIM; i++) {
    featureMeans[i] = means[i].as<float>();
    featureStds[i] = stds[i].as<float>();
    if (fabs(featureStds[i]) < 1e-6) featureStds[i] = 1.0;
    modelWeights[i] = wts[i].as<float>();
  }

  modelBias = pkg["bias"] | 0.0;
  modelDecisionThreshold = pkg["decision_threshold"] | 0.55;
  modelHysteresisHigh = pkg["hysteresis_high"] | modelDecisionThreshold;
  modelHysteresisLow = pkg["hysteresis_low"] | (modelDecisionThreshold * 0.9);
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

void updateCalibrationRuntime(const String& stage, int progress, const String& message, bool inProgress) {
  calibrationStage = stage;
  calibrationProgress = progress;
  calibrationMessage = message;
  calibrationInProgress = inProgress;
}

void pushCalibrationToBlynk() {
  Blynk.virtualWrite(V10, calibrationStage + " | " + calibrationMessage);
  Blynk.virtualWrite(V11, calibrationProgress);
  Blynk.virtualWrite(V12, calibrationInProgress ? 255 : 0);
  Blynk.virtualWrite(V9, modelVersion);
}

bool startCalibrationJobOnBackend(bool newDeviceSetup, const char* triggerSource) {
  if (!backendEnabled || WiFi.status() != WL_CONNECTED) {
    return false;
  }
  if (calibrationInProgress) {
    return true;
  }

  StaticJsonDocument<768> req;
  req["machine_id"] = MACHINE_ID;
  req["device_id"] = DEVICE_ID;
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
  Blynk.logEvent("machine_alert", "Calibration started for device setup");
  return true;
}

void pollCalibrationJobStatus() {
  if (!backendEnabled) return;
  if (!calibrationInProgress) return;
  if (calibrationJobId.length() == 0) return;

  String response;
  String url = String(BACKEND_BASE_URL) + "/api/v1/calibrate/status/" + calibrationJobId;
  if (!httpGetJson(url, response, MODEL_HTTP_TIMEOUT_MS)) {
    return;
  }

  DynamicJsonDocument doc(16384);
  if (deserializeJson(doc, response) != DeserializationError::Ok) {
    return;
  }

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
      Blynk.logEvent("machine_alert", "Calibration completed and weights applied");
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

  String response;
  String url = String(BACKEND_BASE_URL) + "/api/v1/model/" + String(MACHINE_ID) + "/" + String(DEVICE_ID);

  if (!httpGetJson(url, response, MODEL_HTTP_TIMEOUT_MS)) {
    Serial.println("Backend model pull failed");
    return;
  }

  DynamicJsonDocument doc(8192);
  DeserializationError err = deserializeJson(doc, response);
  if (err) {
    Serial.println("Model JSON parse error");
    return;
  }

  JsonObject pkg = doc["model_package"].as<JsonObject>();
  if (pkg.isNull()) {
    Serial.println("No model package in response");
    return;
  }

  int incomingVersion = pkg["model_version"] | 0;
  if (incomingVersion < modelVersion) {
    return;
  }

  if (applyModelPackage(pkg)) {
    Serial.print("Model package applied, version=");
    Serial.println(modelVersion);
  }
}

void requestCalibrationFromBackend() {
  startCalibrationJobOnBackend(false, "scheduled_timer");
}

void sendStreamToBackend() {
  if (!backendEnabled) return;
  if (emergencyTriggered && !debugMode) return;

  streamAttemptCount++;

  StaticJsonDocument<512> req;
  req["machine_id"] = MACHINE_ID;
  req["device_id"] = DEVICE_ID;

  JsonObject sample = req.createNestedObject("sample");
  sample["timestamp"] = getTimeString();
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
    if (httpCode == -1) {
      lastStreamResult = "FAIL: WIFI_DOWN";
    } else if (httpCode == -2) {
      lastStreamResult = "FAIL: HTTP_BEGIN";
    } else {
      lastStreamResult = "FAIL: HTTP_" + String(httpCode);
    }

    Serial.print("STREAM_FAIL,code=");
    Serial.print(httpCode);
    Serial.print(",wifi=");
    Serial.println(WiFi.status());
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

// Blynk button V8: start training as new device configuration
BLYNK_WRITE(V8) {
  int trigger = param.asInt();
  if (trigger == 1) {
    startCalibrationJobOnBackend(calibrationAsNewDevice, "blynk_button");
    Blynk.virtualWrite(V8, 0);
  }
}

// Blynk switch V13: choose whether button means new-device setup
BLYNK_WRITE(V13) {
  calibrationAsNewDevice = (param.asInt() == 1);
}

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
    Blynk.logEvent("machine_alert", "AI anomaly detected by local edge inference");
  }

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
  Blynk.virtualWrite(V16, (int)streamSuccessCount);
  Blynk.virtualWrite(V17, (int)streamFailCount);
  Blynk.virtualWrite(V18, lastStreamHttpCode);
  Blynk.virtualWrite(V19, lastStreamResult);
  pushCalibrationToBlynk();
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

void updateThingSpeak() {
  if (emergencyTriggered && !debugMode) return;

  float accAvg = 0;
  if (sampleCount > 0) {
    accAvg = accSum / sampleCount;
  }

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

void setup() {
  Serial.begin(115200);
  Wire.begin(21, 22);
  mpu.initialize();

  pinMode(SW420_PIN, INPUT);
  pinMode(RELAY_PIN, OUTPUT);
  digitalWrite(RELAY_PIN, HIGH);

  attachInterrupt(digitalPinToInterrupt(SW420_PIN), emergencyKillSwitch, RISING);

  if (debugMode) {
    Serial.println("DEBUG MODE ON: relay kill disabled");
  } else {
    Serial.println("PRODUCTION MODE: relay kill armed");
  }

  Blynk.begin(BLYNK_AUTH_TOKEN, ssid, password);
  ThingSpeak.begin(tsClient);
  configTime(gmtOffset_sec, daylightOffset_sec, ntpServer);

  loadModelFromNvs();
  updateCalibrationRuntime("idle", 0, "Ready", false);

  // Fast sensor loop + cloud loops
  timer.setInterval(100L, readSensorsAndPredict);
  timer.setInterval(1000L, updateBlynk);
  timer.setInterval(1000L, sendStreamToBackend);
  timer.setInterval(5000L, reportBackendTelemetry);
  timer.setInterval(16000L, updateThingSpeak);

  // Backend sync
  timer.setTimeout(15000L, pullModelPackageFromBackend);
  timer.setInterval(300000L, pullModelPackageFromBackend);      // every 5 min
  timer.setInterval(1800000L, requestCalibrationFromBackend);   // every 30 min
  timer.setInterval(2000L, pollCalibrationJobStatus);           // every 2 sec
}

void loop() {
  if (emergencyTriggered) {
    if (!debugMode) {
      Blynk.logEvent("critical_failure", "SW-420 hardware kill switch activated");
      Serial.println("EMERGENCY SHUTDOWN: system locked");
      Blynk.virtualWrite(V5, 1);
      while (true) {
        delay(1000);
      }
    } else {
      Serial.println("DEBUG WARNING: SW-420 trigger detected (shutdown bypassed)");
      emergencyTriggered = false;
    }
  }

  Blynk.run();
  timer.run();
}
