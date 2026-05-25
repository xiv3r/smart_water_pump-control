/*
 * ============================================================================================
 * SYSTEM: Automatic Pump & Compressor Control System (Dual-Core + Master/Slave)
 * HARDWARE: ESP32-S3 | LCD 20x4 I2C | ZMPT101B | Ultrasonic | Flow Sensor
 * AUTHOR: [AMK Smart Pump Control System]
 * VERSION: 2.1 (Premium Updated)
 * UPDATED: 2026-05-16
 * ============================================================================================
 *
 * --- SYSTEM OVERVIEW ---
 *
 * 1. DUAL-CORE ARCHITECTURE (FreeRTOS):
 *    - Core 1 (Control Loop): Real-time safety and control logic.
 *      Handles Ultrasonic/Voltage/Flow sensing, Pump/Solenoid output, alarms, and LCD UI.
 *    - Core 0 (Network Loop): Connectivity services.
 *      Handles WiFi STA/AP, Captive DNS, Web Server (:80), MQTT TLS Cloud (:8883), and OTA.
 *    - Benefit: Network delays do not block safety-critical motor control.
 *
 * 2. MASTER-SLAVE CONNECTION (PAIR MODE):
 *    - Standalone: Device runs independently.
 *    - Master (Sump Tank): Publishes pump/status state to MQTT topic:
 *      smartpump/<master_device_id>/status
 *    - Slave (Upper Tank): Subscribes to linked Master status topic and enforces sequential logic.
 *      Slave waits when Master is pumping, unsafe, settling, or link state is invalid.
 *    - Link timeout: If Master status is not updated for 5 minutes, Slave enters fallback behavior.
 *
 * 3. OPERATION MODES:
 *    - Water Pump Mode: Start at low level %, stop at full level %.
 *    - Air Compressor Mode: Pre-vent and post-vent solenoid timing (unloader sequence) around motor run.
 *
 * 4. PROTECTION & RECOVERY:
 *    - Voltage Guard: High/Low cutoff + resume gap + delayed restart.
 *    - Dry-Run Guard: Motor ON with no flow within delay => alarm + lock.
 *    - Auto-Retry: Re-attempt start after configured wait time (e.g., 30/60 minutes).
 *    - 1-Hour Runtime Cool-Down: Mandatory rest window to reduce motor heat stress.
 *
 * 5. CONNECTIVITY & CONTROL FUNCTIONS:
 *    - Local Web Dashboard: Live data, settings, role pairing, maintenance controls.
 *    - MQTT Cloud Control: Remote command/status with device PIN checks.
 *    - Smart Scheduling (DND): NTP-based time window prevents undesired night auto-start.
 *    - OTA: Local firmware upload + remote GitHub update check/start.
 *
 * 6. DATA PERSISTENCE & SECURITY:
 *    - NVS (Preferences): Saves thresholds, role mode, runtime state, WiFi/cloud settings.
 *    - Licensing: Token-based validity system (Base64/MD5 flow).
 *    - TLS: Root CA-based secure MQTT transport.
 *
 * 7. PERFORMANCE NOTES (CURRENT IMPLEMENTATION):
 *    - General scheduler tick: ~1 second control cadence.
 *    - Ultrasonic measurement cycle: ~4 seconds.
 *    - MQTT publish throttle: ~500 ms minimum spacing.
 *    - MQTT reconnect backoff: ~10 seconds between attempts.
 *    - KeepAlive/Socket tuning: 60s keepalive, 10s socket timeout for stable cloud sessions.
 *
 * ============================================================================================
 */

// --- Required Libraries ---
#include <Arduino.h>
#include <Wire.h>
#include <LiquidCrystal_I2C.h>
#include <WiFi.h>
#include <WiFiMulti.h>
#include <WebServer.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include <Preferences.h>
#include <time.h>
#include <WiFiClientSecure.h>
#include <WiFiUdp.h>
#include <ZMPT101B.h>
#include <Adafruit_NeoPixel.h>
#include <ESPmDNS.h>
#include <HTTPClient.h>
#include <HTTPUpdate.h>
#include <Update.h>
#include <MD5Builder.h>
#include <mbedtls/base64.h>
#include "logo.h"
#include <esp_task_wdt.h>

// ============================================================================
// CONFIGURATION & PINOUT
// ============================================================================

const int SDA_PIN = 1;
const int SCL_PIN = 2;
const int VOLTAGE_SENSOR_PIN = 4;
const int UPPER_TANK_TRIG_PIN = 5;
const int UPPER_TANK_ECHO_PIN = 6;
const int BUZZER_PIN = 7;
const int MOTOR_PIN = 8;
const int MANUAL_BTN_PIN = 9;
const int SOLENOID_PIN = 10;
const int FLOW_SENSOR_PIN = 18;
const int RGB_LED_PIN = 48;

LiquidCrystal_I2C lcd(0x27, 20, 4);

byte bar1[8] = { B11100, B11110, B11110, B11110, B11110, B11110, B11110, B11100 };
byte bar2[8] = { B00111, B01111, B01111, B01111, B01111, B01111, B01111, B00111 };
byte bar3[8] = { B11111, B11111, B00000, B00000, B00000, B00000, B11111, B11111 };
byte bar4[8] = { B11110, B11100, B00000, B00000, B00000, B00000, B11000, B11100 };
byte bar5[8] = { B01111, B00111, B00000, B00000, B00000, B00000, B00011, B01111 };
byte bar6[8] = { B00000, B00000, B00000, B00000, B00000, B00000, B11111, B11111 };
byte bar7[8] = { B00000, B00000, B00000, B00000, B00000, B00000, B00111, B01111 };
byte bar8[8] = { B11111, B11111, B00000, B00000, B00000, B00000, B00000, B00000 };

const char* mqtt_server = "210195b635414206adcd944325fe6f59.s1.eu.hivemq.cloud";
const int mqtt_port = 8883;
const char* mqtt_user = "Smart_Pump";
const char* mqtt_pass = "Sm@rt_Pump_2026";

static const char MQTT_ROOT_CA[] PROGMEM = R"EOF(
-----BEGIN CERTIFICATE-----
MIIFazCCA1OgAwIBAgIRAIIQz7DSQONZRGPgu2OCiwAwDQYJKoZIhvcNAQELBQAw
TzELMAkGA1UEBhMCVVMxKTAnBgNVBAoTIEludGVybmV0IFNlY3VyaXR5IFJlc2Vh
cmNoIEdyb3VwMRUwEwYDVQQDEwxJU1JHIFJvb3QgWDEwHhcNMTUwNjA0MTEwNDM4
WhcNMzUwNjA0MTEwNDM4WjBPMQswCQYDVQQGEwJVUzEpMCcGA1UEChMgSW50ZXJu
ZXQgU2VjdXJpdHkgUmVzZWFyY2ggR3JvdXAxFTATBgNVBAMTDElTUkcgUm9vdCBY
MTCCAiIwDQYJKoZIhvcNAQEBBQADggIPADCCAgoCggIBAK3oJHP0FDfzm54rVygc
h77ct984kIxuPOZXoHj3dcKi/vVqbvYATyjb3miGbESTtrFj/RQSa78f0uoxmyF+
0TM8ukj13Xnfs7j/EvEhmkvBioZxaUpmZmyPfjxwv60pIgbz5MDmgK7iS4+3mX6U
A5/TR5d8mUgjU+g4rk8Kb4Mu0UlXjIB0ttov0DiNewNwIRt18jA8+o+u3dpjq+sW
T8KOEUt+zwvo/7V3LvSye0rgTBIlDHCNAymg4VMk7BPZ7hm/ELNKjD+Jo2FR3qyH
B5T0Y3HsLuJvW5iB4YlcNHlsdu87kGJ55tukmi8mxdAQ4Q7e2RCOFvu396j3x+UC
B5iPNgiV5+I3lg02dZ77DnKxHZu8A/lJBdiB3QW0KtZB6awBdpUKD9jf1b0SHzUv
KBds0pjBqAlkd25HN7rOrFleaJ1/ctaJxQZBKT5ZPt0m9STJEadao0xAH0ahmbWn
OlFuhjuefXKnEgV4We0+UXgVCwOPjdAvBbI+e0ocS3MFEvzG6uBQE3xDk3SzynTn
jh8BCNAw1FtxNrQHusEwMFxIt4I7mKZ9YIqioymCzLq9gwQbooMDQaHWBfEbwrbw
qHyGO0aoSCqI3Haadr8faqU9GY/rOPNk3sgrDQoo//fb4hVC1CLQJ13hef4Y53CI
rU7m2Ys6xt0nUW7/vGT1M0NPAgMBAAGjQjBAMA4GA1UdDwEB/wQEAwIBBjAPBgNV
HRMBAf8EBTADAQH/MB0GA1UdDgQWBBR5tFnme7bl5AFzgAiIyBpY9umbbjANBgkq
hkiG9w0BAQsFAAOCAgEAVR9YqbyyqFDQDLHYGmkgJykIrGF1XIpu+ILlaS/V9lZL
ubhzEFnTIZd+50xx+7LSYK05qAvqFyFWhfFQDlnrzuBZ6brJFe+GnY+EgPbk6ZGQ
3BebYhtF8GaV0nxvwuo77x/Py9auJ/GpsMiu/X1+mvoiBOv/2X/qkSsisRcOj/KK
NFtY2PwByVS5uCbMiogziUwthDyC3+6WVwW6LLv3xLfHTjuCvjHIInNzktHCgKQ5
ORAzI4JMPJ+GslWYHb4phowim57iaztXOoJwTdwJx4nLCgdNbOhdjsnvzqvHu7Ur
TkXWStAmzOVyyghqpZXjFaH3pO3JLF+l+/+sKAIuvtd7u+Nxe5AW0wdeRlN8NwdC
jNPElpzVmbUq4JUagEiuTDkHzsxHpFKVK7q4+63SM1N95R1NbdWhscdCb+ZAJzVc
oyi3B43njTOQ5yOf+1CceWxG1bQVs5ZufpsMljq4Ui0/1lvh+wjChP4kqKOJ2qxq
4RgqsahDYVvTH9w7jXbyLeiNdd8XM2w9U/t7y0Ff/9yi0GE44Za4rF2LN9d11TPA
mRGunUHBcnWEvgJBQl9nJEiU0Zsnvgc/ubhPgXRR4Xq37Z0j4r7g1SgEEzwxA57d
emyPxgcYxn/eR44/KJ4EBs+lVDR3veyJm+kXQ99b21/+jh5Xos1AnX5iItreGCc=
-----END CERTIFICATE-----
)EOF";

const int FIRMWARE_VERSION = 2;
const char* FW_URL_BASE = "https://raw.githubusercontent.com/AungMoeKhine/smart_water_pump-control/main/";

#define SAMPLE_BUFFER_SIZE 20
#define MAX_DISTANCE 84
#define ULTRASONIC_INTERVAL 4000
#define GENERAL_INTERVAL 1000
const unsigned long MASTER_LINK_TIMEOUT = 300000UL;  // 5 Minutes

// ============================================================================
// DATA STRUCTURES
// ============================================================================

struct VoltageConfig {
  int HIGH_THRESHOLD = 250;
  int LOW_THRESHOLD = 170;
  int RESUME_GAP = 5;
  const int WAIT_SECONDS_SET = 15;
  int waitSeconds = 15;
  int status = 1;
  float currentVoltage = 0.0f;
  unsigned long lastCheck = 0;
};

struct TankConfig {
  int LOW_THRESHOLD = 50;   // Pump starts when water drops to this %
  int FULL_THRESHOLD = 100;  // Pump stops when water reaches this %

  static constexpr float MIN_HEIGHT = 12.0f;  // 1 Foot
  static constexpr float MAX_HEIGHT = 84.0f;  // 7 Feet

  // BUFFER_HEIGHT (Blind Zone)
  static constexpr float BUFFER_HEIGHT = 10.0f;

  float upperHeight = MIN_HEIGHT;
  int rawUpperPercentage = 0;
  int displayUpperPercentage = 0;
  float upperDistance = 0;
  int upperInvalidCount = 0;
  static const int MAX_INVALID_COUNT = 10;
  bool errorAck = false;
  bool firstReadingDone = false;
};

struct PumpConfig {
  int motorStatus = 0;
  bool isRunning = false;
  bool flowDetected = false;
  bool manualOverride = false;
  bool wasRunningBeforeVoltageError = false;
  bool wasRunningBeforeCoolDown = false;
  unsigned long lastFlowTime = 0;
};

struct DryRunConfig {
  int WAIT_SECONDS_SET = 60;
  int waitSeconds = 60;
  int error = 0;
  unsigned long lastUpdate = 0;
  unsigned long alarmStartTime = 0;
  int autoRetryMinutes = 30;
  int retryCountdown = 0;
  unsigned long lastRetryUpdate = 0;
};

struct CoolDownConfig {
  int restMinutes = 0;
  unsigned long runStartTime = 0;
  unsigned long restStartTime = 0;
  bool isResting = false;
};

struct CompressorConfig {
  int opMode = 0;      // 0 = Water Pump, 1 = Air Compressor
  int valveDelay = 5;  // Starts from 5 Seconds
  bool isPreVenting = false;
  bool isPostVenting = false;
  unsigned long ventStartTime = 0;
  bool lastTargetStatus = false;
} compConfig;

// --- NEW: Master/Slave Configuration ---
struct MasterSlaveConfig {
  int sysRole = 0;           // 0 = Standalone, 1 = Master (Sump), 2 = Slave (Upper)
  String linkedID = "";      // Device ID of the Master
  int settlingMinutes = 10;  // Master Settling Time
  unsigned long settleStartTime = 0;
  bool isSettling = false;

  // Slave's cache of the Master's status
  bool masterOnline = false;
  String masterInfo = "UNKNOWN";
  String masterPStat = "OFF";
  unsigned long lastMasterUpdate = 0;
} msConfig;

struct OTAConfig {
  bool updateAvailable;
  int newVersion;
  int remoteVersion;
} otaConfig = { false, 0, 0 };

struct ScheduleConfig {
  int dndStart = 22;
  int dndEnd = 6;
  bool enabled = false;
  float timezoneOffset = 6.5;
};

ScheduleConfig scheduleConfig;

enum class PumpState {
  IDLE,
  PRE_START_VALVE,
  PUMPING,
  POST_STOP_VALVE,
  DRY_RUN_ALARM,
  DRY_RUN_LOCKED,
  SENSOR_ERROR,
  VOLTAGE_ERROR,
  VOLTAGE_WAIT,
  COOLING_DOWN,
  SETTLING_WATER  // NEW: For Master Sump
};

// ============================================================================
// GLOBAL OBJECTS & PROTOTYPES
// ============================================================================

SemaphoreHandle_t systemMutex;
volatile bool pendingMqttPublish = false;

unsigned long installDate = 0;
unsigned long lastTokenTime = 0;
int validDays = 10;
bool isSystemExpired = false;
bool apModeActive = false;                // Starts FALSE by default
unsigned long apManualTriggerTime = 0;    // Tracks when AP was last started
unsigned long lastStationActiveTime = 0;  // Tracks if a user is currently connected
String uploadedLicenseToken = "";

VoltageConfig voltageConfig;
TankConfig tankConfig;
PumpConfig pumpConfig;
DryRunConfig dryRunConfig;
CoolDownConfig coolDownConfig;
PumpState currentState = PumpState::IDLE;
bool currentDndActive = false;

Preferences preferences;
WiFiMulti wifiMulti;
WebServer server(80);
WiFiClientSecure espClient;
PubSubClient mqttClient(espClient);
Adafruit_NeoPixel rgbLed(1, RGB_LED_PIN, NEO_GRB + NEO_KHZ800);
ZMPT101B voltageSensor(VOLTAGE_SENSOR_PIN, 50.0);

String deviceID = "";
unsigned long showIpUntil = 0;
String subTopic = "";
String statusTopic = "";
String onlineTopic = "";
String devicePin = "123456";
int sysLang = 0;
String webAlertMsg = "";
unsigned long webAlertTime = 0;

WiFiUDP dnsUdp;
const byte DNS_PORT = 53;

TaskHandle_t NetworkTaskHandle;

String ssid_saved = "";
String pass_saved = "";
bool localOtaSuccess = false;
String localOtaError = "";

// Prototypes
void checkExpiry();
void networkTask(void* parameter);
void publishState();
void updatePumpLogic();
void saveMotorStatus();
void checkOTA();
void startOTA();
void handleUpdatePage();
void mqttCallback(char* topic, byte* payload, unsigned int length);
void handleRoot();
void handleSettings();
void handleConfig();
void handleSave();
void handleStatus();
void handleToggle();
void handleReset();
void handleScan();
void handleLogo();
String generateStatusJson();
bool reconnectMQTT();
void updateLCD();
void updateLEDStatus();
void monitorSensors();
void monitorButton();
String processManualToggle();
void setLedColor(uint8_t r, uint8_t g, uint8_t b);
String getDeviceID();
void handleAdmin();
void handleApplyLicenseText();
void handleLicenseUpload();
void handleLicenseUploadData();
void handleLocalOtaUpload();
void handleLocalOtaUploadData();

// --- Time Sync Helper (NON-BLOCKING) ---
void requestTimeSync() {
  Serial.println("[NTP] Requesting Time Sync in background...");
  configTime(scheduleConfig.timezoneOffset * 3600, 0, "pool.ntp.org", "time.google.com", "time.nist.gov");
}

void checkAndSaveInstallDate() {
  if (installDate != 0) return;  // Already saved

  time_t now = time(nullptr);
  if (now > 1600000000) {  // If time is successfully synced
    if (xSemaphoreTakeRecursive(systemMutex, pdMS_TO_TICKS(1000))) {
      installDate = (unsigned long)now;
      preferences.begin("pump-control", false);
      preferences.putULong("installDate", installDate);
      preferences.end();
      xSemaphoreGiveRecursive(systemMutex);
      checkExpiry();
    }
  }
}

// --- Captive Portal DNS Redirect ---
// This intercepts DNS requests and redirects everything to the ESP32 IP
// so the user's phone automatically opens the login/config page.
void processDNS() {
  if (WiFi.getMode() == WIFI_STA) return;  // Only run in Access Point mode
  int packetSize = dnsUdp.parsePacket();
  if (packetSize > 0) {
    unsigned char buf[512];
    dnsUdp.read(buf, 512);
    if ((buf[2] & 0x80) == 0) {
      buf[2] = 0x81;  // Standard DNS response flags
      buf[3] = 0x80;
      buf[7] = 0x01;
      int pos = 12;
      while (buf[pos] != 0 && pos < packetSize) pos += buf[pos] + 1;
      pos += 5;
      buf[pos++] = 0xC0;
      buf[pos++] = 0x0C;
      buf[pos++] = 0x00;
      buf[pos++] = 0x01;
      buf[pos++] = 0x00;
      buf[pos++] = 0x01;
      buf[pos++] = 0x00;
      buf[pos++] = 0x00;
      buf[pos++] = 0x00;
      buf[pos++] = 0x3C;
      buf[pos++] = 0x00;
      buf[pos++] = 0x04;
      IPAddress ip = WiFi.softAPIP();  // Redirect all queries to this device
      buf[pos++] = ip[0];
      buf[pos++] = ip[1];
      buf[pos++] = ip[2];
      buf[pos++] = ip[3];
      dnsUdp.beginPacket(dnsUdp.remoteIP(), dnsUdp.remotePort());
      dnsUdp.write(buf, pos);
      dnsUdp.endPacket();
    }
  }
}

// --- Web Interface HTML (Dashboard) ---
const char index_html[] PROGMEM = R"rawliteral(
<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="theme-color" content="#121212"><meta name="apple-mobile-web-app-capable" content="yes"><meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<title>Smart Pump Dashboard</title>
<style>
  body{font-family:sans-serif;background:#121212;background-image:radial-gradient(circle at 50% 0%, #2a2a2a 0%, #121212 70%);color:white;text-align:center;padding:20px;margin:0;min-height:100vh;}
  .logo { width: 80px; height: auto; margin-bottom: 10px; border-radius: 50%; border: 2px solid rgba(255,255,255,0.2); box-shadow: 0 4px 10px rgba(0,0,0,0.5); }
  .card{background:rgba(30, 30, 30, 0.7);backdrop-filter:blur(12px);-webkit-backdrop-filter:blur(12px);border-radius:16px;padding:20px;max-width:400px;margin:auto;box-shadow:0 8px 32px rgba(0,0,0,0.5);border:1px solid rgba(255,255,255,0.08);position:relative;}
  .tabs { display: flex; max-width: 440px; margin: 0 auto 15px auto; gap: 10px; }
  .tab { flex: 1; padding: 12px; text-decoration: none; border-radius: 12px; font-weight: bold; font-size: 1.05rem; transition: 0.3s; border: 1px solid transparent; }
  .tab-active { background: rgba(30, 30, 30, 0.8); color: #fff; border: 1px solid rgba(255,255,255,0.1); box-shadow: 0 4px 15px rgba(0,0,0,0.5); }
  .tab-inactive { background: transparent; color: #888; }
  .tab-inactive:hover { background: rgba(255,255,255,0.05); color: #ccc; }
  .conn-dot{width:10px;height:10px;background:#28a745;border-radius:50%;display:inline-block;margin-right:5px;box-shadow:0 0 8px #28a745;}
  .off{background:#dc3545;box-shadow:0 0 8px #dc3545;}
  .row{display:flex;justify-content:space-between;align-items:center;font-size:1.05rem;margin:14px 0;border-bottom:1px solid rgba(255,255,255,0.05);padding-bottom:12px;}
  .btn{width:100%;padding:14px 4px;color:white;background:#007bff;border:none;border-radius:10px;margin-top:15px;font-weight:bold;cursor:pointer;font-size:1.1rem; transition: transform 0.1s, opacity 0.2s; white-space: nowrap; overflow: hidden; box-shadow: 0 4px 10px rgba(0,0,0,0.3);}
  .btn-green{background:linear-gradient(135deg, #28a745, #20c997) !important; box-shadow:0 4px 15px rgba(40,167,69,0.3);}
  .btn-red{background:linear-gradient(135deg, #dc3545, #ff6b6b) !important; box-shadow:0 4px 15px rgba(220,53,69,0.3);}
  .btn-grey{background:#555 !important;color:#aaa !important;cursor:not-allowed;}
  
  .badge { padding: 5px 12px; border-radius: 20px; font-size: 0.85rem; font-weight: bold; text-transform: uppercase; letter-spacing: 0.5px; box-shadow: 0 2px 5px rgba(0,0,0,0.3); text-shadow: 0 1px 2px rgba(0,0,0,0.5); }
  .bg-success { background: rgba(40,167,69,0.2); color: #28a745; border: 1px solid rgba(40,167,69,0.5); box-shadow: 0 0 12px rgba(40,167,69,0.4), inset 0 0 8px rgba(40,167,69,0.2); }
  .bg-danger { background: rgba(220,53,69,0.2); color: #ff6b6b; border: 1px solid rgba(220,53,69,0.5); box-shadow: 0 0 12px rgba(220,53,69,0.5), inset 0 0 8px rgba(220,53,69,0.2); }
  .bg-warning { background: rgba(255,193,7,0.15); color: #ffc107; border: 1px solid rgba(255,193,7,0.5); box-shadow: 0 0 12px rgba(255,193,7,0.3), inset 0 0 8px rgba(255,193,7,0.15); }
  .bg-info { background: rgba(23,162,184,0.2); color: #17a2b8; border: 1px solid rgba(23,162,184,0.5); box-shadow: 0 0 12px rgba(23,162,184,0.4), inset 0 0 8px rgba(23,162,184,0.2); }
  .bg-secondary { background: rgba(108,117,125,0.2); color: #adb5bd; border: 1px solid rgba(108,117,125,0.5); }

  /* Myanmar Badge & Mobile Shrink Fix */
  .lang-mm .row { font-size: 0.95rem; }
  .lang-mm .badge { font-size: 0.8rem; letter-spacing: 0; }
  .lang-mm .row span:first-child { flex: 1; text-align: left; white-space: nowrap; }
  .lang-mm .row span:last-child { flex: none; text-align: right; }
  
  @media screen and (max-width: 440px) { 
      .lang-mm .row { font-size: 0.85rem !important; letter-spacing: -0.5px; } 
      .lang-mm .tab { font-size: 0.72rem; padding: 12px 1px; white-space: nowrap; } 
      .lang-mm .btn { font-size: 0.85rem !important; padding: 14px 2px; }
  }

  .tank-wrap { width: 140px; height: 180px; border: 4px solid #444; border-radius: 15px; margin: 30px auto; position: relative; background: linear-gradient(90deg, #1a1a1a 0%, #2a2a2a 50%, #1a1a1a 100%); overflow: visible; box-shadow: inset 0 0 20px rgba(0,0,0,0.8); }
  .tank-wrap::before { content: ''; position: absolute; top: -14px; left: 50%; transform: translateX(-50%); width: 80px; height: 14px; background: linear-gradient(90deg, #1a1a1a 0%, #333 50%, #1a1a1a 100%); border-left: 4px solid #444; border-right: 4px solid #444; z-index: 1; }
  .tank-wrap::after { content: ''; position: absolute; top: -24px; left: 50%; transform: translateX(-50%); width: 100px; height: 10px; background: linear-gradient(90deg, #1a1a1a 0%, #333 50%, #1a1a1a 100%); border: 3px solid #444; border-radius: 4px; z-index: 5; }
  .tank-inner { position: absolute; top: 0; left: 0; right: 0; bottom: 0; border-radius: 10px; overflow: hidden; z-index: 2; }
  .tank-ridges { position: absolute; top: 0; left: 0; width: 100%; height: 100%; pointer-events: none; z-index: 3; }
  .tank-ridges::after { content: ''; position: absolute; left: -4px; right: -4px; top: 25%; height: 2px; background: rgba(0, 0, 0, 0.4); box-shadow: 0 45px 0 rgba(0, 0, 0, 0.4), 0 90px 0 rgba(0, 0, 0, 0.4); }
  
  .tank-fill { position: absolute; bottom: 0; left: 0; width: 100%; background: #039be5; transition: height 0.8s cubic-bezier(0.4, 0, 0.2, 1), background 0.5s; height: 0%; overflow: visible; }
  .tank-fill::before, .tank-fill::after { content: ''; position: absolute; left: 0; width: 200%; height: 25px; opacity: 0; transition: opacity 0.5s, background-image 0.5s; background-repeat: repeat-x; background-size: 50% 100%; top: -15px; }
  .tank-fill::before { background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 800 50'%3E%3Cpath d='M0,25 Q100,5 200,25 T400,25 T600,25 T800,25 v35 h-800 z' fill='%2387CEFA' opacity='0.7'/%3E%3C/svg%3E"); animation: wave 4s linear infinite; z-index: 1; }
  .tank-fill::after { background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 800 50'%3E%3Cpath d='M0,25 Q100,45 200,25 T400,25 T600,25 T800,25 v35 h-800 z' fill='%23039be5'/%3E%3C/svg%3E"); animation: wave 3s linear infinite reverse; z-index: 2; margin-top: 2px; }
  
  .tank-fill.low-water { background: #dc3545; box-shadow: 0 0 15px rgba(220,53,69,0.5); }
  .tank-fill.low-water::before { background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 800 50'%3E%3Cpath d='M0,25 Q100,5 200,25 T400,25 T600,25 T800,25 v35 h-800 z' fill='%23ff6b6b' opacity='0.7'/%3E%3C/svg%3E"); }
  .tank-fill.low-water::after { background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 800 50'%3E%3Cpath d='M0,25 Q100,45 200,25 T400,25 T600,25 T800,25 v35 h-800 z' fill='%23dc3545'/%3E%3C/svg%3E"); }
  
  .tank-fill.pumping::before, .tank-fill.pumping::after { opacity: 1; }
  @keyframes wave { 0% { transform: translateX(0); } 100% { transform: translateX(-50%); } }
  .tank-glaze { position: absolute; top: 0; left: 0; width: 100%; height: 100%; z-index: 5; pointer-events: none; background: linear-gradient(90deg, transparent 0%, rgba(255,255,255,0.1) 50%, transparent 100%); border-radius: 10px; }
  .tank-text { position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%); font-weight: bold; font-size: 1.6rem; color: #fff; z-index: 6; text-shadow: 0px 2px 5px rgba(0,0,0,0.9); }

  .tank-guides { position: absolute; inset: 0; z-index: 4; pointer-events: none; }
  .tank-ruler { position: absolute; left: -15px; top: 0; bottom: 0; width: 12px; z-index: 4; pointer-events: none; }
  .tank-ruler-tick { position: absolute; left: 0; width: 5px; height: 1px; background: rgba(210, 220, 235, 0.20); }
  .tank-ruler-tick.major { width: 8px; background: rgba(210, 220, 235, 0.30); }
  .tank-guide-line { position: absolute; left: 8px; right: 8px; height: 1px; background: rgba(255, 255, 255, 0.16); }
  .tank-guide-line.low-marker { left: 4px; right: 4px; height: 2px; background: rgba(255, 193, 7, 0.65); box-shadow: 0 0 4px rgba(255, 193, 7, 0.40); }
  .tank-guide-line.full-marker { left: 4px; right: 4px; height: 2px; background: rgba(40, 167, 69, 0.65); box-shadow: 0 0 4px rgba(40, 167, 69, 0.40); }
  .tank-guide-label { position: absolute; left: -82px; transform: translateY(-50%); font-size: 0.62rem; font-weight: 700; letter-spacing: 0.4px; text-align: right; width: 60px; opacity: 0.72; text-shadow: 0 1px 1px rgba(0, 0, 0, 0.6); white-space: nowrap; }
  .low-marker .tank-guide-label { color: rgba(255, 193, 7, 0.78); }
  .full-marker .tank-guide-label { color: rgba(40, 167, 69, 0.78); }
  
  .modal { display: none; position: fixed; z-index: 1000; left: 0; top: 0; width: 100%; height: 100%; background-color: rgba(0,0,0,0.6); backdrop-filter: blur(5px); }
  .modal-content { background: rgba(30, 30, 30, 0.9); position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%); padding: 25px; border-left: 5px solid #ffc107; border-radius: 12px; width: 85%; max-width: 320px; box-shadow: 0 10px 40px rgba(0,0,0,0.8); text-align: left; box-sizing: border-box; }
  .modal-title { color: #ffc107; font-size: 1.3rem; margin: 0 0 10px 0; display: flex; align-items: center; gap: 8px; }
  .modal-text { color: #ddd; margin-bottom: 25px; font-size: 1.05rem; line-height: 1.5; }
  .modal-close { background: #333; color: white; padding: 12px; border: none; border-radius: 8px; cursor: pointer; font-weight: bold; width: 100%; font-size: 1.1rem; transition: background 0.2s; }
</style></head><body>
  <div id="warnModal" class="modal"><div class="modal-content"><h3 class="modal-title"><span>🔔</span> System Notice</h3><div id="warnText" class="modal-text">System processing...</div><button class="modal-close" onclick="closeModal()">Understood</button></div></div>
  
  <div id="expiryBanner" style="display:none; background:rgba(220,53,69,0.2); border:1px solid #dc3545; color:#ff6b6b; padding:12px; border-radius:12px; margin-bottom:15px; font-weight:bold; max-width:440px; margin:0 auto 15px auto; backdrop-filter:blur(10px);">
      ⚠️ SYSTEM EXPIRED ⚠️<br><span style="font-size:0.8rem; font-weight:normal; color:#ddd;">Pump operations are disabled.</span>
  </div>
  <div id="warnBanner" style="display:none; background:rgba(255,193,7,0.15); border:1px solid #ffc107; color:#ffc107; padding:12px; border-radius:12px; margin-bottom:15px; font-weight:bold; max-width:440px; margin:0 auto 15px auto; backdrop-filter:blur(10px);">
      ⚠️ SUBSCRIPTION ENDING SOON ⚠️<br><span style="font-size:0.8rem; font-weight:normal; color:#ddd;" id="warnMsg"></span>
  </div>

  <div class="tabs">
    <a href="/" class="tab tab-active">🏠 Home</a>
    <a href="/settings" class="tab tab-inactive">⚙️ Settings</a>
  </div>

  <div class="card">
    <div style="font-size:0.8rem; display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
    <div><span id="dot" class="conn-dot"></span><span id="cStat" style="color:#aaa;">Device: Online</span></div>
    <div id="roleBadge" class="badge" style="font-size:0.65rem; padding: 3px 8px; display:none;"></div>
</div>
    <h2 style="color:#03ef;margin-top:0; font-weight:800; letter-spacing: -0.5px;">
      <img src="/logo.png" class="logo"><br>
      💧Tank Water Level
      <div id="dndBadge" style="display:none; font-size: 0.8rem; background: rgba(111,66,193,0.2); border: 1px solid rgba(111,66,193,0.6); color: #d8b4fe; padding: 4px 12px; border-radius: 20px; margin: 8px auto; width: fit-content; text-transform: uppercase; font-weight: bold; letter-spacing: 1px;">🌙 DND Active</div>
    </h2>
    <div class="tank-wrap"><div class="tank-inner"><div class="tank-fill" id="tankFill"></div><div class="tank-glaze"></div></div><div class="tank-ruler" id="tankRuler" aria-hidden="true"></div><div class="tank-ridges"></div><div class="tank-guides" id="tankGuides"><div class="tank-guide-line low-marker" id="lowTankMarker" style="bottom: 30%;"><span class="tank-guide-label" id="lowTankMarkerLabel">LOW 30%</span></div><div class="tank-guide-line full-marker" id="fullTankMarker" style="bottom: 100%;"><span class="tank-guide-label" id="fullTankMarkerLabel">FULL 100%</span></div></div><div class="tank-text" id="tankVal">-- %</div></div>
    
    <div class="row"><span>Voltage:</span><span id="volt" style="font-weight:bold; font-size:1.2rem;">-- V</span></div>
    <div class="row"><span>Volt Status:</span><span id="vstat" class="badge bg-secondary">--</span></div>
    <div class="row"><span>Pump:</span><span id="state" class="badge bg-secondary">--</span></div>
    <div class="row"><span>System Info:</span><span id="info" class="badge bg-secondary">--</span></div>
    <div class="row" id="cdRow" style="display:none;color:#ff6b6b;font-weight:bold;"><span>Dry-Run in:</span><span id="cd" style="background:rgba(220,53,69,0.1); padding:2px 8px; border-radius:6px;">--</span></div>
    
    <div id="otaHub" style="display:none; margin:15px 0; padding:15px; border:1px solid rgba(40,167,69,0.3); border-radius:12px; background:rgba(40,167,69,0.05);">
        <div style="color:#28a745; font-weight:bold; margin-bottom:10px;" id="otaMsg">New Version Available!</div>
        <button class="btn btn-green" style="margin-top:0;" onclick="startOTA()">Update Now</button>
    </div>
    <button class="btn" id="btnToggle" onclick="togglePump()">WAITING SYNC...</button>
    <button class="btn btn-red" id="btnReset" onclick="resetPump()" style="display:none;">Reset Alarm</button>
  </div>
  <div style="margin-top:25px;color:#555;font-size:0.75rem;">
    <div style="margin-bottom:8px;">Cloud ID: <span id="cid" style="color:#888;">--</span></div>
    Device IP: <span id="dip" style="color:#888;">--</span>
  </div>
<script>
  const showModal = (msg) => { document.getElementById('warnText').innerText = msg; document.getElementById('warnModal').style.display = 'block'; if(navigator.vibrate) navigator.vibrate([50, 50, 50]); };
  const closeModal = () => { document.getElementById('warnModal').style.display = 'none'; };
  const togglePump = () => { if(navigator.vibrate) navigator.vibrate(50); fetch('/toggle').then(r=>r.json()).then(d=>{ if(d.status === 'blocked') showModal(d.reason); else upd(); }).catch(e=>{}); };
  const resetPump = () => { if(navigator.vibrate) navigator.vibrate(50); fetch('/reset').then(r=>r.json()).then(d=>{ if(d.status === 'blocked') showModal(d.reason); else upd(); }).catch(e=>{}); };
  const checkOTA = () => { window.location.href = '/update_github'; };
  const startOTA = () => { if(confirm('Are you sure you want to update? The system will reboot.')) fetch('/start-ota').then(()=>{ document.body.innerHTML='<h2 style="color:white;text-align:center;margin-top:50px;">Updating... Please wait.</h2>'; }); };
    
  const setBadge = (id, text, type) => { let el = document.getElementById(id); el.innerText = text; el.className = 'badge bg-' + type; };
  
  let lastWebAlert = "";

  const renderTankRulerTicks = () => { const ruler = document.getElementById('tankRuler'); if (!ruler || ruler.dataset.ready === '1') return; let html = ''; for (let v = 0; v <= 100; v += 5) { const cls = (v % 25 === 0) ? 'tank-ruler-tick major' : 'tank-ruler-tick'; html += '<div class="' + cls + '" style="bottom:' + v + '%;"></div>'; } ruler.innerHTML = html; ruler.dataset.ready = '1'; };
  const setTankGuides = (d) => { const low = parseInt(d.lowTank, 10) || 30; const full = parseInt(d.fullPoint || d.fullTank, 10) || 100; const lowMarker = document.getElementById('lowTankMarker'); const fullMarker = document.getElementById('fullTankMarker'); if (lowMarker) lowMarker.style.bottom = low + '%'; if (fullMarker) fullMarker.style.bottom = full + '%'; document.getElementById('lowTankMarkerLabel').innerText = (d.lang == 1 ? 'စတင် ' : 'LOW ') + low + '%'; document.getElementById('fullTankMarkerLabel').innerText = (d.lang == 1 ? 'ပြည့် ' : 'FULL ') + full + '%'; };

  const dict = { "Smart Pump Dashboard": "ရေမော်တာ ထိန်းချုပ်စနစ်", "🏠 Home": "🏠 ပင်မစာမျက်နှာ", "⚙️ Settings": "⚙️ ဆက်တင်များ", "💧Tank Water Level": "💧ရေတိုင်ကီ ရေအမှတ်", "Voltage:": "လျှပ်စစ်အား-", "Volt Status:": "ဗို့အား အခြေအနေ-", "Pump:": "ရေမော်တာ-", "System Info:": "စက် အချက်အလက်-", "Cloud ID:": "ကလောက် အိုင်ဒီ-", "Device IP:": "စက် အိုင်ပီ-", "Update Now": "ယခု အဆင့်မြှင့်မည်", "System Notice": "စနစ် အသိပေးချက်", "System processing...": "စနစ် အလုပ်လုပ်နေပါသည်...", "Understood": "နားလည်ပါသည်" };
  const applyDict = () => { if(window.lSet) return; document.title = dict["Smart Pump Dashboard"]; const w = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, null, false); let n; while(n = w.nextNode()){ let t=n.nodeValue.trim(); if(dict[t]) n.nodeValue = n.nodeValue.replace(t, dict[t]); } document.getElementById('expiryBanner').innerHTML="⚠️ စနစ် သက်တမ်းကုန်ဆုံးသွားပါပြီ ⚠️<br><span style='font-size:0.8rem; font-weight:normal; color:#ddd;'>ရေမော်တာ အသုံးပြုခွင့် ပိတ်ထားပါသည်။</span>"; document.getElementById('warnBanner').innerHTML="⚠️ သက်တမ်းကုန်ဆုံးရန် နီးကပ်နေပါပြီ ⚠️<br><span style='font-size:0.8rem; font-weight:normal; color:#ddd;' id='warnMsg'></span>"; window.lSet = true; };
  
  const upd = () => { return fetch('/status').then(r=>r.json()).then(d=>{
    if(d.lang == 1) { document.body.classList.add('lang-mm'); applyDict(); } else document.body.classList.remove('lang-mm');
      document.getElementById('dot').className='conn-dot'; document.getElementById('cStat').innerText=d.lang==1?'စက် အွန်လိုင်း':'Device: Online';
      // --- Update Role Badge ---
      let rb = document.getElementById('roleBadge');
      if (d.sysRole == 1) {
          rb.innerText = d.lang == 1 ? "📥 အဓိကစက် (အောက်တိုင်ကီ)" : "📥 MASTER (SUMP)";
          rb.className = "badge bg-info"; rb.style.display = "block";
      } else if (d.sysRole == 2) {
          rb.innerText = d.lang == 1 ? "📤 လက်အောက်ခံစက် (အပေါ်တိုင်ကီ)" : "📤 SLAVE (ROOF)";
          rb.className = "badge bg-warning"; rb.style.display = "block"; rb.style.color = "#ffc107";
      } else {
          rb.style.display = "none"; // Hide if standalone
      }
      renderTankRulerTicks();
      setTankGuides(d);
      document.getElementById('dip').innerText = d.ip; 
      
      let tf = document.getElementById('tankFill');
      tf.style.height = d.tank + '%';
      if (d.tank <= d.lowTank) tf.classList.add('low-water'); else tf.classList.remove('low-water');
      if (d.pStat == "ON") tf.classList.add('pumping'); else tf.classList.remove('pumping');
      
      const rawInfo = d.info || "";
let vS = d.vStat; let iF = d.info; let tS = d.tStr; let pS = d.pStat;
      if (d.lang==1) {
        if(vS=="NORMAL") vS="ပုံမှန်"; else if(vS=="OVER") vS="ကျော်လွန်"; else if(vS=="UNDER") vS="လျော့နည်း"; else if(vS=="DELAY") vS="စောင့်ဆိုင်း";
        if(iF=="DRY_RUN_ALARM!") iF="ရေမရှိ အချက်ပေး!"; else if(iF=="PUMP_LOCKED!") iF="ပိတ်သိမ်းထားသည်!"; else if(iF=="WAITING_RETRY!") iF="ပြန်စရန်စောင့်နေသည်!"; else if(iF=="SENSOR_ERROR!") iF="ဆင်ဆာ ချို့ယွင်းချက်!"; else if(iF=="SYSTEM_STANDBY!") iF="အသင့်အနေအထား!"; else if(iF=="FLOW_DETECTED!") iF="ရေစီးဆင်းမှုရှိသည်!"; else if(iF=="FLOW_CHECKING!") iF="ရေစီးဆင်းမှုစစ်နေ!"; else if(iF=="COOLING_DOWN!") iF="အအေးခံနေသည်!"; else if(iF=="VENTING_VALVE!") iF="လေလျှော့နေသည်!"; else if(iF=="OVER_VOLTAGE!") iF="ဗို့အားကျော်လွန်နေသည်!"; else if(iF=="UNDER_VOLTAGE!") iF="ဗို့အားလျော့နည်းနေသည်!"; else if(iF.startsWith("VOLT_DELAY")) { let secs = iF.replace(/[^\d]/g, ""); iF = "ဗို့အားပြန်တည်ငြိမ်ရန်စောင့်ချိန်! (" + secs + "s)"; } else if(iF.startsWith("SETTLING_WATER!")) { let mins = iF.replace(/[^\d]/g, ""); iF = "ရေအနည်ထိုင်ရန်စောင့်ချိန်! (" + mins + "m)"; }
else if(iF.startsWith("SETTLING_WATER!")) { let mins = iF.replace(/[^\d]/g, ""); iF = "ရေအနည်ထိုင်ရန်စောင့်ချိန်! (" + mins + "m)"; }
        if(tS=="FULL") tS="ပြည့်"; else if(tS=="LOW") tS="နည်း"; else if(tS=="ERR") tS="ချို့";
        if(pS=="ON") pS="ဖွင့်"; else if(pS=="OFF") pS="ပိတ်"; else if(pS=="PUMPING") pS="ရေတင်နေသည်"; else if(pS=="STANDBY") pS="အသင့်အနေအထား"; else if(pS=="DRY ALRM") pS="ရေမရှိ အချက်ပေး"; else if(pS=="LOCKED") pS="ပိတ်သိမ်းထားသည်";
      }
      
      document.getElementById('tankVal').innerText = tS; if (document.getElementById('cid')) document.getElementById('cid').innerText = d.id;
      document.getElementById('volt').innerText=d.volt+' V'; 
      
      // 1. Voltage Status Colors
      let vType = 'secondary';
      if (d.vStat === "NORMAL") vType = 'success';
      else if (d.vStat === "DELAY") vType = 'warning';
      else if (d.vStat === "OVER" || d.vStat === "UNDER") vType = 'danger';
      setBadge('vstat', vS, vType);

      // 2. Pump Status Colors
      let pType = 'secondary';
      if (d.pStat === "ON") pType = 'success';
      else if (d.pStat === "DRY ALRM" || d.pStat === "LOCKED") pType = 'danger';
      setBadge('state', pS, pType);
      
      // 3. System Info Colors (use rawInfo, not translated iF)
let iType = 'success';
if (
    rawInfo.includes("OVER_VOLTAGE") ||
    rawInfo.includes("UNDER_VOLTAGE") ||
    rawInfo.includes("ALARM") ||
    rawInfo.includes("ERROR")
) {
    iType = 'danger';
} else if (
    rawInfo.includes("VOLT_DELAY") ||
    rawInfo.includes("WAITING") ||
    rawInfo.includes("LOCKED") ||
    rawInfo.includes("COOLING") ||
    rawInfo.includes("VENTING") ||
    rawInfo.includes("SETTLING")
) {
    iType = 'warning';
} else if (rawInfo.includes("FLOW_CHECKING")) {
    iType = 'info';
}
setBadge('info', iF, iType);

      if (d.dndAct == 1) { document.getElementById('dndBadge').style.display = 'block'; } else { document.getElementById('dndBadge').style.display = 'none'; }
      
      let btn=document.getElementById('btnToggle'); let rst=document.getElementById('btnReset');
      if(d.err){
         rst.style.display='block'; btn.style.display='none';
         rst.innerText = d.lang==1 ? 'အချက်ပေး ပြန်ပိတ်မည်' : 'Reset Alarm';
      } else if(d.sErr && !d.ack){
         btn.style.display='block';rst.style.display='none';
         btn.innerText=d.lang==1?'အချက်ပေး ပြန်ပိတ်မည်':'Reset Alarm'; btn.className='btn btn-red';
      } else if(iF == "VENTING_VALVE!" || iF == "လေလျှော့နေသည်!") {
         btn.style.display='block'; rst.style.display='none';
         btn.innerText = d.lang==1 ? "စောင့်ဆိုင်းပါ..." : "PLEASE WAIT...";
         btn.className = 'btn btn-grey'; btn.disabled = true;
      } else {
         rst.style.display='none'; btn.style.display='block'; btn.disabled = false;
         btn.innerText = d.lang==1 ? ((d.pStat == "ON") ? 'ရေမော်တာ ပိတ်မည်' : 'ရေမော်တာ ဖွင့်မည်') : ((d.pStat == "ON") ? 'Stop the Pump' : 'Start the Pump');
         btn.className = (d.pStat == "ON") ? "btn btn-red" : "btn btn-green";
      }
      
      let cd=document.getElementById('cdRow');
      if(d.err == 2 && d.rM > 0) { cd.style.display='flex'; cd.children[0].innerText = d.lang==1?'ပြန်လည်စတင်ရန်-':'Retry in:'; let m = Math.floor(d.rCd / 60); let s = d.rCd % 60; document.getElementById('cd').innerText = m + 'm ' + s + 's'; } 
      else if(d.isDR) { cd.style.display='flex'; cd.children[0].innerText = d.lang==1?'ရေမရှိ အချက်ပေးရန်-':'Dry-Run in:'; document.getElementById('cd').innerText = "(" + d.cd + "s)"; } 
      else if(d.info=="COOLING_DOWN!" || d.info=="အအေးခံနေသည်!") { cd.style.display='flex'; cd.children[0].innerText = d.lang==1?'အအေးခံရန် ကျန်ချိန်-':'Cooling in:'; let m = Math.floor(d.rstCd / 60); let s = d.rstCd % 60; document.getElementById('cd').innerText = m + 'm ' + s + 's'; } 
      else { cd.style.display='none'; }

      let ota=document.getElementById('otaHub');
      if(d.ota){ ota.style.display='block'; document.getElementById('otaMsg').innerText = d.lang==1 ? 'ဗားရှင်းသစ် v' + d.nVer + ' ရနိုင်ပါပြီ!' : 'New Version v' + d.nVer + ' Available!'; } else { ota.style.display='none'; }

      document.getElementById('expiryBanner').style.display = 'none'; document.getElementById('warnBanner').style.display = 'none';
      if(d.alertMsg && d.alertMsg !== lastWebAlert) { showModal(d.alertMsg); lastWebAlert = d.alertMsg; } else if (!d.alertMsg) { lastWebAlert = ""; }
    }).catch(e=>{ document.getElementById('dot').className='conn-dot off'; document.getElementById('cStat').innerText=(document.body.classList.contains('lang-mm') ? 'စက် အော့ဖ်လိုင်း (ချိတ်ဆက်နေသည်...)' : 'Device: Offline (Connecting...)'); }); }
  setInterval(upd,1000); 
</script></body></html>
)rawliteral";

const char settings_html[] PROGMEM = R"rawliteral(
<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="theme-color" content="#121212"><meta name="apple-mobile-web-app-capable" content="yes"><meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<style>
  body{font-family:sans-serif;background:#121212;background-image:radial-gradient(circle at 50% 0%, #2a2a2a 0%, #121212 70%);color:white;text-align:center;padding:20px;margin:0;min-height:100vh;}
  .card{background:rgba(30, 30, 30, 0.7);backdrop-filter:blur(12px);-webkit-backdrop-filter:blur(12px);border-radius:16px;padding:20px;max-width:420px;margin:auto;box-shadow:0 8px 32px rgba(0,0,0,0.5);border:1px solid rgba(255,255,255,0.08);position:relative;text-align:left;}
  .conn-dot{width:10px;height:10px;background:#28a745;border-radius:50%;display:inline-block;margin-right:5px;box-shadow:0 0 8px #28a745;}
  .off{background:#dc3545;box-shadow:0 0 8px #dc3545;}
  .badge { padding: 5px 12px; border-radius: 20px; font-size: 0.85rem; font-weight: bold; text-transform: uppercase; letter-spacing: 0.5px; box-shadow: 0 2px 5px rgba(0,0,0,0.3); text-shadow: 0 1px 2px rgba(0,0,0,0.5); }
  .bg-info { background: rgba(23,162,184,0.2); color: #17a2b8; border: 1px solid rgba(23,162,184,0.5); box-shadow: 0 0 12px rgba(23,162,184,0.4), inset 0 0 8px rgba(23,162,184,0.2); }
  .bg-warning { background: rgba(255,193,7,0.15); color: #ffc107; border: 1px solid rgba(255,193,7,0.5); box-shadow: 0 0 12px rgba(255,193,7,0.3), inset 0 0 8px rgba(255,193,7,0.15); }
  .tabs { display: flex; max-width: 440px; margin: 0 auto 15px auto; gap: 10px; }
  .tab { flex: 1; padding: 12px; text-decoration: none; border-radius: 12px; font-weight: bold; font-size: 1rem; transition: 0.3s; border: 1px solid transparent; text-align: center; }
  .tab-active { background: rgba(30, 30, 30, 0.8); color: #fff; border: 1px solid rgba(255,255,255,0.1); box-shadow: 0 4px 15px rgba(0,0,0,0.5); }
  .tab-inactive { background: transparent; color: #888; }
  .tab-inactive:hover { background: rgba(255,255,255,0.05); color: #ccc; }
  .lbl-wrap { display: flex; justify-content: space-between; align-items: center; margin: 15px 0 5px; flex-wrap: wrap; gap: 5px; }
  label{font-weight:bold;color:#aaa; font-size: 0.9rem;}
  .logo { width: 80px; height: auto; margin: 0 auto 10px; display: block; border-radius: 50%; border: 2px solid rgba(255,255,255,0.2); box-shadow: 0 4px 10px rgba(0,0,0,0.5); }
  .range { color: #555; font-size: 0.75rem; }
  input, select{width:100%;padding:12px;background:rgba(42, 42, 42, 0.8);border:1px solid rgba(255, 255, 255, 0.1);color:white;border-radius:8px;box-sizing:border-box; font-size: 1rem;}
  .show-pass { margin-top: 10px; font-size: 0.85rem; color: #888; display: flex; align-items: center; cursor: pointer; justify-content: flex-start; text-align: left; width: 100%; }
  .show-pass input { width: auto; margin-right: 8px; }
  .btn{width:100%;padding:14px 4px;color:white;background:#007bff;border:none;border-radius:10px;margin-top:15px;font-weight:bold;cursor:pointer;font-size:1.05rem; transition: 0.2s; white-space: nowrap; overflow: hidden; box-shadow: 0 4px 10px rgba(0,0,0,0.3);}
  .btn-green{background:linear-gradient(135deg, #28a745, #20c997) !important;}
  .btn-purple{background:linear-gradient(135deg, #6f42c1, #a958a5) !important;}
  .btn-red{background:linear-gradient(135deg, #dc3545, #ff6b6b) !important;}
  hr { border: 0; border-top: 1px solid rgba(255, 255, 255, 0.1); margin: 25px 0; }
  .modal { display: none; position: fixed; z-index: 1000; left: 0; top: 0; width: 100%; height: 100%; background-color: rgba(0,0,0,0.6); backdrop-filter: blur(5px); }
  .modal-content { background: rgba(30, 30, 30, 0.9); position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%); padding: 25px; border-left: 5px solid #ffc107; border-radius: 12px; width: 85%; max-width: 320px; box-shadow: 0 10px 40px rgba(0,0,0,0.8); text-align: left; box-sizing: border-box; }
  .modal-title { color: #ffc107; font-size: 1.3rem; margin: 0 0 10px 0; display: flex; align-items: center; gap: 8px; }
  .modal-text { color: #ddd; margin-bottom: 25px; font-size: 1.05rem; line-height: 1.5; }
  .modal-close { background: #333; color: white; padding: 12px; border: none; border-radius: 8px; cursor: pointer; font-weight: bold; width: 100%; font-size: 1.1rem; transition: background 0.2s; }
  @media screen and (max-width: 440px) { 
      .lang-mm .tab { font-size: 0.7rem; padding: 12px 1px; white-space: nowrap; } 
      .lang-mm .btn { font-size: 0.8rem !important; padding: 14px 2px; }
      .lang-mm label { font-size: 0.82rem; }
  }
</style></head><body>
  <div id="warnModal" class="modal"><div class="modal-content"><h3 class="modal-title"><span>🔔</span> System Notice</h3><div id="warnText" class="modal-text">System processing...</div><button class="modal-close" onclick="closeModal()">Understood</button></div></div>
  <div id="expiryBanner" style="display:none;"></div>
  <div id="warnBanner" style="display:none;"></div>
  <div class="tabs"><a href="/" class="tab tab-inactive">🏠 Home</a><a href="/settings" class="tab tab-active">⚙️ Settings</a></div>
  <div class="card">
    <div style="font-size:0.8rem; display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
      <div><span id="dot" class="conn-dot"></span><span id="cStat" style="color:#aaa;">Device: Online</span></div>
      <div id="roleBadge" class="badge" style="font-size:0.65rem; padding: 3px 8px; display:none;"></div>
    </div>
    <img src="/logo.png" class="logo">
    <form action="/save" method="POST">
      <div class="lbl-wrap"><label>WiFi SSID</label><button type="button" onclick="scn()" style="font-size:0.7rem; color:#03ef; background:none; border:1px solid rgba(255,255,255,0.2); border-radius:4px; cursor:pointer; padding:4px 8px; white-space:nowrap;">Scan for Networks</button></div>
      <select id="ss" name="ssid_sel" onchange="chSS(this)"><option value="">Loading...</option></select>
      <input type="text" id="mi" name="ssid_man" placeholder="Type Network Name" style="display:none; margin-top:10px;">
      <div class="lbl-wrap"><label>WiFi Password</label></div>
      <div class="pass-row"><input type="password" id="p" name="pass" placeholder="Leave empty to keep current"><label class="show-pass"><input type="checkbox" onclick="togglePass('p')"> Show WiFi Password</label></div>
      <div class="lbl-wrap"><label>Device PIN (for Cloud)</label></div>
      <div class="pass-row"><input type="password" id="pin" name="pin" required><label class="show-pass"><input type="checkbox" onclick="togglePass('pin')"> Show PIN</label></div>
      <hr>
      <div class="lbl-wrap"><label>Interface Language</label></div><select name="sysLang" id="sysLang"><option value="0">English</option><option value="1">Myanmar (မြန်မာ)</option></select>
      <hr>
      <div class="lbl-wrap"><label>System Role (Pairing)</label></div><select name="sysRole" id="sysRole" onchange="toggleRole()"><option value="0">Standalone</option><option value="1">Master (Sump Tank)</option><option value="2">Slave (Upper Tank)</option></select>
      <div id="masterOpts" style="display:none;"><div class="lbl-wrap"><label>Master Settling Time</label><span class="range">0 - 30 min</span></div><select name="setM" id="setM_s"></select></div>
      <div id="slaveOpts" style="display:none;"><div class="lbl-wrap"><label>Linked Master ID</label></div><input type="text" name="linkID" id="linkID_s" placeholder="Enter Master's Cloud ID" style="margin-top:5px;text-transform:uppercase;"></div>
      <hr>
      <div class="lbl-wrap"><label>Operating Mode</label></div><select name="opM" id="opM" onchange="toggleVDly()"><option value="0">Water Pump</option><option value="1">Air Compressor</option></select>
      <div class="lbl-wrap" id="vWrap" style="display:none;"><label>Compressor Valve Delay</label><span class="range">5 - 15s</span></div><select name="vDly" id="vDly" style="display:none;"></select>
      <hr>
      <div class="lbl-wrap"><label>Tank Height</label><span class="range">1.0 - 7.0 ft</span></div><select name="uH" id="uH_s"></select>
      <div class="lbl-wrap"><label>High Tank Stop Level</label><span class="range">80 - 100 %</span></div><select name="fullTank" id="fullTank_s"></select>
      <div class="lbl-wrap"><label>Low Tank Start Level</label><span class="range">20 - 50 %</span></div><select name="lowTank" id="lowTank_s"></select>
      <div class="lbl-wrap"><label>High Voltage Set</label><span class="range">230 - 260 V</span></div><select name="vH" id="vH_s"></select>
      <div class="lbl-wrap"><label>Low Voltage Set</label><span class="range">150 - 190 V</span></div><select name="vL" id="vL_s"></select>
      <div class="lbl-wrap"><label>Voltage Resume Gap</label><span class="range">1 - 10 V</span></div><select name="vG" id="vG_s"></select>
      <div class="lbl-wrap"><label>Dry-Run Delay</label><span class="range">60 - 180 s</span></div><select name="dD" id="dD_s"></select>
      <div class="lbl-wrap"><label>Pump Cool-down (After 1Hr)</label><span class="range">Disable / 5 - 15m</span></div><select name="rstM" id="rstM_s"></select>
      <div class="lbl-wrap"><label>Auto-Retry Wait</label><span class="range">Disable / 30 / 60</span></div><select name="rM" id="rM_s"></select>
      <hr>
      <div class="lbl-wrap"><label>🌙 Smart Scheduling (DND)</label></div><select name="dndEn" id="dndEn_s"><option value="0">Disabled</option><option value="1">Enabled</option></select>
      <div style="display:flex; gap:10px; margin-top:10px;"><div style="flex:1;"><label>Start Hour</label><select name="dndS" id="dndS_s"></select></div><div style="flex:1;"><label>End Hour</label><select name="dndE" id="dndE_s"></select></div></div>
      <div class="lbl-wrap"><label>📍 Home Time Zone (GMT)</label></div><select name="tzOf" id="tz_s"></select>
      <button type="submit" class="btn btn-green">Save & Reboot</button>
    </form>
    <hr>
    <div style="text-align:center;"><h3 style="color:#d8b4fe; margin:0 0 15px 0;">🛠️ Maintenance</h3><button class="btn btn-purple" onclick="window.location.href='/update_github'">Check for Updates</button><button class="btn" style="margin-top:15px; background:rgba(0,106,255,0.7); border:1px solid rgba(0,106,255,0.9);" onclick="window.open('https://aungmoekhine.github.io/smart_water_pump-control/user_guide_premium.html', '_blank')">📖 User Guide</button></div>
    <hr>
<div style="text-align:center; background:rgba(220,53,69,0.05); border:1px solid rgba(220,53,69,0.35); border-radius:12px; padding:15px;">
  <h3 style="color:#ff6b6b; margin:0 0 12px 0;">Firmware Update</h3>
  <form method="POST" action="/ota-upload" enctype="multipart/form-data" onsubmit="return confirmFwUpload();">
    <label style="display:flex; align-items:center; white-space:nowrap; overflow:hidden; padding:8px; background:rgba(0,0,0,0.2); border:1px solid rgba(255,255,255,0.1); cursor:pointer; color:#aaa; border-radius:8px; margin-bottom:8px; width:100%; box-sizing:border-box; text-align:left;">
      <span style="background:#007bff; color:white; padding:5px 8px; border-radius:4px; margin-right:6px; font-size:0.8rem;">Choose File</span>
      <span id="fwName" style="white-space:nowrap; overflow:hidden; font-size:0.8rem; flex:1;">No file chosen</span>
      <input type="file" name="firmware" accept=".bin" required style="display:none;" onchange="document.getElementById('fwName').innerText=this.files[0]?this.files[0].name:'No file chosen'">
    </label>
    <button type="submit" class="btn btn-red" style="margin-top:5px;">Update Firmware</button>
  </form>
</div>
    <hr>
    <div style="text-align:center; background:rgba(40,167,69,0.05); border:1px solid rgba(40,167,69,0.3); border-radius:12px; padding:15px; margin-bottom:20px;">
      <h3 style="color:#28a745; margin:0 0 10px 0;">🔑 License Management</h3>
      <div style="background:rgba(0,0,0,0.2); padding:10px; border-radius:8px; font-size:0.85rem; margin-bottom:15px; text-align:left;">
        <div style="margin-bottom:5px;"><strong>Device ID:</strong> <span id="did" style="color:#03ef;">loading...</span></div>
        <div><strong>Device IP:</strong> <span id="dip" style="color:#888;">loading...</span></div>
      </div>
      <form method='POST' action='/upload_license' enctype='multipart/form-data'><label style="display:flex; align-items:center; white-space:nowrap; overflow:hidden; padding: 8px; background:rgba(0,0,0,0.2); border:1px solid rgba(255,255,255,0.1); cursor:pointer; color: #aaa; border-radius:8px; margin-bottom:5px; width:100%; box-sizing:border-box; text-align:left;"><span style="background:#007bff; color:white; padding:5px 8px; border-radius:4px; margin-right:5px; font-size:0.8rem;">Choose File</span><span id="fileName" style="white-space:nowrap; overflow:hidden; font-size: 0.8rem; flex:1;">No file chosen</span><input type="file" name="license" accept=".key,.txt" style="display:none;" onchange="document.getElementById('fileName').innerText=this.files[0]?this.files[0].name:'No file chosen'"></label><button type='submit' class="btn btn-green" style="margin-top:5px; margin-bottom:15px;">Upload Token File</button></form>
      <div style="color:#666; margin-bottom:15px; font-size:0.9rem;">- OR -</div>
      <form method='POST' action='/apply_license'><input type='text' name='key' placeholder="Paste Token String (MTc...)" style="margin-bottom:5px;"><button type='submit' class="btn btn-purple" style="margin-top:5px;">Activate via Text</button></form>
    </div>
  </div>
  <div style="margin-top:25px;color:#555;font-size:0.75rem; text-align:center;">
    <div style="margin-bottom:8px;">Cloud ID: <span id="did_footer" style="color:#888;">--</span></div>
    Device IP: <span id="dip_footer" style="color:#888;">--</span>
  </div>
<script>
  const closeModal = () => { document.getElementById('warnModal').style.display = 'none'; };
  function toggleRole() { let r = document.getElementById('sysRole').value; document.getElementById('masterOpts').style.display = (r=="1")?"block":"none"; document.getElementById('slaveOpts').style.display = (r=="2")?"block":"none"; }
  function toggleVDly() { let m = document.getElementById('opM').value; document.getElementById('vWrap').style.display = (m=="1")?"flex":"none"; document.getElementById('vDly').style.display = (m=="1")?"block":"none"; }
  const togglePass = (id) => { let x = document.getElementById(id); x.type = x.type==="password"?"text":"password"; };
  const chSS = (s) => { let m = document.getElementById('mi'); if(s.value === '__man__') { m.style.display='block'; m.required=true; } else { m.style.display='none'; m.required=false; } };
  const scn = () => { if(navigator.vibrate) navigator.vibrate(20); fetch('/scan').then(()=>alert('Scan started...')).then(()=>setTimeout(()=>location.reload(),6000)); };
  const updStatus = () => { 
    fetch('/status').then(r=>r.json()).then(d=>{
      document.getElementById('dot').className='conn-dot'; 
      document.getElementById('cStat').innerText=d.lang==1?'စက် အွန်လိုင်း':'Device: Online';
      let rb = document.getElementById('roleBadge');
      if (d.sysRole == 1) {
          rb.innerText = d.lang == 1 ? "📥 အဓိကစက်" : "📥 MASTER (SUMP)";
          rb.className = "badge bg-info"; rb.style.display = "block";
      } else if (d.sysRole == 2) {
          rb.innerText = d.lang == 1 ? "📤 လက်အောက်ခံစက်" : "📤 SLAVE (ROOF)";
          rb.className = "badge bg-warning"; rb.style.display = "block"; rb.style.color = "#ffc107";
      } else rb.style.display = "none";
    }).catch(e=>{
      document.getElementById('dot').className='conn-dot off'; 
      document.getElementById('cStat').innerText=document.body.classList.contains('lang-mm')?'စက် အော့ဖ်လိုင်း':'Device: Offline';
    });
  };
  setInterval(updStatus, 2000);
  const confirmFwUpload = () => { return confirm(document.body.classList.contains('lang-mm') ? "ဖမ်းဝဲ တင်ပြီး စက်ကို ယခုပြန်ဖွင့်မလား?" : "Upload firmware and reboot now?"); };
  const dict = { "⚙️ Settings": "⚙️ ဆက်တင်များ", "🏠 Home": "🏠 ပင်မစာမျက်နှာ", "WiFi SSID": "ဝိုင်ဖိုင် အမည်", "High Tank Stop Level": "ရေမော်တာရပ်မည့် ရေမှတ်", "WiFi Password": "ဝိုင်ဖိုင် စကားဝှက်", "Device PIN (for Cloud)": "လုံခြုံရေး ပင်နံပါတ်", "Interface Language": "ဘာသာစကား", "Tank Height": "ရေတိုင်ကီ အမြင့်", "Low Tank Start Level": "ရေမော်တာစတင်မည့် ရေမှတ်", "High Voltage Set": "ဗို့အားလွန် သတ်မှတ်ချက်", "Low Voltage Set": "ဗို့အားလျော့ သတ်မှတ်ချက်", "Voltage Resume Gap": "ဗို့အားပြန်ဖွင့် ကွာဟချက်", "Dry-Run Delay": "ရေမရှိ အချက်ပေးချိန်", "Operating Mode": "စက် အမျိုးအစား", "Compressor Valve Delay": "အဆို့ရှင် ဖွင့်ချိန်", "Water Pump": "ရေမော်တာ", "Air Compressor": "လေကွန်ပရက်ဆာ", "Pump Cool-down (After 1Hr)": "၁နာရီမောင်းပြီး အနားပေးချိန်", "Auto-Retry Wait": "ပြန်လည်စတင်ရန် စောင့်ချိန်", "🌙 Smart Scheduling (DND)": "ညဘက် အသံပိတ်စနစ် (DND)", "Start Hour": "စတင်ရန် အချိန်", "End Hour": "ပြီးဆုံးရန် အချိန်", "📍 Home Time Zone (GMT)": "အချိန်ဇုန် (GMT)", "Save & Reboot": "သိမ်းဆည်း၍ ပြန်ဖွင့်မည်", "🛠️ Maintenance": "🛠️ ပြုပြင်ထိန်းသိမ်းမှု", "Check for Updates": "ဗားရှင်း အသစ်စစ်ရန်", "📖 User Guide": "📖 အသုံးပြုနည်း လမ်းညွှန်", "🔑 License Management": "🔑 လိုင်စင် စီမံခန့်ခွဲမှု", "Upload Token File": "ဖိုင်ဖြင့် သက်တမ်းတိုးမည်", "Activate via Text": "စာသားဖြင့် သက်တမ်းတိုးမည်", "Leave empty to keep current": "မပြောင်းလိုပါက အလွတ်ထားပါ", "Show WiFi Password": "ဝိုင်ဖိုင် စကားဝှက် ပြမည်", "Show PIN": "ပင်နံပါတ် ပြမည်", "Scan for Networks": "ဝိုင်ဖိုင် ရှာမည်", "Type Network Name": "ဝိုင်ဖိုင် အမည် ရိုက်ထည့်ပါ", "Choose File": "ဖိုင်ရွေးမည်", "No file chosen": "ဖိုင်ရွေးချယ်ထားခြင်းမရှိပါ", "Paste Token String (MTc...)": "ဖုန်းဖြင့်ရသော တိုကင်စာသားကို ဤနေရာတွင် ထည့်ပါ", "Firmware Update": "ဖမ်းဝဲ အပ်ဒိတ်", "Update Firmware": "ဖမ်းဝဲ အပ်ဒိတ်တင်မည်", "Upload firmware and reboot now?": "ဖမ်းဝဲ တင်ပြီး စက်ကို ယခုပြန်ဖွင့်မလား?", "Cloud ID:": "ကလောက် အိုင်ဒီ-", "Device IP:": "စက် အိုင်ပီ-", "System Role (Pairing)": "စနစ် အဆင့်အတန်း (Role)", "Standalone": "သီးသန့်စက်", "Master (Sump Tank)": "အဓိကစက် (အောက်တိုင်ကီ)", "Slave (Upper Tank)": "လက်အောက်ခံစက် (အပေါ်တိုင်ကီ)", "Device ID:": "စက် အိုင်ဒီ-" };
  window.onload = () => {
    fetch('/config').then(r=>r.json()).then(c=>{
       let so = `<option value="">-- Keep Current (${c.ssid==""?"None":c.ssid}) --</option>`;
       if(c.ws==-1) so+=`<option value="" disabled>Scanning...</option>`;
       else if(c.nets) { for(let n of c.nets) so+=`<option value="${n.s}" ${n.s==c.ssid?"selected":""}>${n.s} (${n.r}dBm)</option>`; }
       so+=`<option value="__man__">Enter SSID Manually...</option>`;
       document.getElementById('ss').innerHTML = so;
       document.getElementById('pin').value = c.pin; document.getElementById('sysLang').value = c.lang;
       document.getElementById('sysRole').value = c.sysRole;
       document.getElementById('linkID_s').value = c.linkID;
       let setMOps=""; for(let i of [0,5,10,15,20,30]) setMOps+=`<option value="${i}" ${c.setM==i?"selected":""}>${i==0?"No Settling":i+" Min"}</option>`; document.getElementById('setM_s').innerHTML = setMOps;
       toggleRole();
       document.getElementById('opM').value = c.opM;
       let vDs=""; for(let i=5; i<=15; i++) vDs+=`<option value="${i}" ${c.vDly==i?"selected":""}>${i} Seconds</option>`; document.getElementById('vDly').innerHTML = vDs;
       let tH=""; for(let f=1.0; f<=7.01; f+=0.5){ let inc=f*12.0; tH+=`<option value="${inc.toFixed(1)}" ${Math.abs(c.uH-inc)<0.1?"selected":""}>${f} ft</option>`;} document.getElementById('uH_s').innerHTML = tH;
       let lowTank=""; for(let i=20; i<=50; i+=5) lowTank+=`<option value="${i}" ${c.lowTank==i?"selected":""}>${i} %</option>`; document.getElementById('lowTank_s').innerHTML = lowTank;
       let fullTank=""; for(let i=80; i<=100; i+=5) fullTank+=`<option value="${i}" ${c.fullTank==i?"selected":""}>${i} %</option>`; document.getElementById('fullTank_s').innerHTML = fullTank;
       let vH=""; for(let i=230; i<=260; i++) vH+=`<option value="${i}" ${c.vH==i?"selected":""}>${i} Volts</option>`; document.getElementById('vH_s').innerHTML = vH;
       let vL=""; for(let i=150; i<=190; i++) vL+=`<option value="${i}" ${c.vL==i?"selected":""}>${i} Volts</option>`; document.getElementById('vL_s').innerHTML = vL;
       const vgSel = Math.min(10, Math.max(1, parseInt(c.vG ?? 5, 10) || 5));
       let vG=""; for(let i=1; i<=10; i++) vG+=`<option value="${i}" ${vgSel==i?"selected":""}>${i} Volts</option>`; document.getElementById('vG_s').innerHTML = vG;
       let dD=""; for(let i=60; i<=180; i+=5) dD+=`<option value="${i}" ${c.dD==i?"selected":""}>${i} Seconds</option>`; document.getElementById('dD_s').innerHTML = dD;
       let rM=""; for(let i of [0,5,10,15]) rM+=`<option value="${i}" ${c.rstM==i?"selected":""}>${i==0?"Disabled":i+" Min"}</option>`; document.getElementById('rstM_s').innerHTML = rM;
       let rmOps=""; for(let i of [0,30,60]) rmOps+=`<option value="${i}" ${c.rM==i?"selected":""}>${i==0?"Disabled":i+" Min"}</option>`; document.getElementById('rM_s').innerHTML = rmOps;
       document.getElementById('dndEn_s').value = c.dndEn;
       let h=""; for(let i=0; i<24; i++) h+=`<option value="${i}">${(i<10?'0':'')+i+":00"}</option>`;
       document.getElementById('dndS_s').innerHTML = h; document.getElementById('dndS_s').value = c.dndS;
       document.getElementById('dndE_s').innerHTML = h; document.getElementById('dndE_s').value = c.dndE;
       let tz=""; for(let f=-12.0; f<=14.0; f+=0.5) tz+=`<option value="${f}" ${Math.abs(c.tzOf-f)<0.1?"selected":""}>GMT ${f>=0?'+':''}${f}</option>`; document.getElementById('tz_s').innerHTML = tz;
       document.getElementById('did').innerText = c.did;
       document.getElementById('did_footer').innerText = c.did;
       document.getElementById('dip').innerText = c.ip;
       document.getElementById('dip_footer').innerText = c.ip;
       toggleVDly();
       if(c.lang==1) {
          document.body.classList.add('lang-mm');
          const w = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, null, false);
          let n; while(n = w.nextNode()){ let t=n.nodeValue.trim(); if(dict[t]) n.nodeValue = n.nodeValue.replace(t, dict[t]); }
          document.querySelectorAll('input[placeholder]').forEach(i => { let pt = i.getAttribute('placeholder').trim(); if(dict[pt]) i.setAttribute('placeholder', dict[pt]); });
          if(document.getElementById('ss').options[0]) document.getElementById('ss').options[0].text = `-- လက်ရှိအတိုင်းထားမည် (${c.ssid===""?"None":c.ssid}) --`;
       }
    });
  };
</script></body></html>
)rawliteral";

// --- SENSOR IMPLEMENTATION ---
class NonBlockingUltrasonic {
private:
  int trigPin, echoPin;
  static const int NUM_SAMPLES = 20;
  static const unsigned long SAMPLE_INTERVAL_MS = 60;
  static const unsigned long MAX_PULSE_DURATION = 15000;
  float samples[NUM_SAMPLES];
  int sampleIndex = 0;
  unsigned long lastSampleTime = 0;
  bool collecting = false;
  float lastMedian = -1.0;

  float median(float arr[], int n) {
    float temp[n];
    memcpy(temp, arr, sizeof(float) * n);
    for (int i = 0; i < n - 1; i++) {
      for (int j = i + 1; j < n; j++) {
        if (temp[j] < temp[i]) {
          float t = temp[i];
          temp[i] = temp[j];
          temp[j] = t;
        }
      }
    }
    return (n % 2 == 0) ? (temp[n / 2 - 1] + temp[n / 2]) / 2.0 : temp[n / 2];
  }

public:
  NonBlockingUltrasonic(int t, int e)
    : trigPin(t), echoPin(e) {
    pinMode(trigPin, OUTPUT);
    pinMode(echoPin, INPUT);
    digitalWrite(trigPin, LOW);
  }
  void start() {
    if (!collecting) {
      sampleIndex = 0;
      collecting = true;
      lastSampleTime = millis() - SAMPLE_INTERVAL_MS;
    }
  }
  void update() {
    unsigned long now = millis();
    if (collecting && (now - lastSampleTime >= SAMPLE_INTERVAL_MS)) {
      lastSampleTime = now;
      digitalWrite(trigPin, LOW);
      delayMicroseconds(9);
      digitalWrite(trigPin, HIGH);
      delayMicroseconds(140);
      digitalWrite(trigPin, LOW);
      long duration = pulseIn(echoPin, HIGH, MAX_PULSE_DURATION);
      float inches = (duration == 0) ? -1.0 : (duration * 0.01356 / 2.0);
      samples[sampleIndex++] = inches;
      if (sampleIndex >= NUM_SAMPLES) {
        collecting = false;
        float v[NUM_SAMPLES];
        int vc = 0;
        for (int i = 0; i < NUM_SAMPLES; i++)
          if (samples[i] > 0 && samples[i] <= MAX_DISTANCE) v[vc++] = samples[i];
        lastMedian = (vc > NUM_SAMPLES / 2) ? median(v, vc) : -1.0;
      }
    }
  }
  float getDistance() {
    return lastMedian;
  }
  bool isBusy() {
    return collecting;
  }
};

NonBlockingUltrasonic upperSensor(UPPER_TANK_TRIG_PIN, UPPER_TANK_ECHO_PIN);

void monitorSensors() {
  static unsigned long lastScan = 0;
  static int lastSentTank = -1;  // Track last sent tank level for cloud
  static int lastSentVolt = -1;  // Track last sent voltage for cloud
  unsigned long now = millis();

  // =================================================================
  // 1. ULTRASONIC TANK SENSOR
  // =================================================================
  if (now - lastScan >= ULTRASONIC_INTERVAL) {
    if (!upperSensor.isBusy()) upperSensor.start();
  }
  upperSensor.update();

  if (!upperSensor.isBusy() && now - lastScan >= ULTRASONIC_INTERVAL) {
    float dist = upperSensor.getDistance();

    // Wait max 50ms instead of portMAX_DELAY
    if (xSemaphoreTakeRecursive(systemMutex, pdMS_TO_TICKS(50))) {
      if (dist > 0) {
        tankConfig.upperInvalidCount = 0;
        tankConfig.errorAck = false;
        float diff = abs(dist - tankConfig.upperDistance);
        if (!tankConfig.firstReadingDone) {
          tankConfig.upperDistance = dist;
          tankConfig.firstReadingDone = true;
        } else if (diff > 0.3f) {
          tankConfig.upperDistance = (0.9f * dist) + (0.1f * tankConfig.upperDistance);
        }
        float effectiveHeight = tankConfig.upperHeight + TankConfig::BUFFER_HEIGHT;
        // Calculate true water depth
        float waterDepth = effectiveHeight - tankConfig.upperDistance;

        // Calculate percentage strictly based on usable tank height
        tankConfig.displayUpperPercentage = (waterDepth * 100.0f) / tankConfig.upperHeight;
        tankConfig.displayUpperPercentage = constrain(tankConfig.displayUpperPercentage, 0, 100);

        // --- IMMEDIATE CLOUD UPDATE FOR TANK LEVEL ---
        if (tankConfig.displayUpperPercentage != lastSentTank) {
          pendingMqttPublish = true;
          lastSentTank = tankConfig.displayUpperPercentage;
        }

      } else {
        tankConfig.upperInvalidCount++;
      }
      xSemaphoreGiveRecursive(systemMutex);

      // Only reset the timer if we successfully got the mutex and saved the data
      lastScan = now;
    }
  }

  // =================================================================
  // 2. VOLTAGE SENSOR (Anti-Spike Filtered)
  // =================================================================
  static unsigned long lastVoltSample = 0;
  static float rawBuf[5] = { 0 };
  static int rawIdx = 0;
  static int rawCount = 0;
  static float filteredVolt = 0.0f;
  static int jumpConfirm = 0;
  static int lowVoltConfirm = 0;

  auto median5 = [](float* a, int n) -> float {
    float t[5];
    for (int i = 0; i < n; i++) t[i] = a[i];
    for (int i = 0; i < n - 1; i++) {
      for (int j = i + 1; j < n; j++) {
        if (t[j] < t[i]) {
          float x = t[i];
          t[i] = t[j];
          t[j] = x;
        }
      }
    }
    return (n % 2 == 0) ? (t[n / 2 - 1] + t[n / 2]) * 0.5f : t[n / 2];
  };

  if (now - lastVoltSample >= 500) {
    lastVoltSample = now;
    float v = voltageSensor.getRmsVoltage();

    if (xSemaphoreTakeRecursive(systemMutex, pdMS_TO_TICKS(5))) {
      // 1) Basic validity gate
      bool valid = (!isnan(v) && v >= 40.0f && v <= 320.0f);

      if (valid) {
        lowVoltConfirm = 0;

        // 2) Median buffer
        rawBuf[rawIdx] = v;
        rawIdx = (rawIdx + 1) % 5;
        if (rawCount < 5) rawCount++;
        float med = median5(rawBuf, rawCount);

        // Initialize filter quickly on startup
        if (filteredVolt < 10.0f) filteredVolt = med;

        // 3) Outlier hold (large jump must repeat 3 times)
        float d = med - filteredVolt;
        if (fabs(d) > 18.0f) {
          jumpConfirm++;
          if (jumpConfirm < 3) med = filteredVolt;  // ignore temporary spike
        } else {
          jumpConfirm = 0;
        }

        // 4) Slew rate limit (max change each 500 ms)
        d = med - filteredVolt;
        const float maxStep = 3.0f;  // V per sample (500ms)
        if (d > maxStep) med = filteredVolt + maxStep;
        else if (d < -maxStep) med = filteredVolt - maxStep;

        // 5) Final smoothing (EMA)
        filteredVolt = 0.25f * med + 0.75f * filteredVolt;
        voltageConfig.currentVoltage = filteredVolt;
      } else {
        // Require repeated low/invalid before dropping to zero
        lowVoltConfirm++;
        if (lowVoltConfirm >= 3) {
          filteredVolt = 0.0f;
          voltageConfig.currentVoltage = 0.0f;
        }
      }

      // Cloud Update
      int currentVInt = (int)voltageConfig.currentVoltage;
      if (currentVInt != lastSentVolt) {
        pendingMqttPublish = true;
        lastSentVolt = currentVInt;
      }

      xSemaphoreGiveRecursive(systemMutex);
    }
  }
}

// ============================================================================
// SYSTEM SETUP
// ============================================================================

void setup() {
  Serial.begin(115200);
  delay(1000);
  Serial.println("\n\n=== DUAL CORE SMART PUMP SYSTEM BOOTING ===");

  systemMutex = xSemaphoreCreateRecursiveMutex();

  pinMode(MOTOR_PIN, OUTPUT);
  pinMode(BUZZER_PIN, OUTPUT);
  pinMode(SOLENOID_PIN, OUTPUT);
  pinMode(FLOW_SENSOR_PIN, INPUT_PULLUP);
  pinMode(MANUAL_BTN_PIN, INPUT_PULLUP);

  digitalWrite(MOTOR_PIN, LOW);
  digitalWrite(BUZZER_PIN, LOW);
  digitalWrite(SOLENOID_PIN, LOW);

  preferences.begin("pump-control", false);

  // Notice how we use the struct's own values as the fallback instead of hardcoded numbers!
  tankConfig.upperHeight = preferences.getFloat("upperH", tankConfig.upperHeight);
  tankConfig.LOW_THRESHOLD = constrain(preferences.getInt("lowTank", tankConfig.LOW_THRESHOLD), 20, 50);
  tankConfig.FULL_THRESHOLD = constrain(preferences.getInt("fullTank", tankConfig.FULL_THRESHOLD), 80, 100);
  voltageConfig.HIGH_THRESHOLD = preferences.getInt("vHigh", voltageConfig.HIGH_THRESHOLD);
  voltageConfig.LOW_THRESHOLD = preferences.getInt("vLow", voltageConfig.LOW_THRESHOLD);
  voltageConfig.RESUME_GAP = constrain(preferences.getInt("vGap", voltageConfig.RESUME_GAP), 1, 10);

  dryRunConfig.WAIT_SECONDS_SET = preferences.getInt("dryDelay", dryRunConfig.WAIT_SECONDS_SET);
  dryRunConfig.waitSeconds = dryRunConfig.WAIT_SECONDS_SET;  // <--- The Boot sync fix!

  dryRunConfig.autoRetryMinutes = preferences.getInt("retryMins", dryRunConfig.autoRetryMinutes);
  coolDownConfig.restMinutes = preferences.getInt("restM", coolDownConfig.restMinutes);
  compConfig.opMode = preferences.getInt("opM", compConfig.opMode);
  compConfig.valveDelay = preferences.getInt("vDly", compConfig.valveDelay);

  msConfig.sysRole = preferences.getInt("sysRole", msConfig.sysRole);
  msConfig.linkedID = preferences.getString("linkID", msConfig.linkedID);
  msConfig.settlingMinutes = preferences.getInt("setM", msConfig.settlingMinutes);

  voltageConfig.status = 0;
  voltageConfig.waitSeconds = voltageConfig.WAIT_SECONDS_SET;

  devicePin = preferences.getString("pin", devicePin);
  sysLang = preferences.getInt("sysLang", sysLang);
  ssid_saved = preferences.getString("ssid", ssid_saved);
  pass_saved = preferences.getString("pass", pass_saved);

  pumpConfig.motorStatus = preferences.getInt("motor", pumpConfig.motorStatus);
  pumpConfig.manualOverride = preferences.getBool("override", pumpConfig.manualOverride);
  pumpConfig.wasRunningBeforeVoltageError = preferences.getBool("wasRunV", pumpConfig.wasRunningBeforeVoltageError);

  scheduleConfig.enabled = preferences.getBool("dndEn", scheduleConfig.enabled);
  scheduleConfig.dndStart = preferences.getInt("dndS", scheduleConfig.dndStart);
  scheduleConfig.dndEnd = preferences.getInt("dndE", scheduleConfig.dndEnd);
  scheduleConfig.timezoneOffset = preferences.getFloat("tzOf", scheduleConfig.timezoneOffset);

  installDate = preferences.getULong("installDate", installDate);
  validDays = preferences.getInt("validDays", validDays);
  lastTokenTime = preferences.getULong("lastTokenTime", lastTokenTime);
  preferences.end();

  checkExpiry();

  rgbLed.begin();
  rgbLed.setBrightness(30);
  voltageSensor.setSensitivity(500.0f);

  Serial.println("Running Initial Hardware Safety Check...");
  for (int i = 0; i < 60; i++) {
    monitorSensors();
    delay(50);
  }
  updatePumpLogic();

  Wire.begin(SDA_PIN, SCL_PIN);
  lcd.init();
  lcd.backlight();
  lcd.createChar(1, bar1);
  lcd.createChar(2, bar2);
  lcd.createChar(3, bar3);
  lcd.createChar(4, bar4);
  lcd.createChar(5, bar5);
  lcd.createChar(6, bar6);
  lcd.createChar(7, bar7);
  lcd.createChar(8, bar8);

  lcd.setCursor(0, 0);
  lcd.print("********************");
  lcd.setCursor(0, 1);
  lcd.print("*  AUTOMATIC PUMP  *");
  lcd.setCursor(0, 2);
  lcd.print("*  CONTROL SYSTEM  *");
  lcd.setCursor(0, 3);
  lcd.print("********************");

  WiFi.setAutoReconnect(true);
  WiFi.setSleep(false);

  if (ssid_saved == "") {
    // No SSID Saved -> Force AP Mode
    Serial.println("[NET] AP Mode Started (No SSID Saved - First Setup)");
    WiFi.mode(WIFI_AP_STA);
    WiFi.softAP("Auto-Pump-Config", "12345678");
    dnsUdp.begin(DNS_PORT);
    apModeActive = true;
    apManualTriggerTime = millis();
    lastStationActiveTime = millis();
  } else {
    // SSID Exists -> STRICTLY Station Mode (No AP mode at all)
    WiFi.mode(WIFI_STA);
    apModeActive = false;
    Serial.print("Connecting to WiFi: " + ssid_saved);
    WiFi.begin(ssid_saved.c_str(), pass_saved.c_str());

    unsigned long startWait = millis();
    while (WiFi.status() != WL_CONNECTED && millis() - startWait < 5000) {
      delay(250);
      Serial.print(".");
    }

    if (WiFi.status() == WL_CONNECTED) {
      Serial.println("\nConnected! IP: " + WiFi.localIP().toString());
      requestTimeSync();
    } else {
      Serial.println("\nOffline Boot. System running locally. Will reconnect in background.");
    }
  }

  deviceID = getDeviceID();
  subTopic = "smartpump/" + deviceID + "/set";
  statusTopic = "smartpump/" + deviceID + "/status";
  onlineTopic = "smartpump/" + deviceID + "/online";

  server.on("/", handleRoot);
  server.on("/settings", handleSettings);
  server.on("/config", handleConfig);
  server.on("/save", HTTP_POST, handleSave);
  server.on("/status", handleStatus);
  server.on("/toggle", handleToggle);
  server.on("/reset", handleReset);
  server.on("/scan", handleScan);
  server.on("/logo.png", handleLogo);
  server.on("/update_github", handleUpdatePage);
  server.on("/check-ota", []() {
    checkOTA();
    server.send(200, "application/json", "{\"status\":\"checking\"}");
  });
  server.on("/start-ota", []() {
    server.send(200, "application/json", "{\"status\":\"starting\"}");
    startOTA();
  });
  server.on("/ota-upload", HTTP_POST, handleLocalOtaUpload, handleLocalOtaUploadData);
  server.on("/upload_license", HTTP_POST, handleLicenseUpload, handleLicenseUploadData);
  server.on("/apply_license", HTTP_POST, handleApplyLicenseText);
  server.on("/admin", handleAdmin);
  server.onNotFound([]() {
    server.sendHeader("Location", "http://192.168.4.1/", true);
    server.send(302, "text/plain", "Redirect");
  });
  server.begin();

  if (MDNS.begin("smartpump")) MDNS.addService("http", "tcp", 80);

  espClient.setCACert(MQTT_ROOT_CA);
  espClient.setHandshakeTimeout(30);
  espClient.setTimeout(15000);
  mqttClient.setServer(mqtt_server, mqtt_port);
  mqttClient.setCallback(mqttCallback);
  mqttClient.setBufferSize(4096);
  mqttClient.setSocketTimeout(10);
  mqttClient.setKeepAlive(60);

  esp_task_wdt_deinit();
  esp_task_wdt_config_t wdt_config = {
    .timeout_ms = 30000,
    .idle_core_mask = 0,
    .trigger_panic = true
  };
  esp_task_wdt_init(&wdt_config);
  esp_task_wdt_add(NULL);

  xTaskCreatePinnedToCore(networkTask, "NetworkTask", 12000, NULL, 1, &NetworkTaskHandle, 0);
  esp_task_wdt_add(NetworkTaskHandle);
}

void networkTask(void* parameter) {
  unsigned long lastPublish = 0;
  unsigned long lastWifiAttempt = 0;
  unsigned long lastMqttAttempt = 0;
  unsigned long heartbeatTimer = 0;

  for (;;) {  // Standard FreeRTOS infinite loop
    // Fix 1: Feed the Watchdog immediately at the start of every loop
    esp_task_wdt_reset();

    bool hasWiFi = (WiFi.status() == WL_CONNECTED);
    unsigned long now = millis();

    // --- 1. AP MODE / CAPTIVE PORTAL MANAGEMENT ---
    if (apModeActive) {
      processDNS();  // Handle redirection requests
      int stations = WiFi.softAPgetStationNum();
      if (stations > 0) lastStationActiveTime = now;

      if (ssid_saved != "") {
        if (now - lastStationActiveTime > 300000UL) {
          Serial.println("[NET] AP Mode Closed (Timeout)");
          WiFi.mode(WIFI_STA);
          dnsUdp.stop();
          apModeActive = false;
        }
      }
    } else if (ssid_saved == "") {
      WiFi.mode(WIFI_AP_STA);
      WiFi.softAP("Auto-Pump-Config", "12345678");
      dnsUdp.begin(DNS_PORT);
      apModeActive = true;
      lastStationActiveTime = now;
    }

    // --- 2. WIFI STATION RECONNECTION ---
    if (!hasWiFi && ssid_saved != "") {
      if (now - lastWifiAttempt > 30000UL) {
        lastWifiAttempt = now;
        Serial.println("[WIFI] Offline. Background reconnecting...");
        WiFi.begin(ssid_saved.c_str(), pass_saved.c_str());
      }
    }

    // --- 3. MQTT & CLOUD MANAGEMENT ---
    if (hasWiFi) {
      if (!mqttClient.connected()) {
        if (now - lastMqttAttempt > 10000) {
          reconnectMQTT();
          lastMqttAttempt = now;
        }
      } else {
        mqttClient.loop();

        if (now - heartbeatTimer > 30000) {
          pendingMqttPublish = true;
          heartbeatTimer = now;
        }

        if (pendingMqttPublish && (now - lastPublish >= 500)) {
          if (xSemaphoreTakeRecursive(systemMutex, pdMS_TO_TICKS(100))) {
            publishState();
            pendingMqttPublish = false;
            xSemaphoreGiveRecursive(systemMutex);
            lastPublish = now;
          }
        }
      }
    }

    // --- 4. WEB SERVER ---
    server.handleClient();

    // Crucial: Yield to the system to prevent CPU starvation
    vTaskDelay(pdMS_TO_TICKS(10));
  }
}

void loop() {
  monitorButton();  // Prioritized: Check button at the very start of every loop
  processDNS();
  monitorSensors();
  updatePumpLogic();
  updateLCD();
  updateLEDStatus();
  delay(1);
  esp_task_wdt_reset();
}

String getStartBlockReason() {
  if (voltageConfig.currentVoltage > voltageConfig.HIGH_THRESHOLD) return "Voltage is OVER (" + String((int)voltageConfig.currentVoltage) + "V).";
  if (voltageConfig.currentVoltage < voltageConfig.LOW_THRESHOLD) return "Voltage is UNDER (" + String((int)voltageConfig.currentVoltage) + "V).";
  if (voltageConfig.status == 0) return "Voltage stabilization in progress. Please wait.";
  if (coolDownConfig.isResting) return "Pump is cooling down. Please wait.";
  if (tankConfig.displayUpperPercentage >= tankConfig.FULL_THRESHOLD) return "The Tank is already FULL.";
  if (tankConfig.upperInvalidCount >= TankConfig::MAX_INVALID_COUNT) return "Sensor Error! Check the ultrasonic sensor.";

  if (compConfig.opMode == 1) {
    if (compConfig.isPreVenting) return "Compressor is already starting (Pre-Venting)...";
    if (compConfig.isPostVenting) return "System is still venting pressure. Please wait.";
  }

  // --- ADAPTIVE MASTER/SLAVE INTERLOCK ---
  if (msConfig.sysRole == 2 && msConfig.linkedID != "") {
    unsigned long now = millis();

    // Fix: Only wait for the VERY first sync for 5 minutes.
    // If millis() is over 5 mins and we still have 0 updates, allow Standalone mode.
    if (msConfig.lastMasterUpdate == 0 && now < MASTER_LINK_TIMEOUT) {
      return "Waiting for first sync...";
    }

    // Check if the Master is Online (Update received within 5 minutes)
    bool masterIsOnline = (WiFi.status() == WL_CONNECTED && (now - msConfig.lastMasterUpdate < MASTER_LINK_TIMEOUT));

    // If we have had at least one sync, or if we have timed out waiting for the first one:
    if (masterIsOnline && msConfig.lastMasterUpdate != 0) {
      // ENFORCE SEQUENTIAL PRIORITY: Master is online, follow its rules.
      if (msConfig.masterPStat == "ON") return "Master is Pumping. Waiting for Sump.";

      bool masterIsSafe = (msConfig.masterInfo.indexOf("STANDBY") != -1 || msConfig.masterInfo.indexOf("FLOW") != -1);

      if (!masterIsSafe) {
        return "Master Not Ready: " + msConfig.masterInfo;
      }
    } else {
      // FALLBACK: Link is dead or timed out. Allow standalone operation.
      // This will now trigger if internet is out for 5 mins OR if master is silent for 5 mins after boot.
      static unsigned long lastLog = 0;
      if (now - lastLog > 60000) {
        Serial.println("[SLAVE] Master Link Offline. Standalone Active.");
        lastLog = now;
      }
    }
  }
  return "";
}

String processManualToggle() {
  String res = "Success";
  // Use a 500ms timeout for the lock to prevent the system from freezing
  if (xSemaphoreTakeRecursive(systemMutex, pdMS_TO_TICKS(500))) {

    // (LICENSE CHECK REMOVED FROM HERE - ONLY CLOUD IS BLOCKED NOW)
    // 1. SENSOR ERROR SILENCE (Highest Priority)
    // If the buzzer is beeping because of a sensor error, the first press stops the noise.
    bool sensorErrorActive = (tankConfig.upperInvalidCount >= TankConfig::MAX_INVALID_COUNT);
    if (sensorErrorActive && !tankConfig.errorAck) {
      tankConfig.errorAck = true;  // This silences the buzzer in updatePumpLogic
      Serial.println("[USER] Sensor Alarm Silenced by User.");
      xSemaphoreGiveRecursive(systemMutex);
      pendingMqttPublish = true;
      return "Silenced";
    }

    // 2. DRY-RUN ALARM RESET
    // If system is in Dry-Run Alarm or Lock, this clears it and stops the motor.
    if (dryRunConfig.error != 0) {
      dryRunConfig.error = 0;
      dryRunConfig.waitSeconds = dryRunConfig.WAIT_SECONDS_SET;
      currentState = PumpState::IDLE;
      digitalWrite(BUZZER_PIN, LOW);
      pumpConfig.motorStatus = 0;
      pumpConfig.manualOverride = true;
      saveMotorStatus();
      Serial.println("[USER] Dry-Run Alarm Reset by User.");
      xSemaphoreGiveRecursive(systemMutex);
      pendingMqttPublish = true;
      return "Success";
    }

    // 3. VENTING LOCK (For Compressor Mode)
    // Block normal Start/Stop commands ONLY if we are currently venting
    if (compConfig.opMode == 1) {
      if (compConfig.isPreVenting) {
        xSemaphoreGiveRecursive(systemMutex);
        return "Blocked:Starting up... Please wait.";
      }
      if (compConfig.isPostVenting) {
        xSemaphoreGiveRecursive(systemMutex);
        return "Blocked:Stopping/Venting... Please wait.";
      }
    }

    // 4. NORMAL TOGGLE (Start or Stop)
    bool wantsToStart = (pumpConfig.motorStatus == 0);
    if (wantsToStart) {
      String blockReason = getStartBlockReason();
      if (blockReason != "") {
        xSemaphoreGiveRecursive(systemMutex);
        return "Blocked:" + blockReason;
      }
      pumpConfig.motorStatus = 1;
      pumpConfig.manualOverride = true;  // VIP Pass to ignore DND
      coolDownConfig.runStartTime = millis();
      dryRunConfig.waitSeconds = dryRunConfig.WAIT_SECONDS_SET;
      Serial.println("[USER] Manual Start Command Accepted.");
    } else {
      pumpConfig.motorStatus = 0;
      pumpConfig.manualOverride = true;
      Serial.println("[USER] Manual Stop Command Accepted.");
    }

    saveMotorStatus();
    xSemaphoreGiveRecursive(systemMutex);
  }
  pendingMqttPublish = true;
  return res;
}

void monitorButton() {
  static int lastReading = HIGH;
  static unsigned long buttonDownTime = 0;
  static bool longPressHandled = false;
  static unsigned long lastDebounceTime = 0;
  static int clickCount = 0;
  static unsigned long lastReleaseTime = 0;
  static bool isButtonPressed = false;

  int reading = digitalRead(MANUAL_BTN_PIN);
  unsigned long now = millis();

  // --- 1. DEBOUNCE (High Speed: 15ms) ---
  if (reading != lastReading) {
    lastDebounceTime = now;
  }

  if ((now - lastDebounceTime) > 15) {
    if (reading == LOW && !isButtonPressed) {
      // BUTTON JUST PRESSED
      isButtonPressed = true;
      buttonDownTime = now;
      longPressHandled = false;

      // --- MOUSE-LIKE DOUBLE CLICK: INSTANT TRIGGER ON SECOND PRESS DOWNSTROKE ---
      if (clickCount == 1 && (now - lastReleaseTime < 850)) {
        Serial.println("[USER] Double Click: Show System Info (INSTANT)");
        showIpUntil = now + 10000;
        // Distinct Double-Beep
        digitalWrite(BUZZER_PIN, HIGH);
        delay(40);
        digitalWrite(BUZZER_PIN, LOW);
        delay(60);
        digitalWrite(BUZZER_PIN, HIGH);
        delay(40);
        digitalWrite(BUZZER_PIN, LOW);

        clickCount = 0;
        longPressHandled = true;  // Prevents release of this click from triggering anything
      }
    } else if (reading == HIGH && isButtonPressed) {
      // BUTTON JUST RELEASED
      isButtonPressed = false;
      unsigned long pressDuration = now - buttonDownTime;

      if (!longPressHandled) {
        if (pressDuration < 600) {  // Valid short click
          clickCount = 1;           // Mark that first click is completed
          lastReleaseTime = now;
        }
      }
      buttonDownTime = 0;
    }
  }
  lastReading = reading;

  // --- 2. LONG PRESS (While holding) ---
  if (isButtonPressed && !longPressHandled && (now - buttonDownTime >= 3000)) {
    Serial.println("[NET] AP Mode Started (Manual Button Trigger)");
    longPressHandled = true;
    clickCount = 0;  // Cancel any pending clicks

    digitalWrite(BUZZER_PIN, HIGH);
    delay(1000);
    digitalWrite(BUZZER_PIN, LOW);

    WiFi.mode(WIFI_AP_STA);
    WiFi.softAP("Auto-Pump-Config", "12345678");
    dnsUdp.begin(DNS_PORT);

    apModeActive = true;
    apManualTriggerTime = now;
    lastStationActiveTime = now;
  }

  // --- 3. SINGLE CLICK TIMEOUT (Decision Logic) ---
  // Only trigger single click if:
  // - We have 1 click recorded
  // - The button is currently NOT pressed (prevents trigger while holding 2nd click)
  // - The 850ms window has expired
  // Decision Window: If 850ms has passed since the last click and no second click
  // arrived, we treat it as a deliberate "Single Click" to toggle the pump.
  if (clickCount == 1 && !isButtonPressed && (now - lastReleaseTime > 850)) {
    Serial.println("[USER] Single Click: Toggle Pump (Action after 850ms)");
    String res = processManualToggle();
    if (res.startsWith("Blocked:")) {
      setLedColor(255, 0, 0);  // Red flash on block
      digitalWrite(BUZZER_PIN, HIGH);
      delay(150);
      digitalWrite(BUZZER_PIN, LOW);
    } else {
      // Standard Single-Beep
      digitalWrite(BUZZER_PIN, HIGH);
      delay(120);
      digitalWrite(BUZZER_PIN, LOW);
    }
    clickCount = 0;
  }
}

void rebootSystem() {
  espClient.stop();
  server.stop();
  delay(2000);
  ESP.restart();
}
String getDeviceID() {
  uint64_t chipid = ESP.getEfuseMac();
  uint16_t chip = (uint16_t)(chipid >> 32);
  char id[23];
  snprintf(id, 23, "%04X%08X", chip, (uint32_t)chipid);
  return String(id);
}
void saveMotorStatus() {
  // Static variables stay in RAM and remember their values between function calls
  static int lastSavedMotor = -1;
  static bool lastSavedOverride = false;
  static bool lastSavedWasRunV = false;

  // Check if anything has ACTUALLY changed compared to our last physical save
  if (pumpConfig.motorStatus != lastSavedMotor || pumpConfig.manualOverride != lastSavedOverride || pumpConfig.wasRunningBeforeVoltageError != lastSavedWasRunV) {

    // Only open Preferences and write to Flash if values are new
    preferences.begin("pump-control", false);
    preferences.putInt("motor", pumpConfig.motorStatus);
    preferences.putBool("override", pumpConfig.manualOverride);
    preferences.putBool("wasRunV", pumpConfig.wasRunningBeforeVoltageError);
    preferences.end();

    // Update the RAM trackers so we don't save these same values again
    lastSavedMotor = pumpConfig.motorStatus;
    lastSavedOverride = pumpConfig.manualOverride;
    lastSavedWasRunV = pumpConfig.wasRunningBeforeVoltageError;

    Serial.println("[NVS] Motor state saved to Flash memory safely.");
  }
}

void setLedColor(uint8_t r, uint8_t g, uint8_t b) {
  rgbLed.setPixelColor(0, rgbLed.Color(r, g, b));
  rgbLed.show();
}

void updateLEDStatus() {
  static unsigned long lastLED = 0;
  if (millis() - lastLED < 500) return;

  PumpState tState;
  bool tRunning;

  // Try to get the mutex for max 50ms. If busy, skip this update cycle to prevent freezing.
  if (xSemaphoreTakeRecursive(systemMutex, pdMS_TO_TICKS(50))) {
    lastLED = millis();  // Update timer only if successful
    tState = currentState;
    tRunning = pumpConfig.isRunning;
    xSemaphoreGiveRecursive(systemMutex);
  } else {
    return;  // Core was busy, gracefully skip this loop
  }

  if (tState == PumpState::DRY_RUN_ALARM || tState == PumpState::DRY_RUN_LOCKED || tState == PumpState::SENSOR_ERROR || tState == PumpState::VOLTAGE_ERROR) {
    static bool flash = false;
    flash = !flash;
    setLedColor(flash ? 255 : 0, 0, 0);
  } else if (tState == PumpState::COOLING_DOWN) {
    setLedColor(0, 0, 255);
  } else if (tState == PumpState::SETTLING_WATER) {
    setLedColor(128, 0, 128);  // Purple LED for Settling
  } else if (tRunning) {
    setLedColor(255, 200, 0);
  } else if (WiFi.status() == WL_CONNECTED && mqttClient.connected()) {
    setLedColor(0, 255, 0);
  } else if (WiFi.status() == WL_CONNECTED) {
    setLedColor(0, 255, 255);
  } else {
    static int bright = 0;
    static int dir = 5;
    bright += dir;
    if (bright >= 50 || bright <= 0) dir = -dir;
    setLedColor(0, 0, bright);
  }
}

void checkExpiry() {
  if (installDate == 0) return;
  time_t now;
  time(&now);
  unsigned long nowEpoch = (unsigned long)now;
  unsigned long expiryDate = installDate + (validDays * 86400UL);
  if (xSemaphoreTakeRecursive(systemMutex, pdMS_TO_TICKS(1000))) {
    isSystemExpired = (nowEpoch > expiryDate);
    xSemaphoreGiveRecursive(systemMutex);
  }
}

bool verifyAndApplyLicense(String tokenBase64, String& outMsg) {
  tokenBase64.trim();
  if (tokenBase64.length() == 0) {
    outMsg = "Empty Token";
    return false;
  }
  unsigned char decoded[512];
  size_t outLen = 0;
  if (mbedtls_base64_decode(decoded, sizeof(decoded), &outLen, (const unsigned char*)tokenBase64.c_str(), tokenBase64.length()) != 0) {
    outMsg = "Invalid Base64";
    return false;
  }
  String raw = "";
  for (size_t i = 0; i < outLen; i++) raw += (char)decoded[i];
  int firstPipe = raw.indexOf('|');
  int lastPipe = raw.lastIndexOf('|');
  if (firstPipe == -1 || lastPipe == -1 || firstPipe == lastPipe) {
    outMsg = "Invalid Token Format";
    return false;
  }
  String tsStr = raw.substring(0, firstPipe);
  String daysStr = raw.substring(firstPipe + 1, lastPipe);
  String signature = raw.substring(lastPipe + 1);
  unsigned long tokenTs = strtoul(tsStr.c_str(), NULL, 10);
  int addDays = daysStr.toInt();
  if (tokenTs <= lastTokenTime) {
    outMsg = "Token Already Used";
    return false;
  }
  time_t now;
  time(&now);
  if ((unsigned long)now > tokenTs + 604800) {
    outMsg = "Token Expired (>7 days old)";
    return false;
  }
  String payload = tsStr + "|" + daysStr + "|ACER123|" + getDeviceID();
  MD5Builder md5;
  md5.begin();
  md5.add(payload);
  md5.calculate();
  if (!md5.toString().equalsIgnoreCase(signature)) {
    outMsg = "Invalid Signature";
    return false;
  }

  // ---> FIXED TO 2000 TICK DELAY <---
  if (xSemaphoreTakeRecursive(systemMutex, pdMS_TO_TICKS(2000))) {
    validDays += addDays;
    lastTokenTime = tokenTs;
    xSemaphoreGiveRecursive(systemMutex);

    preferences.begin("pump-control", false);
    preferences.putInt("validDays", validDays);
    preferences.putULong("lastTokenTime", lastTokenTime);
    preferences.end();
    checkExpiry();
    outMsg = "Success! Extended by " + String(addDays) + " days.";
    return true;
  } else {
    outMsg = "System Busy. Try Again.";
    return false;
  }
}

void processLicenseTokenString(String token) {
  String msg;
  bool success = verifyAndApplyLicense(token, msg);
  if (mqttClient.connected()) {
    DynamicJsonDocument doc(256);
    doc["alert"] = success ? "License Updated" : "License Failed";
    doc["reason"] = msg;
    String alertJson;
    serializeJson(doc, alertJson);
    mqttClient.publish(statusTopic.c_str(), alertJson.c_str());
  }
}

void sendLicenseResponse(int httpCode, String title, String color, String message) {
  String html = "<html><head><meta name='viewport' content='width=device-width, initial-scale=1'><style>body{font-family:sans-serif;text-align:center;padding:50px;background:#121212;color:white;}</style></head><body><h2 style='color:" + color + ";'>" + title + "</h2><p>" + message + "</p><br><a href='/settings' style='color:#03ef;text-decoration:none;font-weight:bold;padding:10px;border:1px solid #03ef;border-radius:8px;'>Back to Settings</a></body></html>";
  server.send(httpCode, "text/html", html);
}
void handleLicenseUpload() {
  server.sendHeader("Connection", "close");
  String msg;
  if (verifyAndApplyLicense(uploadedLicenseToken, msg)) sendLicenseResponse(200, "Success!", "#28a745", msg);
  else sendLicenseResponse(403, "Failed", "#dc3545", "Error: " + msg);
}
void handleLicenseUploadData() {
  HTTPUpload& upload = server.upload();
  if (upload.status == UPLOAD_FILE_START) uploadedLicenseToken = "";
  else if (upload.status == UPLOAD_FILE_WRITE)
    for (size_t i = 0; i < upload.currentSize; i++) uploadedLicenseToken += (char)upload.buf[i];
}
void handleApplyLicenseText() {
  if (server.hasArg("key")) {
    String msg;
    if (verifyAndApplyLicense(server.arg("key"), msg)) sendLicenseResponse(200, "Activated!", "#28a745", msg);
    else sendLicenseResponse(403, "Activation Failed", "#dc3545", "Error: " + msg);
  } else {
    sendLicenseResponse(400, "Error", "#dc3545", "No token provided.");
  }
}
void handleAdmin() {
  if (!server.hasArg("secret") || server.arg("secret") != "ACER123") {
    server.send(403, "text/plain", "Forbidden");
    return;
  }

  if (server.hasArg("extend")) {
    // ---> FIXED TO 2000 TICK DELAY <---
    if (xSemaphoreTakeRecursive(systemMutex, pdMS_TO_TICKS(2000))) {
      validDays += server.arg("extend").toInt();
      preferences.begin("pump-control", false);
      preferences.putInt("validDays", validDays);
      preferences.end();
      xSemaphoreGiveRecursive(systemMutex);
      server.send(200, "text/plain", "Success! Total Valid Days: " + String(validDays));
    } else {
      server.send(503, "text/plain", "System Busy. Please try again.");
    }
  } else if (server.hasArg("reset")) {
    // ---> FIXED TO 2000 TICK DELAY <---
    if (xSemaphoreTakeRecursive(systemMutex, pdMS_TO_TICKS(2000))) {
      installDate = 0;
      preferences.begin("pump-control", false);
      preferences.putULong("installDate", 0);
      preferences.end();
      xSemaphoreGiveRecursive(systemMutex);
      server.send(200, "text/plain", "License Reset.");
    } else {
      server.send(503, "text/plain", "System Busy. Please try again.");
    }
  } else {
    server.send(200, "text/plain", "Admin Mode OK\nValid Days: " + String(validDays) + "\nExpired: " + String(isSystemExpired ? "YES" : "NO"));
  }
  checkExpiry();
}

void handleBuzzerPatterns() {
  unsigned long now = millis();
  if (currentState == PumpState::VOLTAGE_ERROR) {
    //digitalWrite(BUZZER_PIN, (now / 150) % 2);  // Rapid
    digitalWrite(BUZZER_PIN, LOW);  // No buzzer on over/under voltage
  } else if (currentState == PumpState::DRY_RUN_ALARM) {
    digitalWrite(BUZZER_PIN, (now / 500) % 2);  // Urgent
  } else if (currentState == PumpState::SENSOR_ERROR && !tankConfig.errorAck) {
    unsigned long cycle = now % 5000;
    if (cycle < 600) digitalWrite(BUZZER_PIN, (cycle / 100) % 2);  // Triple beep
    else digitalWrite(BUZZER_PIN, LOW);
  } else {
    if (currentState != PumpState::PUMPING && currentState != PumpState::PRE_START_VALVE && currentState != PumpState::POST_STOP_VALVE) {
      digitalWrite(BUZZER_PIN, LOW);
    }
  }
}

void updatePumpLogic() {
  unsigned long currentMillis = millis();
  bool triggerPublish = false;

  static unsigned long lastExpiryCheck = 0;
  if (currentMillis - lastExpiryCheck > 3600000UL) {
    checkExpiry();
    lastExpiryCheck = currentMillis;
  }

  if (!xSemaphoreTakeRecursive(systemMutex, pdMS_TO_TICKS(50))) return;

  // --- NEW: CONTINUOUS MASTER/SLAVE MONITORING (Strict Version) ---
  if (msConfig.sysRole == 2 && pumpConfig.motorStatus == 1) {
    unsigned long now = millis();

    // Check if Master is Online
    bool masterIsOnline = (WiFi.status() == WL_CONNECTED && msConfig.lastMasterUpdate != 0 && (now - msConfig.lastMasterUpdate < MASTER_LINK_TIMEOUT));

    if (masterIsOnline) {
      // Check if Master is specifically in a SAFE state
      // Safe states are ONLY "SYSTEM_STANDBY!", "FLOW_DETECTED!", or "FLOW_CHECKING!"
      bool masterIsSafe = (msConfig.masterInfo.indexOf("STANDBY") != -1 || msConfig.masterInfo.indexOf("FLOW") != -1);

      // If Master is NOT safe (is Empty, Busy, Settling, Voltage Error, or Sensor Error), Slave MUST stop.
      if (!masterIsSafe) {
        pumpConfig.motorStatus = 0;
        pumpConfig.manualOverride = false;  // Reset override so it stays stopped
        Serial.println("[SLAVE] Master Not Ready (" + msConfig.masterInfo + "). Sequential Stop triggered.");
        saveMotorStatus();
        triggerPublish = true;
      }
    }
  }

  handleBuzzerPatterns();

  static PumpState lastReportedState = (PumpState)-1;

  // --- 2. SENSORS & FLOW PERSISTENCE ---
  bool physicalFlow = (digitalRead(FLOW_SENSOR_PIN) == LOW);
  if (physicalFlow) pumpConfig.lastFlowTime = currentMillis;
  if (pumpConfig.motorStatus == 0) { pumpConfig.lastFlowTime = 0; }

  bool isInitialGrace = (pumpConfig.motorStatus == 1 && (currentMillis - coolDownConfig.runStartTime < 60000UL));
  bool isInsideSlugWindow = (pumpConfig.lastFlowTime > 0 && (currentMillis - pumpConfig.lastFlowTime < 60000UL));
  bool effectiveFlow = (physicalFlow || isInitialGrace || isInsideSlugWindow);
  pumpConfig.flowDetected = effectiveFlow;

  bool sensorError = (tankConfig.upperInvalidCount >= TankConfig::MAX_INVALID_COUNT);
  const int vGap = constrain(voltageConfig.RESUME_GAP, 1, 10);
  const float highCutoff = (float)voltageConfig.HIGH_THRESHOLD;
  const float lowCutoff = (float)voltageConfig.LOW_THRESHOLD;
  const float highResume = highCutoff - vGap;
  const float lowResume = lowCutoff + vGap;
  bool voltAbnormal = (voltageConfig.status == 1) ? (voltageConfig.currentVoltage > highCutoff || voltageConfig.currentVoltage < lowCutoff)
                                                  : (voltageConfig.currentVoltage > highResume || voltageConfig.currentVoltage < lowResume);

  // --- 3. CHECK DND STATUS ---
  if (scheduleConfig.enabled) {
    struct tm timeinfo;
    bool timeIsReady = false;
    if (getLocalTime(&timeinfo, 10) && timeinfo.tm_year > 120) {
      timeIsReady = true;
      int hour = timeinfo.tm_hour;
      bool isInsideDndWindow = false;
      if (scheduleConfig.dndStart > scheduleConfig.dndEnd) {
        if (hour >= scheduleConfig.dndStart || hour < scheduleConfig.dndEnd) isInsideDndWindow = true;
      } else {
        if (hour >= scheduleConfig.dndStart && hour < scheduleConfig.dndEnd) isInsideDndWindow = true;
      }
      if (currentDndActive != isInsideDndWindow) {
        currentDndActive = isInsideDndWindow;
        triggerPublish = true;
      }
    }
    if (!timeIsReady && currentDndActive == true) {
      currentDndActive = false;
      triggerPublish = true;
    }
  } else {
    currentDndActive = false;
  }

  // --- 4. AUTO START/STOP LOGIC ---
  if (tankConfig.displayUpperPercentage >= tankConfig.FULL_THRESHOLD && pumpConfig.motorStatus == 1) {
    pumpConfig.motorStatus = 0;
    pumpConfig.manualOverride = false;
    saveMotorStatus();
    triggerPublish = true;
  }

  if (pumpConfig.motorStatus == 0 && tankConfig.displayUpperPercentage <= tankConfig.LOW_THRESHOLD && voltageConfig.status == 1 && dryRunConfig.error == 0 && !sensorError && !coolDownConfig.isResting && !currentDndActive && getStartBlockReason() == "") {
    pumpConfig.motorStatus = 1;
    coolDownConfig.runStartTime = currentMillis;
    dryRunConfig.waitSeconds = dryRunConfig.WAIT_SECONDS_SET;
    saveMotorStatus();
    triggerPublish = true;
  }

  // --- 5. TRANSITION & VENTING ---
  bool targetRunning = (pumpConfig.motorStatus == 1);
  if (targetRunning && !compConfig.lastTargetStatus) {
    msConfig.isSettling = false;
    if (compConfig.opMode == 1 && compConfig.valveDelay > 0) {
      compConfig.isPreVenting = true;
      compConfig.isPostVenting = false;
      compConfig.ventStartTime = currentMillis;
    }
  } else if (!targetRunning && compConfig.lastTargetStatus) {
    if (msConfig.sysRole == 1 && msConfig.settlingMinutes > 0) {
      msConfig.isSettling = true;
      msConfig.settleStartTime = currentMillis;
    }
    if (compConfig.opMode == 1 && compConfig.valveDelay > 0) {
      compConfig.isPostVenting = true;
      compConfig.isPreVenting = false;
      compConfig.ventStartTime = currentMillis;
    }
  }
  compConfig.lastTargetStatus = targetRunning;

  if (compConfig.isPreVenting) {
    if (currentState == PumpState::PRE_START_VALVE) {
      if (currentMillis - compConfig.ventStartTime >= (unsigned long)compConfig.valveDelay * 1000UL) {
        compConfig.isPreVenting = false;
        triggerPublish = true;
      }
    } else {
      compConfig.ventStartTime = currentMillis;
    }
  }
  if (compConfig.isPostVenting) {
    if (currentState == PumpState::POST_STOP_VALVE) {
      if (currentMillis - compConfig.ventStartTime >= (unsigned long)compConfig.valveDelay * 1000UL) {
        compConfig.isPostVenting = false;
        triggerPublish = true;
      }
    } else {
      compConfig.ventStartTime = currentMillis;
    }
  }

  // --- 6. COOL-DOWN & VOLTAGE PROTECTION ---
  if (pumpConfig.motorStatus == 1 && !compConfig.isPreVenting && !compConfig.isPostVenting) {
    if (coolDownConfig.runStartTime == 0) coolDownConfig.runStartTime = currentMillis;
    if (coolDownConfig.restMinutes > 0 && (currentMillis - coolDownConfig.runStartTime >= 3600000UL)) {
      coolDownConfig.isResting = true;
      pumpConfig.wasRunningBeforeCoolDown = true;
      coolDownConfig.restStartTime = currentMillis;
      pumpConfig.motorStatus = 0;
      saveMotorStatus();
    }
  } else if (pumpConfig.motorStatus == 0 && !coolDownConfig.isResting) {
    coolDownConfig.runStartTime = 0;
  }

  if (coolDownConfig.isResting) {
    if (currentMillis - coolDownConfig.restStartTime >= (unsigned long)coolDownConfig.restMinutes * 60000UL) {
      coolDownConfig.isResting = false;
      String blockReason = getStartBlockReason();
      if (pumpConfig.wasRunningBeforeCoolDown && blockReason == "" && voltageConfig.status == 1 && (!currentDndActive || pumpConfig.manualOverride)) {
        pumpConfig.motorStatus = 1;
        coolDownConfig.runStartTime = currentMillis;
      }
      pumpConfig.wasRunningBeforeCoolDown = false;
      saveMotorStatus();
      triggerPublish = true;
    }
  }

  if (voltAbnormal) {
    if (voltageConfig.status == 1) {
      pumpConfig.wasRunningBeforeVoltageError = (pumpConfig.motorStatus == 1);
      voltageConfig.status = 0;
      voltageConfig.waitSeconds = voltageConfig.WAIT_SECONDS_SET;
      pumpConfig.motorStatus = 0;
      saveMotorStatus();
    }
    voltageConfig.lastCheck = currentMillis;
  } else if (voltageConfig.status == 0) {
    if (currentMillis - voltageConfig.lastCheck >= 1000) {
      voltageConfig.waitSeconds--;
      voltageConfig.lastCheck = currentMillis;
      triggerPublish = true;
      if (voltageConfig.waitSeconds <= 0) {
        voltageConfig.status = 1;
        String blockReason = getStartBlockReason();
        if (pumpConfig.wasRunningBeforeVoltageError && blockReason == "" && !sensorError && !coolDownConfig.isResting && (!currentDndActive || pumpConfig.manualOverride)) {
          pumpConfig.motorStatus = 1;
          coolDownConfig.runStartTime = currentMillis;
        }
        pumpConfig.wasRunningBeforeVoltageError = false;
        saveMotorStatus();
      }
    }
  }

  if (msConfig.isSettling) {
    if (currentMillis - msConfig.settleStartTime >= (unsigned long)msConfig.settlingMinutes * 60000UL) {
      msConfig.isSettling = false;
      triggerPublish = true;
    }
  }

  // --- 7. STATE DETERMINATION ---
  if (voltAbnormal) currentState = PumpState::VOLTAGE_ERROR;
  else if (voltageConfig.status == 0) currentState = PumpState::VOLTAGE_WAIT;
  else if (compConfig.isPostVenting) currentState = PumpState::POST_STOP_VALVE;
  else if (sensorError) currentState = PumpState::SENSOR_ERROR;
  else if (coolDownConfig.isResting) currentState = PumpState::COOLING_DOWN;
  else if (msConfig.isSettling) currentState = PumpState::SETTLING_WATER;
  else if (dryRunConfig.error == 1) currentState = PumpState::DRY_RUN_ALARM;
  else if (dryRunConfig.error == 2) currentState = PumpState::DRY_RUN_LOCKED;
  else if (pumpConfig.motorStatus == 1) {
    if (compConfig.isPreVenting) currentState = PumpState::PRE_START_VALVE;
    else currentState = PumpState::PUMPING;
  } else currentState = PumpState::IDLE;

  // --- 8. HARDWARE EXECUTION ---
  switch (currentState) {
    case PumpState::PUMPING:
      digitalWrite(MOTOR_PIN, HIGH);
      digitalWrite(SOLENOID_PIN, LOW);
      pumpConfig.isRunning = true;
      if (effectiveFlow) {
        dryRunConfig.waitSeconds = dryRunConfig.WAIT_SECONDS_SET;
      } else {
        if (currentMillis - dryRunConfig.lastUpdate >= 1000) {
          dryRunConfig.waitSeconds--;
          dryRunConfig.lastUpdate = currentMillis;
          triggerPublish = true;
          if (dryRunConfig.waitSeconds <= 0) {
            dryRunConfig.error = 1;
            dryRunConfig.alarmStartTime = currentMillis;
            pumpConfig.motorStatus = 0;
            saveMotorStatus();
          }
        }
      }
      break;

    case PumpState::PRE_START_VALVE:
    case PumpState::POST_STOP_VALVE:
      digitalWrite(MOTOR_PIN, LOW);
      digitalWrite(SOLENOID_PIN, HIGH);
      pumpConfig.isRunning = false;
      break;

    case PumpState::SENSOR_ERROR:
    case PumpState::DRY_RUN_ALARM:
    case PumpState::DRY_RUN_LOCKED:
    case PumpState::SETTLING_WATER:
    default:
      digitalWrite(MOTOR_PIN, LOW);
      digitalWrite(SOLENOID_PIN, LOW);
      pumpConfig.isRunning = false;
      if (currentState == PumpState::DRY_RUN_ALARM) {
        if (currentMillis - dryRunConfig.alarmStartTime >= 60000) {
          dryRunConfig.error = 2;
          dryRunConfig.retryCountdown = dryRunConfig.autoRetryMinutes * 60;
          dryRunConfig.lastRetryUpdate = currentMillis;
        }
      } else if (currentState == PumpState::DRY_RUN_LOCKED) {
        if (dryRunConfig.autoRetryMinutes > 0 && currentMillis - dryRunConfig.lastRetryUpdate >= 1000) {
          dryRunConfig.retryCountdown--;
          dryRunConfig.lastRetryUpdate = currentMillis;
          triggerPublish = true;
          if (dryRunConfig.retryCountdown <= 0) {
            dryRunConfig.error = 0;
            dryRunConfig.waitSeconds = dryRunConfig.WAIT_SECONDS_SET;
            pumpConfig.motorStatus = 1;
            coolDownConfig.runStartTime = currentMillis;
            saveMotorStatus();
          }
        }
      }
      break;
  }

  if (currentState != lastReportedState) {
    lastReportedState = currentState;
    triggerPublish = true;
  }

  if (triggerPublish) pendingMqttPublish = true;
  xSemaphoreGiveRecursive(systemMutex);
}

// --- LCD Display Helpers ---
void custom0(int col, int r) {
  lcd.setCursor(col, r);
  lcd.write(2);
  lcd.write(8);
  lcd.write(1);
  lcd.setCursor(col, r + 1);
  lcd.write(2);
  lcd.write(6);
  lcd.write(1);
}
void custom1(int col, int r) {
  lcd.setCursor(col, r);
  lcd.write(32);
  lcd.write(32);
  lcd.write(1);
  lcd.setCursor(col, r + 1);
  lcd.write(32);
  lcd.write(32);
  lcd.write(1);
}
void custom2(int col, int r) {
  lcd.setCursor(col, r);
  lcd.write(5);
  lcd.write(3);
  lcd.write(1);
  lcd.setCursor(col, r + 1);
  lcd.write(2);
  lcd.write(6);
  lcd.write(6);
}
void custom3(int col, int r) {
  lcd.setCursor(col, r);
  lcd.write(5);
  lcd.write(3);
  lcd.write(1);
  lcd.setCursor(col, r + 1);
  lcd.write(7);
  lcd.write(6);
  lcd.write(1);
}
void custom4(int col, int r) {
  lcd.setCursor(col, r);
  lcd.write(2);
  lcd.write(6);
  lcd.write(1);
  lcd.setCursor(col, r + 1);
  lcd.write(32);
  lcd.write(32);
  lcd.write(1);
}
void custom5(int col, int r) {
  lcd.setCursor(col, r);
  lcd.write(2);
  lcd.write(3);
  lcd.write(4);
  lcd.setCursor(col, r + 1);
  lcd.write(7);
  lcd.write(6);
  lcd.write(1);
}
void custom6(int col, int r) {
  lcd.setCursor(col, r);
  lcd.write(2);
  lcd.write(3);
  lcd.write(4);
  lcd.setCursor(col, r + 1);
  lcd.write(2);
  lcd.write(6);
  lcd.write(1);
}
void custom7(int col, int r) {
  lcd.setCursor(col, r);
  lcd.write(2);
  lcd.write(8);
  lcd.write(1);
  lcd.setCursor(col, r + 1);
  lcd.write(32);
  lcd.write(32);
  lcd.write(1);
}
void custom8(int col, int r) {
  lcd.setCursor(col, r);
  lcd.write(2);
  lcd.write(3);
  lcd.write(1);
  lcd.setCursor(col, r + 1);
  lcd.write(2);
  lcd.write(6);
  lcd.write(1);
}
void custom9(int col, int r) {
  lcd.setCursor(col, r);
  lcd.write(2);
  lcd.write(3);
  lcd.write(1);
  lcd.setCursor(col, r + 1);
  lcd.write(7);
  lcd.write(6);
  lcd.write(1);
}
void printNumber(int value, int col, int r) {
  switch (value) {
    case 0: custom0(col, r); break;
    case 1: custom1(col, r); break;
    case 2: custom2(col, r); break;
    case 3: custom3(col, r); break;
    case 4: custom4(col, r); break;
    case 5: custom5(col, r); break;
    case 6: custom6(col, r); break;
    case 7: custom7(col, r); break;
    case 8: custom8(col, r); break;
    case 9: custom9(col, r); break;
  }
}

void updateLCD() {
  static bool infoModeActive = false;
  static unsigned long lastLCD = 0;
  unsigned long now = millis();

  // --- 1. HANDLE INFO OVERLAY (DOUBLE-CLICK MODE) ---
  if (now < showIpUntil) {
    if (!infoModeActive) {
      lcd.clear();
      delay(150);  // Increased delay for stability after clear

      lcd.setCursor(0, 0);
      lcd.print("--- SYSTEM INFO ---");

      // Calculate Days Left
      long daysLeft = 0;
      if (installDate > 0) {
        time_t nowT;
        time(&nowT);
        if (nowT > 1000000) {  // Ensure time is synced
          unsigned long expiry = installDate + (validDays * 86400UL);
          if (expiry > (unsigned long)nowT) daysLeft = (expiry - (unsigned long)nowT) / 86400;
        }
      }
      lcd.setCursor(0, 1);
      if (installDate == 0) lcd.print("License: PENDING");
      else if (isSystemExpired) lcd.print("License: EXPIRED");
      else lcd.print("License: " + String(daysLeft) + " Days");

      // Combine "IP: " and the address into one string for Line 2
      lcd.setCursor(0, 2);
      String ipAddr = (WiFi.status() == WL_CONNECTED) ? WiFi.localIP().toString() : "OFFLINE";
      lcd.print("IP: " + ipAddr);

      // Show Device ID on the bottom line
      lcd.setCursor(0, 3);
      lcd.print("ID: " + getDeviceID());

      infoModeActive = true;
    }
    return;
  }

  // --- 2. TRANSITION BACK TO NORMAL MODE ---
  if (infoModeActive) {
    lcd.clear();
    delay(100);
    infoModeActive = false;
  }

  // --- 3. NORMAL PUMP SCREEN THROTTLING ---
  if (now - lastLCD < 500) return;
  lastLCD = now;

  // --- 4. FETCH DATA (SAFE MUTEX) ---
  int val = 0, vVal = 0;
  bool voltAbnormal = false, isWait = false, pRunning = false, cDown = false;
  int errState = 0, autoRetMins = 0, retCd = 0, invCount = 0;
  int restMinsLeft = 0, setMinsLeft = 0;
  bool physFlow = false, isVenting = false, isSettling = false, slaveWait = false;
  float curVolt = 0;

  if (xSemaphoreTakeRecursive(systemMutex, portMAX_DELAY)) {
    val = tankConfig.displayUpperPercentage;
    int vGap = constrain(voltageConfig.RESUME_GAP, 1, 10);
    float highResume = (float)voltageConfig.HIGH_THRESHOLD - vGap;
    float lowResume = (float)voltageConfig.LOW_THRESHOLD + vGap;
    voltAbnormal = (voltageConfig.status == 1) ? (voltageConfig.currentVoltage > voltageConfig.HIGH_THRESHOLD || voltageConfig.currentVoltage < voltageConfig.LOW_THRESHOLD)
                                               : (voltageConfig.currentVoltage > highResume || voltageConfig.currentVoltage < lowResume);
    isWait = (!voltAbnormal && voltageConfig.status == 0);
    vVal = isWait ? voltageConfig.waitSeconds : (int)voltageConfig.currentVoltage;
    curVolt = voltageConfig.currentVoltage;
    pRunning = pumpConfig.isRunning;
    errState = dryRunConfig.error;
    autoRetMins = dryRunConfig.autoRetryMinutes;
    retCd = dryRunConfig.retryCountdown;
    invCount = tankConfig.upperInvalidCount;
    cDown = coolDownConfig.isResting;
    isVenting = (currentState == PumpState::PRE_START_VALVE || currentState == PumpState::POST_STOP_VALVE);
    physFlow = (digitalRead(FLOW_SENSOR_PIN) == LOW);
    if (cDown) restMinsLeft = (((coolDownConfig.restMinutes * 60000UL) - (now - coolDownConfig.restStartTime)) / 60000UL) + 1;

    isSettling = msConfig.isSettling;
    if (isSettling) setMinsLeft = (((msConfig.settlingMinutes * 60000UL) - (now - msConfig.settleStartTime)) / 60000UL) + 1;

    if (msConfig.sysRole == 2 && msConfig.linkedID != "") {
      if ((now - msConfig.lastMasterUpdate > 300000UL) || msConfig.masterPStat == "ON" || (msConfig.masterInfo != "SYSTEM_STANDBY!" && msConfig.masterInfo != "FLOW_DETECTED!" && msConfig.masterInfo != "FLOW_CHECKING!")) {
        slaveWait = true;
      }
    }
    xSemaphoreGiveRecursive(systemMutex);
  }

  // --- 5. DRAW BIG NUMBERS & CLEANUP COLUMN 9 ---
  printNumber(val / 100, 0, 0);
  printNumber((val / 10) % 10, 3, 0);
  printNumber(val % 10, 6, 0);

  lcd.setCursor(9, 0);
  lcd.print("%");
  lcd.setCursor(9, 1);
  lcd.print(" ");

  char vUnit = isWait ? 's' : 'V';
  printNumber(vVal / 100, 0, 2);
  printNumber((vVal / 10) % 10, 3, 2);
  printNumber(vVal % 10, 6, 2);

  lcd.setCursor(9, 2);
  lcd.print(vUnit);
  lcd.setCursor(9, 3);
  lcd.print(" ");

  // --- 6. DRAW RIGHT-SIDE STATUS LABELS ---
  lcd.setCursor(10, 0);
  if (cDown) lcd.print(" PUMP REST");
  else lcd.print(pRunning ? " PUMP ON  " : " PUMP OFF ");

  String info;
  if (errState == 1) info = " DRY ALRM ";
  else if (errState == 2) {
    if (autoRetMins == 0) info = " LOCKED   ";
    else {
      char buf[11];
      snprintf(buf, sizeof(buf), " WAIT %02dM", (retCd + 59) / 60);
      info = String(buf);
    }
  } else if (invCount >= TankConfig::MAX_INVALID_COUNT) info = " SNR ERR  ";
  else if (isVenting) info = " VENTING  ";
  else if (cDown) {
    char buf[11];
    snprintf(buf, sizeof(buf), " WAIT %02dM", restMinsLeft);
    info = String(buf);
  } else if (isSettling) {
    char buf[11];
    snprintf(buf, sizeof(buf), " SETTL %02dM", setMinsLeft);
    info = String(buf);
  } else if (slaveWait && !pRunning) {
    info = " WAIT SUMP";
  } else if (pRunning) {
    info = physFlow ? " FLOW OK  " : " FLOW CHK ";
  } else info = " STANDBY  ";

  while (info.length() < 10) info += " ";
  lcd.setCursor(10, 1);
  lcd.print(info.substring(0, 10));

  lcd.setCursor(10, 2);
  if (WiFi.status() != WL_CONNECTED) lcd.print(" NO WIFI  ");
  else if (!mqttClient.connected()) lcd.print(" WAITING  ");
  else lcd.print(" ONLINE   ");

  lcd.setCursor(10, 3);
  if (voltAbnormal) lcd.print(curVolt > voltageConfig.HIGH_THRESHOLD ? " OVER     " : " UNDER    ");
  else if (isWait) lcd.print(" DELAY    ");
  else lcd.print(" NORMAL   ");

  // --- 7. AP MODE INDICATOR (Blinking Dot) ---
  static bool blinkState = false;
  if (apModeActive) {
    blinkState = !blinkState;
    lcd.setCursor(19, 3);  // Bottom-right corner
    lcd.print(blinkState ? "." : " ");
  } else if (blinkState) {
    blinkState = false;
    // We don't necessarily need to clear it here because the " NORMAL   "
    // string is 10 chars long and naturally overwrites position 19 with a space,
    // but putting it here guarantees it clears safely!
    lcd.setCursor(19, 3);
    lcd.print(" ");
  }
}

bool reconnectMQTT() {
  if (WiFi.status() != WL_CONNECTED) return false;

  struct tm timeinfo;
  static unsigned long lastNtpRetry = 0;

  // Re-apply the timezone offset from your settings
  configTime(scheduleConfig.timezoneOffset * 3600, 0, "pool.ntp.org", "time.google.com");

  // Check if year is greater than 2020 (means time is synced)
  // Unix time starts at 1900. tm_year is 'years since 1900'.
  // 120 means the year is at least 2020. If less, NTP sync hasn't happened yet.
  if (!getLocalTime(&timeinfo, 10) || timeinfo.tm_year < 120) {
    if (millis() - lastNtpRetry > 30000) {
      requestTimeSync();
      lastNtpRetry = millis();
    }
    return false;
  }

  static bool ntpPrinted = false;
  if (!ntpPrinted) {
    Serial.print("[NTP] Time Synchronized: ");
    Serial.println(&timeinfo, "%A, %B %d %Y %H:%M:%S");
    ntpPrinted = true;
  }

  checkAndSaveInstallDate();
  espClient.stop();
  espClient.setHandshakeTimeout(10);
  espClient.setTimeout(10000);

  if (mqttClient.connect(("Pump-" + getDeviceID()).c_str(), mqtt_user, mqtt_pass, onlineTopic.c_str(), 1, true, "0")) {
    mqttClient.publish(onlineTopic.c_str(), "1", true);
    mqttClient.subscribe(subTopic.c_str());

    // NEW: If I am a Slave, subscribe to the Master's status topic
    if (msConfig.sysRole == 2 && msConfig.linkedID.length() > 4) {
      String masterTopic = "smartpump/" + msConfig.linkedID + "/status";
      mqttClient.subscribe(masterTopic.c_str());
      Serial.println("[MQTT] Linked to Master: " + masterTopic);
    }

    pendingMqttPublish = true;
    return true;
  }
  return false;
}

void handleScan() {
  if (WiFi.status() != WL_CONNECTED) WiFi.disconnect();
  WiFi.mode(WIFI_AP_STA);
  WiFi.scanNetworks(true);
  server.send(200, "text/plain", "OK");
}
void handleLogo() {
  server.send_P(200, "image/png", (const char*)logo_png, sizeof(logo_png));
}

void handleRoot() {
  if (WiFi.scanComplete() == -2) {
    if (WiFi.status() != WL_CONNECTED) WiFi.disconnect();
    WiFi.scanNetworks(true);
  }
  server.send_P(200, "text/html", index_html);
}

void handleSettings() {
  if (WiFi.scanComplete() == -2) {
    if (WiFi.status() != WL_CONNECTED) WiFi.disconnect();
    WiFi.scanNetworks(true);
  }
  server.send_P(200, "text/html", settings_html);
}

void handleConfig() {
  if (xSemaphoreTakeRecursive(systemMutex, pdMS_TO_TICKS(100))) {
    DynamicJsonDocument doc(2048);
    doc["ssid"] = ssid_saved;
    doc["uH"] = tankConfig.upperHeight;
    doc["lowTank"] = tankConfig.LOW_THRESHOLD;
    doc["fullTank"] = tankConfig.FULL_THRESHOLD;
    doc["vH"] = voltageConfig.HIGH_THRESHOLD;
    doc["vL"] = voltageConfig.LOW_THRESHOLD;
    doc["vG"] = voltageConfig.RESUME_GAP;
    doc["dD"] = dryRunConfig.WAIT_SECONDS_SET;
    doc["rstM"] = coolDownConfig.restMinutes;
    doc["rM"] = dryRunConfig.autoRetryMinutes;
    doc["sysRole"] = msConfig.sysRole;
    doc["linkID"] = msConfig.linkedID;
    doc["setM"] = msConfig.settlingMinutes;
    doc["opM"] = compConfig.opMode;
    doc["vDly"] = compConfig.valveDelay;
    doc["pin"] = devicePin;
    doc["lang"] = sysLang;
    doc["dndEn"] = scheduleConfig.enabled ? 1 : 0;
    doc["dndS"] = scheduleConfig.dndStart;
    doc["dndE"] = scheduleConfig.dndEnd;
    doc["tzOf"] = scheduleConfig.timezoneOffset;
    doc["ver"] = String(FIRMWARE_VERSION);
    doc["did"] = getDeviceID();
    doc["ip"] = WiFi.localIP().toString();  // <--- ADD THIS LINE

    int n = WiFi.scanComplete();
    doc["ws"] = n;
    if (n > 0) {
      JsonArray nets = doc.createNestedArray("nets");
      for (int i = 0; i < n; ++i) {
        JsonObject n_obj = nets.createNestedObject();
        n_obj["s"] = WiFi.SSID(i);
        n_obj["r"] = WiFi.RSSI(i);
      }
    }

    String json;
    serializeJson(doc, json);
    xSemaphoreGiveRecursive(systemMutex);
    server.send(200, "application/json", json);
  } else {
    server.send(503, "application/json", "{}");
  }
}

void handleSave() {
  // Wait up to 3000ms. If it fails, reject gracefully.
  if (xSemaphoreTakeRecursive(systemMutex, pdMS_TO_TICKS(3000))) {
    preferences.begin("pump-control", false);

    if (server.hasArg("ssid_sel") && server.arg("ssid_sel") != "__man__") ssid_saved = server.arg("ssid_sel");
    else if (server.hasArg("ssid_man")) ssid_saved = server.arg("ssid_man");
    if (ssid_saved != "") preferences.putString("ssid", ssid_saved);

    if (server.hasArg("pass") && server.arg("pass") != "") {
      pass_saved = server.arg("pass");
      preferences.putString("pass", pass_saved);
    }
    if (server.hasArg("uH")) {
      float h = server.arg("uH").toFloat();
      tankConfig.upperHeight = constrain(h, TankConfig::MIN_HEIGHT, TankConfig::MAX_HEIGHT);
      preferences.putFloat("upperH", tankConfig.upperHeight);
    }
    if (server.hasArg("lowTank")) {
      tankConfig.LOW_THRESHOLD = constrain(server.arg("lowTank").toInt(), 20, 50);
      preferences.putInt("lowTank", tankConfig.LOW_THRESHOLD);
    }
    if (server.hasArg("fullTank")) {
      tankConfig.FULL_THRESHOLD = constrain(server.arg("fullTank").toInt(), 80, 100);
      preferences.putInt("fullTank", tankConfig.FULL_THRESHOLD);
    }
    if (server.hasArg("vH")) {
      voltageConfig.HIGH_THRESHOLD = server.arg("vH").toInt();
      preferences.putInt("vHigh", voltageConfig.HIGH_THRESHOLD);
    }
    if (server.hasArg("vL")) {
      voltageConfig.LOW_THRESHOLD = server.arg("vL").toInt();
      preferences.putInt("vLow", voltageConfig.LOW_THRESHOLD);
    }
    if (server.hasArg("vG")) {
      voltageConfig.RESUME_GAP = constrain(server.arg("vG").toInt(), 1, 10);
      preferences.putInt("vGap", voltageConfig.RESUME_GAP);
    }
    if (server.hasArg("dD")) {
      dryRunConfig.WAIT_SECONDS_SET = server.arg("dD").toInt();
      preferences.putInt("dryDelay", dryRunConfig.WAIT_SECONDS_SET);
    }
    if (server.hasArg("sysRole")) {
      msConfig.sysRole = server.arg("sysRole").toInt();
      preferences.putInt("sysRole", msConfig.sysRole);
    }
    if (server.hasArg("linkID")) {
      msConfig.linkedID = server.arg("linkID");
      msConfig.linkedID.trim();
      msConfig.linkedID.toUpperCase();  // Force uppercase for Cloud ID
      preferences.putString("linkID", msConfig.linkedID);
    }
    if (server.hasArg("setM")) {
      msConfig.settlingMinutes = server.arg("setM").toInt();
      preferences.putInt("setM", msConfig.settlingMinutes);
    }
    if (server.hasArg("opM")) {
      compConfig.opMode = server.arg("opM").toInt();
      preferences.putInt("opM", compConfig.opMode);
    }
    if (server.hasArg("vDly")) {
      compConfig.valveDelay = server.arg("vDly").toInt();
      preferences.putInt("vDly", compConfig.valveDelay);
    }
    if (server.hasArg("rstM")) {
      coolDownConfig.restMinutes = server.arg("rstM").toInt();
      preferences.putInt("restM", coolDownConfig.restMinutes);
    }
    if (server.hasArg("rM")) {
      dryRunConfig.autoRetryMinutes = server.arg("rM").toInt();
      preferences.putInt("retryMins", dryRunConfig.autoRetryMinutes);
    }
    if (server.hasArg("pin")) {
      devicePin = server.arg("pin");
      preferences.putString("pin", devicePin);
    }
    if (server.hasArg("sysLang")) {
      sysLang = server.arg("sysLang").toInt();
      preferences.putInt("sysLang", sysLang);
    }
    if (server.hasArg("dndEn")) {
      scheduleConfig.enabled = (server.arg("dndEn").toInt() == 1);
      preferences.putBool("dndEn", scheduleConfig.enabled);
    }
    if (server.hasArg("dndS")) {
      scheduleConfig.dndStart = server.arg("dndS").toInt();
      preferences.putInt("dndS", scheduleConfig.dndStart);
    }
    if (server.hasArg("dndE")) {
      scheduleConfig.dndEnd = server.arg("dndE").toInt();
      preferences.putInt("dndE", scheduleConfig.dndEnd);
    }
    if (server.hasArg("tzOf")) {
      scheduleConfig.timezoneOffset = server.arg("tzOf").toFloat();
      preferences.putFloat("tzOf", scheduleConfig.timezoneOffset);
    }

    preferences.end();
    xSemaphoreGiveRecursive(systemMutex);

    String html = "<!DOCTYPE html><html><head><meta http-equiv=\"refresh\" content=\"20;url=/\" >"
                  "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\"><title>Rebooting</title>"
                  "<style>body{background:#121212;color:white;font-family:sans-serif;text-align:center;margin-top:50px;}</style></head>"
                  "<body><h2>Settings Saved!</h2><p>Rebooting device. Page will refresh in 20 seconds...</p></body></html>";
    server.send(200, "text/html", html);
    delay(1000);
    rebootSystem();

  } else {
    // Timeout occurred! Core saved from crash.
    server.send(503, "text/plain", "System Busy (Mutex Locked). Please try saving again.");
    Serial.println("[ERR] handleSave timed out waiting for Mutex!");
  }
}

void handleUpdatePage() {
  checkOTA();
  String html = "<!DOCTYPE html><html><head><meta charset='UTF-8'><meta name='viewport' content='width=device-width, initial-scale=1.0'><title>OTA Update</title><style>body{font-family:sans-serif; background:#121212; color:white; text-align:center; padding:40px 20px; margin:0;} .card{background:#1e1e1e; border-radius:12px; padding:30px 20px; max-width:400px; margin:auto; border:1px solid #333; box-shadow:0 10px 25px rgba(0,0,0,0.5);} h1{font-size:1.8rem; margin:0 0 15px 0; color:#03ef;} p{font-size:1.1rem; color:#ddd; margin-bottom:10px;} .info{color:#888; font-size:0.9rem; margin-top:25px; line-height:1.6; padding:15px; background:#121212; border-radius:8px; border:1px solid #333;} .btn{width:100%; padding:15px; background:#28a745; color:white; border:none; border-radius:8px; margin-top:20px; font-weight:bold; cursor:pointer; font-size:1.1rem;} .btn:active{transform:scale(0.98); opacity:0.9;} .back-link{display:inline-block; margin-top:25px; color:#aaa; text-decoration:none; font-size:1rem; padding:10px 20px; border:1px solid #333; border-radius:8px; background:#121212; transition:0.3s;} .back-link:hover{color:#fff; background:#2a2a2a;}</style></head><body><div class='card'>";
  if (otaConfig.updateAvailable) {
    html += "<h1>Update Available!</h1><p>A newer version (<strong>v" + String(otaConfig.remoteVersion) + "</strong>) is available.</p><p style='font-size:0.95rem; color:#aaa;'>Current version: v" + String(FIRMWARE_VERSION) + "</p><button class='btn' onclick=\"fetch('/start-ota').then(()=>document.body.innerHTML='<h2 style=\\'color:white;text-align:center;margin-top:50px;\\'>Updating...<br><span style=\\'font-size:1rem;color:#888;\\'>Please wait, device will reboot.</span></h2>');\">Start Update Now</button>";
  } else {
    html += "<h1 style='color:#28a745;'>No Updates</h1><p>You are on the latest version (v" + String(FIRMWARE_VERSION) + ").</p>";
  }
  html += "<div class='info'>Remote Version read: " + String(otaConfig.remoteVersion) + "<br>Free Heap: " + String(ESP.getFreeHeap()) + " bytes<br></div><a href='/settings' class='back-link'>← Back to Settings</a></div></body></html>";
  server.send(200, "text/html", html);
}

void handleLocalOtaUploadData() {
  HTTPUpload& upload = server.upload();

  if (upload.status == UPLOAD_FILE_START) {
    localOtaSuccess = false;
    localOtaError = "";

    if (mqttClient.connected()) mqttClient.disconnect();

    if (!Update.begin(UPDATE_SIZE_UNKNOWN)) {
      localOtaError = "Not enough space to start OTA.";
      Update.printError(Serial);
    }
  } else if (upload.status == UPLOAD_FILE_WRITE) {
    if (localOtaError == "") {
      size_t written = Update.write(upload.buf, upload.currentSize);
      if (written != upload.currentSize) {
        localOtaError = "Write failed.";
        Update.printError(Serial);
      }
    }
  } else if (upload.status == UPLOAD_FILE_END) {
    if (localOtaError == "") {
      if (Update.end(true)) {
        localOtaSuccess = true;
      } else {
        localOtaError = "Finalize failed.";
        Update.printError(Serial);
      }
    } else {
      Update.abort();
    }
  } else if (upload.status == UPLOAD_FILE_ABORTED) {
    localOtaError = "Upload aborted.";
    Update.abort();
  }
}

void handleLocalOtaUpload() {
  server.sendHeader("Connection", "close");

  if (localOtaSuccess && !Update.hasError()) {
    server.send(200, "text/html",
                "<!DOCTYPE html><html><head><meta charset='UTF-8'>"
                "<meta name='viewport' content='width=device-width,initial-scale=1.0'><meta http-equiv='refresh' content='10;url=/'>"
                "<style>body{font-family:sans-serif;background:#121212;color:#fff;text-align:center;padding:40px;}"
                "a{color:#03ef;}</style></head><body>"
                "<h2 style='color:#28a745;'>Firmware Updated!</h2>"
                "<p>Device is rebooting...</p></body></html>");
    delay(3000);
    ESP.restart();
  } else {
    String msg = (localOtaError.length() > 0) ? localOtaError : "Unknown OTA error.";
    String html = "<!DOCTYPE html><html><head><meta charset='UTF-8'>"
                  "<meta name='viewport' content='width=device-width,initial-scale=1.0'>"
                  "<style>body{font-family:sans-serif;background:#121212;color:#fff;text-align:center;padding:40px;}"
                  "a{color:#03ef;}</style></head><body>"
                  "<h2 style='color:#dc3545;'>Update Failed</h2>"
                  "<p>"
                  + msg + "</p><p><a href='/settings'>Back to Settings</a></p></body></html>";
    server.send(500, "text/html", html);
  }
}

void handleStatus() {
  if (xSemaphoreTakeRecursive(systemMutex, pdMS_TO_TICKS(100))) {
    String json = generateStatusJson();
    xSemaphoreGiveRecursive(systemMutex);
    server.send(200, "application/json", json);
  } else {
    server.send(503, "application/json", "{}");
  }
}

void handleToggle() {
  String res = processManualToggle();
  if (res == "Silenced") server.send(200, "application/json", "{\"status\":\"success\", \"msg\":\"Silenced\"}");
  else if (res.startsWith("Blocked:")) server.send(200, "application/json", "{\"status\":\"blocked\", \"reason\":\"" + res.substring(8) + "\"}");
  else server.send(200, "application/json", "{\"status\":\"success\"}");
}

void handleReset() {
  if (xSemaphoreTakeRecursive(systemMutex, pdMS_TO_TICKS(2000))) {
    dryRunConfig.error = 0;
    dryRunConfig.waitSeconds = dryRunConfig.WAIT_SECONDS_SET;
    digitalWrite(BUZZER_PIN, LOW);
    saveMotorStatus();
    xSemaphoreGiveRecursive(systemMutex);

    pendingMqttPublish = true;
    server.send(200, "application/json", "{\"status\":\"success\"}");
  } else {
    server.send(503, "application/json", "{\"status\":\"error\", \"reason\":\"System Busy\"}");
  }
}

String generateStatusJson() {
  DynamicJsonDocument doc(2048);  // Increased to 2048 to be safe with longer strings

  struct tm ti;
  if (getLocalTime(&ti)) {
    doc["debugHour"] = ti.tm_hour;
    doc["debugMin"] = ti.tm_min;
  } else {
    doc["debugTime"] = "NTP NOT SYNCED";
  }

  // --- VOLTAGE LOGIC (Matching updatePumpLogic exactly) ---
  int vGap = constrain(voltageConfig.RESUME_GAP, 1, 10);
  float curV = voltageConfig.currentVoltage;
  float highLimit = (voltageConfig.status == 1) ? (float)voltageConfig.HIGH_THRESHOLD : (float)voltageConfig.HIGH_THRESHOLD - vGap;
  float lowLimit = (voltageConfig.status == 1) ? (float)voltageConfig.LOW_THRESHOLD : (float)voltageConfig.LOW_THRESHOLD + vGap;

  // Data for the Dashboard
  doc["pump"] = pumpConfig.motorStatus;
  doc["tank"] = tankConfig.displayUpperPercentage;
  doc["tStr"] = tankConfig.upperInvalidCount >= TankConfig::MAX_INVALID_COUNT ? "ERR" : (tankConfig.displayUpperPercentage >= tankConfig.FULL_THRESHOLD ? "FULL" : (tankConfig.displayUpperPercentage <= tankConfig.LOW_THRESHOLD ? "LOW" : String(tankConfig.displayUpperPercentage) + "%"));
  doc["volt"] = (int)curV;

  // --- VOLT STATUS SELECTION ---
  if (curV > highLimit) doc["vStat"] = "OVER";
  else if (curV < lowLimit) doc["vStat"] = "UNDER";
  else if (voltageConfig.status == 0) doc["vStat"] = "DELAY";
  else doc["vStat"] = "NORMAL";

  doc["pStat"] = pumpConfig.isRunning ? "ON" : "OFF";

  // --- SYSTEM INFO LOGIC (With Countdown) ---
  String info;
  if (curV > highLimit) info = "OVER_VOLTAGE!";
  else if (curV < lowLimit) info = "UNDER_VOLTAGE!";
  else if (voltageConfig.status == 0) info = "VOLT_DELAY (" + String(voltageConfig.waitSeconds) + "s)";
  else if (coolDownConfig.isResting) info = "COOLING_DOWN!";
  else if (dryRunConfig.error == 1) info = "DRY_RUN_ALARM!";
  else if (dryRunConfig.error == 2) info = (dryRunConfig.autoRetryMinutes == 0) ? "PUMP_LOCKED!" : "WAITING_RETRY!";
  else if (tankConfig.upperInvalidCount >= TankConfig::MAX_INVALID_COUNT) info = "SENSOR_ERROR!";
  else if (compConfig.isPreVenting || compConfig.isPostVenting) info = "VENTING_VALVE!";
  else if (msConfig.isSettling) {
    int setMinsLeft = (((msConfig.settlingMinutes * 60000UL) - (millis() - msConfig.settleStartTime)) / 60000UL) + 1;
    if (setMinsLeft < 0) setMinsLeft = 0;
    info = "SETTLING_WATER!(" + String(setMinsLeft) + "m)";
  } else if (pumpConfig.isRunning) info = (digitalRead(FLOW_SENSOR_PIN) == LOW) ? "FLOW_DETECTED!" : "FLOW_CHECKING!";
  else info = "SYSTEM_STANDBY!";
  doc["info"] = info;

  // Settings values
  doc["id"] = deviceID;
  doc["sysRole"] = msConfig.sysRole;
  doc["linkID"] = msConfig.linkedID;
  doc["setM"] = msConfig.settlingMinutes;
  doc["ip"] = WiFi.localIP().toString();
  doc["uH"] = tankConfig.upperHeight;
  doc["lowTank"] = tankConfig.LOW_THRESHOLD;
  doc["fullTank"] = tankConfig.FULL_THRESHOLD;
  doc["vH"] = voltageConfig.HIGH_THRESHOLD;
  doc["vL"] = voltageConfig.LOW_THRESHOLD;
  doc["vG"] = voltageConfig.RESUME_GAP;
  doc["dD"] = dryRunConfig.WAIT_SECONDS_SET;
  doc["opM"] = compConfig.opMode;
  doc["vDly"] = compConfig.valveDelay;
  doc["rstM"] = coolDownConfig.restMinutes;
  doc["rM"] = dryRunConfig.autoRetryMinutes;
  doc["ssid"] = ssid_saved;
  doc["lang"] = sysLang;
  doc["pin"] = devicePin;
  doc["dndEn"] = scheduleConfig.enabled ? 1 : 0;
  doc["dndS"] = scheduleConfig.dndStart;
  doc["dndE"] = scheduleConfig.dndEnd;
  doc["tzOf"] = scheduleConfig.timezoneOffset;
  doc["dndAct"] = currentDndActive ? 1 : 0;
  doc["sErr"] = (tankConfig.upperInvalidCount >= TankConfig::MAX_INVALID_COUNT) ? 1 : 0;
  doc["ack"] = tankConfig.errorAck ? 1 : 0;
  doc["ver"] = FIRMWARE_VERSION;
  doc["nVer"] = otaConfig.remoteVersion;
  doc["ota"] = otaConfig.updateAvailable ? 1 : 0;
  doc["err"] = dryRunConfig.error;
  doc["cd"] = dryRunConfig.waitSeconds;
  doc["rCd"] = dryRunConfig.retryCountdown;
  doc["rstCd"] = coolDownConfig.isResting ? ((coolDownConfig.restMinutes * 60000UL) - (millis() - coolDownConfig.restStartTime)) / 1000 : 0;
  doc["isDR"] = (pumpConfig.isRunning && dryRunConfig.waitSeconds < dryRunConfig.WAIT_SECONDS_SET) ? 1 : 0;

  // License info
  long daysLeft = 0;
  if (installDate > 0) {
    time_t nowTime;
    time(&nowTime);
    unsigned long expiryDate = installDate + (validDays * 86400UL);
    if ((unsigned long)nowTime < expiryDate) daysLeft = (expiryDate - (unsigned long)nowTime + 86399) / 86400;
  }
  doc["expired"] = isSystemExpired ? 1 : 0;
  doc["daysLeft"] = daysLeft;

  String json;
  serializeJson(doc, json);
  return json;
}

void publishState() {
  if (WiFi.status() == WL_CONNECTED && mqttClient.connected()) {
    String json = "";
    if (xSemaphoreTakeRecursive(systemMutex, pdMS_TO_TICKS(100))) {
      json = generateStatusJson();
      xSemaphoreGiveRecursive(systemMutex);
    }
    if (json != "") {
      if (!mqttClient.publish(statusTopic.c_str(), json.c_str(), false)) Serial.println("MQTT Publish Failed!");
    }
  }
}

void mqttCallback(char* topic, byte* payload, unsigned int length) {
  String msg((char*)payload, length);
  Serial.println("MQTT Received: " + msg);

  DynamicJsonDocument doc(1024);
  DeserializationError error = deserializeJson(doc, msg);
  if (error) return;

  // --- NEW: Intercept Master's Status Update ---
  if (msConfig.sysRole == 2 && msConfig.linkedID != "") {
    if (String(topic).indexOf(msConfig.linkedID) != -1) {
      if (xSemaphoreTakeRecursive(systemMutex, pdMS_TO_TICKS(100))) {
        msConfig.masterInfo = doc["info"].as<String>();
        msConfig.masterPStat = doc["pStat"].as<String>();
        msConfig.lastMasterUpdate = millis();
        msConfig.masterOnline = true;
        xSemaphoreGiveRecursive(systemMutex);
      }
      return;  // Do not process this as a command
    }
  }
  // ---------------------------------------------

  if (doc.containsKey("lic")) {
    processLicenseTokenString(doc["lic"].as<String>());
    pendingMqttPublish = true;
    return;
  }

  if (doc.containsKey("toggle")) {
    if (doc.containsKey("pin") && String(doc["pin"].as<const char*>()) == devicePin) {

      if (isSystemExpired) {
        DynamicJsonDocument resp(256);
        resp["alert"] = "Cloud Disabled";
        resp["reason"] = "License Expired! Please renew to use cloud control.";
        String respStr;
        serializeJson(resp, respStr);
        mqttClient.publish(statusTopic.c_str(), respStr.c_str());
        return;
      }

      String result = processManualToggle();

      if (result.startsWith("Blocked:")) {
        DynamicJsonDocument resp(256);
        resp["alert"] = "System Notice";
        resp["reason"] = result.substring(8);
        String respStr;
        serializeJson(resp, respStr);
        mqttClient.publish(statusTopic.c_str(), respStr.c_str());
      }
    }
  } else if (doc.containsKey("otaCheck")) {
    if (doc.containsKey("pin") && String(doc["pin"].as<const char*>()) == devicePin) {
      checkOTA();
      DynamicJsonDocument resp(256);
      if (otaConfig.updateAvailable) {
        resp["alert"] = "Update Found!";
        resp["reason"] = "A new version (v" + String(otaConfig.remoteVersion) + ") is available.";
      } else {
        resp["alert"] = "System Up to Date";
        resp["reason"] = "You are already using the latest version: v" + String(FIRMWARE_VERSION);
      }
      String respStr;
      serializeJson(resp, respStr);
      mqttClient.publish(statusTopic.c_str(), respStr.c_str());
    }
  } else if (doc.containsKey("otaStart")) {
    if (doc.containsKey("pin") && String(doc["pin"].as<const char*>()) == devicePin) startOTA();
  } else if (doc.containsKey("reset")) {
    if (doc.containsKey("pin") && String(doc["pin"].as<const char*>()) == devicePin) {

      // ---> FIXED TO 2000 TICK DELAY <---
      if (xSemaphoreTakeRecursive(systemMutex, pdMS_TO_TICKS(2000))) {
        dryRunConfig.error = 0;
        dryRunConfig.waitSeconds = dryRunConfig.WAIT_SECONDS_SET;
        digitalWrite(BUZZER_PIN, LOW);
        saveMotorStatus();
        xSemaphoreGiveRecursive(systemMutex);
        pendingMqttPublish = true;
      } else {
        Serial.println("[ERR] MQTT Reset dropped - System Busy");
      }
    }
  } else if (doc.containsKey("get")) {
    if (doc.containsKey("pin") && String(doc["pin"].as<const char*>()) == devicePin) pendingMqttPublish = true;
    else {
      DynamicJsonDocument resp(256);
      resp["alert"] = "Access Denied";
      resp["reason"] = "Unauthorized Request - Wrong PIN!";
      String respStr;
      serializeJson(resp, respStr);
      mqttClient.publish(statusTopic.c_str(), respStr.c_str());
    }
  } else if (doc.containsKey("save")) {
    if (doc.containsKey("pin") && String(doc["pin"].as<const char*>()) == devicePin) {

      if (isSystemExpired) {
        DynamicJsonDocument resp(256);
        resp["alert"] = "Cloud Disabled";
        resp["reason"] = "License Expired! Cannot save settings via Cloud.";
        String respStr;
        serializeJson(resp, respStr);
        mqttClient.publish(statusTopic.c_str(), respStr.c_str());
        return;
      }

      // ---> FIXED TO 3000 TICK DELAY <---
      if (xSemaphoreTakeRecursive(systemMutex, pdMS_TO_TICKS(3000))) {
        preferences.begin("pump-control", false);

        if (doc.containsKey("ssid") && doc["ssid"].as<String>() != "") {
          ssid_saved = doc["ssid"].as<String>();
          preferences.putString("ssid", ssid_saved);
        }
        if (doc.containsKey("pass") && doc["pass"].as<String>() != "") {
          pass_saved = doc["pass"].as<String>();
          preferences.putString("pass", pass_saved);
        }
        if (doc.containsKey("uH")) {
          tankConfig.upperHeight = doc["uH"].as<float>();
          preferences.putFloat("upperH", tankConfig.upperHeight);
        }
        if (doc.containsKey("lowTank")) {
          tankConfig.LOW_THRESHOLD = constrain(doc["lowTank"].as<int>(), 20, 50);
          preferences.putInt("lowTank", tankConfig.LOW_THRESHOLD);
        }
        if (doc.containsKey("fullTank")) {
          tankConfig.FULL_THRESHOLD = constrain(doc["fullTank"].as<int>(), 80, 100);
          preferences.putInt("fullTank", tankConfig.FULL_THRESHOLD);
        }
        if (doc.containsKey("vH")) {
          voltageConfig.HIGH_THRESHOLD = doc["vH"].as<int>();
          preferences.putInt("vHigh", voltageConfig.HIGH_THRESHOLD);
        }
        if (doc.containsKey("vL")) {
          voltageConfig.LOW_THRESHOLD = doc["vL"].as<int>();
          preferences.putInt("vLow", voltageConfig.LOW_THRESHOLD);
        }
        if (doc.containsKey("vG")) {
          voltageConfig.RESUME_GAP = constrain(doc["vG"].as<int>(), 1, 10);
          preferences.putInt("vGap", voltageConfig.RESUME_GAP);
        }
        if (doc.containsKey("dD")) {
          dryRunConfig.WAIT_SECONDS_SET = doc["dD"].as<int>();
          preferences.putInt("dryDelay", dryRunConfig.WAIT_SECONDS_SET);
        }
        if (doc.containsKey("sysRole")) {
          msConfig.sysRole = doc["sysRole"].as<int>();
          preferences.putInt("sysRole", msConfig.sysRole);
        }
        if (doc.containsKey("linkID")) {
          msConfig.linkedID = doc["linkID"].as<String>();
          msConfig.linkedID.trim();
          msConfig.linkedID.toUpperCase();
          preferences.putString("linkID", msConfig.linkedID);
        }
        if (doc.containsKey("setM")) {
          msConfig.settlingMinutes = doc["setM"].as<int>();
          preferences.putInt("setM", msConfig.settlingMinutes);
        }
        if (doc.containsKey("opM")) {
          compConfig.opMode = doc["opM"].as<int>();
          preferences.putInt("opM", compConfig.opMode);
        }
        if (doc.containsKey("vDly")) {
          compConfig.valveDelay = doc["vDly"].as<int>();
          preferences.putInt("vDly", compConfig.valveDelay);
        }
        if (doc.containsKey("rstM")) {
          coolDownConfig.restMinutes = doc["rstM"].as<int>();
          preferences.putInt("restM", coolDownConfig.restMinutes);
        }
        if (doc.containsKey("rM")) {
          dryRunConfig.autoRetryMinutes = doc["rM"].as<int>();
          preferences.putInt("retryMins", dryRunConfig.autoRetryMinutes);
        }
        if (doc.containsKey("newPin")) {
          devicePin = doc["newPin"].as<String>();
          preferences.putString("pin", devicePin);
        }
        if (doc.containsKey("sysLang")) {
          sysLang = doc["sysLang"].as<int>();
          preferences.putInt("sysLang", sysLang);
        }
        if (doc.containsKey("dndEn")) {
          scheduleConfig.enabled = (doc["dndEn"].as<int>() == 1);
          preferences.putBool("dndEn", scheduleConfig.enabled);
        }
        if (doc.containsKey("dndS")) {
          scheduleConfig.dndStart = doc["dndS"].as<int>();
          preferences.putInt("dndS", scheduleConfig.dndStart);
        }
        if (doc.containsKey("dndE")) {
          scheduleConfig.dndEnd = doc["dndE"].as<int>();
          preferences.putInt("dndE", scheduleConfig.dndEnd);
        }
        if (doc.containsKey("tzOf")) {
          scheduleConfig.timezoneOffset = doc["tzOf"].as<float>();
          preferences.putFloat("tzOf", scheduleConfig.timezoneOffset);
        }

        xSemaphoreGiveRecursive(systemMutex);
        preferences.end();
        Serial.println("Cloud Settings Saved. Rebooting...");
        delay(500);
        rebootSystem();
      } else {
        Serial.println("[ERR] MQTT Save dropped - System Busy");
      }
    }
  }
}

// --- OTA Implementation ---
void checkOTA() {
  if (WiFi.status() != WL_CONNECTED) return;
  WiFiClientSecure client;
  client.setCACert(MQTT_ROOT_CA);
  client.setTimeout(10000);
  HTTPClient http;
  String verUrl = String(FW_URL_BASE) + "version.txt";
  http.setFollowRedirects(HTTPC_STRICT_FOLLOW_REDIRECTS);
  http.setUserAgent("ESP32-Auto-Pump");
  if (http.begin(client, verUrl)) {
    int httpCode = http.GET();
    if (httpCode == HTTP_CODE_OK) {
      String payload = http.getString();
      String cleanVer = "";
      for (int i = 0; i < payload.length(); i++)
        if (isdigit(payload[i])) cleanVer += payload[i];
      if (cleanVer.length() > 0) {
        int remoteVer = cleanVer.toInt();

        if (xSemaphoreTakeRecursive(systemMutex, pdMS_TO_TICKS(2000))) {
          otaConfig.remoteVersion = remoteVer;
          if (remoteVer > FIRMWARE_VERSION) {
            otaConfig.updateAvailable = true;
            otaConfig.newVersion = remoteVer;
          } else {
            otaConfig.updateAvailable = false;
          }
          xSemaphoreGiveRecursive(systemMutex);
        }
      }
      pendingMqttPublish = true;
    }
    http.end();
  }
}

void startOTA() {
  if (WiFi.status() != WL_CONNECTED || !otaConfig.updateAvailable) return;
  if (mqttClient.connected()) mqttClient.disconnect();
  WiFiClientSecure client;
  client.setCACert(MQTT_ROOT_CA);
  client.setTimeout(15000);
  httpUpdate.setLedPin(RGB_LED_PIN, LOW);
  lcd.clear();
  lcd.setCursor(0, 0);
  lcd.print("UPDATING SYSTEM");
  lcd.setCursor(0, 1);
  lcd.print("Do not turn off");
  t_httpUpdate_return ret = httpUpdate.update(client, String(FW_URL_BASE) + "firmware.bin");
  if (ret == HTTP_UPDATE_FAILED) {
    lcd.clear();
    lcd.setCursor(0, 0);
    lcd.print("UPDATE FAILED!");
    delay(5000);
    ESP.restart();
  }
}
