#include <WiFi.h>
#include <PubSubClient.h>
#include <Preferences.h>
#include <ArduinoJson.h>
#include <time.h>
#include "config.h"

WiFiClient netClient;
PubSubClient mqtt(netClient);
Preferences prefs;

// Định nghĩa loại cảm biến
#define KIND_ELEC 0
#define KIND_WATER 1

// Cấu trúc 1 bản ghi
struct Reading
{
  uint64_t seq;
  uint8_t kind; // 0: Điện, 1: Nước
  double value;
  int64_t readingAt;
  bool sent;
};

Reading buffer[MAX_BUFFERED_READINGS];
size_t bufferCount = 0;
uint64_t nextSeq = 1;

// Biến giả lập cảm biến (sau này thay bằng đọc chân GPIO thật :)) )
double mockElectricAccum = 1000.0;
double mockWaterAccum = 50.0;

// Quản lý bộ đệm (Lưu vào Flash - NVS)
void persistState()
{
  prefs.begin("meter", false);
  prefs.putULong64("nextSeq", nextSeq);
  prefs.putDouble("mockElec", mockElectricAccum);
  prefs.putDouble("mockWater", mockWaterAccum);
  prefs.putUInt("bufCount", bufferCount);
  if (bufferCount > 0)
  {
    prefs.putBytes("buffer", buffer, sizeof(Reading) * bufferCount);
  }
  prefs.end();
}

void loadState()
{
  prefs.begin("meter", true);
  nextSeq = prefs.getULong64("nextSeq", 1);
  mockElectricAccum = prefs.getDouble("mockElec", 1000.0);
  mockWaterAccum = prefs.getDouble("mockWater", 50.0);
  bufferCount = prefs.getUInt("bufCount", 0);

  if (bufferCount > MAX_BUFFERED_READINGS)
    bufferCount = 0;
  if (bufferCount > 0)
  {
    prefs.getBytes("buffer", buffer, sizeof(Reading) * bufferCount);
  }
  prefs.end();
}

// Thêm bản ghi mới vào bộ đệm
void enqueueReading(uint8_t kind, double value, int64_t ts)
{
  if (bufferCount >= MAX_BUFFERED_READINGS)
  {
    // Nếu đầy, đẩy bản ghi cũ nhất ra khỏi hàng đợi (FIFO)
    memmove(buffer, buffer + 1, sizeof(Reading) * (MAX_BUFFERED_READINGS - 1));
    bufferCount = MAX_BUFFERED_READINGS - 1;
  }

  Reading &r = buffer[bufferCount++];
  r.seq = nextSeq++;
  r.kind = kind;
  r.value = value;
  r.readingAt = ts;
  r.sent = false;

  persistState();
  Serial.printf("Enqueued: seq=%llu, kind=%d, val=%.2f\n", r.seq, r.kind, r.value);
}

// Gửi dữ liệu lên MQTT
bool sendReading(const Reading &r)
{
  JsonDocument doc;
  doc["device_id"] = DEVICE_ID;
  doc["room_id"] = ROOM_ID;
  doc["value"] = r.value;
  doc["reading_at"] = r.readingAt;
  doc["seq"] = r.seq;

  const char *topic = "";
  if (r.kind == KIND_ELEC)
  {
    topic = TOPIC_METER_ELEC;
    doc["unit"] = "kwh";
  }
  else
  {
    topic = TOPIC_METER_WATER;
    doc["unit"] = "m3";
  }

  char payload[256];
  size_t len = serializeJson(doc, payload, sizeof(payload));

  bool ok = mqtt.publish(topic, (const uint8_t *)payload, len, false);
  if (ok)
  {
    Serial.printf("Sent seq=%llu to %s\n", r.seq, topic);
  }
  return ok;
}

// Xả bộ đệm
void flushBuffer()
{
  if (!mqtt.connected() || bufferCount == 0)
    return;

  size_t sentCount = 0;
  for (size_t i = 0; i < bufferCount; i++)
  {
    if (buffer[i].sent)
      continue; // Bỏ qua nếu đã gửi (đề phòng crash giữa chừng)

    if (sendReading(buffer[i]))
    {
      buffer[i].sent = true;
      sentCount++;
      delay(50); // Nghỉ 1 nhịp nhỏ tránh nghẽn broker
    }
    else
    {
      Serial.println("Publish failed, will retry later.");
      break; // Mất mạng -> Dừng gửi, đợi vòng loop sau
    }
  }

  // Dọn dẹp các bản ghi đã gửi thành công khỏi bộ đệm
  if (sentCount > 0)
  {
    size_t shift = 0;
    while (shift < bufferCount && buffer[shift].sent)
      shift++;

    if (shift > 0)
    {
      memmove(buffer, buffer + shift, sizeof(Reading) * (bufferCount - shift));
      bufferCount -= shift;
      persistState();
      Serial.printf("Flushed %d readings. Remaining in buffer: %d\n", sentCount, bufferCount);
    }
  }
}

// Xử lý lệnh từ Server
void onMqttMessage(char *topic, byte *payload, unsigned int len)
{
  if (strcmp(topic, TOPIC_CMD) != 0)
    return;

  JsonDocument doc;
  if (deserializeJson(doc, payload, len) != DeserializationError::Ok)
    return;

  const char *cmd = doc["cmd"] | "";

  if (strcmp(cmd, "read_now") == 0)
  {
    Serial.println("Command: read_now received.");
    int64_t ts = (int64_t)time(nullptr);
    enqueueReading(KIND_ELEC, mockElectricAccum, ts);
    enqueueReading(KIND_WATER, mockWaterAccum, ts);
    flushBuffer();
  }
  else if (strcmp(cmd, "reboot") == 0)
  {
    Serial.println("Command: reboot received. Restarting...");
    persistState();
    delay(200);
    ESP.restart();
  }
}

// Kết nối WiFi & MQTT
void ensureWifi()
{
  if (WiFi.status() == WL_CONNECTED)
    return;

  Serial.printf("Connecting to WiFi %s ", WIFI_SSID);
  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);

  unsigned long t0 = millis();
  while (WiFi.status() != WL_CONNECTED && millis() - t0 < 20000)
  {
    delay(500);
    Serial.print(".");
  }

  if (WiFi.status() == WL_CONNECTED)
  {
    Serial.println("\nWiFi Connected.");
    configTime(0, 0, "pool.ntp.org", "time.google.com");
  }
  else
  {
    Serial.println("\nWiFi Connect Failed. Will retry.");
  }
}

void ensureMqtt()
{
  if (mqtt.connected())
    return;
  if (WiFi.status() != WL_CONNECTED)
    return;

  Serial.print("Connecting to MQTT... ");
  String clientId = String("node-") + DEVICE_ID;
  String willPayload = "offline";

  bool ok = mqtt.connect(
      clientId.c_str(),
      strlen(MQTT_USER) ? MQTT_USER : nullptr,
      strlen(MQTT_PASS) ? MQTT_PASS : nullptr,
      TOPIC_STATUS, 1, true, willPayload.c_str());

  if (ok)
  {
    Serial.println("Connected.");
    mqtt.publish(TOPIC_STATUS, "online", true);
    mqtt.subscribe(TOPIC_CMD, 1);
    flushBuffer();
  }
  else
  {
    Serial.printf("Failed (rc=%d). Will retry.\n", mqtt.state());
  }
}

// Hàm đọc cảm biến (Sau này sửa ở đây)
void readSensors()
{
  // TODO: Thay bằng code đọc cảm biến thật.
  // Ví dụ: Đọc xung từ chân GPIO, tính ra số điện tiêu thụ.
  mockElectricAccum += 0.5 + (random(0, 30) / 10.0);
  mockWaterAccum += 0.1 + (random(0, 10) / 100.0);
}

// Setup & Loop
void setup()
{
  Serial.begin(115200);
  delay(500);
  Serial.println("\n--- ESP32 Meter Node Starting ---");

  loadState();
  if (nextSeq == 0)
    nextSeq = 1;

  mqtt.setServer(MQTT_HOST, MQTT_PORT);
  mqtt.setCallback(onMqttMessage);
  mqtt.setBufferSize(512);
  mqtt.setKeepAlive(30);

  ensureWifi();
}

unsigned long lastReadMillis = 0;

void loop()
{
  ensureWifi();
  ensureMqtt();
  mqtt.loop();

  unsigned long now = millis();
  if (now - lastReadMillis >= READ_INTERVAL_MS)
  {
    lastReadMillis = now;

    // 1. Đọc cảm biến
    readSensors();
    int64_t ts = (int64_t)time(nullptr);

    // 2. Lưu vào bộ đệm
    enqueueReading(KIND_ELEC, mockElectricAccum, ts);
    enqueueReading(KIND_WATER, mockWaterAccum, ts);

    // 3. Cố gắng gửi đi
    flushBuffer();
  }

  // Nhường CPU cho hệ thống
  delay(100);
}