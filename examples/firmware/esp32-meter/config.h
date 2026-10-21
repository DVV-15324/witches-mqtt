#pragma once

#define WIFI_SSID ""
#define WIFI_PASSWORD ""

#define MQTT_HOST ""
#define MQTT_PORT 1883
#define MQTT_USER ""
#define MQTT_PASS ""

#define DEVICE_ID "cam-101a"
#define ROOM_ID "101"

#define TOPIC_METER_ELEC "v1/devices/" DEVICE_ID "/meter/electric"
#define TOPIC_METER_WATER "v1/devices/" DEVICE_ID "/meter/water"
#define TOPIC_STATUS "v1/devices/" DEVICE_ID "/status"
#define TOPIC_CMD "v1/devices/" DEVICE_ID "/cmd"

// Thời gian đọc cảm biến 10000ms
#define READ_INTERVAL_MS 10000

// Số bản ghi tối đa lưu trong bộ nhớ Flash khi mất mạng
#define MAX_BUFFERED_READINGS 50