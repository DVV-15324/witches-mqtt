package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	witchesmqtt "github.com/DVV-15324/witches-mqtt"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	store := witchesmqtt.NewMemoryStore()
	router := witchesmqtt.NewRouter()

	// Bus: fan-out event tới nhiều handler.
	// - store.Save: lưu raw với status=pending
	// - notify: giả lập gửi Zalo xác nhận
	// Không handler nào tự chốt số — chỉ lưu pending + thông báo.
	bus := witchesmqtt.NewEventBus(
		witchesmqtt.HandlerFunc(func(ctx context.Context, e witchesmqtt.Event) error {
			return store.Save(ctx, e)
		}),
		witchesmqtt.HandlerFunc(func(_ context.Context, e witchesmqtt.Event) error {
			log.Info("would notify via Zalo",
				"device", e.DeviceID,
				"kind", e.Kind,
				"value", e.Value,
				"unit", e.Unit,
				"seq", e.Seq,
			)
			return nil
		}),
	)

	// Router handlers: parse payload thô → build Event → bus.Publish.
	// KHÔNG gọi store, KHÔNG gửi thông báo — bus làm việc đó.
	// Đây là ranh giới giữa adapter (MQTT) và business (Event).
	router.Handle("v1/devices/+/meter/electric", meterHandler("electric", bus))
	router.Handle("v1/devices/+/meter/water", meterHandler("water", bus))
	router.Handle("v1/devices/+/status", statusHandler(log))

	mod, err := witchesmqtt.Mount(witchesmqtt.Config{
		BrokerURL: "tcp://localhost:1883",
		ClientID:  "backend-dev",
	}, witchesmqtt.Options{
		Router: router,
		Bus:    bus,
		Store:  store,
		Logger: log,
	})
	if err != nil {
		log.Error("mount failed", "err", err)
		os.Exit(1)
	}

	// Health ticker — dừng đúng khi ctx cancel, không leak goroutine
	go func() {
		t := time.NewTicker(15 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				h := mod.Health(context.Background())
				log.Info("health", "up", h.Up, "patterns", h.Subscribed)
			}
		}
	}()

	if err := mod.Run(ctx); err != nil {
		log.Error("run failed", "err", err)
	}
	log.Info("shutdown complete")
}

// meterHandler parse payload từ topic, build Event, đẩy vào bus.
// Trả TopicHandler vì đây là tầng adapter (biết về topic, payload thô).
func meterHandler(kind string, bus *witchesmqtt.EventBus) witchesmqtt.TopicHandler {
	return func(ctx context.Context, topic string, payload []byte) error {
		var p struct {
			DeviceID  string  `json:"device_id"`
			RoomID    string  `json:"room_id"`
			Value     float64 `json:"value"`
			Unit      string  `json:"unit"`
			ReadingAt int64   `json:"reading_at"`
			Seq       uint64  `json:"seq"`
		}
		if err := json.Unmarshal(payload, &p); err != nil {
			return fmt.Errorf("meter[%s]: unmarshal: %w", kind, err)
		}
		if p.DeviceID == "" || p.Seq == 0 {
			return fmt.Errorf("meter[%s]: missing device_id or seq", kind)
		}
		return bus.Publish(ctx, witchesmqtt.Event{
			DeviceID:   p.DeviceID,
			Kind:       "meter." + kind,
			Value:      p.Value,
			Unit:       p.Unit,
			ReadingAt:  time.Unix(p.ReadingAt, 0),
			ReceivedAt: time.Now(),
			Seq:        p.Seq,
			Status:     "pending",
		})
	}
}

// statusHandler chỉ log — status không có Seq nên không đi qua Store được
// (dedupe key sẽ collapse hết về "deviceID:0"). Nếu sau này cần lưu status,
// tạo Store riêng hoặc thêm dimension vào key.
func statusHandler(log *slog.Logger) witchesmqtt.TopicHandler {
	return func(_ context.Context, topic string, payload []byte) error {
		log.Info("device status", "topic", topic, "payload", string(payload))
		return nil
	}
}
