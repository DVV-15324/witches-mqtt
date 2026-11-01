package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	witchesmqtt "github.com/DVV-15324/witches-mqtt"
	logg "github.com/DVV-15324/witches/pkg/core/response/logger"
)

func main() {
	log, err := logg.NewFileLogger("logs/logs.log", 1, 20, 30)
	if err != nil {
		fmt.Printf("failed to create logger: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	store := witchesmqtt.NewMemoryStore()
	router := witchesmqtt.NewRouter()

	bus := witchesmqtt.NewEventBus(
		witchesmqtt.HandlerFunc(func(ctx context.Context, e witchesmqtt.Event) error {
			return store.Save(ctx, e)
		}),
		witchesmqtt.HandlerFunc(func(_ context.Context, e witchesmqtt.Event) error {
			log.Info(fmt.Sprintf("would notify via Zalo, device=%s, kind=%s, value=%f, unit=%s, seq=%d",
				e.DeviceID, e.Kind, e.Value, e.Unit, e.Seq))
			return nil
		}),
	)

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
		log.Error(fmt.Sprintf("mount failed: %v", err))
		os.Exit(1)
	}

	go func() {
		t := time.NewTicker(15 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				h := mod.Health(context.Background())
				log.Info(fmt.Sprintf("health, up=%v, patterns=%d", h.Up, h.Subscribed))
			}
		}
	}()

	if err := mod.Run(ctx); err != nil {
		log.Error(fmt.Sprintf("run failed: %v", err))
	}
	log.Info("shutdown complete")
}

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
			RoomID:     p.RoomID,
			ReadingAt:  time.Unix(p.ReadingAt, 0),
			ReceivedAt: time.Now(),
			Seq:        p.Seq,
			Status:     "pending",
		})
	}
}

func statusHandler(log witchesmqtt.ILogger) witchesmqtt.TopicHandler {
	return func(_ context.Context, topic string, payload []byte) error {
		log.Info(fmt.Sprintf("device status, topic=%s, payload=%s", topic, string(payload)))
		return nil
	}
}
