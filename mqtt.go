package witchesmqtt

import (
	"context"
	"log/slog"

	"golang.org/x/sync/errgroup"
)

type Module struct {
	client *Client
	log    *slog.Logger
}

// Mount gắn MQTT vào app. Trả Module để caller có thể dùng Health, Client, v.v.
//
// Ví dụ:
//
//	m, err := witchesmqtt.Mount(witchesmqtt.Config{
//	    BrokerURL: "tcp://localhost:1883",
//	    ClientID:  "backend-prod",
//	}, witchesmqtt.Options{
//	    Router: router,
//	    Bus:    bus,
//	    Store:  store,
//	    Logger: slog.Default(),
//	})
func Mount(cfg Config, opts Options) (*Module, error) {
	cfg.setDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	if opts.Router == nil {
		opts.Router = NewRouter()
	}
	if opts.Bus == nil {
		opts.Bus = NewEventBus()
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Store == nil {
		opts.Store = NewMemoryStore()
	}

	c := newClient(cfg, opts.Router, opts.Bus, opts.Store, opts.Logger)
	if err := c.connect(); err != nil {
		return nil, err
	}

	return &Module{client: c, log: opts.Logger}, nil
}

type Options struct {
	Router *Router
	Bus    *EventBus
	Store  Store
	Logger *slog.Logger
}

// Run block cho tới khi ctx bị cancel, sau đó đóng kết nối sạch.
func (m *Module) Run(ctx context.Context) error {
	<-ctx.Done()
	m.client.close()
	return nil
}

// Health cho HTTP /health endpoint.
func (m *Module) Health(ctx context.Context) Health {
	return m.client.Health(ctx)
}

// Publish cho phép app chủ động gửi lệnh xuống device.
// Trả lỗi nếu chưa kết nối — caller nên retry hoặc log.
func (m *Module) Publish(topic string, payload []byte, qos byte, retained bool) error {
	return m.client.publish(topic, payload, qos, retained)
}

// ensure errgroup dùng được khi caller muốn chạy song song với HTTP server.
var _ = errgroup.Group{}
