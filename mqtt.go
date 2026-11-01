package witchesmqtt

import (
	"context"
	"golang.org/x/sync/errgroup"
)

type Options struct {
	Router *Router
	Bus    *EventBus
	Store  Store
	Logger ILogger
}

type Module struct {
	client *Client
	log    ILogger
}

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
		opts.Logger = &defaultLogger{}
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

func (m *Module) Run(ctx context.Context) error {
	<-ctx.Done()
	m.client.close()
	return nil
}

func (m *Module) Health(ctx context.Context) Health {
	return m.client.Health(ctx)
}

func (m *Module) Publish(topic string, payload []byte, qos byte, retained bool) error {
	return m.client.publish(topic, payload, qos, retained)
}

var _ = errgroup.Group{}

type defaultLogger struct{}

func (l *defaultLogger) Info(msg string)  {}
func (l *defaultLogger) Warn(msg string)  {}
func (l *defaultLogger) Error(msg string) {}
func (l *defaultLogger) Debug(msg string) {}
