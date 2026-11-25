package witchesmqtt

import (
	"context"
	"golang.org/x/sync/errgroup"
)

var _ = errgroup.Group{}

type Options struct {
	Router      *Router
	Logger      ILogger
	EnableQueue bool // enable bounded queue + worker pool
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

	c := newClient(cfg, opts.Router, opts.Logger)
	if err := c.connect(); err != nil {
		return nil, err
	}
	c.enableQueue = opts.EnableQueue

	if err := c.startWorkers(); err != nil {
		return nil, err
	}

	if err := c.connect(); err != nil {
		c.close()
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
