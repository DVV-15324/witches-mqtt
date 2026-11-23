package witchesmqtt

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
)

var (
	ErrClientClosed = errors.New("witches-mqtt: client closed")
	ErrNotReady     = errors.New("witches-mqtt: client not ready")
)

type Client struct {
	cfg    Config
	router *Router
	bus    *EventBus
	store  Store
	paho   paho.Client
	log    ILogger

	ctx    context.Context
	cancel context.CancelFunc

	connected atomic.Bool
	ready     atomic.Bool
	up        atomic.Bool

	sem chan struct{}

	mu        sync.Mutex
	closed    bool
	workers   sync.WaitGroup
	closeOnce sync.Once

	publishMu sync.Mutex

	connectMu sync.Mutex
}

func newClient(
	cfg Config,
	r *Router,
	bus *EventBus,
	store Store,
	log ILogger,
) *Client {
	ctx, cancel := context.WithCancel(context.Background())

	c := &Client{
		cfg:    cfg,
		router: r,
		bus:    bus,
		store:  store,
		log:    log,
		ctx:    ctx,
		cancel: cancel,
	}

	if cfg.MaxConcurrency > 0 {
		c.sem = make(chan struct{}, cfg.MaxConcurrency)
	}

	return c
}

func (c *Client) connect() error {
	c.connectMu.Lock()
	defer c.connectMu.Unlock()
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClientClosed
	}
	c.mu.Unlock()

	opts := paho.NewClientOptions().
		AddBroker(c.cfg.BrokerURL).
		SetClientID(c.cfg.ClientID).
		SetKeepAlive(c.cfg.KeepAlive).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(c.cfg.ConnectRetry).
		SetMaxReconnectInterval(30 * time.Second).
		SetConnectTimeout(10 * time.Second).
		SetCleanSession(c.cfg.CleanSession).
		SetOrderMatters(false)

	if c.cfg.TLS != nil {
		tlsCfg, err := buildTLS(c.cfg.TLS)
		if err != nil {
			return fmt.Errorf("witches-mqtt: tls: %w", err)
		}
		opts.SetTLSConfig(tlsCfg)
	}

	opts.SetOnConnectHandler(func(client paho.Client) {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return
		}
		c.connected.Store(true)
		c.ready.Store(false)
		c.up.Store(false)
		c.mu.Unlock()

		c.log.Info(fmt.Sprintf("connected broker %s", c.cfg.BrokerURL))

		if err := c.subscribeAll(client); err != nil {
			c.log.Error(fmt.Sprintf("subscribe failed: %v", err))
			c.ready.Store(false)
			c.up.Store(false)
			return
		}

		c.mu.Lock()
		if !c.closed {
			c.ready.Store(true)
			c.up.Store(true)
		}
		c.mu.Unlock()
	})

	opts.SetConnectionLostHandler(func(_ paho.Client, err error) {
		c.connected.Store(false)
		c.ready.Store(false)
		c.up.Store(false)
		c.log.Warn(fmt.Sprintf("connection lost: %v", err))
	})

	client := paho.NewClient(opts)

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClientClosed
	}
	c.paho = client
	c.mu.Unlock()

	token := client.Connect()

	if !token.WaitTimeout(15 * time.Second) {
		client.Disconnect(0)
		c.connected.Store(false)
		c.ready.Store(false)
		c.up.Store(false)
		c.cancel()

		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()

		return errors.New("witches-mqtt: connect timeout")
	}

	if err := token.Error(); err != nil {
		client.Disconnect(0)
		c.connected.Store(false)
		c.ready.Store(false)
		c.up.Store(false)
		c.cancel()

		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()

		return fmt.Errorf("witches-mqtt: connect: %w", err)
	}

	return nil
}

func buildTLS(c *TLSConfig) (*tls.Config, error) {
	ca, err := os.ReadFile(c.CAFile)
	if err != nil {
		return nil, fmt.Errorf("read ca: %w", err)
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, errors.New("invalid CA pem")
	}

	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load client keypair: %w", err)
	}

	return &tls.Config{
		RootCAs:      pool,
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}, nil
}

func (c *Client) subscribeAll(client paho.Client) error {
	patterns := c.router.Patterns()
	if len(patterns) == 0 {
		return nil
	}

	topics := make(map[string]byte, len(patterns))
	for _, p := range patterns {
		topics[p] = c.cfg.QoS
	}

	token := client.SubscribeMultiple(topics, c.onMessage)

	if !token.WaitTimeout(15 * time.Second) {
		return errors.New("witches-mqtt: subscribe timeout")
	}

	return token.Error()
}

func (c *Client) onMessage(_ paho.Client, msg paho.Message) {
	topic := msg.Topic()
	payload := append([]byte(nil), msg.Payload()...)

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}

	if c.sem != nil {
		select {
		case c.sem <- struct{}{}:
		default:
			c.log.Warn(fmt.Sprintf(
				"concurrency limit hit, dropping topic=%s",
				topic,
			))
			c.mu.Unlock()
			return
		}
	}

	c.workers.Add(1)
	c.mu.Unlock()

	go func() {
		defer c.workers.Done()

		if c.sem != nil {
			defer func() { <-c.sem }()
		}

		if err := c.router.Dispatch(c.ctx, topic, payload); err != nil {
			c.log.Error(fmt.Sprintf(
				"dispatch failed, topic=%s, err=%v",
				topic,
				err,
			))
		}
	}()
}

func (c *Client) publish(
	topic string,
	payload []byte,
	qos byte,
	retained bool,
) error {
	c.publishMu.Lock()
	defer c.publishMu.Unlock()

	c.mu.Lock()
	closed := c.closed
	client := c.paho
	c.mu.Unlock()

	if closed {
		return ErrClientClosed
	}

	if !c.ready.Load() || client == nil {
		return ErrNotReady
	}

	token := client.Publish(topic, qos, retained, payload)

	if !token.WaitTimeout(5 * time.Second) {
		return errors.New("witches-mqtt: publish timeout")
	}

	return token.Error()
}

func (c *Client) close() {

	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		client := c.paho
		c.ready.Store(false)
		c.up.Store(false)
		c.mu.Unlock()

		// Stop workers from accepting further work.
		c.cancel()
		c.connectMu.Lock()
		defer c.connectMu.Unlock()

		// Wait for all accepted handlers.
		c.workers.Wait()

		// Serialize shutdown with regular Publish calls.
		c.publishMu.Lock()
		defer c.publishMu.Unlock()

		if client != nil {
			if c.cfg.StatusTopic != "" && c.connected.Load() {
				token := client.Publish(
					c.cfg.StatusTopic,
					c.cfg.QoS,
					true,
					[]byte("offline"),
				)
				token.WaitTimeout(2 * time.Second)
			}

			client.Disconnect(500)
		}

		c.connected.Store(false)
		c.ready.Store(false)
		c.up.Store(false)
	})
}
