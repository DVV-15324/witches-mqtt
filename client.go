package witchesmqtt

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
)

type Client struct {
	cfg    Config
	router *Router
	bus    *EventBus
	store  Store
	paho   paho.Client
	log    *slog.Logger
	ctx    context.Context
	cancel context.CancelFunc
	up     atomic.Bool
	sem    chan struct{} // giới hạn concurrency
}

func newClient(cfg Config, r *Router, bus *EventBus, store Store, log *slog.Logger) *Client {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Client{
		cfg:    cfg,
		router: r,
		bus:    bus,
		store:  store,
		log:    log.With("component", "witches-mqtt"),
		ctx:    ctx,
		cancel: cancel,
	}
	if cfg.MaxConcurrency > 0 {
		c.sem = make(chan struct{}, cfg.MaxConcurrency)
	}
	return c
}

func (c *Client) connect() error {
	opts := paho.NewClientOptions().
		AddBroker(c.cfg.BrokerURL).
		SetClientID(c.cfg.ClientID).
		SetUsername(c.cfg.Username).
		SetPassword(c.cfg.Password).
		SetKeepAlive(c.cfg.KeepAlive).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(c.cfg.ConnectRetry).
		SetMaxReconnectInterval(30 * time.Second).
		SetConnectTimeout(10 * time.Second).
		SetCleanSession(c.cfg.CleanSession).
		SetOrderMatters(false)

	if c.cfg.StatusTopic != "" {
		opts.SetWill(c.cfg.StatusTopic, "offline", c.cfg.QoS, true)
	}

	if c.cfg.TLS != nil {
		tlsCfg, err := buildTLS(c.cfg.TLS)
		if err != nil {
			return fmt.Errorf("witches-mqtt: tls: %w", err)
		}
		opts.SetTLSConfig(tlsCfg)
	}

	opts.SetOnConnectHandler(func(_ paho.Client) {
		c.up.Store(true)
		c.log.Info("connected", "broker", c.cfg.BrokerURL)
		if err := c.subscribeAll(); err != nil {
			c.log.Error("resubscribe failed", "err", err)
		}
	})
	opts.SetConnectionLostHandler(func(_ paho.Client, err error) {
		c.up.Store(false)
		c.log.Warn("connection lost", "err", err)
	})

	c.paho = paho.NewClient(opts)
	token := c.paho.Connect()
	if !token.WaitTimeout(15 * time.Second) {
		return errors.New("witches-mqtt: connect timeout")
	}
	return token.Error()
}

func buildTLS(c *TLSConfig) (*tls.Config, error) {
	cfg := &tls.Config{InsecureSkipVerify: c.Insecure}
	if c.CAFile != "" {
		ca, err := os.ReadFile(c.CAFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(ca)
		cfg.RootCAs = pool
	}
	if c.CertFile != "" && c.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			return nil, err
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

func (c *Client) subscribeAll() error {
	patterns := c.router.Patterns()
	if len(patterns) == 0 {
		return nil
	}
	topics := make(map[string]byte, len(patterns))
	for _, p := range patterns {
		topics[p] = c.cfg.QoS
	}
	token := c.paho.SubscribeMultiple(topics, c.onMessage)
	if !token.WaitTimeout(15 * time.Second) {
		return errors.New("witches-mqtt: subscribe timeout")
	}
	return token.Error()
}

func (c *Client) onMessage(_ paho.Client, msg paho.Message) {
	if c.sem != nil {
		select {
		case c.sem <- struct{}{}:
			defer func() { <-c.sem }()
		default:
			c.log.Warn("concurrency limit hit, dropping", "topic", msg.Topic())
			return
		}
	}
	go func() {
		if err := c.router.Dispatch(c.ctx, msg.Topic(), msg.Payload()); err != nil {
			c.log.Error("dispatch failed", "topic", msg.Topic(), "err", err)
		}
	}()
}

func (c *Client) publish(topic string, payload []byte, qos byte, retained bool) error {
	if !c.up.Load() {
		return errors.New("witches-mqtt: not connected")
	}
	token := c.paho.Publish(topic, qos, retained, payload)
	if !token.WaitTimeout(5 * time.Second) {
		return errors.New("witches-mqtt: publish timeout")
	}
	return token.Error()
}

func (c *Client) close() {
	if c.paho != nil && c.up.Load() {
		if c.cfg.StatusTopic != "" {
			c.paho.Publish(c.cfg.StatusTopic, c.cfg.QoS, true, []byte("offline"))
		}
		c.paho.Disconnect(500)
	}
	c.cancel()
}
