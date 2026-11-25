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

type inboundMessage struct {
	topic   string
	payload []byte
}
type Client struct {
	cfg         Config
	router      *Router
	paho        paho.Client
	log         ILogger
	ctx         context.Context
	cancel      context.CancelFunc
	connected   atomic.Bool
	ready       atomic.Bool
	up          atomic.Bool
	sem         chan struct{}
	mu          sync.Mutex
	closed      bool
	workers     sync.WaitGroup
	closeOnce   sync.Once
	publishMu   sync.Mutex
	connectMu   sync.Mutex
	enableQueue bool
	queue       chan inboundMessage
	workerCount int
}

func newClient(cfg Config, r *Router, log ILogger) *Client {
	ctx, cancel := context.WithCancel(context.Background())

	c := &Client{
		cfg:    cfg,
		router: r,
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
	in := inboundMessage{
		topic:   msg.Topic(),
		payload: append([]byte(nil), msg.Payload()...),
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return
	}

	// Queue mode
	if c.enableQueue {
		select {
		case c.queue <- in:
			// Successfully enqueued.
		default:
			c.log.Warn(fmt.Sprintf(
				"mqtt queue full, dropping topic=%s",
				in.topic,
			))
		}
		return
	}

	// Direct dispatch mode (existing behavior)
	if c.sem != nil {
		select {
		case c.sem <- struct{}{}:
		default:
			c.log.Warn(fmt.Sprintf(
				"concurrency limit hit, dropping topic=%s",
				in.topic,
			))
			return
		}
	}

	c.workers.Add(1)

	go func() {
		defer c.workers.Done()

		if c.sem != nil {
			defer func() { <-c.sem }()
		}

		if err := c.router.Dispatch(c.ctx, in.topic, in.payload); err != nil {
			c.log.Error(fmt.Sprintf(
				"dispatch failed topic=%s err=%v",
				in.topic,
				err,
			))
		}
	}()
}

func (c *Client) publish(topic string, payload []byte, qos byte, retained bool) error {
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

		c.connectMu.Lock()
		defer c.connectMu.Unlock()

		c.publishMu.Lock()
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
		c.publishMu.Unlock()

		if c.enableQueue {
			c.mu.Lock()
			close(c.queue)
			c.mu.Unlock()

			// Drain accepted messages before canceling context.
			c.workers.Wait()
			c.cancel()
		} else {
			c.cancel()
			c.workers.Wait()
		}

		c.connected.Store(false)
		c.ready.Store(false)
		c.up.Store(false)
	})
}

func (c *Client) startWorkers() error {
	if !c.enableQueue {
		return nil
	}

	size := c.cfg.BufferSize
	if size <= 0 {
		size = 512
	}

	workerCount := c.cfg.MaxConcurrency
	if workerCount <= 0 {
		workerCount = 64
	}

	c.queue = make(chan inboundMessage, size)
	c.workerCount = workerCount

	for i := 0; i < workerCount; i++ {
		c.workers.Add(1)
		go c.worker(i)
	}

	return nil
}

func (c *Client) worker(id int) {
	defer c.workers.Done()
	//fmt.Printf("[Worker %d] started\n", id)
	for msg := range c.queue {
		//fmt.Printf("[Worker %d] received topic=%s payload=%s\n",
		//	id, msg.topic, string(msg.payload))

		err := c.router.Dispatch(c.ctx, msg.topic, msg.payload)
		if err != nil {
			fmt.Printf("[Worker %d] dispatch error: %v\n", id, err)
		}
	}
	//fmt.Printf("[Worker %d] stopped\n", id)
}
