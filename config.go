package witchesmqtt

import (
	"errors"
	"fmt"
	"time"
)

type Config struct {
	// Broker
	BrokerURL    string
	ClientID     string
	Username     string
	Password     string
	KeepAlive    time.Duration
	ConnectRetry time.Duration
	CleanSession bool

	// Behaviour
	QoS            byte   // default 1
	BufferSize     int    // default 512
	MaxConcurrency int    // default 64, 0 = unlimited
	StatusTopic    string // default v1/services/{client_id}/status

	// TLS (Bỏ comment khi production)
	TLS *TLSConfig
}

type TLSConfig struct {
	CAFile   string
	CertFile string
	KeyFile  string
	Insecure bool
}

func (c *Config) setDefaults() {
	if c.KeepAlive == 0 {
		c.KeepAlive = 30 * time.Second
	}
	if c.ConnectRetry == 0 {
		c.ConnectRetry = 5 * time.Second
	}
	if c.QoS == 0 {
		c.QoS = 1
	}
	if c.BufferSize == 0 {
		c.BufferSize = 512
	}
	if c.StatusTopic == "" && c.ClientID != "" {
		c.StatusTopic = fmt.Sprintf("v1/services/%s/status", c.ClientID)
	}
}

func (c *Config) validate() error {
	if c.BrokerURL == "" {
		return errors.New("witches-mqtt: broker_url required")
	}
	if c.ClientID == "" {
		return errors.New("witches-mqtt: client_id required")
	}
	if c.QoS > 2 {
		return errors.New("witches-mqtt: qos must be 0, 1, or 2")
	}
	return nil
}
