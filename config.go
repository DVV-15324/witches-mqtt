package witchesmqtt

import (
	"errors"
	"fmt"
	"time"
)

type Config struct {
	// Broker
	BrokerURL    string        `yaml:"broker_url"`
	ClientID     string        `yaml:"client_id"`
	Username     string        `yaml:"username"`
	Password     string        `yaml:"password"`
	KeepAlive    time.Duration `yaml:"keep_alive"`
	ConnectRetry time.Duration `yaml:"connect_retry"`
	CleanSession bool          `yaml:"clean_session"`

	// Behaviour
	QoS            byte   `yaml:"qos"`             // default 1
	BufferSize     int    `yaml:"buffer_size"`     // default 512
	MaxConcurrency int    `yaml:"max_concurrency"` // default 64, 0 = unlimited
	StatusTopic    string `yaml:"status_topic"`    // default v1/services/{client_id}/status

	// TLS
	TLS *TLSConfig `yaml:"tls,omitempty"`
}

type TLSConfig struct {
	CAFile   string `yaml:"ca_file"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
	Insecure bool   `yaml:"insecure"`
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
