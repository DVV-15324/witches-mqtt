package witchesmqtt

import "context"

type Health struct {
	Up         bool   `json:"up"`
	BrokerURL  string `json:"broker_url"`
	ClientID   string `json:"client_id"`
	Subscribed int    `json:"subscribed_patterns"`
}

func (c *Client) Health(_ context.Context) Health {
	return Health{
		Up:         c.up.Load(),
		BrokerURL:  c.cfg.BrokerURL,
		ClientID:   c.cfg.ClientID,
		Subscribed: len(c.router.Patterns()),
	}
}
