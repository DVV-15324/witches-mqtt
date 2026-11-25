package witchesmqtt

import (
	"context"
	"time"
)

type Event struct {
	DeviceID   string         `json:"device_id"`
	Kind       string         `json:"kind"` // meter.electric | meter.water | status | ...
	Value      float64        `json:"value,omitempty"`
	RoomID     string         `json:"room_id"`
	Unit       string         `json:"unit,omitempty"`
	ReadingAt  time.Time      `json:"reading_at"`
	ReceivedAt time.Time      `json:"received_at"`
	Seq        uint64         `json:"seq,omitempty"`
	Raw        map[string]any `json:"raw,omitempty"`
}

type Handler interface {
	Handle(ctx context.Context, e Event) error
}

type HandlerFunc func(ctx context.Context, e Event) error

func (f HandlerFunc) Handle(ctx context.Context, e Event) error { return f(ctx, e) }
