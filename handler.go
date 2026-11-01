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
	Status     string         `json:"status"` // pending | confirmed | disputed
}

type Handler interface {
	Handle(ctx context.Context, e Event) error
}

type HandlerFunc func(ctx context.Context, e Event) error

func (f HandlerFunc) Handle(ctx context.Context, e Event) error { return f(ctx, e) }

type EventBus struct {
	handlers []Handler
}

func NewEventBus(handlers ...Handler) *EventBus {
	return &EventBus{handlers: handlers}
}

func (b *EventBus) Subscribe(h Handler) {
	b.handlers = append(b.handlers, h)
}

func (b *EventBus) Publish(ctx context.Context, e Event) error {
	var firstErr error
	for _, h := range b.handlers {
		if err := h.Handle(ctx, e); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
