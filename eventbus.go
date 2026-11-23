package witchesmqtt

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var ErrNilHandler = errors.New("eventbus: nil handler")

type EventBus struct {
	mu       sync.RWMutex
	handlers []Handler
}

func NewEventBus(handlers ...Handler) *EventBus {
	b := &EventBus{}

	for _, h := range handlers {
		if h != nil {
			b.handlers = append(b.handlers, h)
		}
	}

	return b
}

func (b *EventBus) Subscribe(h Handler) {
	if h == nil {
		return
	}

	b.mu.Lock()
	b.handlers = append(b.handlers, h)
	b.mu.Unlock()
}

func (b *EventBus) Publish(ctx context.Context, e Event) error {
	b.mu.RLock()
	handlers := append([]Handler(nil), b.handlers...)
	b.mu.RUnlock()

	var firstErr error

	for _, h := range handlers {
		if err := ctx.Err(); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			break
		}

		err := callHandler(ctx, h, e)
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

func callHandler(ctx context.Context, h Handler, e Event) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("eventbus: handler panic: %v", r)
		}
	}()

	return h.Handle(ctx, e)
}
