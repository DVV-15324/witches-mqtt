package witchesmqtt

import (
	"context"
	"errors"
	"sync"
)

var (
	ErrInvalidSequence = errors.New("cannot confirm event with sequence zero")
	ErrEventNotFound   = errors.New("event not found")
)

type Store interface {
	Save(ctx context.Context, e Event) error
	Pending(ctx context.Context, deviceID string) ([]Event, error)
	Confirm(ctx context.Context, deviceID string, seq uint64) error
}

type MemoryStore struct {
	mu     sync.Mutex
	data   map[string]Event
	ord    []string
	nextID uint64
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		data: make(map[string]Event),
	}
}

func (m *MemoryStore) Save(_ context.Context, e Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if e.Status == "" {
		e.Status = "pending"
	}

	key := storeKey(e)

	// Events without sequence or timestamps cannot be reliably deduplicated.
	// Give them unique keys rather than collapsing unrelated events.
	if e.Seq == 0 && e.ReadingAt.IsZero() && e.ReceivedAt.IsZero() {
		m.nextID++
		key = e.DeviceID + ":u:" + uitoa(m.nextID)
	}

	if _, exists := m.data[key]; exists {
		return nil
	}

	e = cloneEvent(e)
	m.data[key] = e
	m.ord = append(m.ord, key)

	return nil
}

func (m *MemoryStore) Pending(
	_ context.Context,
	deviceID string,
) ([]Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([]Event, 0)

	for _, key := range m.ord {
		e, exists := m.data[key]
		if !exists {
			continue
		}

		if e.DeviceID == deviceID && e.Status == "pending" {
			out = append(out, cloneEvent(e))
		}
	}

	return out, nil
}

func (m *MemoryStore) Confirm(
	_ context.Context,
	deviceID string,
	seq uint64,
) error {
	if seq == 0 {
		return ErrInvalidSequence
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	key := deviceID + ":s:" + uitoa(seq)

	e, exists := m.data[key]
	if !exists {
		return ErrEventNotFound
	}

	if e.Status == "confirmed" {
		return nil // idempotent confirmation
	}

	e.Status = "confirmed"
	m.data[key] = e

	return nil
}

func cloneEvent(e Event) Event {
	if e.Raw != nil {
		raw := make(map[string]any, len(e.Raw))
		for k, v := range e.Raw {
			raw[k] = v
		}
		e.Raw = raw
	}
	return e
}

func uitoa(u uint64) string {
	if u == 0 {
		return "0"
	}

	var b [20]byte
	i := len(b)

	for u > 0 {
		i--
		b[i] = byte('0' + u%10)
		u /= 10
	}

	return string(b[i:])
}

func storeKey(e Event) string {
	if e.Seq > 0 {
		return e.DeviceID + ":s:" + uitoa(e.Seq)
	}

	ts := e.ReadingAt
	if ts.IsZero() {
		ts = e.ReceivedAt
	}

	return e.DeviceID + ":t:" + uitoa(uint64(ts.UnixNano()))
}
