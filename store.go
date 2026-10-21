package witchesmqtt

import (
	"context"
	"sync"
)

// Store lưu event. Idempotent theo (DeviceID, Seq) — QoS 1 có thể trùng.
type Store interface {
	Save(ctx context.Context, e Event) error
	Pending(ctx context.Context, deviceID string) ([]Event, error)
	Confirm(ctx context.Context, deviceID string, seq uint64) error
}

// MemoryStore cho dev/test. Production: Postgres với UNIQUE(device_id, seq).
type MemoryStore struct {
	mu   sync.Mutex
	data map[string]Event // key: deviceID:seq
	ord  []string
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{data: make(map[string]Event)}
}

func (m *MemoryStore) Save(_ context.Context, e Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := e.DeviceID + ":" + uitoa(e.Seq)
	if _, ok := m.data[key]; ok {
		return nil // dedupe
	}
	m.data[key] = e
	m.ord = append(m.ord, key)
	return nil
}

func (m *MemoryStore) Pending(_ context.Context, deviceID string) ([]Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Event
	for _, k := range m.ord {
		e := m.data[k]
		if e.DeviceID == deviceID && e.Status == "pending" {
			out = append(out, e)
		}
	}
	return out, nil
}

func (m *MemoryStore) Confirm(_ context.Context, deviceID string, seq uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := deviceID + ":" + uitoa(seq)
	if e, ok := m.data[key]; ok {
		e.Status = "confirmed"
		m.data[key] = e
	}
	return nil
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
