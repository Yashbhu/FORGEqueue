package auth

import (
	"context"
	"sync"
	"time"
)

// StateStore persists short-lived OIDC flow state (CSRF state, PKCE verifier)
// and one-time login codes across the redirect round trip.
type StateStore interface {
	Save(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Load(ctx context.Context, key string) ([]byte, bool, error)
	Delete(ctx context.Context, key string) error
}

type memEntry struct {
	value   []byte
	expires time.Time
}

// MemoryStore is an in-process StateStore for tests and single-instance use.
// It is safe for concurrent use and lazily expires entries.
type MemoryStore struct {
	mu    sync.Mutex
	items map[string]memEntry
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{items: make(map[string]memEntry)}
}

func (m *MemoryStore) Save(_ context.Context, key string, value []byte, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.items[key] = memEntry{value: append([]byte(nil), value...), expires: time.Now().Add(ttl)}
	return nil
}

func (m *MemoryStore) Load(_ context.Context, key string) ([]byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.items[key]
	if !ok {
		return nil, false, nil
	}
	if time.Now().After(e.expires) {
		delete(m.items, key)
		return nil, false, nil
	}
	return append([]byte(nil), e.value...), true, nil
}

func (m *MemoryStore) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.items, key)
	return nil
}
