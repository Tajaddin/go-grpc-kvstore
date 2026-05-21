// Package store is an in-memory key-value store with TTL expiry and a
// prefix-based watch (pub/sub). It is safe for concurrent use.
package store

import (
	"sort"
	"strings"
	"sync"
	"time"
)

// EventType mirrors the proto enum without importing it, so the store has no
// dependency on generated code.
type EventType int

const (
	EventSet EventType = iota
	EventDelete
	EventExpire
)

// Event is a change notification delivered to watchers.
type Event struct {
	Type  EventType
	Key   string
	Value []byte
}

type entry struct {
	value     []byte
	expiresAt time.Time // zero = no expiry
}

func (e entry) expired(now time.Time) bool {
	return !e.expiresAt.IsZero() && now.After(e.expiresAt)
}

type watcher struct {
	prefix string
	ch     chan Event
}

// Store is a concurrent in-memory KV store.
type Store struct {
	mu       sync.RWMutex
	data     map[string]entry
	watchers map[int]*watcher
	nextID   int

	now    func() time.Time // injectable clock for tests
	stopCh chan struct{}
	stopped bool
}

// New creates a Store and starts a background sweeper that evicts expired keys
// every sweepInterval. Call Close to stop the sweeper.
func New(sweepInterval time.Duration) *Store {
	s := &Store{
		data:     make(map[string]entry),
		watchers: make(map[int]*watcher),
		now:      time.Now,
		stopCh:   make(chan struct{}),
	}
	if sweepInterval > 0 {
		go s.sweepLoop(sweepInterval)
	}
	return s
}

// Get returns the value for key. found is false if the key is absent or expired.
func (s *Store) Get(key string) (value []byte, found bool) {
	s.mu.RLock()
	e, ok := s.data[key]
	s.mu.RUnlock()
	if !ok || e.expired(s.now()) {
		return nil, false
	}
	out := make([]byte, len(e.value))
	copy(out, e.value)
	return out, true
}

// Set stores value under key with an optional ttl (0 = no expiry). It reports
// whether an existing value was replaced.
func (s *Store) Set(key string, value []byte, ttl time.Duration) (replaced bool) {
	stored := make([]byte, len(value))
	copy(stored, value)

	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = s.now().Add(ttl)
	}

	s.mu.Lock()
	_, existed := s.data[key]
	s.data[key] = entry{value: stored, expiresAt: expiresAt}
	s.mu.Unlock()

	s.publish(Event{Type: EventSet, Key: key, Value: stored})
	return existed
}

// Delete removes key. It reports whether the key existed.
func (s *Store) Delete(key string) (existed bool) {
	s.mu.Lock()
	_, existed = s.data[key]
	delete(s.data, key)
	s.mu.Unlock()
	if existed {
		s.publish(Event{Type: EventDelete, Key: key})
	}
	return existed
}

// List returns the keys with the given prefix, sorted. Expired keys are
// skipped but not eagerly evicted (the sweeper handles eviction).
func (s *Store) List(prefix string) []string {
	now := s.now()
	s.mu.RLock()
	keys := make([]string, 0, len(s.data))
	for k, e := range s.data {
		if e.expired(now) {
			continue
		}
		if prefix == "" || strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	s.mu.RUnlock()
	sort.Strings(keys)
	return keys
}

// Len returns the number of live (non-expired) keys.
func (s *Store) Len() int {
	return len(s.List(""))
}

// Watch registers a watcher for events under prefix. It returns a receive-only
// channel and a cancel function. The caller must call cancel to release
// resources. The channel is buffered; if the consumer is too slow, events for
// that watcher are dropped rather than blocking writers.
func (s *Store) Watch(prefix string, buffer int) (<-chan Event, func()) {
	if buffer <= 0 {
		buffer = 16
	}
	w := &watcher{prefix: prefix, ch: make(chan Event, buffer)}
	s.mu.Lock()
	id := s.nextID
	s.nextID++
	s.watchers[id] = w
	s.mu.Unlock()

	cancel := func() {
		s.mu.Lock()
		if _, ok := s.watchers[id]; ok {
			delete(s.watchers, id)
			close(w.ch)
		}
		s.mu.Unlock()
	}
	return w.ch, cancel
}

func (s *Store) publish(ev Event) {
	s.mu.RLock()
	for _, w := range s.watchers {
		if w.prefix != "" && !strings.HasPrefix(ev.Key, w.prefix) {
			continue
		}
		select {
		case w.ch <- ev:
		default:
			// Slow consumer: drop the event rather than block the writer.
		}
	}
	s.mu.RUnlock()
}

func (s *Store) sweepLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.sweep()
		}
	}
}

func (s *Store) sweep() {
	now := s.now()
	var expired []string
	s.mu.Lock()
	for k, e := range s.data {
		if e.expired(now) {
			expired = append(expired, k)
			delete(s.data, k)
		}
	}
	s.mu.Unlock()
	for _, k := range expired {
		s.publish(Event{Type: EventExpire, Key: k})
	}
}

// Close stops the background sweeper. Safe to call once.
func (s *Store) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.stopped {
		s.stopped = true
		close(s.stopCh)
	}
}

// setClock swaps the clock; used by tests.
func (s *Store) setClock(now func() time.Time) {
	s.mu.Lock()
	s.now = now
	s.mu.Unlock()
}
