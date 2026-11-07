// Package store is an in-memory key-value store with TTL expiry and a
// prefix-based watch (pub/sub). It is safe for concurrent use.
package store

import (
	"hash/maphash"
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

// shardCount is the number of independently locked partitions of the key
// space. Writes to different shards take different shard locks, though all
// of them read-lock watchMu to publish their events.
const shardCount = 32

type shard struct {
	mu   sync.RWMutex
	data map[string]entry
}

// Store is a concurrent in-memory KV store. Keys are hashed to one of
// shardCount shards, each guarded by its own RWMutex.
type Store struct {
	shards [shardCount]shard
	seed   maphash.Seed

	watchMu  sync.RWMutex // guards watchers and nextID
	watchers map[int]*watcher
	nextID   int

	now       func() time.Time // injectable clock for tests
	stopCh    chan struct{}
	closeOnce sync.Once
}

// New creates a Store and starts a background sweeper that evicts expired keys
// every sweepInterval. Call Close to stop the sweeper.
func New(sweepInterval time.Duration) *Store {
	s := &Store{
		seed:     maphash.MakeSeed(),
		watchers: make(map[int]*watcher),
		now:      time.Now,
		stopCh:   make(chan struct{}),
	}
	for i := range s.shards {
		s.shards[i].data = make(map[string]entry)
	}
	if sweepInterval > 0 {
		go s.sweepLoop(sweepInterval)
	}
	return s
}

func (s *Store) shardFor(key string) *shard {
	return &s.shards[maphash.String(s.seed, key)%shardCount]
}

// Get returns the value for key. found is false if the key is absent or expired.
func (s *Store) Get(key string) (value []byte, found bool) {
	sh := s.shardFor(key)
	sh.mu.RLock()
	e, ok := sh.data[key]
	sh.mu.RUnlock()
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

	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	_, existed := sh.data[key]
	sh.data[key] = entry{value: stored, expiresAt: expiresAt}
	s.publish(Event{Type: EventSet, Key: key, Value: stored})
	return existed
}

// Delete removes key. It reports whether the key existed.
func (s *Store) Delete(key string) (existed bool) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	_, existed = sh.data[key]
	delete(sh.data, key)
	if existed {
		s.publish(Event{Type: EventDelete, Key: key})
	}
	return existed
}

// List returns the keys with the given prefix, sorted. Expired keys are
// skipped but not eagerly evicted (the sweeper handles eviction). Shards are
// read one at a time, so writes that run concurrently with List may or may
// not be reflected in its result.
func (s *Store) List(prefix string) []string {
	now := s.now()
	var keys []string
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		for k, e := range sh.data {
			if e.expired(now) {
				continue
			}
			if prefix == "" || strings.HasPrefix(k, prefix) {
				keys = append(keys, k)
			}
		}
		sh.mu.RUnlock()
	}
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
	s.watchMu.Lock()
	id := s.nextID
	s.nextID++
	s.watchers[id] = w
	s.watchMu.Unlock()

	cancel := func() {
		s.watchMu.Lock()
		if _, ok := s.watchers[id]; ok {
			delete(s.watchers, id)
			close(w.ch)
		}
		s.watchMu.Unlock()
	}
	return w.ch, cancel
}

// publish delivers ev to the matching watchers. The caller holds the write
// lock of ev.Key's shard and has just applied the change, so watchers see the
// events for any one key in the order the changes were applied.
func (s *Store) publish(ev Event) {
	s.watchMu.RLock()
	defer s.watchMu.RUnlock()
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

// sweep evicts expired keys one shard at a time, so it blocks only
// operations on the shard it is currently sweeping.
func (s *Store) sweep() {
	now := s.now()
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		for k, e := range sh.data {
			if e.expired(now) {
				delete(sh.data, k)
				s.publish(Event{Type: EventExpire, Key: k})
			}
		}
		sh.mu.Unlock()
	}
}

// Close stops the background sweeper. Safe to call once.
func (s *Store) Close() {
	s.closeOnce.Do(func() { close(s.stopCh) })
}

// setClock swaps the clock; used by tests before the store is in use.
func (s *Store) setClock(now func() time.Time) {
	s.now = now
}
