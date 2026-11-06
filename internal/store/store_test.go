package store

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSetGet(t *testing.T) {
	s := New(0)
	defer s.Close()

	if replaced := s.Set("a", []byte("1"), 0); replaced {
		t.Fatal("first set should not report replaced")
	}
	v, ok := s.Get("a")
	if !ok || string(v) != "1" {
		t.Fatalf("got (%q,%v), want (1,true)", v, ok)
	}
	if replaced := s.Set("a", []byte("2"), 0); !replaced {
		t.Fatal("second set should report replaced")
	}
}

func TestGetMissing(t *testing.T) {
	s := New(0)
	defer s.Close()
	if _, ok := s.Get("nope"); ok {
		t.Fatal("missing key should return found=false")
	}
}

func TestDelete(t *testing.T) {
	s := New(0)
	defer s.Close()
	s.Set("k", []byte("v"), 0)
	if !s.Delete("k") {
		t.Fatal("delete of existing key should report existed=true")
	}
	if s.Delete("k") {
		t.Fatal("delete of missing key should report existed=false")
	}
	if _, ok := s.Get("k"); ok {
		t.Fatal("key should be gone after delete")
	}
}

func TestTTLExpiry(t *testing.T) {
	s := New(0)
	defer s.Close()
	now := time.Unix(1000, 0)
	s.setClock(func() time.Time { return now })

	s.Set("temp", []byte("v"), 100*time.Millisecond)
	if _, ok := s.Get("temp"); !ok {
		t.Fatal("key should be present before expiry")
	}
	now = now.Add(150 * time.Millisecond)
	if _, ok := s.Get("temp"); ok {
		t.Fatal("key should be expired")
	}
}

func TestListPrefix(t *testing.T) {
	s := New(0)
	defer s.Close()
	s.Set("user:1", []byte("a"), 0)
	s.Set("user:2", []byte("b"), 0)
	s.Set("task:1", []byte("c"), 0)

	got := s.List("user:")
	if len(got) != 2 || got[0] != "user:1" || got[1] != "user:2" {
		t.Fatalf("prefix list = %v, want [user:1 user:2]", got)
	}
	if all := s.List(""); len(all) != 3 {
		t.Fatalf("list all = %v, want 3 keys", all)
	}
}

func TestValueIsCopied(t *testing.T) {
	s := New(0)
	defer s.Close()
	original := []byte("hello")
	s.Set("k", original, 0)
	original[0] = 'X' // mutate caller's slice
	v, _ := s.Get("k")
	if string(v) != "hello" {
		t.Fatalf("stored value was mutated by caller: %q", v)
	}
}

func TestWatchReceivesEvents(t *testing.T) {
	s := New(0)
	defer s.Close()
	ch, cancel := s.Watch("user:", 8)
	defer cancel()

	s.Set("user:1", []byte("a"), 0)
	s.Set("task:9", []byte("ignored"), 0) // outside prefix, must not arrive
	s.Delete("user:1")

	first := <-ch
	if first.Type != EventSet || first.Key != "user:1" {
		t.Fatalf("first event = %+v, want Set user:1", first)
	}
	second := <-ch
	if second.Type != EventDelete || second.Key != "user:1" {
		t.Fatalf("second event = %+v, want Delete user:1", second)
	}
}

func TestWatchDropsOnSlowConsumer(t *testing.T) {
	s := New(0)
	defer s.Close()
	// buffer of 1; flood with writes; publish must never block.
	_, cancel := s.Watch("", 1)
	defer cancel()
	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			s.Set(fmt.Sprintf("k%d", i), []byte("v"), 0)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("publish blocked on a slow consumer")
	}
}

// Two concurrent Sets on one key: the last event a watcher sees must carry the
// value the store ended up with.
func TestWatchOrderMatchesConcurrentSets(t *testing.T) {
	s := New(0)
	defer s.Close()
	ch, cancel := s.Watch("", 4)
	defer cancel()

	for round := 0; round < 100000; round++ {
		var wg sync.WaitGroup
		for g := 0; g < 2; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				s.Set("k", []byte(fmt.Sprintf("%d-%d", round, g)), 0)
			}(g)
		}
		wg.Wait()
		<-ch
		last := <-ch
		if v, _ := s.Get("k"); string(v) != string(last.Value) {
			t.Fatalf("round %d: store holds %q but the last event was Set %q", round, v, last.Value)
		}
	}
}

// The sweeper must not report a key as expired after a newer, non-expiring
// value has been written and announced.
func TestWatchExpireNeverFollowsLaterSet(t *testing.T) {
	s := New(0)
	defer s.Close()
	var tick atomic.Int64 // every clock read is 1ns later, so a 1ns TTL has passed by the next read
	s.setClock(func() time.Time { return time.Unix(0, tick.Add(1)) })
	const n = 20000
	ch, cancel := s.Watch("", 3*n)
	defer cancel()

	stop := make(chan struct{})
	swept := make(chan struct{})
	go func() {
		defer close(swept)
		for {
			select {
			case <-stop:
				return
			default:
				s.sweep()
			}
		}
	}()
	for i := 0; i < n; i++ {
		s.Set("k", []byte("ttl"), time.Nanosecond)
		runtime.Gosched() // let the sweeper catch the short-lived value
		s.Set("k", []byte("permanent"), 0)
	}
	close(stop)
	<-swept
	cancel()

	var prev Event
	expires := 0
	for ev := range ch {
		if ev.Type == EventExpire {
			expires++
			if prev.Type == EventSet && string(prev.Value) == "permanent" {
				t.Fatal("Expire delivered after Set of a value without a TTL")
			}
		}
		prev = ev
	}
	if expires == 0 {
		t.Fatal("the sweeper expired nothing, so the race was not exercised")
	}
}

func TestKeysSpreadAcrossShards(t *testing.T) {
	s := New(0)
	defer s.Close()
	for i := 0; i < 10000; i++ {
		s.Set(fmt.Sprintf("key:%d", i), []byte("v"), 0)
	}
	for i := range s.shards {
		if n := len(s.shards[i].data); n == 0 {
			t.Fatalf("shard %d holds no keys out of 10000", i)
		}
	}
}

// A writer holding one shard must not block reads and writes of keys that
// live in other shards.
func TestShardLocksAreIndependent(t *testing.T) {
	s := New(0)
	defer s.Close()
	busy := s.shardFor("busy")
	other := "other"
	for i := 0; s.shardFor(other) == busy; i++ {
		other = fmt.Sprintf("other%d", i)
	}

	busy.mu.Lock()
	defer busy.mu.Unlock()
	done := make(chan struct{})
	go func() {
		s.Set(other, []byte("v"), 0)
		s.Get(other)
		s.Delete(other)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("operations on another shard waited for a locked shard")
	}
}

func TestConcurrentAccess(t *testing.T) {
	s := New(0)
	defer s.Close()
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				key := fmt.Sprintf("g%d:k%d", g, i)
				s.Set(key, []byte("v"), 0)
				s.Get(key)
			}
		}(g)
	}
	wg.Wait()
	if n := s.Len(); n != 16*500 {
		t.Fatalf("expected %d keys, got %d", 16*500, n)
	}
}
