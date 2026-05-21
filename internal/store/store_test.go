package store

import (
	"fmt"
	"sync"
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
