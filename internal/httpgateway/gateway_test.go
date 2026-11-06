package httpgateway

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/Tajaddin/go-grpc-kvstore/internal/store"
)

func newTestServer(t *testing.T) (*httptest.Server, func()) {
	t.Helper()
	st := store.New(0)
	srv := httptest.NewServer(New(st).Handler())
	return srv, func() {
		srv.Close()
		st.Close()
	}
}

func TestHTTPSetGetDelete(t *testing.T) {
	srv, cleanup := newTestServer(t)
	defer cleanup()

	// PUT
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/kv/greeting", strings.NewReader("hello"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("put failed: %v status=%v", err, resp.StatusCode)
	}

	// GET
	resp, _ = http.Get(srv.URL + "/kv/greeting")
	body, _ := io.ReadAll(resp.Body)
	var got struct {
		Found bool   `json:"found"`
		Value string `json:"value"`
	}
	json.Unmarshal(body, &got)
	if !got.Found {
		t.Fatal("expected found=true")
	}
	decoded, _ := base64.StdEncoding.DecodeString(got.Value)
	if string(decoded) != "hello" {
		t.Fatalf("value = %q, want hello", decoded)
	}

	// DELETE
	req, _ = http.NewRequest(http.MethodDelete, srv.URL+"/kv/greeting", nil)
	resp, _ = http.DefaultClient.Do(req)
	delBody, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(delBody), `"existed":true`) {
		t.Fatalf("delete body = %s, want existed=true", delBody)
	}
}

func TestHTTPListPrefix(t *testing.T) {
	srv, cleanup := newTestServer(t)
	defer cleanup()
	for _, k := range []string{"a:1", "a:2", "b:1"} {
		req, _ := http.NewRequest(http.MethodPut, srv.URL+"/kv/"+k, strings.NewReader("v"))
		http.DefaultClient.Do(req)
	}
	resp, _ := http.Get(srv.URL + "/kv?prefix=a:")
	body, _ := io.ReadAll(resp.Body)
	var got struct {
		Keys []string `json:"keys"`
	}
	json.Unmarshal(body, &got)
	if len(got.Keys) != 2 {
		t.Fatalf("keys = %v, want 2", got.Keys)
	}
}

// failingBody yields some bytes and then a read error, like a client that
// disconnects halfway through its upload.
type failingBody struct{ sent bool }

func (b *failingBody) Read(p []byte) (int, error) {
	if b.sent {
		return 0, errors.New("connection reset by peer")
	}
	b.sent = true
	return copy(p, "partial"), nil
}

func (b *failingBody) Close() error { return nil }

func TestHTTPSetRejectsTruncatedBody(t *testing.T) {
	st := store.New(0)
	defer st.Close()
	req := httptest.NewRequest(http.MethodPut, "/kv/k", nil)
	req.Body = &failingBody{}
	rec := httptest.NewRecorder()
	New(st).Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if v, found := st.Get("k"); found {
		t.Fatalf("a truncated upload was stored: %q", v)
	}
}

func TestHTTPSetRejectsOversizedValue(t *testing.T) {
	st := store.New(0)
	defer st.Close()
	// net/http can return a body's last bytes together with io.EOF.
	body := iotest.DataErrReader(strings.NewReader(strings.Repeat("x", 1<<20+1)))
	req := httptest.NewRequest(http.MethodPut, "/kv/big", body)
	rec := httptest.NewRecorder()
	New(st).Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", rec.Code)
	}
	if _, found := st.Get("big"); found {
		t.Fatal("an oversized value was stored")
	}
}

func TestHTTPHealthz(t *testing.T) {
	srv, cleanup := newTestServer(t)
	defer cleanup()
	resp, _ := http.Get(srv.URL + "/healthz")
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "ok" {
		t.Fatalf("healthz = %q, want ok", body)
	}
}
