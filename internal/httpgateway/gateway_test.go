package httpgateway

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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

func TestHTTPHealthz(t *testing.T) {
	srv, cleanup := newTestServer(t)
	defer cleanup()
	resp, _ := http.Get(srv.URL + "/healthz")
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "ok" {
		t.Fatalf("healthz = %q, want ok", body)
	}
}
