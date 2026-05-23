// Package httpgateway exposes the same store over a small REST/JSON API, so
// the service is reachable from curl and browsers as well as gRPC clients.
package httpgateway

import (
	"encoding/base64"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/Tajaddin/go-grpc-kvstore/internal/store"
)

type Gateway struct {
	store *store.Store
}

func New(s *store.Store) *Gateway {
	return &Gateway{store: s}
}

// Handler returns an http.Handler routing:
//
//	GET    /kv/{key}            -> {"found":bool,"value":"<base64>"}
//	PUT    /kv/{key}?ttl_ms=N   -> body is the raw value; {"replaced":bool}
//	DELETE /kv/{key}            -> {"existed":bool}
//	GET    /kv?prefix=p         -> {"keys":[...]}
//	GET    /healthz             -> "ok"
func (g *Gateway) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /kv", g.list)
	mux.HandleFunc("GET /kv/{key}", g.get)
	mux.HandleFunc("PUT /kv/{key}", g.set)
	mux.HandleFunc("DELETE /kv/{key}", g.delete)
	return mux
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.Printf("writeJSON encode: %v", err)
	}
}

func (g *Gateway) get(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	value, found := g.store.Get(key)
	writeJSON(w, http.StatusOK, map[string]any{
		"found": found,
		"value": base64.StdEncoding.EncodeToString(value),
	})
}

func (g *Gateway) set(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "key is required"})
		return
	}
	defer r.Body.Close()
	buf := make([]byte, 0, 1024)
	tmp := make([]byte, 4096)
	for {
		n, err := r.Body.Read(tmp)
		buf = append(buf, tmp[:n]...)
		if err != nil {
			break
		}
		if len(buf) > 1<<20 { // 1 MiB cap
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "value too large"})
			return
		}
	}

	var ttl time.Duration
	if v := r.URL.Query().Get("ttl_ms"); v != "" {
		ms, err := strconv.ParseInt(v, 10, 64)
		if err != nil || ms < 0 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "ttl_ms must be a non-negative integer"})
			return
		}
		ttl = time.Duration(ms) * time.Millisecond
	}

	replaced := g.store.Set(key, buf, ttl)
	writeJSON(w, http.StatusOK, map[string]bool{"replaced": replaced})
}

func (g *Gateway) delete(w http.ResponseWriter, r *http.Request) {
	existed := g.store.Delete(r.PathValue("key"))
	writeJSON(w, http.StatusOK, map[string]bool{"existed": existed})
}

func (g *Gateway) list(w http.ResponseWriter, r *http.Request) {
	keys := g.store.List(r.URL.Query().Get("prefix"))
	writeJSON(w, http.StatusOK, map[string][]string{"keys": keys})
}
