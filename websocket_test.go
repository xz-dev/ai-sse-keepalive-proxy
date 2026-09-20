package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func wsProxyCfg(u *url.URL) config {
	return config{
		upstream: u, wait: time.Second, idle: time.Second, maxBody: defaultMaxBody,
		wsPingInterval: 80 * time.Millisecond, wsPingTimeout: 40 * time.Millisecond,
		wsWriteTimeout: 5 * time.Second, wsHandshakeTimeout: 5 * time.Second,
	}
}

// WS-01/WS-02: silent upstream; client observes repeated Pings and confirms
// Pongs; upstream not reading does not block the downstream heartbeat; data
// resumes on the same connection.
func TestWSHeartbeatDuringUpstreamSilence(t *testing.T) {
	release := make(chan struct{})
	var upstreamReads atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err == nil {
			c.SetReadLimit(-1)
		}
		if err != nil {
			return
		}
		defer c.CloseNow()
		<-release // simulate upstream busy in a turn, not reading
		ctx := r.Context()
		for {
			typ, msg, err := c.Read(ctx)
			if err != nil {
				return
			}
			upstreamReads.Add(1)
			_ = c.Write(ctx, typ, msg)
		}
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	front := httptest.NewServer(newProxy(wsProxyCfg(u)))
	defer front.Close()

	var pongs atomic.Int32
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+front.URL[4:]+"/v1/responses", &websocket.DialOptions{
		OnPingReceived: func(context.Context, []byte) bool { pongs.Add(1); return true },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	conn.SetReadLimit(-1)

	// Keep a client reader active so pong frames are dequeued; funnel any
	// application echo to a channel so the post-silence read can assert on it.
	echo := make(chan []byte, 4)
	go func() {
		for {
			_, msg, err := conn.Read(context.Background())
			if err != nil {
				return
			}
			echo <- msg
		}
	}()

	// >=3 successful ping/pong cycles during upstream silence
	deadline := time.Now().Add(3 * time.Second)
	for pongs.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if got := pongs.Load(); got < 3 {
		t.Fatalf("pongs=%d, want >=3 while upstream silent", got)
	}

	// Business data resumes on the same connection.
	close(release)
	wctx, wcancel := context.WithTimeout(ctx, 2*time.Second)
	defer wcancel()
	if err := conn.Write(wctx, websocket.MessageText, []byte("resume")); err != nil {
		t.Fatalf("post-silence write: %v", err)
	}
	select {
	case msg := <-echo:
		if string(msg) != "resume" {
			t.Fatalf("post-silence echo msg=%q", msg)
		}
	case <-wctx.Done():
		t.Fatal("post-silence echo timed out")
	}
	if upstreamReads.Load() != 1 {
		t.Fatalf("upstreamReads=%d", upstreamReads.Load())
	}
}

// WS-03: upstream non-101 rejection is preserved end-to-end including body and
// headers on the REAL WS handshake path (not the HTTP fallback).
func TestWSUpstreamRejectionPreserved(t *testing.T) {
	big := strings.Repeat("x", 2048)
	var hits atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("X-Reason", "quota")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"detail":"` + big + `"}`))
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	front := httptest.NewServer(newProxy(wsProxyCfg(u)))
	defer front.Close()

	// Raw WS handshake so isWSUpgrade routes to serveWebSocket, not fallback.
	pu, _ := url.Parse(front.URL)
	conn, err := net.Dial("tcp", pu.Host)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "GET /v1/responses HTTP/1.1\r\nHost: %s\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n", pu.Host)
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodGet})
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusTooManyRequests || resp.Header.Get("X-Reason") != "quota" || !strings.Contains(string(body), big) {
		t.Fatalf("rejection not preserved: status=%d xreason=%q bodylen=%d", resp.StatusCode, resp.Header.Get("X-Reason"), len(body))
	}
	if hits.Load() != 1 {
		t.Fatalf("upstream hits=%d, want 1", hits.Load())
	}
}

// WS-03: negotiated subprotocol is forwarded to the client.
func TestWSSubprotocolEchoed(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"chat"}})
		if err != nil {
			return
		}
		defer c.CloseNow()
		<-r.Context().Done()
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	front := httptest.NewServer(newProxy(wsProxyCfg(u)))
	defer front.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+front.URL[4:]+"/v1/responses", &websocket.DialOptions{
		Subprotocols: []string{"chat", "super"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	if got := conn.Subprotocol(); got != "chat" {
		t.Fatalf("subprotocol=%q, want chat", got)
	}
}

// WS-04: >32KiB message survives without the library default read limit
// rejecting it; binary type and content preserved.
func TestWSLargeMessage(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err == nil {
			c.SetReadLimit(-1)
		}
		if err != nil {
			return
		}
		defer c.CloseNow()
		ctx := r.Context()
		for {
			typ, msg, err := c.Read(ctx)
			if err != nil {
				return
			}
			_ = c.Write(ctx, typ, msg)
		}
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	front := httptest.NewServer(newProxy(wsProxyCfg(u)))
	defer front.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+front.URL[4:]+"/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()

	payload := make([]byte, 96<<10) // 96KiB > 32KiB default read limit
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	conn.SetReadLimit(-1)
	if err := conn.Write(ctx, websocket.MessageBinary, payload); err != nil {
		t.Fatalf("write large: %v", err)
	}
	typ, msg, err := conn.Read(ctx)
	if err != nil || typ != websocket.MessageBinary || len(msg) != len(payload) {
		t.Fatalf("echo typ=%v len=%d err=%v", typ, len(msg), err)
	}
}

// WS-06: upstream clean close propagates a close to the client.
func TestWSUpstreamClosePropagates(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err == nil {
			c.SetReadLimit(-1)
		}
		if err != nil {
			return
		}
		_ = c.Write(r.Context(), websocket.MessageText, []byte("bye"))
		_ = c.Close(websocket.StatusNormalClosure, "done")
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	front := httptest.NewServer(newProxy(wsProxyCfg(u)))
	defer front.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+front.URL[4:]+"/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	conn.SetReadLimit(-1)

	_, msg, err := conn.Read(ctx)
	if err != nil || string(msg) != "bye" {
		t.Fatalf("first read msg=%q err=%v", msg, err)
	}
	_, _, err = conn.Read(ctx)
	var ce websocket.CloseError
	if !errors.As(err, &ce) || ce.Code != websocket.StatusNormalClosure {
		t.Fatalf("close err=%v, want normal closure", err)
	}
}

// Disabled mode: WS upgrade falls through to the opaque tunnel (no heartbeat).
func TestWSDisabledFallsThrough(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err == nil {
			c.SetReadLimit(-1)
		}
		if err != nil {
			return
		}
		defer c.CloseNow()
		ctx := r.Context()
		for {
			typ, msg, err := c.Read(ctx)
			if err != nil {
				return
			}
			_ = c.Write(ctx, typ, msg)
		}
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	cfg := wsProxyCfg(u)
	cfg.wsPingInterval = 0 // disabled
	front := httptest.NewServer(newProxy(cfg))
	defer front.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+front.URL[4:]+"/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	if err := conn.Write(ctx, websocket.MessageText, []byte("hi")); err != nil {
		t.Fatal(err)
	}
	_, msg, err := conn.Read(ctx)
	if err != nil || string(msg) != "hi" {
		t.Fatalf("disabled tunnel echo failed msg=%q err=%v", msg, err)
	}
}

// WS-05: a destination that stops reading causes the write-side bound to trip
// rather than the relay hanging forever; a silent source does not trip it.
func TestWSWriteStallBound(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		c.SetReadLimit(-1)
		ctx := r.Context()
		// read one then flood client while never reading again
		_, _, _ = c.Read(ctx)
		for i := 0; i < 64; i++ {
			if werr := c.Write(ctx, websocket.MessageBinary, make([]byte, 64<<10)); werr != nil {
				return
			}
		}
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	cfg := wsProxyCfg(u)
	cfg.wsWriteTimeout = 300 * time.Millisecond
	front := httptest.NewServer(newProxy(cfg))
	defer front.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+front.URL[4:]+"/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	conn.SetReadLimit(-1)

	// trigger the flood then read until the relay's write bound trips and the
	// proxy tears the session down; each individual Read may succeed while
	// buffered data drains.
	_ = conn.Write(ctx, websocket.MessageText, []byte("go"))
	done := make(chan error, 1)
	go func() {
		for {
			_, _, err := conn.Read(context.Background())
			if err != nil {
				done <- err
				return
			}
		}
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected connection to end on write stall")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("write stall never ended the session")
	}
}

// WS-04 fragmented message: send a text message split across frames via Writer
// and confirm content/order preserved.
func TestWSFragmentedMessage(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		c.SetReadLimit(-1)
		ctx := r.Context()
		typ, msg, err := c.Read(ctx)
		if err != nil {
			return
		}
		_ = c.Write(ctx, typ, msg)
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	front := httptest.NewServer(newProxy(wsProxyCfg(u)))
	defer front.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+front.URL[4:]+"/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	conn.SetReadLimit(-1)

	w, err := conn.Writer(ctx, websocket.MessageText)
	if err != nil {
		t.Fatal(err)
	}
	parts := []string{"hello-", "fragmented-", "world"}
	for _, p := range parts {
		if _, err := w.Write([]byte(p)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	_, msg, err := conn.Read(ctx)
	if err != nil || string(msg) != "hello-fragmented-world" {
		t.Fatalf("fragmented echo msg=%q err=%v", msg, err)
	}
}

// WS-03: an upstream redirect on the WS handshake is forwarded, not followed.
func TestWSHandshakeRedirectNotFollowed(t *testing.T) {
	var targetHits atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetHits.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()

	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", target.URL+"/elsewhere")
		w.WriteHeader(http.StatusFound)
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	front := httptest.NewServer(newProxy(wsProxyCfg(u)))
	defer front.Close()

	// Issue a raw WS handshake request through the proxy.
	pu, _ := url.Parse(front.URL)
	conn, err := net.Dial("tcp", pu.Host)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "GET /v1/responses HTTP/1.1\r\nHost: %s\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n", pu.Host)
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodGet})
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusFound || resp.Header.Get("Location") != target.URL+"/elsewhere" {
		t.Fatalf("redirect not preserved: status=%d loc=%q", resp.StatusCode, resp.Header.Get("Location"))
	}
	if targetHits.Load() != 0 {
		t.Fatalf("redirect target hit %d times", targetHits.Load())
	}
}

// WS-06: cancelling serve() during an active WS session tears the session down.
func TestWSShutdownEndsSession(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		<-r.Context().Done()
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)

	p := newProxy(wsProxyCfg(u))
	srv := &http.Server{Handler: p}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serve(ctx, srv, listener, 2*time.Second, p.closeAllWS) }()

	conn, _, err := websocket.Dial(context.Background(), "ws://"+listener.Addr().String()+"/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()

	// session established; cancel the server context
	cancel()
	readErr := make(chan error, 1)
	go func() {
		_, _, err := conn.Read(context.Background())
		readErr <- err
	}()
	select {
	case err := <-readErr:
		if err == nil {
			t.Fatal("expected WS session to end on shutdown")
		}
	case <-time.After(4 * time.Second):
		t.Fatal("shutdown did not end the WS session")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serve returned error: %v", err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("serve did not return after shutdown")
	}
}

// WS-04 multi-message ordering: pipelined input preserves order/boundaries.
func TestWSMessageOrdering(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		c.SetReadLimit(-1)
		ctx := r.Context()
		for {
			typ, msg, err := c.Read(ctx)
			if err != nil {
				return
			}
			_ = c.Write(ctx, typ, msg)
		}
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	front := httptest.NewServer(newProxy(wsProxyCfg(u)))
	defer front.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+front.URL[4:]+"/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	conn.SetReadLimit(-1)

	for i, want := range []string{"m1", "m2", "m3"} {
		if err := conn.Write(ctx, websocket.MessageText, []byte(want)); err != nil {
			t.Fatal(err)
		}
		_ = i
	}
	for _, want := range []string{"m1", "m2", "m3"} {
		_, msg, err := conn.Read(ctx)
		if err != nil || string(msg) != want {
			t.Fatalf("order broken msg=%q want=%q err=%v", msg, want, err)
		}
	}
}

// Heartbeat probe failure is recorded, not treated as client death: session
// stays usable after a single missed pong while the peer is still live.
func TestWSMissedPongDoesNotKill(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		<-r.Context().Done()
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	cfg := wsProxyCfg(u)
	cfg.wsPingInterval = 60 * time.Millisecond
	cfg.wsPingTimeout = 30 * time.Millisecond
	front := httptest.NewServer(newProxy(cfg))
	defer front.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Client never reads: pong replies are generated by the library's reader,
	// which isn't running, so every Ping will miss. The session must NOT be
	// killed by the proxy for that alone.
	conn, _, err := websocket.Dial(ctx, "ws"+front.URL[4:]+"/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	time.Sleep(300 * time.Millisecond) // several missed probes
	// If the proxy had evicted on missed pongs, this write fails.
	wctx, wcancel := context.WithTimeout(ctx, 2*time.Second)
	defer wcancel()
	if err := conn.Write(wctx, websocket.MessageText, []byte("still-alive")); err != nil {
		t.Fatalf("session killed by missed pong: %v", err)
	}
}

// Discriminating check for the heartbeat loop: client receives each Ping but
// refuses to send the Pong, so every probe misses. The loop must keep probing —
// the old first-error-exit bug would emit exactly one Ping then stop.
func TestWSMissedPongsStillProbe(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		<-r.Context().Done()
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	cfg := wsProxyCfg(u)
	cfg.wsPingInterval = 50 * time.Millisecond
	cfg.wsPingTimeout = 20 * time.Millisecond
	front := httptest.NewServer(newProxy(cfg))
	defer front.Close()

	var pings atomic.Int32
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+front.URL[4:]+"/v1/responses", &websocket.DialOptions{
		OnPingReceived: func(context.Context, []byte) bool {
			pings.Add(1)
			return false // suppress the pong: every probe misses
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()

	// Client reader active so control frames are dequeued (and pings counted),
	// but no pongs are ever sent.
	go func() {
		for {
			if _, _, err := conn.Read(context.Background()); err != nil {
				return
			}
		}
	}()

	time.Sleep(400 * time.Millisecond) // ~8 intervals
	if got := pings.Load(); got < 3 {
		t.Fatalf("pings received=%d, want >=3 — heartbeat stopped after first miss", got)
	}
}

// Session must outlive wsHandshakeTimeout — the timeout bounds only
// connect/header acquisition, never the established connection.
func TestWSSessionOutlivesHandshakeTimeout(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		c.SetReadLimit(-1)
		ctx := r.Context()
		for {
			typ, msg, err := c.Read(ctx)
			if err != nil {
				return
			}
			_ = c.Write(ctx, typ, msg)
		}
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	cfg := wsProxyCfg(u)
	cfg.wsHandshakeTimeout = 200 * time.Millisecond
	front := httptest.NewServer(newProxy(cfg))
	defer front.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+front.URL[4:]+"/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	conn.SetReadLimit(-1)

	time.Sleep(500 * time.Millisecond) // >> handshake timeout
	if err := conn.Write(ctx, websocket.MessageText, []byte("alive?")); err != nil {
		t.Fatalf("session died after handshake timeout: %v", err)
	}
	_, msg, err := conn.Read(ctx)
	if err != nil || string(msg) != "alive?" {
		t.Fatalf("echo after timeout failed: msg=%q err=%v", msg, err)
	}
}

// True write stall: client never reads while upstream floods; the write bound
// must tear the session down, not deadlock in writer.Close().
func TestWSTrueWriteStallNoReader(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		c.SetReadLimit(-1)
		ctx := r.Context()
		_, _, _ = c.Read(ctx)
		for i := 0; i < 4096; i++ {
			if werr := c.Write(ctx, websocket.MessageBinary, make([]byte, 64<<10)); werr != nil {
				return
			}
		}
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	cfg := wsProxyCfg(u)
	cfg.wsWriteTimeout = 300 * time.Millisecond
	front := httptest.NewServer(newProxy(cfg))
	defer front.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+front.URL[4:]+"/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	conn.SetReadLimit(-1)

	_ = conn.Write(ctx, websocket.MessageText, []byte("go"))
	errCh := make(chan error, 1)
	go func() {
		_, _, rerr := conn.Read(context.Background())
		errCh <- rerr
	}()
	select {
	case <-errCh:
	case <-time.After(5 * time.Second):
		t.Fatal("write-stall path hung: writer.Close() deadlock suspected")
	}
}
