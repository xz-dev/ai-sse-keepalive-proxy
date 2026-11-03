package main

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// The transport permits HTTP 101, then makes a real relay Write wait until
// explicitly released. Close must release it too, including every failure path.
type controlledWriteGate struct {
	entered, release, closed          chan struct{}
	enterOnce, releaseOnce, closeOnce sync.Once
}

type controlledWriteConn struct {
	net.Conn
	gate     *controlledWriteGate
	upgraded atomic.Bool
}

func (c *controlledWriteConn) Write(p []byte) (int, error) {
	if !c.upgraded.Load() {
		n, err := c.Conn.Write(p)
		if err == nil && bytes.HasPrefix(p, []byte("HTTP/1.1 101 ")) {
			c.upgraded.Store(true)
		}
		return n, err
	}
	c.gate.enterOnce.Do(func() { close(c.gate.entered) })
	select {
	case <-c.gate.release:
		return c.Conn.Write(p)
	case <-c.gate.closed:
		return 0, net.ErrClosed
	}
}

func (c *controlledWriteConn) Close() error {
	c.gate.closeOnce.Do(func() { close(c.gate.closed) })
	return c.Conn.Close()
}

type controlledWriteListener struct {
	net.Listener
	gate *controlledWriteGate
}

func (l *controlledWriteListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &controlledWriteConn{Conn: c, gate: l.gate}, nil
}

func TestWSTransparentControlledWrite(t *testing.T) {
	gate := &controlledWriteGate{entered: make(chan struct{}), release: make(chan struct{}), closed: make(chan struct{})}
	defer gate.releaseOnce.Do(func() { close(gate.release) })
	backendDone := make(chan struct{})
	backendError := make(chan error, 1)
	payloads := [][]byte{bytes.Repeat([]byte("first-0123456789!"), 4096), bytes.Repeat([]byte("second-012345678"), 4096)}
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(backendDone)
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			backendError <- err
			return
		}
		defer c.CloseNow()
		c.SetReadLimit(-1)
		ctx := r.Context()
		if _, _, err := c.Read(ctx); err != nil {
			backendError <- err
			return
		}
		for _, payload := range payloads {
			if err := c.Write(ctx, websocket.MessageBinary, payload); err != nil {
				backendError <- err
				return
			}
		}
		// Stay alive after sending; actual backend closure is not the signal.
		for {
			typ, data, err := c.Read(ctx)
			if err != nil {
				return
			}
			if err := c.Write(ctx, typ, data); err != nil {
				backendError <- err
				return
			}
		}
	}))
	defer up.Close()
	t.Setenv("UPSTREAM_URL", up.URL)
	t.Setenv("WS_PING_INTERVAL", "1m")
	t.Setenv("WS_PING_TIMEOUT", "5s")
	// Baseline understands this old knob; the cleaned implementation removes it.
	t.Setenv("WS_WRITE_TIMEOUT", "80ms")
	cfg, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	front := httptest.NewUnstartedServer(newProxy(cfg))
	front.Listener = &controlledWriteListener{Listener: front.Listener, gate: gate}
	front.Start()
	defer front.Close()
	defer gate.releaseOnce.Do(func() { close(gate.release) })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, "ws"+front.URL[4:]+"/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseNow()
	c.SetReadLimit(-1)
	if err := c.Write(ctx, websocket.MessageText, []byte("start")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-gate.entered:
	case <-ctx.Done():
		t.Fatal("fixture never blocked a post-upgrade transport Write")
	}
	select {
	case <-gate.closed:
		t.Fatal("proxy independently closed a blocked destination while backend remained connected")
	case <-backendDone:
		select {
		case err := <-backendError:
			t.Fatalf("backend ended before explicit release: %v", err)
		default:
			t.Fatal("backend ended before explicit release")
		}
	case <-time.After(320 * time.Millisecond):
	}
	gate.releaseOnce.Do(func() { close(gate.release) })
	for i, want := range payloads {
		typ, got, err := c.Read(ctx)
		if err != nil || typ != websocket.MessageBinary || !bytes.Equal(got, want) {
			t.Fatalf("frame %d lost type/content/order after resume: type=%v bytes=%d err=%v", i, typ, len(got), err)
		}
	}
	if err := c.Write(ctx, websocket.MessageText, []byte("same-connection-sentinel")); err != nil {
		t.Fatal(err)
	}
	typ, data, err := c.Read(ctx)
	if err != nil || typ != websocket.MessageText || string(data) != "same-connection-sentinel" {
		t.Fatalf("same-connection echo failed: type=%v data=%q err=%v", typ, data, err)
	}
	if err := c.CloseNow(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-backendDone:
	case <-ctx.Done():
		t.Fatal("client close did not release the upstream relay")
	}
}
