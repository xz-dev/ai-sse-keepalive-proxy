package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// TestWSBackpressureOutlastsWriteBudget: a destination that pauses reading
// must not end the session on a proxy-local write timer. The backend stays
// open after a finite unique flood; after the reader resumes, every message
// arrives in order on the SAME connection and a post-resume sentinel echoes
// back. The removed WS_WRITE_TIMEOUT policy used to tear the session down
// while the reader was paused; its old regression proved that old policy.
func TestWSBackpressureOutlastsWriteBudget(t *testing.T) {
	const flood = 24
	release := make(chan struct{})
	var upErr error
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		c.SetReadLimit(-1)
		ctx := r.Context()
		_, _, _ = c.Read(ctx)
		for i := 0; i < flood; i++ {
			if werr := c.Write(ctx, websocket.MessageBinary, []byte(fmt.Sprintf("flood-%03d", i))); werr != nil {
				return
			}
		}
		// Stay open: echo any post-flood text frame until the test releases us.
		for {
			typ, msg, rerr := c.Read(ctx)
			if rerr != nil {
				return
			}
			if werr := c.Write(ctx, typ, msg); werr != nil {
				upErr = werr
				return
			}
		}
	}))
	defer up.Close()
	defer close(release) // registered before any Fatal path

	u, _ := url.Parse(up.URL)
	cfg := wsProxyCfg(u)
	front := httptest.NewServer(newProxy(cfg))
	defer front.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+front.URL[4:]+"/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	conn.SetReadLimit(-1)
	if err := conn.Write(ctx, websocket.MessageText, []byte("go")); err != nil {
		t.Fatal(err)
	}

	// ONE reader loop; separate first-data and terminal-error signals.
	type frame struct {
		typ websocket.MessageType
		msg []byte
	}
	frames := make(chan frame, flood+16)
	firstData := make(chan struct{}, 1)
	termErr := make(chan error, 1)
	var once sync.Once
	go func() {
		defer func() {
			if r := recover(); r != nil {
				termErr <- fmt.Errorf("reader panic: %v", r)
			}
		}()
		for {
			typ, msg, rerr := conn.Read(context.Background())
			if rerr != nil {
				termErr <- rerr
				return
			}
			select {
			case frames <- frame{typ, msg}:
			default:
				termErr <- fmt.Errorf("reader overflow: unexpected message count")
				return
			}
			once.Do(func() { close(firstData) })
		}
	}()

	// Pause the client reader far beyond the old 300ms write budget.
	time.Sleep(900 * time.Millisecond)
	select {
	case err := <-termErr:
		t.Fatalf("session ended while reader paused (proxy-local deadline): %v", err)
	default:
	}

	// Resume: drain the flood in order, verifying exact content.
	for i := 0; i < flood; i++ {
		select {
		case f := <-frames:
			want := fmt.Sprintf("flood-%03d", i)
			if f.typ != websocket.MessageBinary || string(f.msg) != want {
				t.Fatalf("frame %d = (%v, %q), want binary %q", i, f.typ, f.msg, want)
			}
		case err := <-termErr:
			t.Fatalf("session ended during drain at %d/%d: %v", i, flood, err)
		case <-time.After(5 * time.Second):
			t.Fatalf("drain stalled at %d/%d", i, flood)
		}
	}

	// Same-connection liveness: sentinel round-trip after full backpressure.
	wctx, wcancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer wcancel()
	if err := conn.Write(wctx, websocket.MessageText, []byte("sentinel-after-resume")); err != nil {
		t.Fatalf("write after backpressure failed: %v", err)
	}
	for {
		select {
		case f := <-frames:
			if f.typ == websocket.MessageText && string(f.msg) == "sentinel-after-resume" {
				goto alive
			}
		case err := <-termErr:
			t.Fatalf("session ended before sentinel echo: %v", err)
		case <-time.After(5 * time.Second):
			t.Fatal("sentinel echo never arrived")
		}
	}
alive:
	select {
	case err := <-termErr:
		t.Fatalf("session ended after successful resume: %v", err)
	default:
	}
	_ = upErr
}
