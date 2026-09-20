package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

const wsCopyBufferSize = 32 << 10

// wsRejection carries a preserved non-101 upstream handshake response so the
// caller can forward the upstream's real status/headers/body to the client
// instead of the truncated diagnostic websocket.Dial exposes. The body is
// captured synchronously inside RoundTrip (bounded) because http.Client.Do
// discards and closes any response returned alongside an error.
type wsRejection struct {
	status    string
	code      int
	header    http.Header
	body      []byte
	truncated bool
}

func (r *wsRejection) Error() string {
	return fmt.Sprintf("upstream rejected websocket upgrade: %s", r.status)
}

const wsRejectionBodyCap = 64 << 10

// wsDialTransport intercepts the upstream handshake response before
// websocket.Dial's diagnostic path can truncate the body. A non-101 response
// is read fully (bounded) into a wsRejection error so the caller can replay
// the real status/headers/body; a 101 body passes through as the hijacked
// stream the library expects.
type wsDialTransport struct {
	base http.RoundTripper
}

func (t *wsDialTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusSwitchingProtocols {
		return resp, nil
	}
	// Read the whole rejection body now — once we return an error the client
	// drops this response and its body is closed.
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, wsRejectionBodyCap+1))
	truncated := int64(len(body)) > wsRejectionBodyCap
	if truncated {
		body = body[:wsRejectionBodyCap]
	}
	_ = resp.Body.Close()
	if readErr != nil {
		return nil, readErr
	}
	return nil, &wsRejection{
		status: resp.Status, code: resp.StatusCode,
		header: resp.Header.Clone(), body: body, truncated: truncated,
	}
}

func isWSUpgrade(r *http.Request) bool {
	return strings.EqualFold(strings.TrimSpace(r.Header.Get("Upgrade")), "websocket") &&
		headerContainsToken(r.Header.Get("Connection"), "upgrade")
}

func headerContainsToken(header, token string) bool {
	for _, part := range strings.Split(header, ",") {
		if strings.EqualFold(strings.TrimSpace(part), token) {
			return true
		}
	}
	return false
}

func headerTokens(h http.Header, name string) []string {
	var out []string
	for _, v := range h.Values(name) {
		for _, part := range strings.Split(v, ",") {
			if tok := strings.TrimSpace(part); tok != "" {
				out = append(out, tok)
			}
		}
	}
	return out
}

type wsSession struct {
	id            uint64
	client        *websocket.Conn
	upstream      *websocket.Conn
	done          chan struct{}
	closeOnce     sync.Once
	pingAttempts  atomic.Int64
	pongConfirmed atomic.Int64
	pongMissed    atomic.Int64
	appMsgs       atomic.Int64
	appBytes      atomic.Int64
	lastDownWrite atomic.Int64 // unix nano of last successful client-bound write
}

var wsSessionSeq atomic.Uint64

// serveWebSocket terminates one client↔proxy and one proxy↔upstream WebSocket,
// relaying application messages in both directions and emitting downstream
// Pings that are independent of AI turn progress.
func (p *proxy) serveWebSocket(w http.ResponseWriter, r *http.Request) {
	if p.cfg.wsPingInterval <= 0 {
		// Feature disabled: preserve the legacy opaque tunnel.
		p.fallback.ServeHTTP(w, r)
		return
	}

	// Upstream-first admission: dial the fixed upstream with the client's
	// handshake-relevant headers before committing a 101 to the client.
	upstreamURL := joinURL(p.cfg.upstream, r.URL)
	upstreamURL.Scheme = "ws"
	upHeader := r.Header.Clone()
	removeHopHeaders(upHeader)
	for _, h := range []string{
		"Sec-WebSocket-Key", "Sec-WebSocket-Version", "Sec-WebSocket-Protocol",
		"Sec-WebSocket-Extensions", "Sec-WebSocket-Accept",
	} {
		upHeader.Del(h)
	}
	subprotocols := headerTokens(r.Header, "Sec-WebSocket-Protocol")

	dialCtx, dialCancel := context.WithTimeout(r.Context(), p.cfg.wsHandshakeTimeout)
	defer dialCancel()
	upConn, _, err := websocket.Dial(dialCtx, upstreamURL.String(), &websocket.DialOptions{
		HTTPClient:      &http.Client{Transport: &wsDialTransport{base: p.wsTransport}},
		HTTPHeader:      upHeader,
		Host:            p.cfg.upstream.Host,
		Subprotocols:    subprotocols,
		CompressionMode: websocket.CompressionContextTakeover,
	})
	if err != nil {
		var rej *wsRejection
		if errors.As(err, &rej) {
			if rej.truncated {
				log.Printf("ws rejection body truncated at %d bytes", wsRejectionBodyCap)
			}
			copyHeader(w.Header(), rej.header)
			// Recompute Content-Length for the (possibly truncated) captured body.
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(rej.body)))
			w.WriteHeader(rej.code)
			_, _ = w.Write(rej.body)
			return
		}
		http.Error(w, "bad gateway", http.StatusBadGateway)
		return
	}
	selectedSub := upConn.Subprotocol()
	// Operator-approved: no proxy-side cap; the backend's own read limit governs.
	upConn.SetReadLimit(-1)

	var acceptSubs []string
	if selectedSub != "" {
		acceptSubs = []string{selectedSub}
	}
	clientConn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols:       acceptSubs,
		InsecureSkipVerify: true, // preserve transparent admission: upstream decides Origin policy
		CompressionMode:    websocket.CompressionContextTakeover,
	})
	if err != nil {
		_ = upConn.CloseNow()
		return
	}
	clientConn.SetReadLimit(-1)

	s := &wsSession{
		id:       wsSessionSeq.Add(1),
		client:   clientConn,
		upstream: upConn,
		done:     make(chan struct{}),
	}
	s.lastDownWrite.Store(time.Now().UnixNano())
	p.registerWS(s)
	start := time.Now()
	log.Printf("ws open id=%d path=%s subproto=%q", s.id, r.URL.Path, selectedSub)
	defer func() {
		s.terminate()
		p.unregisterWS(s)
		log.Printf("ws close id=%d duration=%s msgs=%d bytes=%d pings=%d pongs=%d missed=%d",
			s.id, time.Since(start).Round(time.Millisecond), s.appMsgs.Load(), s.appBytes.Load(),
			s.pingAttempts.Load(), s.pongConfirmed.Load(), s.pongMissed.Load())
	}()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		s.relay(p.cfg, clientConn, upConn) // client -> upstream
		s.terminate()                      // first leg to end tears down the pair
	}()
	go func() {
		defer wg.Done()
		s.relay(p.cfg, upConn, clientConn) // upstream -> client
		s.terminate()
	}()
	go s.heartbeat(p.cfg)
	wg.Wait()
}

// relay copies messages from src to dst: one reader per leg, one writer per
// destination, streaming through a bounded buffer. Backpressure pauses
// forwarding rather than buffering whole messages.
func (s *wsSession) relay(cfg config, src, dst *websocket.Conn) {
	buf := make([]byte, wsCopyBufferSize)
	for {
		typ, reader, err := src.Reader(context.Background())
		if err != nil {
			return
		}
		writer, err := dst.Writer(context.Background(), typ)
		if err != nil {
			return
		}
		n, copyErr := io.CopyBuffer(&wsWriteTimeoutWriter{w: writer, timeout: cfg.wsWriteTimeout}, reader, buf)
		if copyErr != nil {
			// A failed copy (e.g. write-stall timeout) leaves the destination
			// writer's internal lock held by the abandoned Write goroutine; calling
			// writer.Close() here would deadlock. Bail and let terminate() force
			// the underlying connection, which releases it.
			return
		}
		if err := writer.Close(); err != nil {
			return
		}
		s.appMsgs.Add(1)
		s.appBytes.Add(n)
		if dst == s.client {
			s.lastDownWrite.Store(time.Now().UnixNano())
		}
	}
}

// wsWriteTimeoutWriter bounds each blocked Write on the destination, not the
// source-side wait for new data: a stalled peer trips the budget while a
// legitimately fragmented or silent source does not.
type wsWriteTimeoutWriter struct {
	w       io.Writer
	timeout time.Duration
}

func (t *wsWriteTimeoutWriter) Write(p []byte) (int, error) {
	type result struct {
		n   int
		err error
	}
	ch := make(chan result, 1)
	go func() {
		n, err := t.w.Write(p)
		ch <- result{n, err}
	}()
	timer := time.NewTimer(t.timeout)
	defer timer.Stop()
	select {
	case r := <-ch:
		return r.n, r.err
	case <-timer.C:
		return 0, errors.New("websocket destination write timed out")
	}
}

// heartbeat emits downstream Pings on the configured interval whenever recent
// application output has not already kept the direction active. A Ping that
// returns an error is an unconfirmed probe, never by itself grounds for
// eviction; the persistent relay readers observe real transport failure.
func (s *wsSession) heartbeat(cfg config) {
	ticker := time.NewTicker(cfg.wsPingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			if time.Since(time.Unix(0, s.lastDownWrite.Load())) < cfg.wsPingInterval {
				continue
			}
			s.pingAttempts.Add(1)
			ctx, cancel := context.WithTimeout(context.Background(), cfg.wsPingTimeout)
			err := s.client.Ping(ctx)
			cancel()
			if err != nil {
				s.pongMissed.Add(1)
			} else {
				s.pongConfirmed.Add(1)
			}
		}
	}
}

// terminate performs first-result, idempotent teardown. The client leg gets a
// polite close frame so a peer reading it sees a real close status; a short
// grace lets the close handshake complete before the socket is forced. The
// upstream leg is closed immediately since nothing needs its handshake.
func (s *wsSession) terminate() {
	s.closeOnce.Do(func() {
		close(s.done)
		_ = s.upstream.CloseNow()
		// Close blocks until the close handshake completes or fails; give it a
		// bounded window then force the socket so relays can't hang on a peer
		// that never answers the close frame.
		done := make(chan struct{})
		go func() {
			_ = s.client.Close(websocket.StatusNormalClosure, "")
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
		_ = s.client.CloseNow()
	})
}

func (p *proxy) registerWS(s *wsSession) {
	p.wsMu.Lock()
	if p.wsClosing {
		// closeAllWS already snapshotted; a session that registered after the
		// snapshot would escape termination. Terminate it inline so a shutdown
		// cannot strand a hijacked socket.
		p.wsMu.Unlock()
		s.terminate()
		return
	}
	if p.wsSessions == nil {
		p.wsSessions = make(map[*wsSession]struct{})
	}
	p.wsSessions[s] = struct{}{}
	p.wsMu.Unlock()
}

func (p *proxy) unregisterWS(s *wsSession) {
	p.wsMu.Lock()
	defer p.wsMu.Unlock()
	delete(p.wsSessions, s)
}

// closeAllWS terminates every registered WS session concurrently under the
// shared shutdown deadline. Sessions that register after the snapshot are
// caught by the wsClosing flag inside registerWS and terminated inline.
func (p *proxy) closeAllWS() {
	p.wsMu.Lock()
	p.wsClosing = true
	sessions := make([]*wsSession, 0, len(p.wsSessions))
	for s := range p.wsSessions {
		sessions = append(sessions, s)
	}
	p.wsMu.Unlock()
	var wg sync.WaitGroup
	for _, s := range sessions {
		wg.Add(1)
		go func(s *wsSession) {
			defer wg.Done()
			s.terminate()
		}(s)
	}
	wg.Wait()
}
