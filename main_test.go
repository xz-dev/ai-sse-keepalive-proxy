package main

import (
	"bufio"
	"bytes"
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
	"testing/fstest"
	"time"
)

func testProxy(t *testing.T, upstream http.Handler, wait, idle time.Duration, maxBody int64) (*httptest.Server, *httptest.Server) {
	t.Helper()
	up := httptest.NewServer(upstream)
	u, err := url.Parse(up.URL)
	if err != nil {
		t.Fatal(err)
	}
	p := httptest.NewServer(newProxy(config{upstream: u, wait: wait, idle: idle, maxBody: maxBody}))
	t.Cleanup(p.Close)
	t.Cleanup(up.Close)
	return p, up
}

func request(t *testing.T, client *http.Client, method, target, contentType, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, target, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func readAll(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func TestEligibleClientIsFixedUpstream(t *testing.T) {
	var proxyHits atomic.Int32
	poisonedProxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxyHits.Add(1)
		http.Error(w, "proxy used", http.StatusBadGateway)
	}))
	defer poisonedProxy.Close()
	t.Setenv("HTTP_PROXY", poisonedProxy.URL)
	t.Setenv("HTTPS_PROXY", poisonedProxy.URL)
	t.Setenv("NO_PROXY", "")

	var upstreamHits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: ok\n\n")
	}))
	defer upstream.Close()
	u, _ := url.Parse(upstream.URL)
	p := newProxy(config{upstream: u, wait: time.Second, idle: time.Second, maxBody: defaultMaxBody})
	transport, ok := p.client.Transport.(*http.Transport)
	if !ok || transport.Proxy != nil || !transport.DisableCompression {
		t.Fatalf("transport=%T proxyDisabled=%v disableCompression=%v", p.client.Transport, transport.Proxy == nil, transport.DisableCompression)
	}
	fallbackTransport, ok := p.fallback.Transport.(*http.Transport)
	if !ok || fallbackTransport.Proxy != nil || !fallbackTransport.DisableCompression {
		t.Fatalf("fallback transport=%T proxyDisabled=%v disableCompression=%v", p.fallback.Transport, fallbackTransport.Proxy == nil, fallbackTransport.DisableCompression)
	}
	front := httptest.NewServer(p)
	defer front.Close()

	resp := request(t, http.DefaultClient, http.MethodPost, front.URL+"/v1/responses", "application/json", `{"stream":true}`)
	if resp.StatusCode != http.StatusOK || readAll(t, resp) != "data: ok\n\n" {
		t.Fatal("eligible request failed")
	}
	if upstreamHits.Load() != 1 || proxyHits.Load() != 0 {
		t.Fatalf("upstream hits=%d proxy hits=%d", upstreamHits.Load(), proxyHits.Load())
	}
}

func TestEligibleRedirectIsReturnedWithoutFollowing(t *testing.T) {
	var targetHits atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetHits.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()

	p, _ := testProxy(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", target.URL+"/escaped")
		w.Header().Set("X-Redirect", "kept")
		w.WriteHeader(http.StatusTemporaryRedirect)
		_, _ = io.WriteString(w, "redirect body")
	}), time.Second, time.Second, defaultMaxBody)
	client := *http.DefaultClient
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp := request(t, &client, http.MethodPost, p.URL+"/v1/responses", "application/json", `{"stream":true}`)
	if resp.StatusCode != http.StatusTemporaryRedirect || resp.Header.Get("X-Redirect") != "kept" || readAll(t, resp) != "redirect body" {
		t.Fatal("redirect response not preserved")
	}
	if targetHits.Load() != 0 {
		t.Fatalf("redirect target hits=%d", targetHits.Load())
	}
}

func TestNonStreamHeartbeatKeepsConnectionWarm(t *testing.T) {
	release := make(chan struct{})
	p, _ := testProxy(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":true}`)
	}), 50*time.Millisecond, 80*time.Millisecond, defaultMaxBody)

	addr := strings.TrimPrefix(p.URL, "http://")
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	body := `{"stream":false}`
	_, _ = fmt.Fprintf(conn, "POST /v1/chat/completions HTTP/1.1\r\nHost: %s\r\nContent-Type: application/json\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s", addr, len(body), body)

	go func() {
		time.Sleep(200 * time.Millisecond)
		close(release)
	}()
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	raw, err := io.ReadAll(conn)
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	if got := strings.Count(text, "HTTP/1.1 102 Processing"); got < 2 {
		t.Fatalf("interim 102 heartbeats = %d, want >=2; raw=%q", got, text)
	}
	if !strings.Contains(text, "HTTP/1.1 200 OK") || !strings.HasSuffix(text, `{"ok":true}`) {
		t.Fatalf("final response missing or corrupted: %q", text)
	}
	if strings.Index(text, "102 Processing") > strings.Index(text, "200 OK") {
		t.Fatalf("interim arrived after final: %q", text)
	}
}

func TestNonStreamTransparentAndFixedUpstream(t *testing.T) {
	body := `{"stream":false,"value":"exact"}`
	var upstreamHost string
	p, up := testProxy(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, _ := io.ReadAll(r.Body)
		if string(got) != body {
			t.Errorf("body = %q", got)
		}
		if r.Host != upstreamHost {
			t.Errorf("host = %q, want %q", r.Host, upstreamHost)
		}
		if r.Header.Get("Authorization") != "Bearer kept" || r.Header.Get("X-Request-ID") != "req-1" {
			t.Errorf("forwarded headers missing: %v", r.Header)
		}
		w.Header().Set("X-Upstream", "yes")
		w.WriteHeader(http.StatusTeapot)
		_, _ = io.WriteString(w, "unchanged")
	}), 50*time.Millisecond, 30*time.Millisecond, defaultMaxBody)
	u, _ := url.Parse(up.URL)
	upstreamHost = u.Host

	req, _ := http.NewRequest(http.MethodPost, p.URL+"/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer kept")
	req.Header.Set("X-Request-ID", "req-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusTeapot || resp.Header.Get("X-Upstream") != "yes" || readAll(t, resp) != "unchanged" {
		t.Fatalf("response not transparent: status=%d headers=%v", resp.StatusCode, resp.Header)
	}
}

func TestFastSSEIdleAndFrequentData(t *testing.T) {
	t.Run("idle heartbeat and real data", func(t *testing.T) {
		p, _ := testProxy(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: first\n\n")
			w.(http.Flusher).Flush()
			time.Sleep(35 * time.Millisecond)
			_, _ = io.WriteString(w, "data: last\n\n")
		}), 100*time.Millisecond, 15*time.Millisecond, defaultMaxBody)
		resp := request(t, http.DefaultClient, http.MethodPost, p.URL+"/v1/responses", "application/json", `{"stream":true}`)
		body := readAll(t, resp)
		if resp.StatusCode != 200 || !strings.Contains(body, "data: first\n\n") || !strings.Contains(body, `response.in_progress`) || !strings.Contains(body, "data: last\n\n") {
			t.Fatalf("unexpected stream: %q", body)
		}
	})

	t.Run("frequent data suppresses heartbeat", func(t *testing.T) {
		p, _ := testProxy(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			for i := range 5 {
				fmt.Fprintf(w, "data: %d\n\n", i)
				w.(http.Flusher).Flush()
				time.Sleep(5 * time.Millisecond)
			}
		}), 100*time.Millisecond, 20*time.Millisecond, defaultMaxBody)
		resp := request(t, http.DefaultClient, http.MethodPost, p.URL+"/v1/chat/completions", "application/json", `{"stream":true}`)
		body := readAll(t, resp)
		if strings.Contains(body, "chatcmpl-keepalive") {
			t.Fatalf("heartbeat emitted during active stream: %q", body)
		}
	})
}

func TestImmediateHeadersDelayedFirstBodyStartsNearThreshold(t *testing.T) {
	wait := 25 * time.Millisecond
	p, _ := testProxy(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		time.Sleep(80 * time.Millisecond)
		_, _ = io.WriteString(w, "data: real\n\n")
	}), wait, time.Second, defaultMaxBody)

	start := time.Now()
	resp := request(t, http.DefaultClient, http.MethodPost, p.URL+"/v1/responses", "application/json", `{"stream":true}`)
	reader := bufio.NewReader(resp.Body)
	first, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	resp.Body.Close()
	if !strings.Contains(first, "response.in_progress") || elapsed < wait/2 || elapsed > 70*time.Millisecond {
		t.Fatalf("first=%q elapsed=%s", first, elapsed)
	}
}

func TestSlowStartupAllShapes(t *testing.T) {
	cases := []struct {
		path string
		want string
	}{
		{"/v1/responses", `data: {"type":"response.in_progress"}`},
		{"/responses", `data: {"type":"response.in_progress"}`},
		{"/backend-api/codex/responses", `data: {"type":"response.in_progress"}`},
		{"/v1/chat/completions", `"id":"chatcmpl-keepalive"`},
		{"/v1/messages", "event: ping\ndata: {\"type\":\"ping\"}"},
		{"/antigravity/v1/messages", "event: ping\ndata: {\"type\":\"ping\"}"},
	}
	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			p, _ := testProxy(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				time.Sleep(35 * time.Millisecond)
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "data: real\n\n")
			}), 10*time.Millisecond, time.Second, defaultMaxBody)
			resp := request(t, http.DefaultClient, http.MethodPost, p.URL+tc.path, "application/json", `{"stream":true}`)
			body := readAll(t, resp)
			if resp.StatusCode != 200 || !strings.Contains(body, tc.want) || !strings.Contains(body, "data: real") {
				t.Fatalf("status=%d body=%q", resp.StatusCode, body)
			}
		})
	}
}

func TestSlowUpstreamFailuresAreGenericInBand(t *testing.T) {
	for _, tc := range []struct {
		name        string
		path        string
		contentType string
		status      int
		want        string
	}{
		{"status", "/v1/responses", "text/event-stream", 503, `"type":"error"`},
		{"non-sse", "/v1/messages", "application/json", 200, "event: error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, _ := testProxy(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				time.Sleep(25 * time.Millisecond)
				w.Header().Set("Content-Type", tc.contentType)
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, "SECRET UPSTREAM DETAIL")
			}), 5*time.Millisecond, time.Second, defaultMaxBody)
			resp := request(t, http.DefaultClient, http.MethodPost, p.URL+tc.path, "application/json", `{"stream":true}`)
			body := readAll(t, resp)
			if resp.StatusCode != 200 || !strings.Contains(body, tc.want) || strings.Contains(body, "SECRET") {
				t.Fatalf("status=%d body=%q", resp.StatusCode, body)
			}
		})
	}
}

func TestFastInvalidResponsePreserved(t *testing.T) {
	p, _ := testProxy(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Reason", "kept")
		w.WriteHeader(429)
		_, _ = io.WriteString(w, `{"detail":"normal path"}`)
	}), 100*time.Millisecond, time.Second, defaultMaxBody)
	resp := request(t, http.DefaultClient, http.MethodPost, p.URL+"/v1/responses", "application/json", `{"stream":true}`)
	if resp.StatusCode != 429 || resp.Header.Get("X-Reason") != "kept" || readAll(t, resp) != `{"detail":"normal path"}` {
		t.Fatal("fast invalid response not preserved")
	}
}

func TestServeWaitsForActiveHandler(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
		_, _ = io.WriteString(w, "done")
	})}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serve(ctx, srv, listener, time.Second) }()

	response := make(chan *http.Response, 1)
	go func() {
		resp, _ := http.Get("http://" + listener.Addr().String())
		response <- resp
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	cancel()
	select {
	case err := <-done:
		t.Fatalf("serve returned while handler blocked: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("serve did not finish after handler release")
	}
	resp := <-response
	if resp == nil || readAll(t, resp) != "done" {
		t.Fatal("active response did not finish")
	}
}

func TestTerminalEventsSuppressSyntheticFrames(t *testing.T) {
	cases := []struct {
		name      string
		path      string
		parts     []string
		synthetic string
	}{
		{"responses", "/v1/responses", []string{"data: {\"type\":\"response.comp", "leted\"}\n\n", "data: trailing\n\n"}, "response.in_progress"},
		{"chat", "/v1/chat/completions", []string{"data: [DO", "NE]\n\n", "data: trailing\n\n"}, "chatcmpl-keepalive"},
		{"messages", "/v1/messages", []string{"event: message_", "stop\ndata: {\"type\":\"message_stop\"}\n\n", "data: trailing\n\n"}, "event: ping"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			release := make(chan struct{})
			p, _ := testProxy(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				for i, part := range tc.parts {
					_, _ = io.WriteString(w, part)
					w.(http.Flusher).Flush()
					if i == 0 {
						time.Sleep(time.Millisecond)
					}
				}
				<-release
			}), 100*time.Millisecond, 10*time.Millisecond, defaultMaxBody)
			resp := request(t, http.DefaultClient, http.MethodPost, p.URL+tc.path, "application/json", `{"stream":true}`)
			time.Sleep(35 * time.Millisecond)
			close(release)
			want := strings.Join(tc.parts, "")
			if body := readAll(t, resp); body != want || strings.Contains(body, tc.synthetic) {
				t.Fatalf("body=%q want=%q", body, want)
			}
		})
	}
}

func TestTerminalEventsSuppressGenericErrors(t *testing.T) {
	cases := []struct {
		kind     streamKind
		terminal string
	}{
		{streamResponses, "data: {\"type\":\"response.failed\"}\n\n"},
		{streamResponses, "data: {\"type\":\"error\",\"message\":\"failed\"}\n\n"},
		{streamChat, "data: [DONE]\n\n"},
		{streamChat, "data: {\"error\":{\"message\":\"failed\"}}\n\n"},
		{streamMessages, "data: {\"type\":\"message_stop\"}\n\n"},
		{streamMessages, "event: error\ndata: {\"type\":\"error\"}\n\n"},
	}
	for _, tc := range cases {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/", nil)
		reads := make(chan readResult, 2)
		reads <- readResult{data: []byte(tc.terminal)}
		reads <- readResult{data: []byte("data: trailing\n\n"), err: errors.New("upstream broke")}
		p := newProxy(config{idle: time.Second})
		p.copySSEAfter(recorder, request, tc.kind, reads, time.Second, newTerminalDetector(tc.kind))
		if got, want := recorder.Body.String(), tc.terminal+"data: trailing\n\n"; got != want {
			t.Fatalf("kind=%d body=%q want=%q", tc.kind, got, want)
		}
	}
}

func TestTerminalDetectorBufferIsBounded(t *testing.T) {
	detector := newTerminalDetector(streamResponses)
	detector.Write(bytes.Repeat([]byte("x"), maxSSEEventSize+1))
	if len(detector.buffer) > maxSSEEventSize {
		t.Fatalf("buffer=%d", len(detector.buffer))
	}
}

func TestStartupTimerConsumesReadyEOF(t *testing.T) {
	p := newProxy(config{wait: time.Millisecond, idle: time.Second})
	for range 100 {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
		reads := make(chan readResult, 1)
		reads <- readResult{err: io.EOF}
		timer := time.NewTimer(0)
		p.handleBeforeThresholdReads(recorder, request, streamResponses, reads, newTerminalDetector(streamResponses), &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"text/event-stream"}}}, timer)
		if body := recorder.Body.String(); body != "" {
			t.Fatalf("startup frame after ready EOF: %q", body)
		}
	}
}

func TestExactSyntheticFrames(t *testing.T) {
	if got, want := string(keepaliveFrame(streamResponses, time.Unix(1, 0))), "data: {\"type\":\"response.in_progress\"}\n\n"; got != want {
		t.Fatalf("responses keepalive=%q", got)
	}
	cases := []struct {
		kind streamKind
		want string
	}{
		{streamResponses, "data: {\"type\":\"error\",\"code\":null,\"message\":\"Upstream stream failed before completion.\",\"param\":null}\n\n"},
		{streamChat, "data: {\"error\":{\"message\":\"Upstream stream failed before completion.\",\"type\":\"stream_error\"}}\n\n"},
		{streamMessages, "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"api_error\",\"message\":\"Upstream stream failed before completion.\"}}\n\n"},
	}
	for _, tc := range cases {
		if got := string(errorFrame(tc.kind)); got != tc.want {
			t.Fatalf("kind=%d frame=%q want=%q", tc.kind, got, tc.want)
		}
	}
}

func TestCompressedSSEIsInvalid(t *testing.T) {
	t.Run("fast response preserved", func(t *testing.T) {
		p, _ := testProxy(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("Content-Encoding", "gzip")
			w.Header().Set("X-Upstream", "kept")
			w.WriteHeader(http.StatusAccepted)
			_, _ = io.WriteString(w, "compressed bytes")
		}), time.Second, time.Second, defaultMaxBody)
		client := &http.Client{Transport: &http.Transport{DisableCompression: true}}
		resp := request(t, client, http.MethodPost, p.URL+"/v1/responses", "application/json", `{"stream":true}`)
		if resp.StatusCode != http.StatusAccepted || resp.Header.Get("Content-Encoding") != "gzip" || resp.Header.Get("X-Upstream") != "kept" || readAll(t, resp) != "compressed bytes" {
			t.Fatal("compressed fast response changed")
		}
	})

	t.Run("slow response gets in-band error", func(t *testing.T) {
		p, _ := testProxy(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(25 * time.Millisecond)
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("Content-Encoding", "br")
			_, _ = io.WriteString(w, "compressed bytes")
		}), 5*time.Millisecond, time.Second, defaultMaxBody)
		resp := request(t, http.DefaultClient, http.MethodPost, p.URL+"/v1/responses", "application/json", `{"stream":true}`)
		body := readAll(t, resp)
		if resp.StatusCode != http.StatusOK || !strings.HasSuffix(body, string(errorFrame(streamResponses))) || strings.Contains(body, "compressed bytes") {
			t.Fatalf("status=%d body=%q", resp.StatusCode, body)
		}
	})
}

func TestEOFAndCancellationStop(t *testing.T) {
	t.Run("EOF has no trailing heartbeat", func(t *testing.T) {
		p, _ := testProxy(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: done\n\n")
		}), 100*time.Millisecond, 5*time.Millisecond, defaultMaxBody)
		resp := request(t, http.DefaultClient, http.MethodPost, p.URL+"/v1/responses", "application/json", `{"stream":true}`)
		if body := readAll(t, resp); body != "data: done\n\n" {
			t.Fatalf("body=%q", body)
		}
	})

	t.Run("cancel closes upstream", func(t *testing.T) {
		cancelled := make(chan struct{})
		p, _ := testProxy(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			w.(http.Flusher).Flush()
			<-r.Context().Done()
			close(cancelled)
		}), 5*time.Millisecond, time.Second, defaultMaxBody)
		ctx, cancel := context.WithCancel(context.Background())
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, p.URL+"/v1/responses", strings.NewReader(`{"stream":true}`))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		cancel()
		resp.Body.Close()
		select {
		case <-cancelled:
		case <-time.After(time.Second):
			t.Fatal("upstream context not cancelled")
		}
	})
}

func TestIneligibleMalformedAndBodyCapPassThrough(t *testing.T) {
	var calls atomic.Int32
	p, _ := testProxy(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		body, _ := io.ReadAll(r.Body)
		w.WriteHeader(207)
		_, _ = w.Write(body)
	}), 10*time.Millisecond, time.Second, 12)
	cases := []struct {
		path string
		ct   string
		body string
	}{
		{"/other", "application/json", `{"stream":true}`},
		{"/v1/responses", "application/json", `{"stream":`},
		{"/v1/responses", "text/plain", `{"stream":true}`},
		{"/v1/responses", "application/json", `{"stream":true,"long":"preserved"}`},
	}
	for _, tc := range cases {
		resp := request(t, http.DefaultClient, http.MethodPost, p.URL+tc.path, tc.ct, tc.body)
		if resp.StatusCode != 207 || readAll(t, resp) != tc.body {
			t.Fatalf("pass-through failed for %q", tc.body)
		}
	}
	if calls.Load() != int32(len(cases)) {
		t.Fatalf("calls=%d", calls.Load())
	}
}

func TestHopHeadersStripped(t *testing.T) {
	p, _ := testProxy(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Remove-Me") != "" || r.Header.Get("Connection") != "" || r.Header.Get("Upgrade") != "" {
			t.Errorf("request hop headers leaked: %v", r.Header)
		}
		if r.Header.Get("Accept-Encoding") != "identity" {
			t.Errorf("Accept-Encoding=%q", r.Header.Get("Accept-Encoding"))
		}
		w.Header().Set("Connection", "X-Response-Hop")
		w.Header().Set("X-Response-Hop", "remove")
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: ok\n\n")
	}), 100*time.Millisecond, time.Second, defaultMaxBody)
	req, _ := http.NewRequest(http.MethodPost, p.URL+"/v1/responses", strings.NewReader(`{"stream":true}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Connection", "X-Remove-Me")
	req.Header.Set("X-Remove-Me", "secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Header.Get("X-Response-Hop") != "" {
		t.Fatalf("response hop header leaked: %v", resp.Header)
	}
	_ = readAll(t, resp)
}

func TestHealthAndHealthcheck(t *testing.T) {
	p, _ := testProxy(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}), time.Second, time.Second, defaultMaxBody)
	resp := request(t, http.DefaultClient, http.MethodGet, p.URL+"/healthz", "", "")
	if resp.StatusCode != 200 || readAll(t, resp) != "ok\n" {
		t.Fatal("health endpoint failed")
	}
	u, _ := url.Parse(p.URL)
	if err := healthcheck(u.Host); err != nil {
		t.Fatal(err)
	}
	if err := healthcheck(u.Port()); err != nil {
		t.Fatal(err)
	}
}

func TestKindForProtocolAliases(t *testing.T) {
	cases := map[string]streamKind{
		"/v1/responses":                streamResponses,
		"/responses":                   streamResponses,
		"/backend-api/codex/responses": streamResponses,
		"/v1/chat/completions":         streamChat,
		"/v1/messages":                 streamMessages,
		"/antigravity/v1/messages":     streamMessages,
	}
	for path, want := range cases {
		if got := kindFor(http.MethodPost, path); got != want {
			t.Errorf("kindFor(POST, %q) = %d, want %d", path, got, want)
		}
	}
	for _, path := range []string{"/v1/responses/response-id", "/backend-api/codex/responses/response-id", "/antigravity/v1/messages/count_tokens"} {
		if got := kindFor(http.MethodPost, path); got != streamNone {
			t.Errorf("kindFor(POST, %q) = %d, want none", path, got)
		}
	}
	if got := kindFor(http.MethodGet, "/responses"); got != streamNone {
		t.Errorf("kindFor(GET, /responses) = %d, want none", got)
	}
}

func TestRouteParsersAndGate(t *testing.T) {
	v4Default := []byte("Iface Destination Gateway Flags RefCnt Use Metric Mask MTU Window IRTT\neth0 00000000 0100000A 0003 0 0 0 00000000 0 0 0\n")
	v6Default := []byte(strings.Repeat("0", 32) + " 00 " + strings.Repeat("0", 32) + " 00 " + strings.Repeat("0", 32) + " 00000000 00000000 00000001 00000003 eth0\n")
	if !hasDefaultRoute(v4Default, nil) || !hasDefaultRoute(nil, v6Default) {
		t.Fatal("default route not detected")
	}
	nonDefaultV4 := []byte("Iface Destination Gateway Flags RefCnt Use Metric Mask MTU Window IRTT\neth0 00000000 0100000A 0003 0 0 0 000000FF 0 0 0\n")
	if hasDefaultRoute(nonDefaultV4, nil) {
		t.Fatal("0.0.0.0/8 must not be treated as default")
	}
	loopbackV6 := bytes.Replace(v6Default, []byte("eth0"), []byte("lo"), 1)
	if hasDefaultRoute([]byte("Iface Destination\n"), loopbackV6) {
		t.Fatal("loopback IPv6 default must be allowed")
	}
	fsys := fstest.MapFS{
		"route":      {Data: []byte("Iface Destination\n")},
		"ipv6_route": {Data: loopbackV6},
	}
	if err := waitForNoDefaultRoute(fsys, time.Millisecond); err != nil {
		t.Fatal(err)
	}
}

func TestWebSocketStyleUpgradePassThrough(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.EqualFold(r.Header.Get("Upgrade"), "testproto") {
			t.Errorf("upgrade=%q", r.Header.Get("Upgrade"))
		}
		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Fatal("upstream cannot hijack")
		}
		conn, rw, err := hj.Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		_, _ = rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: testproto\r\n\r\n")
		_ = rw.Flush()
		line, _ := rw.ReadString('\n')
		_, _ = rw.WriteString("echo:" + line)
		_ = rw.Flush()
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	front := httptest.NewServer(newProxy(config{upstream: u, wait: time.Second, idle: time.Second, maxBody: defaultMaxBody}))
	defer front.Close()
	fu, _ := url.Parse(front.URL)
	conn, err := net.Dial("tcp", fu.Host)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "GET /socket HTTP/1.1\r\nHost: %s\r\nConnection: Upgrade\r\nUpgrade: testproto\r\n\r\n", fu.Host)
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	resp, err := http.ReadResponse(rw.Reader, &http.Request{Method: http.MethodGet})
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	_, _ = rw.WriteString("hello\n")
	_ = rw.Flush()
	line, err := rw.ReadString('\n')
	if err != nil || line != "echo:hello\n" {
		t.Fatalf("line=%q err=%v", line, err)
	}
}
