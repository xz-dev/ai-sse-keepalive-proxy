package main

import (
	"bufio"
	"bytes"
	"context"
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
		{"/v1/chat/completions", `"id":"chatcmpl-keepalive"`},
		{"/v1/messages", "event: ping\ndata: {\"type\":\"ping\"}"},
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

func TestRouteParsersAndGate(t *testing.T) {
	v4Default := []byte("Iface Destination Gateway Flags RefCnt Use Metric Mask MTU Window IRTT\neth0 00000000 0100000A 0003 0 0 0 00000000 0 0 0\n")
	v6Default := []byte(strings.Repeat("0", 32) + " 00 " + strings.Repeat("0", 32) + " 00 " + strings.Repeat("0", 32) + " 00000000 00000000 00000001 00000003 eth0\n")
	if !hasDefaultRoute(v4Default, nil) || !hasDefaultRoute(nil, v6Default) {
		t.Fatal("default route not detected")
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
