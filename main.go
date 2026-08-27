package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"mime"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	defaultMaxBody  = 16 << 20
	maxSSEEventSize = 64 << 10
)

type config struct {
	upstream              *url.URL
	listen                string
	wait                  time.Duration
	idle                  time.Duration
	maxBody               int64
	requireNoDefaultRoute bool
}

type proxy struct {
	cfg      config
	fallback *httputil.ReverseProxy
	client   *http.Client
}

type streamKind int

const (
	streamNone streamKind = iota
	streamResponses
	streamChat
	streamMessages
)

func main() {
	if len(os.Args) == 2 && os.Args[1] == "healthcheck" {
		if err := healthcheck(os.Getenv("LISTEN_ADDR")); err != nil {
			log.Print(err)
			os.Exit(1)
		}
		return
	}
	cfg, err := loadConfig()
	if err != nil {
		log.Fatal(err)
	}
	p := newProxy(cfg)
	if cfg.requireNoDefaultRoute {
		if err := waitForNoDefaultRoute(os.DirFS("/proc/net"), 5*time.Second); err != nil {
			log.Fatal(err)
		}
	}
	srv := &http.Server{
		Addr:              cfg.listen,
		Handler:           logging(p),
		ReadHeaderTimeout: 10 * time.Second,
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	listener, err := net.Listen("tcp", cfg.listen)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("listening on %s", listener.Addr())
	if err := serve(ctx, srv, listener, 10*time.Second); err != nil {
		log.Fatal(err)
	}
}

func serve(ctx context.Context, srv *http.Server, listener net.Listener, shutdownTimeout time.Duration) error {
	result := make(chan error, 1)
	go func() { result <- srv.Serve(listener) }()

	select {
	case err := <-result:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	err := srv.Shutdown(shutdownCtx)
	cancel()
	if err != nil {
		_ = srv.Close()
	}
	serveErr := <-result
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		return serveErr
	}
	return nil
}

func healthcheck(listen string) error {
	if listen == "" {
		listen = ":8080"
	}
	if !strings.Contains(listen, ":") {
		listen = ":" + listen
	}
	_, port, err := net.SplitHostPort(listen)
	if err != nil || port == "" {
		return fmt.Errorf("invalid LISTEN_ADDR %q", listen)
	}
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://127.0.0.1:" + port + "/healthz")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("healthcheck returned %s", resp.Status)
	}
	return nil
}

func loadConfig() (config, error) {
	raw := os.Getenv("UPSTREAM_URL")
	if raw == "" {
		return config{}, errors.New("UPSTREAM_URL is required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "http" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return config{}, errors.New("UPSTREAM_URL must be a plain http URL without userinfo, query, or fragment")
	}
	wait, err := envDuration("HEADER_WAIT", 2*time.Second)
	if err != nil {
		return config{}, err
	}
	idle, err := envDuration("IDLE_INTERVAL", 15*time.Second)
	if err != nil {
		return config{}, err
	}
	maxBody, err := envPositiveInt("MAX_INSPECT_BODY_BYTES", defaultMaxBody)
	if err != nil {
		return config{}, err
	}
	listen := os.Getenv("LISTEN_ADDR")
	if listen == "" {
		listen = ":8080"
	}
	requireNoDefaultRoute, err := envBool("REQUIRE_NO_DEFAULT_ROUTE", false)
	if err != nil {
		return config{}, err
	}
	return config{upstream: u, listen: listen, wait: wait, idle: idle, maxBody: maxBody, requireNoDefaultRoute: requireNoDefaultRoute}, nil
}

func envBool(name string, fallback bool) (bool, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("%s must be true or false", name)
	}
	return value, nil
}

func waitForNoDefaultRoute(fsys fs.FS, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		v4, err4 := fs.ReadFile(fsys, "route")
		v6, err6 := fs.ReadFile(fsys, "ipv6_route")
		if err4 == nil && err6 == nil && !hasDefaultRoute(v4, v6) {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("default-route isolation check failed")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func hasDefaultRoute(v4, v6 []byte) bool {
	for _, line := range strings.Split(string(v4), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 8 && fields[1] == "00000000" && fields[7] == "00000000" && fields[3] != "0000" {
			return true
		}
	}
	for _, line := range strings.Split(string(v6), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 10 && fields[0] == strings.Repeat("0", 32) && fields[1] == "00" && fields[9] != "lo" {
			return true
		}
	}
	return false
}

func envDuration(name string, fallback time.Duration) (time.Duration, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s must be a positive Go duration", name)
	}
	return d, nil
}

func envPositiveInt(name string, fallback int64) (int64, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback, nil
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return n, nil
}

func newProxy(cfg config) *proxy {
	rp := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(cfg.upstream)
			pr.Out.Host = cfg.upstream.Host
			for _, h := range []string{"Forwarded", "X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto"} {
				pr.Out.Header[h] = append([]string(nil), pr.In.Header.Values(h)...)
			}
		},
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DisableCompression = true
	client := &http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return &proxy{cfg: cfg, fallback: rp, client: client}
}

func (p *proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
		return
	}
	kind, body, ok := p.inspect(r)
	if !ok {
		p.fallback.ServeHTTP(w, r)
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	p.serveStream(w, r, kind)
}

func (p *proxy) inspect(r *http.Request) (streamKind, []byte, bool) {
	kind := kindFor(r.Method, r.URL.Path)
	if kind == streamNone || r.Body == nil || !isJSON(r.Header.Get("Content-Type")) {
		return streamNone, nil, false
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, p.cfg.maxBody+1))
	if err != nil {
		r.Body = struct {
			io.Reader
			io.Closer
		}{io.MultiReader(bytes.NewReader(body), r.Body), r.Body}
		return streamNone, nil, false
	}
	if int64(len(body)) > p.cfg.maxBody {
		r.Body = struct {
			io.Reader
			io.Closer
		}{io.MultiReader(bytes.NewReader(body), r.Body), r.Body}
		return streamNone, nil, false
	}
	_ = r.Body.Close()
	r.Body = io.NopCloser(bytes.NewReader(body))
	var envelope struct {
		Stream *bool `json:"stream"`
	}
	if json.Unmarshal(body, &envelope) != nil || envelope.Stream == nil || !*envelope.Stream {
		return streamNone, nil, false
	}
	return kind, body, true
}

func kindFor(method, path string) streamKind {
	if method != http.MethodPost {
		return streamNone
	}
	switch path {
	case "/v1/responses":
		return streamResponses
	case "/v1/chat/completions":
		return streamChat
	case "/v1/messages":
		return streamMessages
	default:
		return streamNone
	}
}

func isJSON(contentType string) bool {
	mediaType, _, err := mime.ParseMediaType(contentType)
	return err == nil && (mediaType == "application/json" || strings.HasSuffix(mediaType, "+json"))
}

type upstreamResult struct {
	response *http.Response
	err      error
}

func (p *proxy) serveStream(w http.ResponseWriter, r *http.Request, kind streamKind) {
	req := r.Clone(r.Context())
	req.URL = joinURL(p.cfg.upstream, r.URL)
	req.Host = p.cfg.upstream.Host
	req.RequestURI = ""
	req.Header = r.Header.Clone()
	removeHopHeaders(req.Header)
	req.Header.Set("Accept-Encoding", "identity")

	result := make(chan upstreamResult, 1)
	go func() {
		resp, err := p.client.Do(req)
		result <- upstreamResult{resp, err}
	}()

	timer := time.NewTimer(p.cfg.wait)
	defer timer.Stop()
	select {
	case got := <-result:
		p.handleBeforeThreshold(w, r, kind, got, timer)
	case <-timer.C:
		p.openSlow(w, kind)
		p.handleAfterThreshold(w, r, kind, result)
	case <-r.Context().Done():
		drainResult(result)
	}
}

func drainResult(result <-chan upstreamResult) {
	go func() {
		got := <-result
		if got.response != nil {
			got.response.Body.Close()
		}
	}()
}

func (p *proxy) handleBeforeThreshold(w http.ResponseWriter, r *http.Request, kind streamKind, got upstreamResult, timer *time.Timer) {
	if got.err != nil {
		http.Error(w, "bad gateway", http.StatusBadGateway)
		return
	}
	defer got.response.Body.Close()
	if !validSSE(got.response) {
		copyHeader(w.Header(), got.response.Header)
		w.WriteHeader(got.response.StatusCode)
		_, _ = io.Copy(w, got.response.Body)
		return
	}

	reads := streamReads(r.Context(), got.response.Body)
	p.handleBeforeThresholdReads(w, r, kind, reads, newTerminalDetector(kind), got.response, timer)
}

func (p *proxy) handleBeforeThresholdReads(w http.ResponseWriter, r *http.Request, kind streamKind, reads <-chan readResult, detector *terminalDetector, resp *http.Response, timer *time.Timer) {
	select {
	case rr := <-reads:
		p.handleInitialRead(w, r, kind, reads, detector, resp, rr)
	case <-timer.C:
		select {
		case rr := <-reads:
			p.handleInitialRead(w, r, kind, reads, detector, resp, rr)
		default:
			p.openSlow(w, kind)
			p.copySSEAfter(w, r, kind, reads, p.cfg.idle, detector)
		}
	case <-r.Context().Done():
	}
}

func (p *proxy) handleInitialRead(w http.ResponseWriter, r *http.Request, kind streamKind, reads <-chan readResult, detector *terminalDetector, resp *http.Response, rr readResult) {
	copyHeader(w.Header(), resp.Header)
	w.Header().Del("Content-Length")
	w.WriteHeader(resp.StatusCode)
	if len(rr.data) > 0 {
		detector.Write(rr.data)
		if !writeFrame(w, rr.data) {
			return
		}
	}
	if rr.err != nil {
		if !errors.Is(rr.err, io.EOF) && r.Context().Err() == nil && !detector.terminal {
			writeFrame(w, errorFrame(kind))
		}
		return
	}
	p.copySSEAfter(w, r, kind, reads, p.cfg.idle, detector)
}

func (p *proxy) openSlow(w http.ResponseWriter, kind streamKind) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	writeFrame(w, keepaliveFrame(kind, time.Now()))
}

func (p *proxy) handleAfterThreshold(w http.ResponseWriter, r *http.Request, kind streamKind, result <-chan upstreamResult) {
	lastWrite := time.Now()
	timer := time.NewTimer(p.cfg.idle)
	defer timer.Stop()
	for {
		select {
		case got := <-result:
			if got.err != nil {
				writeFrame(w, errorFrame(kind))
				return
			}
			defer got.response.Body.Close()
			if !validSSE(got.response) {
				writeFrame(w, errorFrame(kind))
				return
			}
			remaining := p.cfg.idle - time.Since(lastWrite)
			p.copySSEAfter(w, r, kind, streamReads(r.Context(), got.response.Body), remaining, newTerminalDetector(kind))
			return
		case <-timer.C:
			if !writeFrame(w, keepaliveFrame(kind, time.Now())) {
				return
			}
			lastWrite = time.Now()
			timer.Reset(p.cfg.idle)
		case <-r.Context().Done():
			drainResult(result)
			return
		}
	}
}

func validSSE(resp *http.Response) bool {
	mediaType, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	encoding := strings.TrimSpace(resp.Header.Get("Content-Encoding"))
	return err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 && mediaType == "text/event-stream" && (encoding == "" || strings.EqualFold(encoding, "identity"))
}

type readResult struct {
	data []byte
	err  error
}

type terminalDetector struct {
	kind     streamKind
	buffer   []byte
	terminal bool
}

func newTerminalDetector(kind streamKind) *terminalDetector {
	return &terminalDetector{kind: kind}
}

func (d *terminalDetector) Write(chunk []byte) {
	if d.terminal || len(chunk) == 0 {
		return
	}
	if len(d.buffer)+len(chunk) > maxSSEEventSize {
		d.buffer = d.buffer[:0]
		if len(chunk) > maxSSEEventSize {
			chunk = chunk[len(chunk)-maxSSEEventSize:]
		}
	}
	d.buffer = append(d.buffer, chunk...)
	for {
		end, size := sseEventEnd(d.buffer)
		if end < 0 {
			return
		}
		event := d.buffer[:end]
		d.buffer = d.buffer[end+size:]
		if isTerminalEvent(d.kind, event) {
			d.terminal = true
			d.buffer = nil
			return
		}
	}
}

func sseEventEnd(data []byte) (int, int) {
	lf := bytes.Index(data, []byte("\n\n"))
	cr := bytes.Index(data, []byte("\r\r"))
	crlf := bytes.Index(data, []byte("\r\n\r\n"))
	end, size := -1, 0
	for _, candidate := range []struct {
		end  int
		size int
	}{{lf, 2}, {cr, 2}, {crlf, 4}} {
		if candidate.end >= 0 && (end < 0 || candidate.end < end) {
			end, size = candidate.end, candidate.size
		}
	}
	return end, size
}

func isTerminalEvent(kind streamKind, raw []byte) bool {
	var eventName string
	var dataLines []string
	normalized := strings.ReplaceAll(strings.ReplaceAll(string(raw), "\r\n", "\n"), "\r", "\n")
	for _, line := range strings.Split(normalized, "\n") {
		if strings.HasPrefix(line, "event:") {
			eventName = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		}
		if strings.HasPrefix(line, "data:") {
			dataLines = append(dataLines, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		}
	}
	payload := strings.Join(dataLines, "\n")
	switch kind {
	case streamChat:
		return payload == "[DONE]"
	case streamResponses:
		var event struct {
			Type string `json:"type"`
		}
		if json.Unmarshal([]byte(payload), &event) != nil {
			return false
		}
		return event.Type == "response.completed" || event.Type == "response.failed" || event.Type == "response.incomplete"
	case streamMessages:
		if eventName == "message_stop" {
			return true
		}
		var event struct {
			Type string `json:"type"`
		}
		return json.Unmarshal([]byte(payload), &event) == nil && event.Type == "message_stop"
	default:
		return false
	}
}

func streamReads(ctx context.Context, body io.Reader) <-chan readResult {
	reads := make(chan readResult, 1)
	go func() {
		buf := make([]byte, 32<<10)
		for {
			n, err := body.Read(buf)
			chunk := append([]byte(nil), buf[:n]...)
			select {
			case reads <- readResult{chunk, err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	return reads
}

func (p *proxy) copySSEAfter(w http.ResponseWriter, r *http.Request, kind streamKind, reads <-chan readResult, firstIdle time.Duration, detector *terminalDetector) {
	if firstIdle <= 0 {
		firstIdle = time.Nanosecond
	}
	timer := time.NewTimer(firstIdle)
	defer timer.Stop()
	for {
		select {
		case rr := <-reads:
			if len(rr.data) > 0 {
				detector.Write(rr.data)
				if !writeFrame(w, rr.data) {
					return
				}
			}
			if rr.err != nil {
				if !errors.Is(rr.err, io.EOF) && r.Context().Err() == nil && !detector.terminal {
					writeFrame(w, errorFrame(kind))
				}
				return
			}
			resetTimer(timer, p.cfg.idle)
		case <-timer.C:
			select {
			case rr := <-reads:
				if len(rr.data) > 0 {
					detector.Write(rr.data)
					if !writeFrame(w, rr.data) {
						return
					}
				}
				if rr.err != nil {
					if !errors.Is(rr.err, io.EOF) && r.Context().Err() == nil && !detector.terminal {
						writeFrame(w, errorFrame(kind))
					}
					return
				}
			default:
				if !detector.terminal && !writeFrame(w, keepaliveFrame(kind, time.Now())) {
					return
				}
			}
			resetTimer(timer, p.cfg.idle)
		case <-r.Context().Done():
			return
		}
	}
}

func writeFrame(w http.ResponseWriter, data []byte) bool {
	if _, err := w.Write(data); err != nil {
		return false
	}
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	return true
}

func keepaliveFrame(kind streamKind, now time.Time) []byte {
	switch kind {
	case streamResponses:
		return []byte("data: {\"type\":\"response.in_progress\"}\n\n")
	case streamChat:
		return []byte(fmt.Sprintf("data: {\"id\":\"chatcmpl-keepalive\",\"object\":\"chat.completion.chunk\",\"created\":%d,\"model\":\"keepalive\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":null}]}\n\n", now.Unix()))
	case streamMessages:
		return []byte("event: ping\ndata: {\"type\":\"ping\"}\n\n")
	default:
		return nil
	}
}

func errorFrame(kind streamKind) []byte {
	switch kind {
	case streamResponses:
		return []byte("data: {\"type\":\"error\",\"code\":null,\"message\":\"Upstream stream failed before completion.\",\"param\":null}\n\n")
	case streamChat:
		return []byte("data: {\"error\":{\"message\":\"Upstream stream failed before completion.\",\"type\":\"stream_error\"}}\n\n")
	case streamMessages:
		return []byte("event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"api_error\",\"message\":\"Upstream stream failed before completion.\"}}\n\n")
	default:
		return nil
	}
}

func resetTimer(timer *time.Timer, d time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(d)
}

func joinURL(base, request *url.URL) *url.URL {
	out := *request
	out.Scheme = base.Scheme
	out.Host = base.Host
	if base.Path != "" && base.Path != "/" {
		out.Path = strings.TrimRight(base.Path, "/") + "/" + strings.TrimLeft(request.Path, "/")
		out.RawPath = ""
	}
	return &out
}

func removeHopHeaders(h http.Header) {
	for _, name := range strings.Split(h.Get("Connection"), ",") {
		if name = strings.TrimSpace(name); name != "" {
			h.Del(name)
		}
	}
	for _, name := range []string{"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Te", "Trailer", "Transfer-Encoding", "Upgrade"} {
		h.Del(name)
	}
}

func copyHeader(dst, src http.Header) {
	clean := src.Clone()
	removeHopHeaders(clean)
	for k, values := range clean {
		dst[k] = append([]string(nil), values...)
	}
}

type logWriter struct {
	http.ResponseWriter
	status int
}

func (w *logWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *logWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(p)
}

func (w *logWriter) Flush() { _ = http.NewResponseController(w.ResponseWriter).Flush() }

func (w *logWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return http.NewResponseController(w.ResponseWriter).Hijack()
}

func (w *logWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		lw := &logWriter{ResponseWriter: w}
		next.ServeHTTP(lw, r)
		status := lw.status
		if status == 0 {
			status = http.StatusOK
		}
		log.Printf("method=%s path=%s status=%d duration=%s", r.Method, r.URL.Path, status, time.Since(start).Round(time.Millisecond))
	})
}
