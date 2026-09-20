# ai-sse-keepalive-proxy

Small fixed-upstream Go reverse proxy for parser-visible startup and idle keepalives in AI SSE protocols. It is intentionally protocol-specific rather than a generic arbitrary-SSE transformer.

```text
API gateway -> directed relay -> ai-sse-keepalive-proxy -> directed relay -> AI upstream
```

The API gateway (for example APISIX) remains the sole public and security boundary. It must authenticate, authorize, rate-limit, validate, and sanitize requests before forwarding here. This service has no admin, metrics, discovery, or user-selected proxy target.

## Why a separate proxy

Plain APISIX/Nginx body filters run only when upstream sends body data, so they cannot produce output during upstream silence. OpenResty `body_filter` also disables control APIs and cosockets; timer callbacks cannot write downstream response bytes. A streaming reverse proxy must own the downstream writer to send bytes while the upstream body is idle.

## Behavior

Only exact JSON `POST` requests with explicit boolean `"stream": true` are inspected for SSE keepalive:

| Path | Protocol-visible keepalive | Terminal events |
|---|---|---|
| `/v1/responses`, `/responses`, `/backend-api/codex/responses` | OpenAI `response.in_progress` | `response.completed`, `response.failed`, `response.incomplete`, `error` |
| `/v1/chat/completions` | OpenAI empty delta chunk | `[DONE]` or top-level `error` |
| `/v1/messages`, `/antigravity/v1/messages` | Anthropic `ping` | `message_stop` or `error` |

Requests to those same paths with `"stream": false` (or no `stream` key) are proxied with **103 Early Hints heartbeat**: while the upstream stays silent past `IDLE_INTERVAL`, the service emits interim `HTTP/1.1 103 Early Hints` frames to keep the downstream connection warm, then forwards the final status, headers, and body unchanged. Interim 1xx frames are the only protocol-legal bytes before a non-stream response; HTTP/1.0 downstreams skip heartbeats and get plain passthrough. Cloudflare forwards 1xx responses, nginx forwards 103 with `early_hints` enabled, and httpx/aiohttp clients skip them transparently.

Body bytes are preserved exactly. Other paths and protocols, malformed JSON, oversized inspected bodies, and WebSocket upgrades use Go standard `httputil.ReverseProxy` unchanged — **unless** `WS_PING_INTERVAL` is set above zero, in which case standards-compliant WebSocket upgrades are terminated at the proxy for downstream keepalive (see below).

## WebSocket keepalive

When `WS_PING_INTERVAL > 0`, any RFC 6455 `GET` upgrade (e.g. `GET /v1/responses` with `Upgrade: websocket`) is proxied as two terminated WebSocket legs rather than an opaque tunnel:

- **Upstream-first admission.** The proxy dials the fixed upstream with the client's handshake-relevant headers and subprotocol offers. A non-101 response (auth failure, rate limit, redirect) is forwarded to the client with its real status, headers, and full body — never replaced by a generic 502 or a truncated diagnostic.
- **Terminated legs, streaming relay.** Client↔proxy and proxy↔upstream each negotiate `permessage-deflate` independently (`CompressionContextTakeover`, matching sub2api's own accept/dial behavior). Application messages stream through a bounded 32 KiB buffer preserving type, order, and boundaries; backpressure pauses forwarding rather than buffering whole messages. There is **no proxy-side message size cap** (`SetReadLimit(-1)`); the backend's own read limit stays authoritative.
- **Downstream heartbeat.** A ticker emits a WebSocket `Ping` toward the client whenever recent application output has not already kept that leg active. The Ping waits for the matching Pong — which is exactly what the permanent per-leg reader provides (this is the piece the old sub2api ingress patch lacked). A missed Pong is recorded as an *unconfirmed probe* only; it is never by itself grounds for disconnecting the client. Real transport failure, close frames, or shutdown end the session through the normal error path.
- **No new deadlines.** There is no read-idle timeout and no session-lifetime cap added by the relay: `WS_WRITE_TIMEOUT` bounds only an actually-blocked destination write, and `WS_HANDSHAKE_TIMEOUT` bounds only connect/header acquisition. Backend admission and business timeouts remain authoritative.

Set `WS_PING_INTERVAL=0` (the default) to keep the legacy opaque `httputil.ReverseProxy` tunnel for WebSocket traffic.

| Variable | Default | Meaning |
|---|---:|---|
| `WS_PING_INTERVAL` | `0s` | Non-negative Go duration; `0` disables WS termination+keepalive (opaque tunnel). Production candidate `15s`. |
| `WS_PING_TIMEOUT` | `5s` | Positive Go duration; must be less than an enabled `WS_PING_INTERVAL`. Bounds one Ping round-trip, not the AI turn. |
| `WS_WRITE_TIMEOUT` | `120s` | Positive Go duration bounding a blocked destination write. Not a read/turn/session timeout. |
| `WS_HANDSHAKE_TIMEOUT` | `10s` | Positive Go duration bounding upstream connect+header acquisition. |

If valid SSE body bytes arrive within `HEADER_WAIT`, upstream status, headers, and body pass through. If first SSE body remains silent past threshold, service commits HTTP 200 `text/event-stream` and emits protocol-specific startup frame. Later frames appear only after `IDLE_INTERVAL` of upstream body silence. Complete protocol terminal or error events suppress all later synthetic keepalive/error frames, while any trailing upstream bytes still pass through unchanged. Once HTTP 200 is committed, later non-2xx, non-SSE, compressed, or stream failure can only be represented as generic protocol-shaped in-band error; upstream detail is never exposed.

All requests use a fixed-upstream transport that ignores environment proxy variables and disables automatic decompression. Eligible streams additionally request identity encoding and return redirects unchanged without contacting their targets; ineligible traffic keeps standard `httputil.ReverseProxy` response behavior.

## Configuration

| Variable | Default | Meaning |
|---|---:|---|
| `UPSTREAM_URL` | required | Fixed plain `http` upstream; userinfo, query, and fragment rejected |
| `LISTEN_ADDR` | `:8080` | Listen address |
| `HEADER_WAIT` | `2s` | Positive Go duration before startup frame |
| `IDLE_INTERVAL` | `15s` | Positive Go duration of upstream body silence between frames (also the non-stream 103 heartbeat interval) |
| `MAX_INSPECT_BODY_BYTES` | `16777216` | Positive inspected body cap |
| `REQUIRE_NO_DEFAULT_ROUTE` | `false` | When true, wait up to 5s for no IPv4 or non-loopback IPv6 default route before listening |

`REQUIRE_NO_DEFAULT_ROUTE=true` is expected in hardened Compose topology where isolated relays provide only directed service edges. Service still requires container/network policy: no public port, read-only filesystem, dropped capabilities, no-new-privileges, fixed upstream, and gateway-only ingress.

## Health

`GET /healthz` returns `200 ok`. Scratch image includes healthcheck mode:

```sh
/ai-sse-keepalive-proxy healthcheck
```

It checks `127.0.0.1` at port from `LISTEN_ADDR` with 2s timeout.

## Container publishing

`Dockerfile` is directly buildable by `docker compose build`. Default-branch pushes publish `ghcr.io/xz-dev/ai-sse-keepalive-proxy:latest` plus an immutable full commit-SHA tag. AI-gateway consumes this repository as a pinned Git submodule and builds the image locally with Compose; it does not depend on the GHCR image.

## Development

Requires Go 1.26 and `github.com/coder/websocket`.

```sh
gofmt -w .
go vet ./...
go test -race ./...
go build ./...
```
