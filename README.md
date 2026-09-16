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

Body bytes are preserved exactly. Other paths and protocols, malformed JSON, oversized inspected bodies, and WebSocket upgrades use Go standard `httputil.ReverseProxy` unchanged.

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

Requires Go 1.26, standard library only.

```sh
gofmt -w .
go vet ./...
go test -race ./...
go build ./...
```
