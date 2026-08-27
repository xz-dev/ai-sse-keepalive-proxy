# apisix-sse-keepalive

Small fixed-upstream Go reverse proxy for SSE startup and idle keepalives. Intended placement:

```text
APISIX -> SOCAT edge -> apisix-sse-keepalive -> SOCAT edge -> Sub2API
```

APISIX remains sole public and security boundary. It must authenticate, authorize, rate-limit, validate, and sanitize requests before forwarding here. This service has no admin, metrics, discovery, or user-selected proxy target.

## Why a separate proxy

Plain APISIX/Nginx body filters run only when upstream sends body data, so they cannot produce output during upstream silence. OpenResty `body_filter` also disables control APIs and cosockets; timer callbacks cannot write downstream response bytes. A streaming reverse proxy must own downstream writer to send bytes while upstream body is idle.

## Behavior

Only exact JSON `POST` requests to `/v1/responses`, `/v1/chat/completions`, and `/v1/messages` with explicit boolean `"stream": true` are inspected. Body bytes are preserved exactly. Other requests, malformed JSON, oversized inspected bodies, and WebSocket upgrades use Go standard `httputil.ReverseProxy` unchanged.

If valid SSE body bytes arrive within `HEADER_WAIT`, upstream status, headers, and body pass through. If first SSE body remains silent past threshold, service commits HTTP 200 `text/event-stream` and emits protocol-specific startup frame. Later frames appear only after `IDLE_INTERVAL` of upstream body silence. Complete protocol terminal or error events suppress all later synthetic keepalive/error frames, while any trailing upstream bytes still pass through unchanged. Once HTTP 200 is committed, later non-2xx, non-SSE, compressed, or stream failure can only be represented as generic protocol-shaped in-band error; upstream detail is never exposed.

All requests use a fixed-upstream transport that ignores environment proxy variables and disables automatic decompression. Eligible streams additionally request identity encoding and return redirects unchanged without contacting their targets; ineligible traffic keeps standard `httputil.ReverseProxy` response behavior.

## Configuration

| Variable | Default | Meaning |
|---|---:|---|
| `UPSTREAM_URL` | required | Fixed plain `http` upstream; userinfo, query, and fragment rejected |
| `LISTEN_ADDR` | `:8080` | Listen address |
| `HEADER_WAIT` | `2s` | Positive Go duration before startup frame |
| `IDLE_INTERVAL` | `15s` | Positive Go duration of upstream body silence between frames |
| `MAX_INSPECT_BODY_BYTES` | `16777216` | Positive inspected body cap |
| `REQUIRE_NO_DEFAULT_ROUTE` | `false` | When true, wait up to 5s for no IPv4 or non-loopback IPv6 default route before listening |

`REQUIRE_NO_DEFAULT_ROUTE=true` is expected in hardened Compose topology where isolated SOCAT networks provide only directed service edges. Service still requires container/network policy: no public port, read-only filesystem, dropped capabilities, no-new-privileges, fixed upstream, and APISIX-only ingress.

## Health

`GET /healthz` returns `200 ok`. Scratch image includes healthcheck mode:

```sh
/sse-keepalive healthcheck
```

It checks `127.0.0.1` at port from `LISTEN_ADDR` with 2s timeout.

## Container publishing

`Dockerfile` is directly buildable by `docker compose build`. Default-branch pushes publish `ghcr.io/xz-dev/apisix-sse-keepalive:latest` plus immutable full commit-SHA tag. AI-gateway may consume this repository as a Git submodule and build the same image locally with Compose; it need not consume GHCR image.

## Development

Requires Go 1.26, standard library only.

```sh
gofmt -w .
go vet ./...
go test -race ./...
go build ./...
```
