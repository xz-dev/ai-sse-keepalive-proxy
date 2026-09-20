# syntax=docker/dockerfile:1
FROM docker.io/library/golang:1.26.7-alpine3.24 AS builder
WORKDIR /src
COPY go.mod go.sum main.go websocket.go ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w -buildid=' -o /ai-sse-keepalive-proxy .

FROM scratch
COPY --from=builder /ai-sse-keepalive-proxy /ai-sse-keepalive-proxy
USER 65534:65534
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s --retries=3 CMD ["/ai-sse-keepalive-proxy", "healthcheck"]
ENTRYPOINT ["/ai-sse-keepalive-proxy"]
