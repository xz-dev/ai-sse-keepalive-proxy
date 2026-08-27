# syntax=docker/dockerfile:1
FROM docker.io/library/golang@sha256:28d89ee9cc0ff9fec75c82ca201e6bf7fdf9a679d4b7b24dfa04f2bb766bb468 AS builder
WORKDIR /src
COPY go.mod main.go ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w -buildid=' -o /sse-keepalive .

FROM scratch
COPY --from=builder /sse-keepalive /sse-keepalive
USER 65534:65534
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s --retries=3 CMD ["/sse-keepalive", "healthcheck"]
ENTRYPOINT ["/sse-keepalive"]
