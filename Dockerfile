FROM golang:1.17 as builder
WORKDIR     /source
COPY        . .
RUN mkdir -p /app/logs /app/files /app/config
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /app/nats-delayqueue -v .
ADD config/config.example.toml /app/config/config.toml

FROM alpine:3.17
COPY --from=builder /app /app
ENTRYPOINT [ "/app/nats-delayqueue", "--config", "/app/config/config.toml", "serve"]