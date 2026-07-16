# Docker Deployment

## Problem

No containerized deployment path. `s3sync serve` runs on bare metal only. Missing:
- Dockerfile
- docker-compose for multi-arch / automatic restart
- Health endpoint for orchestrator probes
- Graceful shutdown handling via SIGTERM

## Solution

### Dockerfile

Multi-stage build targeting `scratch` or `alpine`:

```dockerfile
FROM golang:1.26 AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -o s3sync .

FROM scratch
COPY --from=build /src/s3sync /s3sync
VOLUME /root/.s3sync
EXPOSE 8080
ENTRYPOINT ["/s3sync"]
CMD ["serve", "--port", "8080"]
```

### docker-compose.yml

```yaml
services:
  s3sync:
    build: .
    ports:
      - "8080:8080"
    volumes:
      - s3sync-config:/root/.s3sync
    environment:
      - BUCKETSYNC_MASTER_KEY=${BUCKETSYNC_MASTER_KEY:-}
    restart: unless-stopped
```

### Health Endpoint

`GET /api/health` already exists. Verify it returns 200 within timeout for k8s/Docker health checks.

### Graceful Shutdown

Current `serve.go` uses `signal.NotifyContext` + `router.Run`. Gin 1.x doesn't natively support `Shutdown()`. Replace with:

```go
srv := &http.Server{Addr: fmt.Sprintf(":%d", port), Handler: s.Router()}
go srv.ListenAndServe()
<-ctx.Done()
srv.Shutdown(context.Background())
```

This ensures in-flight requests and syncs complete before exit.

### Changed Files

| File | Change |
|------|--------|
| `Dockerfile` | New |
| `docker-compose.yml` | New |
| `.dockerignore` | New (exclude s3sync binary, node_modules, docs) |
| `cmd/serve.go` | Replace `router.Run` with `http.Server` + graceful `Shutdown` |
| `README.md` | Add Docker section |
