FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags "-X main.version=${VERSION:-dev}" -o s3sync . \
 && mkdir -p /out/home/.s3sync

# distroless static ships CA certificates and tzdata, which the scratch image
# lacked — without CA certs every HTTPS call to an S3 endpoint fails with
# "x509: certificate signed by unknown authority".
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /src/s3sync /s3sync

# The config directory is created here with nonroot ownership so a fresh named
# volume inherits it. Otherwise Docker creates the mountpoint owned by root and
# the non-root process cannot write the SQLite DB or the bbolt cache.
COPY --from=build --chown=65532:65532 /out/home/.s3sync /home/nonroot/.s3sync

# Go resolves os.UserHomeDir() from $HOME alone on Unix, so this must be set
# explicitly or the config directory resolves to a relative, unwritable path.
ENV HOME=/home/nonroot

VOLUME /home/nonroot/.s3sync
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["/s3sync", "health"]
ENTRYPOINT ["/s3sync"]
CMD ["serve", "--port", "8080"]
