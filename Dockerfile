FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o s3sync .

FROM scratch
COPY --from=build /src/s3sync /s3sync
VOLUME /root/.s3sync
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["/s3sync", "health"]
ENTRYPOINT ["/s3sync"]
CMD ["serve", "--port", "8080"]
