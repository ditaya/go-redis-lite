FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -o /out/go-redis-lite ./cmd/go-redis-lite

FROM alpine:3.20
RUN adduser -D -u 10001 appuser && mkdir /data && chown appuser /data
COPY --from=build /out/go-redis-lite /usr/local/bin/go-redis-lite
USER appuser
WORKDIR /data
EXPOSE 6380
ENTRYPOINT ["go-redis-lite"]
CMD ["-addr", ":6380", "-aof", "/data/go-redis-lite.aof"]
