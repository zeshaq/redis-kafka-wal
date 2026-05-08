# Build stage shared by producer + consumer; we expose two final stages so
# docker-compose can pick one with `target:`. go.sum is generated during
# build (go mod tidy) so the repo can be cloned without a host Go install.
FROM golang:1.23-alpine AS build
WORKDIR /src
RUN apk add --no-cache git
COPY . .
RUN go mod tidy
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/consumer ./cmd/consumer
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/producer ./cmd/producer
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/bridge   ./cmd/bridge

FROM alpine:3.20 AS consumer
RUN apk add --no-cache ca-certificates
COPY --from=build /out/consumer /usr/local/bin/consumer
ENTRYPOINT ["/usr/local/bin/consumer"]

FROM alpine:3.20 AS producer
RUN apk add --no-cache ca-certificates
COPY --from=build /out/producer /usr/local/bin/producer
ENTRYPOINT ["/usr/local/bin/producer"]

FROM alpine:3.20 AS bridge
RUN apk add --no-cache ca-certificates
COPY --from=build /out/bridge /usr/local/bin/bridge
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/bridge"]
