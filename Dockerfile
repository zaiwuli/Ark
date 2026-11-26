FROM --platform=$BUILDPLATFORM golang:1.24-alpine AS builder
ARG TARGETOS=linux
ARG TARGETARCH=amd64
ARG APP_VERSION=0.1.0
ARG BUILD_COMMIT=local
ARG BUILD_TIME=""

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath \
    -ldflags="-s -w -X github.com/zaiwuli/Ark/internal/config.Version=${APP_VERSION} -X github.com/zaiwuli/Ark/internal/config.Commit=${BUILD_COMMIT} -X github.com/zaiwuli/Ark/internal/config.BuildTime=${BUILD_TIME}" \
    -o /out/ark ./cmd/ark

FROM alpine:3.21 AS runtime
ARG TARGETARCH=amd64

RUN apk add --no-cache ca-certificates tzdata xz wget \
    && addgroup -g 1000 ark \
    && adduser -D -u 1000 -G ark ark

COPY scripts/install_7zip.sh /tmp/install_7zip.sh
RUN chmod +x /tmp/install_7zip.sh \
    && /tmp/install_7zip.sh "$TARGETARCH" \
    && rm -f /tmp/install_7zip.sh \
    && apk del xz wget

WORKDIR /app
COPY --from=builder /out/ark /usr/local/bin/ark
COPY THIRD_PARTY_NOTICES.md /app/THIRD_PARTY_NOTICES.md

RUN mkdir -p /config /data && chown -R 1000:1000 /config /data

ENV TZ=Asia/Shanghai \
    ARK_LISTEN_ADDR=:5656 \
    ARK_DATA_DIR=/config \
    ARK_WORK_DIR=/data \
    ARK_EXTRACT_BIN=/usr/local/bin/7zz \
    ARK_MEM_LIMIT_MB=48 \
    GOMEMLIMIT=48MiB \
    GOGC=50

USER 1000:1000
EXPOSE 5656

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s \
  CMD ["/usr/local/bin/ark", "--healthcheck"]

ENTRYPOINT ["/usr/local/bin/ark"]
