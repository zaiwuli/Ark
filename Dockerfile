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

RUN apk add --no-cache ca-certificates tzdata su-exec xz wget

COPY scripts/install_7zip.sh /tmp/install_7zip.sh
RUN sed -i 's/\r$//' /tmp/install_7zip.sh \
    && chmod +x /tmp/install_7zip.sh \
    && /tmp/install_7zip.sh "$TARGETARCH" \
    && rm -f /tmp/install_7zip.sh \
    && apk del xz wget

WORKDIR /app
COPY --from=builder /out/ark /usr/local/bin/ark
COPY scripts/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
COPY THIRD_PARTY_NOTICES.md /app/THIRD_PARTY_NOTICES.md

RUN sed -i 's/\r$//' /usr/local/bin/docker-entrypoint.sh \
    && chmod +x /usr/local/bin/docker-entrypoint.sh \
    && mkdir -p /config /data

ENV TZ=Asia/Shanghai \
    UMASK=022 \
    ARK_LISTEN_ADDR=:5656 \
    ARK_DATA_DIR=/config \
    ARK_WORK_DIR=/data \
    ARK_EXTRACT_BIN=/usr/local/bin/7zz \
    ARK_MEM_LIMIT_MB=48 \
    GOMEMLIMIT=48MiB \
    GOGC=50

EXPOSE 5656

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s \
  CMD ["/usr/local/bin/ark", "--healthcheck"]

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
