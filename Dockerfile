# syntax=docker/dockerfile:1.7

ARG ZIG_VERSION=0.16.0

FROM ghcr.io/ziglang/zig:${ZIG_VERSION} AS build

ARG TARGETPLATFORM

WORKDIR /src

COPY build.zig ./
COPY src ./src

RUN --mount=type=cache,target=/root/.cache/zig \
    --mount=type=cache,target=/src/.zig-cache \
    case "$TARGETPLATFORM" in \
      "linux/amd64") zig_target="x86_64-linux-musl" ;; \
      "linux/arm64") zig_target="aarch64-linux-musl" ;; \
      *) echo "Unsupported Docker target platform: $TARGETPLATFORM" >&2; exit 1 ;; \
    esac && \
    zig build \
      -Doptimize=ReleaseFast \
      -Dtarget="$zig_target" \
      -p /out \
      --cache-dir /src/.zig-cache

FROM alpine:3.22 AS runtime

RUN apk add --no-cache ca-certificates && \
    addgroup -S indexer && \
    adduser -S -D -H -h /nonexistent -s /sbin/nologin -G indexer indexer

WORKDIR /app

COPY --from=build --chown=indexer:indexer /out/bin/raw /app/raw

USER indexer:indexer

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD pidof raw >/dev/null || exit 1

ENTRYPOINT ["/app/raw"]
