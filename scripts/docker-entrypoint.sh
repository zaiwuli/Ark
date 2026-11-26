#!/bin/sh
set -eu

# Fast path for healthcheck invocation.
if [ "${1:-}" = "--healthcheck" ]; then
  exec /usr/local/bin/ark "$@"
fi

DATA_DIR="${ARK_DATA_DIR:-/config}"
WORK_DIR="${ARK_WORK_DIR:-/data}"
UMASK_VAL="${UMASK:-022}"
umask "$UMASK_VAL"

# If container was explicitly started as a non-root user (e.g. compose `user:`), run directly.
if [ "$(id -u)" -ne 0 ]; then
  exec /usr/local/bin/ark "$@"
fi

# Support standard NAS PUID/PGID privilege drop without recursively chowning large media trees.
if [ -n "${PUID:-}" ] || [ -n "${PGID:-}" ]; then
  TARGET_UID="${PUID:-0}"
  TARGET_GID="${PGID:-0}"
  mkdir -p "$DATA_DIR"
  chown -R "${TARGET_UID}:${TARGET_GID}" "$DATA_DIR" 2>/dev/null || true
  if [ -n "$WORK_DIR" ]; then
    mkdir -p "$WORK_DIR" \
      "$WORK_DIR/本地监控" \
      "$WORK_DIR/解压输出" \
      "$WORK_DIR/归档区" \
      "$WORK_DIR/归档区/原包留存" \
      "$WORK_DIR/归档区/下载缓存" \
      "$WORK_DIR/归档区/历史归档"
    chown "${TARGET_UID}:${TARGET_GID}" \
      "$WORK_DIR" \
      "$WORK_DIR/本地监控" \
      "$WORK_DIR/解压输出" \
      "$WORK_DIR/归档区" \
      "$WORK_DIR/归档区/原包留存" \
      "$WORK_DIR/归档区/下载缓存" \
      "$WORK_DIR/归档区/历史归档" 2>/dev/null || true
  fi
  exec su-exec "${TARGET_UID}:${TARGET_GID}" /usr/local/bin/ark "$@"
fi

# Default: run directly as root so UGREEN/Synology/fnOS bind mounts work out of the box.
exec /usr/local/bin/ark "$@"
