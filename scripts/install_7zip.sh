#!/bin/sh
set -eu

ARCH="${1:-amd64}"
VER="2601"

case "$ARCH" in
  amd64|x86_64)
    URL="https://www.7-zip.org/a/7z${VER}-linux-x64.tar.xz"
    SHA256="8ea0fc8a135e7b848e80a4116fe22dff56c8c4518dde1f43cce67f4e340b437a"
    ;;
  arm64|aarch64)
    URL="https://www.7-zip.org/a/7z${VER}-linux-arm64.tar.xz"
    SHA256="39f8c9070c300a63c7484d9a983119ef3edf841e1ddf69f1affae29fdec5f612"
    ;;
  *)
    echo "Unsupported TARGETARCH: $ARCH" >&2
    exit 1
    ;;
esac

TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

wget -qO "$TMPDIR/7z.tar.xz" "$URL"
echo "${SHA256}  $TMPDIR/7z.tar.xz" | sha256sum -c -
tar -xJf "$TMPDIR/7z.tar.xz" -C "$TMPDIR"

install -m 0755 "$TMPDIR/7zz" /usr/local/bin/7zz
mkdir -p /usr/local/share/licenses/7zip
if [ -f "$TMPDIR/License.txt" ]; then
  install -m 0644 "$TMPDIR/License.txt" /usr/local/share/licenses/7zip/License.txt
fi

/usr/local/bin/7zz i | head -n 5
