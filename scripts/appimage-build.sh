#!/usr/bin/env bash

# Run inside the native-architecture Ubuntu 22.04 container, never on the host.
set -euo pipefail

if [[ "$#" != 1 ]]; then
  echo 'Usage: appimage-build.sh TARGET' >&2
  exit 2
fi
case "$1" in
  x86_64-unknown-linux-gnu | aarch64-unknown-linux-gnu) build_target="$1" ;;
  *) echo "Unsupported build target: $1" >&2; exit 2 ;;
esac
: "${CARGO_HOME:?CARGO_HOME must identify the container Rust installation}"
: "${FEATURES:?FEATURES must identify the release build features}"

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y --no-install-recommends \
  build-essential ca-certificates curl git pkg-config mold cmake \
  libssl-dev libdbus-1-dev libxkbcommon-dev libxkbcommon-x11-dev \
  libfontconfig1 libfreetype6-dev

# Jammy supplies OpenSSL 3, xkbcommon-x11 and mold on both supported architectures.
# Use the repository-pinned Rust toolchain, with a distinct Jammy artifact cache.
export PATH="${CARGO_HOME}/bin:$PATH"
rustup target add "$build_target"
cargo build --release --features "$FEATURES" --target "$build_target"
mkdir -p AppDir/usr/bin
cp "target/$build_target/release/dbflux" AppDir/usr/bin/dbflux
bash scripts/appimage-libraries.sh bundle AppDir

# Retain Jammy package copyright/license notices for redistributed libraries.
for copyright_notice in /usr/share/doc/*/copyright; do
  package_directory=${copyright_notice%/copyright}
  license_directory="AppDir/usr/share/licenses/system/${package_directory##*/}"
  mkdir -p "$license_directory"
  cp -L "$copyright_notice" "$license_directory/copyright"
done

bash scripts/appimage-libraries.sh verify AppDir
