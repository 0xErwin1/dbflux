#!/usr/bin/env bash
# Print the AppImage update information for a release channel and arch.
#
# The output is the update information string appimagetool embeds via -u.
# Stable resolves through GitHub's `latest` release, nightly through the
# rolling `nightly` tag. RC releases stay manual and print nothing, so the
# caller can skip embedding entirely.
#
# Usage: appimage-update-info.sh <stable|rc|nightly> <x86_64|aarch64>
set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "Usage: $(basename "$0") <stable|rc|nightly> <x86_64|aarch64>" >&2
  exit 2
fi

channel=$1
arch=$2

case "$channel" in
  stable|rc|nightly) ;;
  *)
    echo "Usage: $(basename "$0") <stable|rc|nightly> <x86_64|aarch64>" >&2
    echo "Unknown channel: $channel" >&2
    exit 2
    ;;
esac

case "$arch" in
  x86_64|aarch64) ;;
  *)
    echo "Usage: $(basename "$0") <stable|rc|nightly> <x86_64|aarch64>" >&2
    echo "Unknown arch: $arch" >&2
    exit 2
    ;;
esac

case "$channel" in
  stable)
    echo "gh-releases-zsync|0xErwin1|dbflux|latest|dbflux-${arch}.AppImage.zsync"
    ;;
  nightly)
    echo "gh-releases-zsync|0xErwin1|dbflux|nightly|dbflux-${arch}.AppImage.zsync"
    ;;
  rc)
    # RC releases are updated manually; no external update information.
    ;;
esac
