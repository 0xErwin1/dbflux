#!/usr/bin/env bash
# Fresh Jammy container: no development packages to mask missing bundled libraries.
set -euo pipefail

if [[ "$#" != 1 || -z "${1:-}" ]]; then
  echo 'Usage: appimage-verify.sh APPIMAGE' >&2
  exit 2
fi
if [[ ! -f "$1" || ! -x "$1" ]]; then
  echo "AppImage must be an executable file: $1" >&2
  exit 2
fi
appimage_path=$(realpath "$1")

isolate_user_directories() {
  local scratch_directory="$1"
  export HOME="$scratch_directory/home"
  export XDG_CONFIG_HOME="$scratch_directory/config" XDG_DATA_HOME="$scratch_directory/data"
  export XDG_STATE_HOME="$scratch_directory/state" XDG_CACHE_HOME="$scratch_directory/cache"
  export XDG_RUNTIME_DIR="$scratch_directory/runtime"
  mkdir -p "$HOME" "$XDG_RUNTIME_DIR"
  chmod 700 "$XDG_RUNTIME_DIR"
}

smoke_test_startup() {
  local startup_exit_status=0
  export WAYLAND_DISPLAY="" GPUI_X11_SCALE_FACTOR=1
  # An early exit is a failure; surviving the bounded smoke window is not a full
  # rendering or database test. Software Vulkan avoids exposing host GPU devices.
  xvfb-run -a timeout 20s ./squashfs-root/AppRun >startup.log 2>&1 || startup_exit_status=$?
  cat startup.log
  if [[ "$startup_exit_status" != 124 ]]; then
    echo "Startup exited early: $startup_exit_status" >&2
    return 1
  fi
}

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y --no-install-recommends binutils xvfb xauth \
  libvulkan1 mesa-vulkan-drivers libgl1 libegl1 fonts-dejavu-core

scratch_directory=$(mktemp -d)
cd "$scratch_directory"
"$appimage_path" --appimage-extract >/dev/null
bash /work/scripts/appimage-libraries.sh glibc "$appimage_path"
bash /work/scripts/appimage-libraries.sh verify squashfs-root
isolate_user_directories "$scratch_directory"
smoke_test_startup
