#!/usr/bin/env bash
set -euo pipefail

print_usage() {
  echo 'Usage: appimage-libraries.sh policy NAME | glibc ELF | bundle APPDIR | verify APPDIR' >&2
}

validate_arguments() {
  if [[ "$#" != 2 ]]; then
    echo 'Expected a mode and one argument' >&2
    print_usage
    return 2
  fi
  case "$1" in
    policy | glibc | bundle | verify) ;;
    *)
      echo "Unknown mode: $1" >&2
      print_usage
      return 2
      ;;
  esac
  if [[ -z "$2" ]]; then
    echo 'Argument must not be empty' >&2
    print_usage
    return 2
  fi
}

library_policy() {
  case "$1" in
    ld-linux*.so.* | libc.so.* | libm.so.* | libdl.so.* | libpthread.so.* | librt.so.* | libresolv.so.* | libutil.so.* | libanl.so.* | libnss_*.so.* | libBrokenLocale.so.* | libthread_db.so.*)
      echo host
      ;;
    libGL*.so.* | libEGL*.so.* | libOpenGL.so.* | libvulkan*.so.* | libdrm*.so.* | libgbm.so.* | libnvidia*.so.*)
      echo host
      ;;
    *) echo bundle ;;
  esac
}

# ldd reports the transitive DT_NEEDED closure. Only inspect our trusted build.
list_dependencies() {
  local binary_path="$1" dependency_output
  if ! dependency_output=$(ldd "$binary_path"); then
    echo "Cannot inspect dependencies: $binary_path" >&2
    return 1
  fi
  if [[ "$dependency_output" == *"not found"* ]]; then
    echo "Unresolved dependencies: $binary_path" >&2
    printf '%s\n' "$dependency_output" >&2
    return 1
  fi
  printf '%s\n' "$dependency_output" | awk '/=> \// {print $1, $3}'
}

check_glibc_requirements() {
  local binary_path="$1" version_information glibc_version
  if ! version_information=$(readelf --version-info "$binary_path"); then
    echo "Cannot inspect GLIBC requirements: $binary_path" >&2
    return 1
  fi
  while read -r glibc_version; do
    [[ -n "$glibc_version" ]] || continue
    if [[ $(printf '%s\n' 2.35 "$glibc_version" | sort -V | tail -1) != 2.35 ]]; then
      echo "GLIBC_$glibc_version exceeds 2.35: $binary_path" >&2
      return 1
    fi
  done < <(printf '%s\n' "$version_information" | grep -oE 'GLIBC_[0-9]+\.[0-9]+' | cut -d_ -f2 | sort -u)
}

require_file() {
  if [[ ! -f "$1" ]]; then
    echo "Required file not found: $1" >&2
    return 1
  fi
}

write_launcher() {
  local app_directory="$1"
  cat >"$app_directory/AppRun" <<'LAUNCHER'
#!/bin/sh
APPDIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
export LD_LIBRARY_PATH="$APPDIR/usr/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
exec "$APPDIR/usr/bin/dbflux" "$@"
LAUNCHER
  chmod +x "$app_directory/AppRun"
}

bundle_libraries() {
  local app_directory fontconfig_path seed_path dependency_closure library_name library_path
  app_directory=$(realpath "$1")
  require_file "$app_directory/usr/bin/dbflux"
  mkdir -p "$app_directory/usr/lib"
  # fontconfig is dlopen'ed, so it is not necessarily in DT_NEEDED.
  fontconfig_path=$(ldconfig -p | awk '/libfontconfig.so.1 / {print $NF; exit}')
  if [[ ! -f "$fontconfig_path" ]]; then
    echo 'Missing runtime fontconfig' >&2
    return 1
  fi
  cp -L "$fontconfig_path" "$app_directory/usr/lib/libfontconfig.so.1"
  for seed_path in "$app_directory/usr/bin/dbflux" "$fontconfig_path"; do
    dependency_closure=$(list_dependencies "$seed_path")
    while read -r library_name library_path; do
      [[ -n "$library_name" ]] || continue
      if [[ $(library_policy "$library_name") == bundle ]]; then
        cp -L "$library_path" "$app_directory/usr/lib/$library_name"
      fi
    done <<<"$dependency_closure"
  done
  write_launcher "$app_directory"
}

verify_libraries() {
  local app_directory binary_path dependency_closure library_name library_path
  app_directory=$(realpath "$1")
  require_file "$app_directory/usr/bin/dbflux"
  require_file "$app_directory/usr/lib/libfontconfig.so.1"
  # Reject libc before setting the search path: even validation tools
  # would otherwise load the forbidden copy.
  for binary_path in "$app_directory"/usr/lib/*; do
    if [[ $(library_policy "${binary_path##*/}") == host ]]; then
      echo "Forbidden bundled system/GPU library: $binary_path" >&2
      return 1
    fi
  done
  export LD_LIBRARY_PATH="$app_directory/usr/lib"
  for binary_path in "$app_directory/usr/bin/dbflux" "$app_directory"/usr/lib/*; do
    check_glibc_requirements "$binary_path"
    dependency_closure=$(list_dependencies "$binary_path")
    while read -r library_name library_path; do
      [[ -n "$library_name" ]] || continue
      if [[ $(library_policy "$library_name") == bundle && "$library_path" != "$app_directory/usr/lib/"* ]]; then
        echo "Unbundled dependency: $library_name ($library_path)" >&2
        return 1
      fi
    done <<<"$dependency_closure"
  done
}

validate_arguments "$@"
case "$1" in
  policy) library_policy "$2" ;;
  glibc) check_glibc_requirements "$2" ;;
  bundle) bundle_libraries "$2" ;;
  verify) verify_libraries "$2" ;;
esac
