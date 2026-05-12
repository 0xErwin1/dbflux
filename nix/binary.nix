# Prebuilt-binary derivation for DBFlux.
#
# Pulls the matching tarball from the GitHub Release named by
# ./release-info.nix and installs it into a Nix store path. Avoids
# compiling from source on the consumer machine.
#
# Supported systems: x86_64-linux, aarch64-linux.
# Other systems should fall back to the source build (see flake.nix).

{
  lib,
  stdenv,
  fetchurl,
  autoPatchelfHook,
  makeWrapper,
  # Runtime libraries — must mirror the set used by default.nix so the
  # patched binary finds the same .so files at runtime.
  openssl,
  sqlite,
  zstd,
  zlib,
  fontconfig,
  freetype,
  wayland,
  libxkbcommon,
  libx11,
  libxcb,
  libxcursor,
  libxrandr,
  libxi,
  vulkan-loader,
  alsa-lib,
  libgit2,
  curl,
  libsecret,
  libssh2,
  dbus,
  stdenv_cc_libs ? stdenv.cc.cc.lib,
}:

let
  releaseInfo = import ./release-info.nix;
  system = stdenv.hostPlatform.system;
  artifact = releaseInfo.artifacts.${system} or (throw
    "dbflux: no prebuilt binary published for system '${system}'. "
    + "Use the source build (packages.dbflux-source) instead.");
in
stdenv.mkDerivation {
  pname = "dbflux";
  version = releaseInfo.version;

  src = fetchurl {
    inherit (artifact) url hash;
  };

  # The tarball expands directly into the working directory (no leading
  # top-level folder), so unpack manually.
  sourceRoot = ".";
  unpackPhase = ''
    runHook preUnpack
    mkdir -p source
    tar -xzf $src -C source
    runHook postUnpack
  '';

  nativeBuildInputs = [
    autoPatchelfHook
    makeWrapper
  ];

  # Libraries the binary links against (autoPatchelfHook resolves these
  # via DT_NEEDED) plus the ones it dlopens at runtime (we add those to
  # LD_LIBRARY_PATH via the wrapper below).
  buildInputs = [
    stdenv_cc_libs
    openssl
    sqlite
    zstd
    zlib
    fontconfig
    freetype
    wayland
    libxkbcommon
    libx11
    libxcb
    libxcursor
    libxrandr
    libxi
    vulkan-loader
    alsa-lib
    libgit2
    curl
    libsecret
    libssh2
    dbus
  ];

  dontBuild = true;
  dontStrip = true;

  installPhase = ''
    runHook preInstall

    cd source

    install -Dm755 dbflux $out/bin/dbflux

    # Desktop entry — substitute the placeholder used by the release tarball.
    install -Dm644 resources/desktop/dbflux.desktop \
      $out/share/applications/dbflux.desktop
    substituteInPlace $out/share/applications/dbflux.desktop \
      --replace-fail '@EXEC_PATH@' "$out/bin/dbflux"

    install -Dm644 resources/icons/dbflux.svg \
      $out/share/icons/hicolor/scalable/apps/dbflux.svg
    install -Dm644 resources/mime/dbflux-sql.xml \
      $out/share/mime/packages/dbflux-sql.xml

    install -Dm644 LICENSE-MIT    $out/share/licenses/dbflux/LICENSE-MIT
    install -Dm644 LICENSE-APACHE $out/share/licenses/dbflux/LICENSE-APACHE

    runHook postInstall
  '';

  # Wayland/Vulkan/X11 libraries are dlopened at runtime; autoPatchelf
  # cannot detect these because they are not in DT_NEEDED. Inject them
  # via LD_LIBRARY_PATH on the wrapper.
  postFixup = ''
    wrapProgram $out/bin/dbflux \
      --prefix LD_LIBRARY_PATH : "${lib.makeLibraryPath [
        wayland
        libxkbcommon
        vulkan-loader
        libx11
        libxcb
        libxcursor
        libxrandr
        libxi
        libsecret
        dbus
      ]}"
  '';

  meta = with lib; {
    description = "A fast, keyboard-first database client (prebuilt binary)";
    homepage = "https://github.com/0xErwin1/dbflux";
    license = with licenses; [ mit asl20 ];
    mainProgram = "dbflux";
    platforms = builtins.attrNames releaseInfo.artifacts;
    sourceProvenance = with sourceTypes; [ binaryNativeCode ];
  };
}
