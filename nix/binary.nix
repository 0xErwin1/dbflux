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
  # Which release-info file to read. Defaults to the stable channel pointer so
  # that callers that pass no argument (i.e. dbflux-bin) are byte-for-byte
  # equivalent to the previous behaviour. Pass ./nightly-info.nix to get the
  # rolling nightly package instead.
  infoFile ? ./release-info.nix,
}:

let
  releaseInfo = import infoFile;
  system = stdenv.hostPlatform.system;
  artifact = releaseInfo.artifacts.${system} or (throw
    "dbflux: no prebuilt binary published for system '${system}'. "
    + "Use the source build (packages.dbflux-source) instead.");

  # Per-channel install identity. Mirrors the runtime `ReleaseChannel` and the
  # deb/rpm packaging: nightly installs under distinct binary/desktop/icon names
  # so a stable and a nightly package can coexist in one profile (e.g. a
  # home-manager `buildEnv`) without colliding on `bin/dbflux`.
  isNightly = lib.hasInfix "nightly" releaseInfo.version;
  appId = if isNightly then "dbflux-nightly" else "dbflux";
  appName = if isNightly then "DBFlux Nightly" else "DBFlux";
  brandDir = if isNightly then "nightly" else "stable";
in
stdenv.mkDerivation {
  pname = appId;
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

    install -Dm755 dbflux $out/bin/${appId}

    # Desktop entry. This derivation installs a published release tarball whose
    # resource layout is fixed at release time, so it must tolerate both the
    # pre- and post-branding-split layouts. @EXEC_PATH@ is present in every
    # tarball; the @APP_*@ identity placeholders only exist in newer ones, so
    # resolve them with sed (a no-op when absent). Names are channel-scoped so a
    # stable and a nightly package can coexist in one profile.
    install -Dm644 resources/desktop/dbflux.desktop \
      $out/share/applications/${appId}.desktop
    substituteInPlace $out/share/applications/${appId}.desktop \
      --replace-fail '@EXEC_PATH@' "$out/bin/${appId}"
    sed -i -e 's/@APP_NAME@/${appName}/g' -e 's/@APP_ID@/${appId}/g' \
      $out/share/applications/${appId}.desktop

    # Brand mark. The mark layout inside a published tarball varies by release:
    # the channel packaging writes the active channel's mark to
    # resources/branding/stable/mark.svg (the channel dir name is not preserved),
    # and tarballs from before the branding split shipped resources/icons/dbflux.svg.
    # Probe each known location and install the first that exists so the build
    # never breaks on a layout this derivation did not author.
    icon_dst=$out/share/icons/hicolor/scalable/apps/${appId}.svg
    if [ -f resources/branding/${brandDir}/mark.svg ]; then
      install -Dm644 resources/branding/${brandDir}/mark.svg "$icon_dst"
    elif [ -f resources/branding/stable/mark.svg ]; then
      install -Dm644 resources/branding/stable/mark.svg "$icon_dst"
    elif [ -f resources/icons/dbflux.svg ]; then
      install -Dm644 resources/icons/dbflux.svg "$icon_dst"
    else
      echo "dbflux: no brand mark found in the release tarball" >&2
      exit 1
    fi

    install -Dm644 resources/mime/dbflux-sql.xml \
      $out/share/mime/packages/${appId}-sql.xml
    sed -i 's/@APP_ID@/${appId}/g' $out/share/mime/packages/${appId}-sql.xml

    install -Dm644 LICENSE-MIT    $out/share/licenses/${appId}/LICENSE-MIT
    install -Dm644 LICENSE-APACHE $out/share/licenses/${appId}/LICENSE-APACHE

    runHook postInstall
  '';

  # Wayland/Vulkan/X11 libraries are dlopened at runtime; autoPatchelf
  # cannot detect these because they are not in DT_NEEDED. Inject them
  # via LD_LIBRARY_PATH on the wrapper.
  postFixup = ''
    wrapProgram $out/bin/${appId} \
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
    mainProgram = appId;
    platforms = builtins.attrNames releaseInfo.artifacts;
    sourceProvenance = with sourceTypes; [ binaryNativeCode ];
  };
}
