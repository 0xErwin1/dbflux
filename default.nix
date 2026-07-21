{
  pkgs ? import <nixpkgs> { },
  craneLib ? null,
  version ? "0.4.0-dev.12",
}:

let
  # Per-channel install identity. Mirrors the runtime `ReleaseChannel` and the
  # deb/rpm packaging: a nightly build installs under distinct binary/desktop/
  # icon names so a stable and a nightly package can coexist in one profile
  # (e.g. a home-manager `buildEnv`) without colliding on `bin/dbflux`.
  isNightly = pkgs.lib.hasInfix "nightly" version;
  appId = if isNightly then "dbflux-nightly" else "dbflux";
  appName = if isNightly then "DBFlux Nightly" else "DBFlux";
  brandDir = if isNightly then "nightly" else "stable";

  # Build inputs needed at runtime
  buildInputs = with pkgs; [
    openssl
    sqlite
    zstd
    zlib
    fontconfig
    freetype
    wayland
    libxkbcommon
    xorg.libX11
    xorg.libxcb
    xorg.libXcursor
    xorg.libXrandr
    xorg.libXi
    vulkan-loader
    vulkan-headers
    alsa-lib
    libgit2
    curl
    libsecret
    libssh2
    dbus
    tree-sitter
  ];

  # Build inputs needed only at build time
  nativeBuildInputs = with pkgs; [
    pkg-config
    cmake
    makeWrapper
    openssl.dev
    # Required by .cargo/config.toml, which links the x86_64-unknown-linux-gnu
    # target with -fuse-ld=mold. Needed by every cargo invocation over this tree
    # (package builds and dev shells), so it lives here and is inherited.
    mold
  ];

  # Library path for runtime
  runtimeLibraryPath = pkgs.lib.makeLibraryPath [
    pkgs.wayland
    pkgs.libxkbcommon
    pkgs.vulkan-loader
    pkgs.xorg.libX11
    pkgs.xorg.libxcb
    pkgs.openssl
    pkgs.libgit2
    pkgs.libssh2
    pkgs.dbus
    pkgs.zstd
    pkgs.sqlite
    pkgs.curl
  ];

  # Full source including resources
  fullSrc = pkgs.lib.cleanSourceWith {
    src = ./.;
    filter =
      path: type:
      (builtins.match ".*\\.git$" path) == null
      && (builtins.match ".*flake\\.nix$" path) == null
      && (builtins.match ".*flake\\.lock$" path) == null
      && (builtins.match ".*shell\\.nix$" path) == null
      && (builtins.match ".*default\\.nix$" path) == null
      && (builtins.match ".*\\.envrc$" path) == null
      && (builtins.match ".*\\.direnv$" path) == null
      && (builtins.match ".*target$" path) == null;
  };

  # Post-install script to copy resources
  postInstallScript = ''
    mkdir -p $out/share/applications
    mkdir -p $out/share/icons/hicolor/scalable/apps
    mkdir -p $out/share/mime/packages
    mkdir -p $out/share/dbflux

    # cargo always emits a binary named `dbflux`; rename it for non-stable
    # channels so two channels can coexist in one profile.
    ${pkgs.lib.optionalString isNightly "mv $out/bin/dbflux $out/bin/${appId}"}

    # Copy desktop file and resolve the templated placeholders
    install -Dm644 ${fullSrc}/resources/desktop/dbflux.desktop $out/share/applications/${appId}.desktop
    substituteInPlace $out/share/applications/${appId}.desktop \
      --replace '@EXEC_PATH@' "$out/bin/${appId}" \
      --replace '@APP_NAME@' '${appName}' \
      --replace '@APP_ID@' '${appId}'

    # Copy icon
    install -Dm644 ${fullSrc}/resources/branding/${brandDir}/mark.svg $out/share/icons/hicolor/scalable/apps/${appId}.svg

    # Copy mime type
    install -Dm644 ${fullSrc}/resources/mime/dbflux-sql.xml $out/share/mime/packages/${appId}-sql.xml
    substituteInPlace $out/share/mime/packages/${appId}-sql.xml \
      --replace '@APP_ID@' '${appId}'

    # Copy resources (with proper permissions)
    cp -r --no-preserve=mode ${fullSrc}/resources $out/share/dbflux/
    chmod -R u+w $out/share/dbflux/resources

    # Copy scripts
    mkdir -p $out/share/dbflux/scripts
    cp -r --no-preserve=mode ${fullSrc}/scripts/* $out/share/dbflux/scripts/ 2>/dev/null || true
    chmod -R u+w $out/share/dbflux/scripts 2>/dev/null || true

    # Wrap binary with LD_LIBRARY_PATH for Wayland/Vulkan/X11
    # Include common system paths for GPU drivers on non-NixOS systems
    wrapProgram $out/bin/${appId} \
      --prefix LD_LIBRARY_PATH : "${runtimeLibraryPath}" \
      --suffix LD_LIBRARY_PATH : "/run/opengl-driver/lib:/usr/lib/x86_64-linux-gnu:/usr/lib64:/usr/lib"
  '';

  # Build with crane (for flake usage)
  buildWithCrane =
    craneLib:
    let
      commonArgs = {
        src = fullSrc;
        inherit buildInputs nativeBuildInputs;
        strictDeps = true;
        ZSTD_SYS_USE_PKG_CONFIG = "1";
      };

      cargoArtifacts = craneLib.buildDepsOnly (
        commonArgs
        // {
          pname = "dbflux-deps";
          inherit version;
        }
      );
    in
    craneLib.buildPackage (
      commonArgs
      // {
        pname = appId;
        inherit version cargoArtifacts;
        cargoExtraArgs = "-p dbflux";
        postInstall = postInstallScript;
        meta.mainProgram = appId;
      }
    );

  # Build with rustPlatform (for non-flake usage)
  buildWithRustPlatform = pkgs.rustPlatform.buildRustPackage {
    pname = appId;
    inherit version;
    src = fullSrc;

    cargoLock = {
      lockFile = ./Cargo.lock;
      allowBuiltinFetchGit = true;
    };

    inherit buildInputs nativeBuildInputs;

    ZSTD_SYS_USE_PKG_CONFIG = "1";

    cargoBuildFlags = [
      "-p"
      "dbflux"
    ];
    cargoTestFlags = [
      "-p"
      "dbflux"
    ];

    postInstall = postInstallScript;

    postFixup = ''
      wrapProgram $out/bin/${appId} \
        --prefix LD_LIBRARY_PATH : "${runtimeLibraryPath}" \
        --suffix LD_LIBRARY_PATH : "/run/opengl-driver/lib:/usr/lib/x86_64-linux-gnu:/usr/lib64:/usr/lib"
    '';

    meta = with pkgs.lib; {
      description = "A fast, keyboard-first database client";
      homepage = "https://github.com/0xErwin1/dbflux";
      license = with licenses; [
        mit
        asl20
      ];
      maintainers = [ ];
      platforms = platforms.linux;
      mainProgram = appId;
    };
  };

in
{
  inherit
    buildInputs
    nativeBuildInputs
    runtimeLibraryPath
    fullSrc
    ;
  inherit buildWithCrane buildWithRustPlatform;

  # Default package (non-flake)
  package = buildWithRustPlatform;

  # Development shell
  shell =
    let
      rustToolchainInputs =
        if pkgs ? rust-bin then
          [
            (pkgs.rust-bin.stable.latest.default.override {
              extensions = [
                "rust-src"
                "rust-analyzer"
              ];
            })
          ]
        else
          [
            pkgs.rustc
            pkgs.cargo
          ];
    in
    pkgs.mkShell {
      nativeBuildInputs =
        nativeBuildInputs
        ++ rustToolchainInputs
        ++ [
          pkgs.rust-analyzer
          pkgs.python3
          # Faster, process-isolated test runner: `cargo nextest run`.
          # (mold is already provided via nativeBuildInputs above.)
          pkgs.cargo-nextest
        ];

      inherit buildInputs;

    # Include system GPU driver paths for non-NixOS systems
    LD_LIBRARY_PATH = pkgs.lib.concatStringsSep ":" [
      runtimeLibraryPath
      "/run/opengl-driver/lib"
      "/usr/lib/x86_64-linux-gnu"
      "/usr/lib64"
      "/usr/lib"
    ];
    ZSTD_SYS_USE_PKG_CONFIG = "1";

      shellHook = ''
        echo "DBFlux development environment loaded"
        echo "Run 'cargo build' to build the project"
        echo "Run 'nix-build' to build the package"
      '';
    };
}
