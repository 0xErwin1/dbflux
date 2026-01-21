{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
  nativeBuildInputs = with pkgs; [
    pkg-config
    cmake
    rustup
    rust-analyzer
  ];

  buildInputs = with pkgs; [
    # OpenSSL for network/TLS
    openssl

    # SQLite
    sqlite

    # Compression
    zstd
    zlib

    # Font rendering
    fontconfig
    freetype

    # Linux display (Wayland + X11)
    wayland
    libxkbcommon
    xorg.libX11
    xorg.libxcb
    xorg.libXcursor
    xorg.libXrandr
    xorg.libXi

    # Vulkan for GPU rendering
    vulkan-loader
    vulkan-headers

    # Audio (GPUI may need this)
    alsa-lib

    # Misc
    libgit2
    curl
  ];

  shellHook = ''
    export LD_LIBRARY_PATH="${pkgs.lib.makeLibraryPath [
      pkgs.wayland
      pkgs.libxkbcommon
      pkgs.vulkan-loader
      pkgs.xorg.libX11
      pkgs.xorg.libxcb
    ]}:$LD_LIBRARY_PATH"

    export ZSTD_SYS_USE_PKG_CONFIG=1

    echo "DBFlux development environment loaded"
    echo "Run 'cargo build' to build the project"
  '';
}
