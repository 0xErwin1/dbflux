{
  description = "DBFlux - A fast, keyboard-first database client";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crane.url = "github:ipetkov/crane";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      nixpkgs,
      rust-overlay,
      crane,
      flake-utils,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ (import rust-overlay) ];
        };

        rustToolchain = pkgs.pkgsBuildHost.rust-bin.stable.latest.default.override {
          extensions = [
            "rust-src"
            "rust-analyzer"
          ];
        };

        craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;

        # OpenSSL built with static libraries for portable binaries.
        # The default nixpkgs openssl only ships shared objects; this override
        # enables the static output so OPENSSL_STATIC=1 works at build time.
        opensslStatic = pkgs.openssl.override { static = true; };

        # Import default.nix with crane support
        dbflux = import ./default.nix {
          inherit pkgs craneLib;
          version = "0.4.2";
        };

        # Main package built with crane
        dbfluxPackage = dbflux.buildWithCrane craneLib;

      in
      {
        # Development shell
        devShells.default = pkgs.mkShell {
          nativeBuildInputs = dbflux.nativeBuildInputs ++ [
            rustToolchain
            pkgs.rust-analyzer
            opensslStatic.dev
          ];

          buildInputs = dbflux.buildInputs;

          LD_LIBRARY_PATH = dbflux.runtimeLibraryPath;
          ZSTD_SYS_USE_PKG_CONFIG = "1";

          # Link OpenSSL statically so the binary runs outside the Nix store
          # (e.g. on Arch Linux without /nix/store available at runtime).
          OPENSSL_STATIC = "1";
          OPENSSL_LIB_DIR = "${opensslStatic.out}/lib";
          OPENSSL_INCLUDE_DIR = "${opensslStatic.dev}/include";

          shellHook = ''
            echo "DBFlux development environment loaded (Nix flake)"
            echo "Run 'cargo build' to build the project"
            echo "Run 'nix build' to build the default package"
            echo "Run 'nix flake check' to run all checks"
          '';
        };

        # Packages
        packages.default = dbfluxPackage;
        packages.dbflux = dbfluxPackage;

        # Formatter
        formatter = pkgs.nixpkgs-fmt;

        # Apps
        apps.default = flake-utils.lib.mkApp {
          drv = dbfluxPackage;
          exePath = "/bin/dbflux";
        };

        apps.dbflux = flake-utils.lib.mkApp {
          drv = dbfluxPackage;
          exePath = "/bin/dbflux";
        };
      }
    );
}
