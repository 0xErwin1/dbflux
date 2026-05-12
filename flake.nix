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
    let
      releaseInfo = import ./nix/release-info.nix;

      # Systems that ship a prebuilt binary in the matching GitHub Release.
      # Other systems can still use the source build.
      prebuiltSystems = builtins.attrNames releaseInfo.artifacts;

      # Per-system outputs (packages, devShells, apps).
      perSystem = flake-utils.lib.eachDefaultSystem (
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
            version = "0.5.0-dev.3";
          };

          # Source build (current behavior, compiles locally via crane).
          dbfluxSource = dbflux.buildWithCrane craneLib;

          # Prebuilt-binary build, only when an artifact exists for this system.
          hasPrebuilt = builtins.elem system prebuiltSystems;
          dbfluxBin =
            if hasPrebuilt then
              pkgs.callPackage ./nix/binary.nix { }
            else
              null;

          # Default package: prefer the prebuilt binary when available
          # (fast install for end users), fall back to the source build.
          dbfluxDefault = if hasPrebuilt then dbfluxBin else dbfluxSource;
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

          # Packages:
          #   .default       -> prebuilt when available, source otherwise
          #   .dbflux        -> alias for .default
          #   .dbflux-bin    -> explicit prebuilt (only on supported systems)
          #   .dbflux-source -> explicit source build
          packages = {
            default = dbfluxDefault;
            dbflux = dbfluxDefault;
            dbflux-source = dbfluxSource;
          } // (if hasPrebuilt then { dbflux-bin = dbfluxBin; } else { });

          formatter = pkgs.nixpkgs-fmt;

          # Apps
          apps.default = flake-utils.lib.mkApp {
            drv = dbfluxDefault;
            exePath = "/bin/dbflux";
          };

          apps.dbflux = flake-utils.lib.mkApp {
            drv = dbfluxDefault;
            exePath = "/bin/dbflux";
          };
        }
      );
    in
    perSystem // {
      # Overlay for downstream consumers:
      #
      #   nixpkgs.overlays = [ inputs.dbflux.overlays.default ];
      #   environment.systemPackages = [ pkgs.dbflux ];
      #
      # `pkgs.dbflux`        -> prebuilt binary (fast)
      # `pkgs.dbflux-source` -> built from source via crane
      overlays.default = final: prev:
        let
          system = prev.stdenv.hostPlatform.system;
          hasSystem = perSystem.packages ? ${system};
        in
        if hasSystem then
          {
            dbflux = perSystem.packages.${system}.dbflux;
            dbflux-source = perSystem.packages.${system}.dbflux-source;
          }
        else
          { };
    };
}
