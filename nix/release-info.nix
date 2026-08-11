{
  version = "0.7.3";

  # SHA256 SRI hashes of each prebuilt artifact published in the matching
  # GitHub Release. This file is a per-branch channel pointer: on `main` it
  # tracks the newest published rc or stable tag; on a release/vX.Y branch it
  # tracks that line's newest tag (-rc.N, then vX.Y.0, then patches). The
  # rolling nightly channel is separate (see nix/nightly-info.nix). See
  # docs/RELEASE.md.
  #
  # To refresh after a new release:
  #
  #   ver=X.Y.Z[-rc.N]
  #   for arch in amd64 arm64; do
  #     curl -fsSL -o /tmp/dbflux-$arch.tar.gz \
  #       "https://github.com/0xErwin1/dbflux/releases/download/v$ver/dbflux-linux-$arch.tar.gz"
  #     nix-hash --to-sri --type sha256 \
  #       "$(sha256sum /tmp/dbflux-$arch.tar.gz | cut -d' ' -f1)"
  #   done
  #
  # Then update `version`, the two `url`s, and the two `hash`es below.
  artifacts = {
    "x86_64-linux" = {
      url = "https://github.com/0xErwin1/dbflux/releases/download/v0.7.3/dbflux-linux-amd64.tar.gz";
      hash = "sha256-sYwcfG0QtNwc3Di/HQefj4XSKtJfgc4RKaRY5QDeLvs=";
    };
    "aarch64-linux" = {
      url = "https://github.com/0xErwin1/dbflux/releases/download/v0.7.3/dbflux-linux-arm64.tar.gz";
      hash = "sha256-BDPG/VGpA+PaaPj6OgGsP1NqAUO+2AAniihLAZ3r6cQ=";
    };
  };
}
