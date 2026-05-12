{
  version = "0.5.0";

  # SHA256 SRI hashes of each prebuilt artifact published in the matching
  # GitHub Release. Only stable (vX.Y.Z without suffix) releases should be
  # reflected here.
  #
  # To refresh after a new stable release:
  #
  #   ver=X.Y.Z
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
      url = "https://github.com/0xErwin1/dbflux/releases/download/v0.5.0/dbflux-linux-amd64.tar.gz";
      hash = "sha256-peCwhFOQZidmp8QWhZ7HL0dzqTfTFd2DA4+NSXrn1sM=";
    };
    "aarch64-linux" = {
      url = "https://github.com/0xErwin1/dbflux/releases/download/v0.5.0/dbflux-linux-arm64.tar.gz";
      hash = "sha256-xJAoVAh/poYqzVb6O1ih3UyfsGZUb3wT5yj/NiCMICU=";
    };
  };
}
