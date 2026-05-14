{
  version = "0.6.0-dev.2";

  # SHA256 SRI hashes of each prebuilt artifact published in the matching
  # GitHub Release. Stable (vX.Y.Z) and -dev.N prereleases are reflected
  # here; -rc.N release-branch prereleases are not.
  #
  # To refresh after a new release:
  #
  #   ver=X.Y.Z[-dev.N]
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
      url = "https://github.com/0xErwin1/dbflux/releases/download/v0.6.0-dev.2/dbflux-linux-amd64.tar.gz";
      hash = "sha256-SzMfGyk0lb8F0+x6pez7orVKcYLXonq4r6M9T5YmmrQ=";
    };
    "aarch64-linux" = {
      url = "https://github.com/0xErwin1/dbflux/releases/download/v0.6.0-dev.2/dbflux-linux-arm64.tar.gz";
      hash = "sha256-+Pf3m5Fl0J1dWk0kzxlMMaQsQBKwuoW0G/+m/iEcPbQ=";
    };
  };
}
