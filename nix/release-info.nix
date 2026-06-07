{
  version = "0.6.0-rc.4";

  # SHA256 SRI hashes of each prebuilt artifact published in the matching
  # GitHub Release. This file is a per-branch channel pointer: on `main` it
  # tracks the newest published tag of any kind (-dev.N, -rc.N, or stable); on
  # a release/vX.Y branch it tracks that line's newest tag (-rc.N, then vX.Y.0,
  # then patches). See docs/RELEASE.md.
  #
  # To refresh after a new release:
  #
  #   ver=X.Y.Z[-dev.N|-rc.N]
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
      url = "https://github.com/0xErwin1/dbflux/releases/download/v0.6.0-rc.4/dbflux-linux-amd64.tar.gz";
      hash = "sha256-WGoF805bXSR0+dFe4SUWBf7RUgJl15y7kwUxo31Hcm0=";
    };
    "aarch64-linux" = {
      url = "https://github.com/0xErwin1/dbflux/releases/download/v0.6.0-rc.4/dbflux-linux-arm64.tar.gz";
      hash = "sha256-yyitlFplEZcZD7FDaD97sR17iDFHZgGKRgYws09CnYg=";
    };
  };
}
