# Rolling nightly channel pointer.
#
# THIS FILE IS AUTO-UPDATED by .github/workflows/nightly.yml on the `nightly`
# git ref immediately before the nightly tag is force-moved.  On `main` it is
# only a seed — the hashes below are placeholders and the file will NOT fetch
# successfully from main.
#
# Consume nightly via the pinned ref:
#
#   nix run github:0xErwin1/dbflux/nightly#dbflux-nightly
#
# Do NOT pin to main for this package. The `nightly` ref always contains
# hashes that match the artifacts published in the rolling nightly release.
#
# Each `url` names a per-build immutable asset
# (dbflux-linux-<arch>-<date>-<sha>.tar.gz) that the workflow keeps for the 14
# most recent builds per platform, so a pin of an older `nightly` commit keeps
# working for that long. The rolling dbflux-linux-<arch>.tar.gz assets are for
# humans and are never pinned.
#
# From-source nightly (no hash needed):
#
#   nix run github:0xErwin1/dbflux/nightly#dbflux-source
{
  version = "nightly";

  artifacts = {
    "x86_64-linux" = {
      url = "https://github.com/0xErwin1/dbflux/releases/download/nightly/dbflux-linux-amd64-0000-00-00-0000000.tar.gz";
      hash = "sha256-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
    };
    "aarch64-linux" = {
      url = "https://github.com/0xErwin1/dbflux/releases/download/nightly/dbflux-linux-arm64-0000-00-00-0000000.tar.gz";
      hash = "sha256-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
    };
  };
}
