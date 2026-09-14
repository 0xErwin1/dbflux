#!/usr/bin/env bash
# Create the self-signed code-signing certificate the macOS release build uses.
#
# macOS ties a keychain grant to the signature of the application that asked
# for it. Signing every release with one stable identity is what lets the
# saved database passwords survive an update without a new password prompt.
# A self-signed certificate is enough for that; it does not satisfy Gatekeeper.
#
# Runs anywhere OpenSSL is installed. It writes two files next to each other
# and prints the two repository secrets to set. See docs/RELEASE.md.
#
# Usage: scripts/macos-signing-cert.sh [output-dir]

set -euo pipefail

OUT_DIR="${1:-.}"
NAME="DBFlux Release Signing"
DAYS=3650

KEY="$OUT_DIR/dbflux-signing.key"
CERT="$OUT_DIR/dbflux-signing.pem"
P12="$OUT_DIR/dbflux-signing.p12"

if [ -e "$P12" ]; then
  echo "error: $P12 already exists; move it away to issue a new certificate" >&2
  exit 1
fi

mkdir -p "$OUT_DIR"

P12_PASSWORD="$(openssl rand -base64 24)"

openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout "$KEY" -out "$CERT" -days "$DAYS" \
  -subj "/CN=$NAME" \
  -addext "keyUsage=critical,digitalSignature" \
  -addext "extendedKeyUsage=critical,codeSigning" \
  -addext "basicConstraints=critical,CA:FALSE"

# macOS rejects PKCS#12 bundles written with OpenSSL 3's default algorithms
# and reports them as a wrong password. SHA-1 MAC with 3DES is what its
# importer accepts, and both live in OpenSSL's default provider.
openssl pkcs12 -export \
  -inkey "$KEY" -in "$CERT" -name "$NAME" \
  -macalg sha1 -keypbe PBE-SHA1-3DES -certpbe PBE-SHA1-3DES \
  -passout pass:"$P12_PASSWORD" \
  -out "$P12"

rm -f "$KEY"
chmod 600 "$P12"

cat <<EOF

Certificate written to $P12 (valid $DAYS days). Keep it out of the repository.

Set these two repository secrets:

  MACOS_CERTIFICATE_P12       the file, base64-encoded:
                                base64 < "$P12" | tr -d '\n'
  MACOS_CERTIFICATE_PASSWORD  $P12_PASSWORD

Store the .p12 and the password somewhere durable. Issuing a new certificate
later means every user re-authorises the keychain once more.
EOF
