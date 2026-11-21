#!/usr/bin/env python3
"""Update an existing AUR PKGBUILD from published stable tarballs and checksums."""
import argparse
import hashlib
import pathlib
import re
import sys

from homebrew_cask import RenderError, read_checksum

VERSION = r'(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)'


def replace_field(text, field, pattern, replacement):
    updated, count = re.subn(r'^' + field + '=' + pattern + r'(?=\n|\Z)',
                             field + '=' + replacement, text, flags=re.MULTILINE)
    if count != 1:
        raise RenderError(f'unsupported PKGBUILD field: {field}')
    return updated


def update(tag, assets, recipe):
    if not re.fullmatch('v' + VERSION, tag):
        raise RenderError('expected canonical stable tag vX.Y.Z')
    text = recipe.read_text(encoding='utf-8')
    current = re.findall(r'^pkgver=(' + VERSION + r')$', text, re.MULTILINE)
    if len(current) != 1:
        raise RenderError('unsupported PKGBUILD field: pkgver')
    version = tag[1:]
    if tuple(map(int, current[0].split('.'))) >= tuple(map(int, version.split('.'))):
        return False
    updated = replace_field(text, 'pkgver', VERSION, version)
    updated = replace_field(updated, 'pkgrel', r'[0-9]+', '1')
    for architecture, asset_architecture in (('x86_64', 'amd64'), ('aarch64', 'arm64')):
        asset = f'dbflux-linux-{asset_architecture}.tar.gz'
        try:
            digest = read_checksum(assets, asset)
            with (assets / asset).open('rb') as stream:
                actual = hashlib.file_digest(stream, 'sha256').hexdigest()
            if digest != actual:
                raise RenderError('digest mismatch')
        except (OSError, RenderError):
            raise RenderError(f'invalid published checksum or artifact: {asset}') from None
        # Only replace the archive digest; detached signatures and signer policy stay intact.
        updated = replace_field(updated, 'sha256sums_' + architecture,
                                r"\(\s*'[0-9a-zA-Z]+'\s+'SKIP'\s*\)",
                                f"('{digest}' 'SKIP')")
    recipe.write_text(updated, encoding='utf-8')
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tag', required=True, help='canonical stable tag vX.Y.Z')
    parser.add_argument('--assets', required=True, type=pathlib.Path,
                        help='published Linux tarballs and their .sha256 files')
    parser.add_argument('--pkgbuild', required=True, type=pathlib.Path,
                        help='existing AUR recipe; versions at or above the tag are unchanged')
    args = parser.parse_args()
    try:
        changed = update(args.tag, args.assets, args.pkgbuild)
    except (OSError, RenderError) as error:
        print(f'error: {error}', file=sys.stderr)
        return 1
    print('updated' if changed else 'unchanged')
    return 0


if __name__ == '__main__':
    sys.exit(main())
