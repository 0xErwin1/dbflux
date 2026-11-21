"""Public CLI checks for AUR release updates."""
import hashlib
import pathlib
import subprocess
import sys
import tempfile
import unittest

SCRIPT = pathlib.Path(__file__).with_name('release_aur.py').resolve()
RECIPE = """pkgname=dbflux
pkgver=0.10.1
pkgrel=3
source_x86_64=("https://example/v${pkgver}/amd64"{,.asc})
source_aarch64=("https://example/v${pkgver}/arm64"{,.asc})
sha256sums_x86_64=('OLD' 'SKIP')
sha256sums_aarch64=('OLD' 'SKIP')
validpgpkeys=('B39EB98E8860DAFB05670073A614B7D25134987A')
package() { echo preserved; }
"""


class AurTests(unittest.TestCase):
    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        self.root = pathlib.Path(self.scratch.name)
        self.recipe = self.root / 'PKGBUILD'
        self.recipe.write_text(RECIPE)
        for architecture in ('amd64', 'arm64'):
            asset = self.root / f'dbflux-linux-{architecture}.tar.gz'
            asset.write_bytes(architecture.encode())
            asset.with_name(asset.name + '.sha256').write_text(
                f'{hashlib.sha256(asset.read_bytes()).hexdigest()}  {asset.name}\n')

    def run_cli(self, tag='v0.11.0'):
        return subprocess.run([sys.executable, str(SCRIPT), '--tag', tag,
                               '--assets', str(self.root), '--pkgbuild', str(self.recipe)],
                              capture_output=True, text=True)

    def test_updates_both_hashes_preserving_signatures_and_package(self):
        result = self.run_cli()
        self.assertEqual((result.returncode, result.stdout, result.stderr), (0, 'updated\n', ''))
        expected = RECIPE.replace('pkgver=0.10.1', 'pkgver=0.11.0').replace('pkgrel=3', 'pkgrel=1')
        for architecture in ('amd64', 'arm64'):
            expected = expected.replace('OLD', hashlib.sha256(architecture.encode()).hexdigest(), 1)
        self.assertEqual(self.recipe.read_text(), expected)
        result = self.run_cli()
        self.assertEqual((result.returncode, result.stdout, result.stderr), (0, 'unchanged\n', ''))
        self.assertEqual(self.recipe.read_text(), expected)

    def test_older_release_does_not_downgrade(self):
        result = self.run_cli('v0.9.9')
        self.assertEqual((result.returncode, result.stdout, result.stderr), (0, 'unchanged\n', ''))
        self.assertEqual(self.recipe.read_text(), RECIPE)

    def test_invalid_tag_is_rejected_without_writes(self):
        for tag in ('v0.11.0-rc.1', '0.11.0', 'v00.11.0', 'v0.11.0\n'):
            result = self.run_cli(tag)
            self.assertEqual((result.returncode, result.stdout, result.stderr),
                             (1, '', 'error: expected canonical stable tag vX.Y.Z\n'))
            self.assertEqual(self.recipe.read_text(), RECIPE)

    def test_bad_checksum_or_asset_leaves_recipe_unchanged(self):
        checksum = self.root / 'dbflux-linux-arm64.tar.gz.sha256'
        for content in ('bad', 'a' * 64 + '  wrong.tar.gz', 'a' * 64 + '  dbflux-linux-arm64.tar.gz'):
            checksum.write_text(content)
            result = self.run_cli()
            self.assertEqual((result.returncode, result.stdout, result.stderr),
                             (1, '', 'error: invalid published checksum or artifact: dbflux-linux-arm64.tar.gz\n'))
            self.assertEqual(self.recipe.read_text(), RECIPE)

    def test_unknown_recipe_shape_fails_closed(self):
        original = RECIPE.replace("'SKIP'", "'unexpected'", 1)
        self.recipe.write_text(original)
        result = self.run_cli()
        self.assertEqual((result.returncode, result.stdout, result.stderr),
                         (1, '', 'error: unsupported PKGBUILD field: sha256sums_x86_64\n'))
        self.assertEqual(self.recipe.read_text(), original)

    def test_equal_version_preserves_existing_pkgrel(self):
        result = self.run_cli('v0.10.1')
        self.assertEqual((result.returncode, result.stdout, result.stderr), (0, 'unchanged\n', ''))
        self.assertEqual(self.recipe.read_text(), RECIPE)

    def test_missing_artifact_leaves_recipe_unchanged(self):
        # Replace a required regular file with a directory without deleting fixture data.
        asset = self.root / 'dbflux-linux-arm64.tar.gz'
        asset.rename(self.root / 'saved.tar.gz')
        asset.mkdir()
        result = self.run_cli()
        self.assertEqual((result.returncode, result.stdout, result.stderr),
                         (1, '', 'error: invalid published checksum or artifact: dbflux-linux-arm64.tar.gz\n'))
        self.assertEqual(self.recipe.read_text(), RECIPE)


class WorkflowTests(unittest.TestCase):
    def test_packaging_and_ssh_credentials_are_in_separate_jobs(self):
        workflow = (SCRIPT.parents[1] / '.github/workflows/sync-release-aur.yml').read_text()
        prepare, publish = workflow.split('\n  publish:\n')
        self.assertNotIn('secrets.AUR_SSH_PRIVATE_KEY', prepare)
        self.assertIn('secrets.AUR_SSH_PRIVATE_KEY', publish)
        self.assertNotIn('makepkg', publish)
        self.assertIn('USER builder', prepare)
        self.assertIn('--network none', prepare)
        self.assertIn('--printsrcinfo', prepare)
        self.assertIn('StrictHostKeyChecking=yes', publish)
        self.assertIn('vars.AUR_KNOWN_HOSTS', publish)
        self.assertIn('= "$EXPECTED_BASE"', publish)
        self.assertNotIn('--force', publish)
        self.assertIn('cancel-in-progress: false', workflow)


if __name__ == '__main__':
    unittest.main()
