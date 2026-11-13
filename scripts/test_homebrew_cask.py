"""Checks for the Homebrew cask renderer used by the release workflow."""
import pathlib
import subprocess
import sys
import tempfile
import unittest

SCRIPT = pathlib.Path(__file__).resolve().parent / "homebrew_cask.py"
RELEASE = pathlib.Path(__file__).resolve().parents[1] / ".github/workflows/release.yml"

HASHES = {
    "dbflux-macos-arm64.dmg": "a" * 64,
    "dbflux-macos-amd64.dmg": "b" * 64,
    "dbflux-linux-arm64.tar.gz": "c" * 64,
    "dbflux-linux-amd64.tar.gz": "d" * 64,
}


def write_checksums(directory, hashes):
    for asset, digest in hashes.items():
        (directory / f"{asset}.sha256").write_text(f"{digest}  {asset}\n", encoding="utf-8")


def run(version, checksum_dir, output):
    return subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--version",
            version,
            "--checksums",
            str(checksum_dir),
            "--output",
            str(output),
        ],
        capture_output=True,
        text=True,
    )


class HomebrewCaskTests(unittest.TestCase):
    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.scratch.name)
        self.checksums = self.root / "checksums"
        self.checksums.mkdir()
        self.output = self.root / "Casks" / "dbflux.rb"

    def tearDown(self):
        self.scratch.cleanup()

    def render(self, version="0.9.0", hashes=HASHES):
        write_checksums(self.checksums, hashes)
        return run(version, self.checksums, self.output)

    def test_renders_the_version_and_each_hash_in_its_platform_slot(self):
        result = self.render()

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stderr, "")
        cask = self.output.read_text(encoding="utf-8")
        self.assertIn('cask "dbflux" do', cask)
        self.assertIn('version "0.9.0"', cask)
        self.assertIn(f'arm:          "{"a" * 64}"', cask)
        self.assertIn(f'intel:        "{"b" * 64}"', cask)
        self.assertIn(f'arm64_linux:  "{"c" * 64}"', cask)
        self.assertIn(f'x86_64_linux: "{"d" * 64}"', cask)

    def test_points_each_platform_at_its_release_asset(self):
        self.render()

        cask = self.output.read_text(encoding="utf-8")
        base = "https://github.com/0xErwin1/dbflux/releases/download/v#{version}"
        self.assertIn(f'url "{base}/dbflux-macos-#{{arch}}.dmg"', cask)
        self.assertIn(f'url "{base}/dbflux-linux-#{{arch}}.tar.gz"', cask)
        self.assertIn('arch arm: "arm64", intel: "amd64"', cask)

    def test_installs_the_app_on_macos_and_the_binary_on_linux(self):
        self.render()

        cask = self.output.read_text(encoding="utf-8")
        macos = cask.split("  on_macos do", 1)[1].split("\n  end\n", 1)[0]
        linux = cask.split("  on_linux do", 1)[1].split("\n  end\n", 1)[0]
        self.assertIn('app "DBFlux.app"', macos)
        self.assertIn('binary "dbflux"', linux)
        self.assertNotIn("quarantine", linux)

    def test_removes_the_quarantine_attribute_on_macos_only(self):
        self.render()

        cask = self.output.read_text(encoding="utf-8")
        steps = cask.split("  postflight_steps do\n", 1)[1]
        self.assertTrue(steps.startswith("    on_macos do\n"), steps)
        self.assertIn(
            'run "/usr/bin/xattr", args: ["-dr", "com.apple.quarantine", "{{appdir}}/DBFlux.app"]',
            steps,
        )
        self.assertNotIn("postflight do", cask)

    def test_rejects_a_prerelease_version_and_writes_nothing(self):
        result = self.render(version="0.10.0-rc.1")

        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout, "")
        self.assertEqual(
            result.stderr,
            "error: version '0.10.0-rc.1' is not a stable X.Y.Z release\n",
        )
        self.assertFalse(self.output.exists())

    def test_rejects_a_checksum_file_for_another_asset(self):
        hashes = dict(HASHES)
        write_checksums(self.checksums, hashes)
        (self.checksums / "dbflux-linux-amd64.tar.gz.sha256").write_text(
            f"{'d' * 64}  dbflux-linux-arm64.tar.gz\n", encoding="utf-8"
        )

        result = run("0.9.0", self.checksums, self.output)

        self.assertEqual(result.returncode, 1)
        self.assertEqual(
            result.stderr,
            "error: dbflux-linux-amd64.tar.gz.sha256 names dbflux-linux-arm64.tar.gz,"
            " expected dbflux-linux-amd64.tar.gz\n",
        )
        self.assertFalse(self.output.exists())

    def test_rejects_a_malformed_hash(self):
        hashes = dict(HASHES)
        hashes["dbflux-macos-amd64.dmg"] = "xyz"

        result = self.render(hashes=hashes)

        self.assertEqual(result.returncode, 1)
        self.assertEqual(
            result.stderr,
            "error: dbflux-macos-amd64.dmg.sha256 does not hold a SHA-256 hex digest\n",
        )
        self.assertFalse(self.output.exists())

    def test_reports_a_missing_checksum_file(self):
        hashes = dict(HASHES)
        del hashes["dbflux-linux-arm64.tar.gz"]

        result = self.render(hashes=hashes)

        self.assertEqual(result.returncode, 1)
        self.assertEqual(
            result.stderr,
            f"error: missing {self.checksums / 'dbflux-linux-arm64.tar.gz.sha256'}\n",
        )
        self.assertFalse(self.output.exists())


class ReleaseWorkflowTapJobTests(unittest.TestCase):
    def setUp(self):
        workflow = RELEASE.read_text(encoding="utf-8")
        self.assertIn("  homebrew-tap:\n", workflow, "release.yml must define the homebrew-tap job")
        # The job is the last one in the file, so it runs to the end.
        self.job = workflow.split("  homebrew-tap:\n", 1)[1]

    def test_runs_after_the_release_for_published_stable_tags_only(self):
        self.assertIn("needs: [classify, release]", self.job)
        self.assertIn(
            "if: needs.classify.outputs.channel == 'stable' && needs.classify.outputs.draft == 'false'",
            self.job,
        )

    def test_skips_with_a_warning_when_the_token_is_missing(self):
        self.assertIn("secrets.HOMEBREW_TAP_TOKEN", self.job)
        self.assertIn("::warning::HOMEBREW_TAP_TOKEN is not set", self.job)

    def test_never_moves_the_cask_to_an_older_version(self):
        self.assertIn("sort -V", self.job)
        self.assertIn("::warning::The tap already ships", self.job)

    def test_renders_the_cask_with_the_repository_script(self):
        self.assertIn("python3 scripts/homebrew_cask.py", self.job)
        self.assertIn("repository: 0xErwin1/homebrew-dbflux", self.job)


if __name__ == "__main__":
    unittest.main()
