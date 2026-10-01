"""Deterministic tests for check_release_links.py; no network or release needed."""
import pathlib
import subprocess
import sys
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/check_release_links.py"

sys.path.insert(0, str(ROOT / "scripts"))
import check_release_links  # noqa: E402


class ExtractionTests(unittest.TestCase):
    def write(self, root, relative, text):
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        return path

    def test_markdown_links_and_files_lists_are_found_with_line_numbers(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            markdown = self.write(
                root, "docs/INSTALL.md",
                "# Install\n\n"
                "wget https://github.com/o/r/releases/latest/download/dbflux-linux-amd64.deb\n",
            )
            component = self.write(
                root, "web/src/components/InstallTabs.astro",
                "const P = [\n  {\n    files: ['dbflux-macos-arm64.dmg',\n"
                "            \"dbflux-windows-amd64.zip\"],\n  },\n];\n",
            )

            self.assertEqual(
                check_release_links.linked_assets(markdown),
                [(3, "dbflux-linux-amd64.deb")],
            )
            self.assertEqual(
                check_release_links.linked_assets(component),
                [(3, "dbflux-macos-arm64.dmg"), (4, "dbflux-windows-amd64.zip")],
            )

    def test_versioned_release_links_are_not_checked(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            markdown = self.write(
                root, "docs/RELEASE.md",
                "curl https://github.com/o/r/releases/download/v$ver/dbflux-linux-$arch.tar.gz.sha256\n",
            )

            self.assertEqual(check_release_links.linked_assets(markdown), [])

    def test_translations_and_readme_are_scanned(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            link = "releases/latest/download/dbflux-linux-amd64.rpm\n"
            for relative in ("docs/INSTALL.md", "docs/es/INSTALL.md", "docs/zh_Hans/INSTALL.md", "README.md"):
                self.write(root, relative, link)

            broken = check_release_links.broken_links(root, {"dbflux-x86_64.AppImage"})

            self.assertEqual(
                sorted(str(path) for path, _line, _name in broken),
                ["README.md", "docs/INSTALL.md", "docs/es/INSTALL.md", "docs/zh_Hans/INSTALL.md"],
            )

    def test_published_links_are_not_reported(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            self.write(root, "docs/INSTALL.md", "releases/latest/download/dbflux-x86_64.AppImage\n")

            self.assertEqual(check_release_links.broken_links(root, {"dbflux-x86_64.AppImage"}), [])


class CommandTests(unittest.TestCase):
    def run_script(self, *arguments):
        return subprocess.run(
            [sys.executable, str(SCRIPT), *arguments], capture_output=True, text=True
        )

    def test_missing_asset_fails_with_an_annotation(self):
        with tempfile.TemporaryDirectory() as directory:
            artifacts = pathlib.Path(directory)
            (artifacts / "linux-amd64").mkdir()
            (artifacts / "linux-amd64" / "dbflux-x86_64.AppImage").write_bytes(b"")

            result = self.run_script(str(artifacts))

            self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
            self.assertIn("::error file=docs/INSTALL.md", result.stdout)

    def test_empty_artifacts_directory_is_a_usage_error(self):
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_script(directory)

            self.assertEqual(result.returncode, 2)
            self.assertIn("no files", result.stderr)

    def test_every_current_link_resolves_against_the_assets_ci_publishes(self):
        """The repository's own links must match the names the build workflows produce."""
        published = {
            "dbflux-linux-amd64.tar.gz", "dbflux-linux-arm64.tar.gz",
            "dbflux-x86_64.AppImage", "dbflux-aarch64.AppImage",
            "dbflux-linux-amd64.deb", "dbflux-linux-arm64.deb",
            "dbflux-linux-amd64.rpm", "dbflux-linux-arm64.rpm",
            "dbflux-macos-amd64.dmg", "dbflux-macos-arm64.dmg",
            "dbflux-windows-amd64.zip", "dbflux-windows-amd64-setup.exe",
        }

        self.assertEqual(check_release_links.broken_links(ROOT, published), [])


class WorkflowTests(unittest.TestCase):
    @staticmethod
    def read(relative):
        return (ROOT / relative).read_text(encoding="utf-8")

    def test_stable_package_names_are_made_after_checksums_and_uploaded(self):
        build = self.read(".github/workflows/build.yml")

        checksums = build.index("- name: Generate checksums")
        stable_names = build.index("- name: Add stable names for the Linux packages")
        upload = build.index("- name: Upload artifacts")

        self.assertLess(checksums, stable_names)
        self.assertLess(stable_names, upload)
        self.assertIn('stable_name="dbflux-linux-${{ matrix.arch }}.$extension"', build)
        for extension in ("deb", "rpm"):
            self.assertIn(f"dbflux-linux-${{{{ matrix.arch }}}}.{extension}.sha256", build[upload:])

    def test_release_and_nightly_check_links_before_publishing(self):
        for workflow in (".github/workflows/release.yml", ".github/workflows/nightly.yml"):
            with self.subTest(workflow=workflow):
                text = self.read(workflow)
                check = text.index("python3 scripts/check_release_links.py artifacts")
                publish = text.index("uses: softprops/action-gh-release")
                download = text.index("- name: Download all artifacts")

                self.assertLess(download, check)
                self.assertLess(check, publish)


if __name__ == "__main__":
    unittest.main()
