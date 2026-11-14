"""Structural CI contract checks; no credentials or Windows runner required."""
import pathlib
import re
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
BUILD = ROOT / ".github/workflows/build.yml"
SIGNED_GATE = "if: inputs.channel == 'stable' || inputs.channel == 'rc'"
SIGNPATH_ACTION = "signpath/github-action-submit-signing-request@f6d04783b4569d051e0c80105fe66e82819d0092"


def step_blocks(workflow):
    return {
        match.group(1): match.group(0)
        for match in re.finditer(
            r"^      - name: ([^\n]+)\n.*?(?=^      - name: |\Z)",
            workflow,
            re.MULTILINE | re.DOTALL,
        )
    }


class WindowsSigningTests(unittest.TestCase):
    def setUp(self):
        self.build = BUILD.read_text(encoding="utf-8")
        self.windows = self.build.split("  build-windows:\n", 1)[1]
        self.steps = step_blocks(self.windows)

    def test_stable_and_rc_signing_use_raw_pe_artifacts_and_downloads_signed_files(self):
        for label, upload_id, source, output in (
            ("executable", "unsigned-executable", "target/x86_64-pc-windows-msvc/release/dbflux.exe", "signed-executable"),
            ("installer", "unsigned-installer", "dbflux-windows-amd64-setup.exe", "signed-installer"),
        ):
            with self.subTest(label=label):
                upload = self.steps.get(f"Upload unsigned Windows {label}", "")
                signing = self.steps.get(f"Sign Windows {label}", "")
                install = self.steps.get(f"Verify signed Windows {label}", "")
                for block in (upload, signing, install):
                    self.assertIn(SIGNED_GATE, block)
                self.assertIn(f"id: {upload_id}", upload)
                self.assertIn(f"path: {source}", upload)
                self.assertIn("archive: false", upload)
                self.assertIn("if-no-files-found: error", upload)
                self.assertIn(SIGNPATH_ACTION, signing)
                self.assertIn("api-token: ${{ secrets.SIGNPATH_API_TOKEN }}", signing)
                self.assertIn("organization-id: '963ebe33-9db3-4a84-9eea-f726ca65dd9c'", signing)
                self.assertIn("project-slug: dbflux", signing)
                self.assertIn("signing-policy-slug: ${{ env.SIGNPATH_SIGNING_POLICY }}", signing)
                self.assertIn(f"github-artifact-id: ${{{{ steps.{upload_id}.outputs.artifact-id }}}}", signing)
                self.assertIn("wait-for-completion: true", signing)
                self.assertIn("wait-for-completion-timeout-in-seconds: 3600", signing)
                self.assertIn("skip-decompress: true", signing)
                self.assertIn(f"output-artifact-directory: {output}", signing)
                self.assertIn("Get-AuthenticodeSignature", install)
                self.assertIn("'Valid'", install)
                self.assertIn("$env:SIGNPATH_SIGNING_POLICY -eq 'test-signing'", install)
                self.assertIn("CN=Test certificate for 'DBFlux [OSS]'", install)
                self.assertIn("TimeStamperCertificate", install)
                self.assertIn("throw", install)
                self.assertIn(f"Copy-Item {output}/", install)
                self.assertIn(source, install)

        names = list(self.steps)
        for earlier, later in (
            ("Verify signed Windows executable", "Create portable package"),
            ("Verify signed Windows executable", "Prepare installer files"),
            ("Build installer", "Upload unsigned Windows installer"),
            ("Verify signed Windows installer", "Sign artifacts"),
            ("Verify signed Windows installer", "Generate checksums"),
            ("Generate checksums", "Upload artifacts"),
        ):
            self.assertLess(names.index(earlier), names.index(later))

    def test_stable_and_rc_signing_fail_closed_without_credentials_or_trusted_ci(self):
        preflight = self.steps.get("Check Windows signing prerequisites", "")
        self.assertIn(SIGNED_GATE, preflight)
        self.assertIn("SIGNPATH_API_TOKEN: ${{ secrets.SIGNPATH_API_TOKEN }}", preflight)
        self.assertIn("IsNullOrWhiteSpace", preflight)
        self.assertIn("RELEASE_VERSION: ${{ inputs.version }}", preflight)
        self.assertIn("IS_PRERELEASE: ${{ inputs.prerelease }}", preflight)
        self.assertIn("CHANNEL: ${{ inputs.channel }}", preflight)
        self.assertIn(r"'^[0-9]+\.[0-9]+\.[0-9]+$'", preflight)
        self.assertIn(r"'^[0-9]+\.[0-9]+\.[0-9]+-rc\.[0-9]+$'", preflight)
        self.assertIn("0xErwin1/dbflux", preflight)
        self.assertIn("$env:ACT", preflight)
        self.assertGreaterEqual(preflight.count("throw"), 3)
        self.assertLess(list(self.steps).index("Check Windows signing prerequisites"), list(self.steps).index("Build DBFlux"))
        self.assertIn("      SIGNPATH_API_TOKEN:\n        required: false", self.build)
        self.assertIn("    permissions:\n      contents: read\n      actions: read", self.windows)
        job_env = self.windows.split("\n    env:\n", 1)[1].split("\n    steps:\n", 1)[0]
        self.assertIn("      SIGNPATH_SIGNING_POLICY: test-signing", job_env)

    def test_publication_excludes_unsigned_artifacts_but_keeps_platform_packages(self):
        for path in ("release.yml", "nightly.yml"):
            with self.subTest(workflow=path):
                text = (ROOT / ".github/workflows" / path).read_text(encoding="utf-8")
                download = step_blocks(text)["Download all artifacts"]
                self.assertIn('pattern: "{linux,macos,windows}-*"', download)
                build_job = text.split("  build:\n", 1)[1].split("  release:\n", 1)[0]
                self.assertIn("    permissions:\n      contents: read\n      actions: read", build_job)
                self.assertIn("secrets: inherit", build_job)
                self.assertIn("artifacts/windows-*/*.zip", text)
                self.assertIn("artifacts/windows-*/*-setup.exe", text)

    def test_nightly_keeps_existing_packaging_gpg_and_checksums(self):
        for name in ("Build DBFlux", "Create portable package", "Prepare installer files", "Build installer", "Sign artifacts", "Generate checksums"):
            self.assertNotIn(SIGNED_GATE, self.steps[name])
        self.assertIn("Copy-Item target/x86_64-pc-windows-msvc/release/dbflux.exe pkg/dbflux.exe", self.steps["Create portable package"])
        self.assertIn("Copy-Item target/x86_64-pc-windows-msvc/release/dbflux.exe installer/dbflux.exe", self.steps["Prepare installer files"])
        for artifact in ("dbflux-windows-amd64.zip", "dbflux-windows-amd64-setup.exe"):
            self.assertIn(f"gpg --armor --detach-sign {artifact}", self.steps["Sign artifacts"])
            self.assertIn(f"Get-FileHash -Algorithm SHA256 {artifact}", self.steps["Generate checksums"])
        before_windows = self.build.split("  build-windows:\n", 1)[0]
        self.assertNotIn("uses: signpath/", before_windows)


if __name__ == "__main__":
    unittest.main()
