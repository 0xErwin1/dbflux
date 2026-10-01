"""Deterministic contract tests for AppImage external update metadata.

Covers scripts/appimage-update-info.sh (subprocess level) and the workflow
wiring that embeds the update information, generates and publishes the
.zsync sidecar. No network, no AppImage tooling, no release binaries.
"""
import pathlib
import os
import stat
import subprocess
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
HELPER = ROOT / "scripts/appimage-update-info.sh"
WORKFLOWS = ROOT / ".github/workflows"

OWNER = "0xErwin1"
REPO = "dbflux"

STABLE = {
    "x86_64": f"gh-releases-zsync|{OWNER}|{REPO}|latest|dbflux-x86_64.AppImage.zsync",
    "aarch64": f"gh-releases-zsync|{OWNER}|{REPO}|latest|dbflux-aarch64.AppImage.zsync",
}
NIGHTLY = {
    "x86_64": f"gh-releases-zsync|{OWNER}|{REPO}|nightly|dbflux-x86_64.AppImage.zsync",
    "aarch64": f"gh-releases-zsync|{OWNER}|{REPO}|nightly|dbflux-aarch64.AppImage.zsync",
}


class HelperTests(unittest.TestCase):
    def run_helper(self, *arguments):
        return subprocess.run(
            ["bash", str(HELPER), *arguments], capture_output=True, text=True
        )

    def test_stable_targets_the_latest_release(self):
        for arch, expected in STABLE.items():
            with self.subTest(arch=arch):
                result = self.run_helper("stable", arch)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout, expected + "\n")

    def test_nightly_targets_the_rolling_nightly_tag(self):
        for arch, expected in NIGHTLY.items():
            with self.subTest(arch=arch):
                result = self.run_helper("nightly", arch)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout, expected + "\n")

    def test_rc_is_successful_and_empty(self):
        for arch in ("x86_64", "aarch64"):
            with self.subTest(arch=arch):
                result = self.run_helper("rc", arch)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout, "")

    def test_unknown_inputs_fail_nonzero_with_clear_stderr(self):
        cases = (
            ("unknown channel", ["beta", "x86_64"]),
            ("unknown arch", ["stable", "amd64"]),
            ("no arguments", []),
            ("missing arch", ["stable"]),
            ("excess arguments", ["stable", "x86_64", "extra"]),
            ("rc with unknown arch", ["rc", "arm64"]),
        )
        for label, arguments in cases:
            with self.subTest(case=label):
                result = self.run_helper(*arguments)
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertIn("Usage:", result.stderr)
                self.assertTrue(result.stderr.strip())


class VerifyStepExecutionTests(unittest.TestCase):
    """Execute the real extracted workflow step against fixture files.

    The verify step is pulled out of build.yml verbatim, GitHub template
    expressions are substituted, and it runs in an isolated cwd whose only
    helper is a symlink to the real scripts/appimage-update-info.sh. The
    produced image is mocked as a tiny runtime that answers
    --appimage-updateinformation only (never launches a payload).
    """

    RUNTIME_HEADER = (
        "zsync: 0.6.2 (zsync 0.6.2 for x86_64-pc-linux-gnu) static binary\n"
        "URL: {url}\n"
        "Filename: dbflux-x86_64.AppImage\n"
        "Mtime: Thu, 01 Jan 2026 00:00:00 +0000\n"
        "Length: {length}\n"
        "Hash-Hex-SHA-256-4K: " + "ab" * 32 + "\n"
    )

    @classmethod
    def setUpClass(cls):
        text = (WORKFLOWS / "build.yml").read_text()
        linux = text[: text.index("  build-macos:")]
        cls.verify_body = cls.step_body(linux, "Verify AppImage update metadata")
        cls.act_body = cls.step_body(linux, "(act) Store artifacts locally")

    @staticmethod
    def step_body(linux, step_name):
        start = linux.index("- name: " + step_name)
        end = linux.find("\n      - name:", start)
        if end == -1:
            end = len(linux)
        block = linux[start:end]
        return block[block.index("run: |\n") + len("run: |\n"):]

    @staticmethod
    def substitute(body, channel):
        return (
            body.replace("${{ inputs.channel }}", channel)
            .replace("${{ matrix.rpm_arch }}", "x86_64")
            .replace("${{ matrix.arch }}", "amd64")
            .replace("${{ env.APPIMAGE_ARCH }}", "x86_64")
        )

    def run_step(self, body, channel, *, info, sidecar="good", act=False):
        with tempfile.TemporaryDirectory(prefix="dbflux-verify-") as temporary:
            root = pathlib.Path(temporary)
            scripts = root / "scripts"
            scripts.mkdir()
            os.symlink(ROOT / "scripts/appimage-update-info.sh",
                       scripts / "appimage-update-info.sh")

            image = root / "dbflux-x86_64.AppImage"
            image.write_text(
                "#!/bin/sh\n"
                "if [ \"$1\" = \"--appimage-updateinformation\" ]; then\n"
                "  printf '%s\\n' \"$FAKE_UPDATE_INFO\"\n"
                "fi\n"
                "exit 0\n"
            )
            image.chmod(image.stat().st_mode | stat.S_IEXEC)

            # Mock of the pinned tool runtime: prints an empty line, exit 0.
            tool = root / "appimagetool-x86_64.AppImage"
            tool.write_text("#!/bin/sh\nprintf '\\n'\nexit 0\n")
            tool.chmod(tool.stat().st_mode | stat.S_IEXEC)

            if sidecar != "missing":
                length = image.stat().st_size
                if sidecar == "empty":
                    content = b""
                elif sidecar == "wrong-url":
                    content = self.zsync(url="dbflux-other.AppImage", length=length)
                elif sidecar == "wrong-length":
                    content = self.zsync(length=length + 999999)
                else:
                    content = self.zsync(length=length)
                (root / "dbflux-x86_64.AppImage.zsync").write_bytes(content)

            if act:
                for name in (
                    "dbflux-linux-amd64.tar.gz", "dbflux-linux-amd64.tar.gz.sha256",
                    "dbflux-linux-amd64.tar.gz.asc", "dbflux-x86_64.AppImage.sha256",
                    "dbflux-x86_64.AppImage.asc", "dbflux_0.0_amd64.deb",
                    "dbflux_0.0_amd64.deb.asc", "dbflux_amd64.deb.sha256",
                    "dbflux_0.0_amd64.rpm", "dbflux_0.0_amd64.rpm.asc",
                    "dbflux_amd64.rpm.sha256", "dbflux-linux-amd64.deb.sha256",
                    "dbflux-linux-amd64.rpm.sha256",
                ):
                    (root / name).write_bytes(b"x")
                body = body.replace("/tmp/artifacts", str(root / "published"))

            script = root / "step.sh"
            script.write_text(self.substitute(body, channel))
            # GitHub's default shell for run: steps is bash -e.
            return subprocess.run(
                ["bash", "-e", str(script)], cwd=root, capture_output=True,
                text=True, env=dict(os.environ, FAKE_UPDATE_INFO=info),
            )

    @staticmethod
    def diagnostics(result):
        # The step reports via echo "::error::..." on stdout.
        return result.stdout + result.stderr

    def zsync(self, *, url="dbflux-x86_64.AppImage", length=1234):
        return self.RUNTIME_HEADER.format(url=url, length=length).encode()

    def test_verify_step_does_not_query_the_tool_runtime(self):
        # The regression: the step once ran the flag against the pinned
        # appimagetool AppImage, which reports the tool, not this image.
        self.assertNotIn("appimagetool-", self.verify_body)

    def test_stable_step_passes_with_real_header_shape(self):
        result = self.run_step(self.verify_body, "stable", info=STABLE["x86_64"])
        self.assertEqual(result.returncode, 0, self.diagnostics(result))

    def test_nightly_step_passes_with_real_header_shape(self):
        result = self.run_step(self.verify_body, "nightly", info=NIGHTLY["x86_64"])
        self.assertEqual(result.returncode, 0, self.diagnostics(result))

    def test_rc_step_passes_without_sidecar(self):
        result = self.run_step(self.verify_body, "rc", info="", sidecar="missing")
        self.assertEqual(result.returncode, 0, self.diagnostics(result))

    def test_missing_sidecar_fails(self):
        result = self.run_step(self.verify_body, "stable", info=STABLE["x86_64"],
                               sidecar="missing")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("missing or empty", self.diagnostics(result))

    def test_empty_sidecar_fails(self):
        result = self.run_step(self.verify_body, "stable", info=STABLE["x86_64"],
                               sidecar="empty")
        self.assertNotEqual(result.returncode, 0)

    def test_wrong_target_url_fails(self):
        result = self.run_step(self.verify_body, "stable", info=STABLE["x86_64"],
                               sidecar="wrong-url")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("relative image URL", self.diagnostics(result))

    def test_length_mismatch_fails(self):
        result = self.run_step(self.verify_body, "stable", info=STABLE["x86_64"],
                               sidecar="wrong-length")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("does not match", self.diagnostics(result))

    def test_act_copy_publishes_sidecar_for_stable(self):
        result = self.run_step(self.act_body, "stable", info=STABLE["x86_64"],
                               act=True)
        self.assertEqual(result.returncode, 0, self.diagnostics(result))

    def test_act_copy_skips_sidecar_for_rc_without_failing(self):
        result = self.run_step(self.act_body, "rc", info="", sidecar="missing",
                               act=True)
        self.assertEqual(result.returncode, 0, self.diagnostics(result))


class BuildWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.build = (WORKFLOWS / "build.yml").read_text()
        # Linux job ends where the next job begins.
        cls.linux = cls.build[: cls.build.index("  build-macos:")]

    def step_block(self, step_name):
        start = self.linux.index("- name: " + step_name)
        end = self.linux.find("\n      - name:", start)
        if end == -1:
            end = len(self.linux)
        return self.linux[start:end]

    def test_installs_zsync_where_appimagetool_runs(self):
        block = self.step_block("Install system dependencies")
        self.assertIn("zsync", block)
        self.assertIn("apt-get install", block)

    def test_update_information_passed_as_arguments_only_when_present(self):
        block = self.step_block("Create AppImage")
        self.assertIn("appimage-update-info.sh", block)
        self.assertIn('UPDATE_INFO=$(bash scripts/appimage-update-info.sh', block)
        self.assertIn('if [ -n "$UPDATE_INFO" ]', block)
        self.assertIn('-u "$UPDATE_INFO"', block)
        self.assertIn('"${tool_args[@]}"', block)
        self.assertNotIn("UPDATE_INFORMATION=", block)

    def test_verification_is_fail_closed_before_signing_and_checksums(self):
        verify = self.step_block("Verify AppImage update metadata")
        sign = self.step_block("Sign artifacts")
        checksums = self.step_block("Generate checksums")
        self.assertIn("--appimage-updateinformation", verify)
        self.assertIn("missing or empty", verify)
        self.assertIn("must not embed update information", verify)
        self.assertIn("does not match the expected update information", verify)
        self.assertIn("Length:", verify)
        # The final image and sidecar must be byte-stable before detached GPG
        # signing and sha256 checksums, which happen later in the same job.
        self.assertLess(self.linux.index("Verify AppImage update metadata"),
                        self.linux.index("- name: Sign artifacts"))
        self.assertLess(self.linux.index("Verify AppImage update metadata"),
                        self.linux.index("- name: Generate checksums"))
        self.assertIn("AppImage", sign)
        self.assertIn(".AppImage.sha256", checksums)

    def test_sidecar_uploaded_and_copied_for_local_runs(self):
        upload = self.step_block("Upload artifacts")
        act = self.step_block("(act) Store artifacts locally")
        for block in (upload, act):
            self.assertIn("dbflux-${{ matrix.rpm_arch }}.AppImage.zsync", block)


class PublishWorkflowTests(unittest.TestCase):
    def test_release_and_nightly_publish_the_sidecar(self):
        for name in ("release.yml", "nightly.yml"):
            with self.subTest(workflow=name):
                text = (WORKFLOWS / name).read_text()
                self.assertIn("artifacts/linux-*/*.AppImage.zsync", text)


if __name__ == "__main__":
    unittest.main()
