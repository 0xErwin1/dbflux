"""Deterministic contract tests; no release binaries or user configuration needed."""
import os
import pathlib
import subprocess
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/appimage-libraries.sh"


class LibraryPolicyTests(unittest.TestCase):
    def policy(self, name):
        return subprocess.run(
            ["bash", str(SCRIPT), "policy", name], capture_output=True, text=True
        )

    def test_bundle_application_dependencies(self):
        for name in ("libxkbcommon-x11.so.0", "libfontconfig.so.1", "libssl.so.3",
                     "libstdc++.so.6", "libxcb.so.1", "libfreetype.so.6"):
            with self.subTest(name=name):
                result = self.policy(name)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout.strip(), "bundle")

    def test_keep_glibc_and_gpu_stack_on_host(self):
        for name in ("libc.so.6", "libm.so.6", "ld-linux-aarch64.so.1",
                     "ld-linux-x86-64.so.2", "libnss_files.so.2", "libGL.so.1",
                     "libEGL.so.1", "libvulkan.so.1", "libdrm.so.2",
                     "libGLX_nvidia.so.0", "libgbm.so.1", "libGLESv2.so.2"):
            with self.subTest(name=name):
                result = self.policy(name)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout.strip(), "host")

    def test_glibc_ceiling_and_unresolved_libraries(self):
        with tempfile.TemporaryDirectory(prefix="dbflux-packaging-") as temporary:
            directory = pathlib.Path(temporary)
            tools = directory / "tools"
            tools.mkdir()
            for name, body in {
                "readelf": 'printf "%s\\n" "${TEST_GLIBC:-GLIBC_2.35}"',
                "ldd": 'printf "%s\\n" "${TEST_LDD:-libc.so.6 => /lib/libc.so.6 (0x1)}"',
            }.items():
                tool = tools / name
                tool.write_text("#!/bin/sh\n" + body + "\n")
                tool.chmod(0o755)
            appdir = directory / "AppDir"
            (appdir / "usr/bin").mkdir(parents=True)
            (appdir / "usr/lib").mkdir()
            (appdir / "usr/bin/dbflux").touch()
            (appdir / "usr/lib/libfontconfig.so.1").touch()
            environment = dict(os.environ, PATH=f"{tools}:{os.environ['PATH']}")

            def verify(**overrides):
                return subprocess.run(
                    ["bash", str(SCRIPT), "verify", str(appdir)],
                    env=dict(environment, **overrides), capture_output=True, text=True,
                )

            self.assertEqual(verify().returncode, 0)
            too_new = verify(TEST_GLIBC="GLIBC_2.39")
            self.assertNotEqual(too_new.returncode, 0)
            self.assertIn("exceeds 2.35", too_new.stderr)
            missing = verify(TEST_LDD="libxkbcommon-x11.so.0 => not found")
            self.assertNotEqual(missing.returncode, 0)
            self.assertIn("not found", missing.stderr)
            host_only = verify(TEST_LDD="libxcb.so.1 => /lib/libxcb.so.1 (0x1)")
            self.assertNotEqual(host_only.returncode, 0)
            self.assertIn("Unbundled dependency", host_only.stderr)
            (appdir / "usr/lib/libc.so.6").touch()
            self.assertIn("Forbidden bundled", verify().stderr)

    def test_invalid_library_arguments_have_explicit_diagnostics(self):
        cases = (
            ([], "Expected a mode and one argument"),
            (["bundle"], "Expected a mode and one argument"),
            (["verify", "AppDir", "extra"], "Expected a mode and one argument"),
            (["invalid", "value"], "Unknown mode: invalid"),
            (["policy", ""], "Argument must not be empty"),
        )
        for arguments, diagnostic in cases:
            with self.subTest(arguments=arguments):
                result = subprocess.run(
                    ["bash", str(SCRIPT), *arguments], capture_output=True, text=True
                )
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertIn(diagnostic, result.stderr)
                self.assertIn("Usage:", result.stderr)

    def test_container_scripts_reject_invalid_arguments_before_installing(self):
        for script_name in ("appimage-build.sh", "appimage-verify.sh"):
            script_path = ROOT / "scripts" / script_name
            for arguments in ([], ["invalid"], ["value", "extra"]):
                with self.subTest(script=script_name, arguments=arguments):
                    result = subprocess.run(
                        ["bash", str(script_path), *arguments],
                        capture_output=True, text=True,
                    )
                    self.assertEqual(result.returncode, 2, result.stderr)
                    self.assertTrue(result.stderr.strip())
                    self.assertNotIn("apt-get", result.stderr)

    def test_missing_appdir_files_have_explicit_diagnostics(self):
        with tempfile.TemporaryDirectory(prefix="dbflux-packaging-") as temporary:
            for mode in ("bundle", "verify"):
                result = subprocess.run(
                    ["bash", str(SCRIPT), mode, temporary],
                    capture_output=True, text=True,
                )
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertIn("Required file not found:", result.stderr)
                self.assertIn("usr/bin/dbflux", result.stderr)

    def test_both_builds_use_older_environment(self):
        workflow = (ROOT / ".github/workflows/build.yml").read_text()
        linux = workflow.split("  build-macos:")[0]
        self.assertIn("ubuntu:22.04", linux)
        self.assertIn("appimage-build.sh", linux)
        self.assertNotIn("ubuntu-22.04-arm", linux)
        self.assertIn("dbflux-${{ matrix.rpm_arch }}.AppImage", linux)


if __name__ == "__main__":
    unittest.main()
