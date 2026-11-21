import json
import pathlib
import subprocess
import sys
import tempfile
import unittest


class ReleaseMetadataTests(unittest.TestCase):
    def run_update(self, tag, current="0.10.2", registry=None, broken=False):
        with tempfile.TemporaryDirectory() as scratch:
            root = pathlib.Path(scratch)
            (root / "nix").mkdir()
            (root / "web").mkdir()
            checksums = root / "checksums"
            checksums.mkdir()
            pointer = root / "nix/release-info.nix"
            pointer.write_text('{\n  version = "' + current + '";\n}\n')
            versions = root / "web/versions.json"
            versions.write_text(json.dumps(registry or [
                {"id": "nightly", "ref": "main", "noindex": True},
                {"id": "v0.10", "ref": "release/v0.10", "current": True},
                {"id": "v0.9", "ref": "release/v0.9"},
            ]))
            before = (pointer.read_text(), versions.read_text())
            for arch in ("amd64", "arm64"):
                asset = f"dbflux-linux-{arch}.tar.gz"
                (checksums / (asset + ".sha256")).write_text(
                    ("bad" if broken else "ab" * 32) + "  " + asset + "\n")
            command = [sys.executable, str(pathlib.Path(__file__).with_name("release_metadata.py")),
                       "--tag", tag, "--root", str(root), "--checksums", str(checksums)]
            result = subprocess.run(command, capture_output=True, text=True)
            after = (pointer.read_text(), versions.read_text())
            if result.returncode == 0:
                replay = subprocess.run(command, capture_output=True, text=True)
                self.assertEqual((replay.returncode, replay.stdout, replay.stderr), (0, "", ""))
                self.assertEqual(after, (pointer.read_text(), versions.read_text()))
            return result, before, after

    def test_rc_registers_minor_without_promoting_current(self):
        result, _, after = self.run_update("v0.11.0-rc.2")
        self.assertEqual((result.returncode, result.stdout, result.stderr), (0, "", ""))
        self.assertIn('version = "0.11.0-rc.2"', after[0])
        self.assertEqual(after[0].count('hash = "sha256-q6urq6urq6urq6urq6urq6urq6urq6urq6urq6urq6s=";'), 2)
        for arch in ("amd64", "arm64"):
            self.assertIn(f"/v0.11.0-rc.2/dbflux-linux-{arch}.tar.gz", after[0])
        entries = json.loads(after[1])
        self.assertEqual(entries[1], {"id": "v0.11", "ref": "release/v0.11"})
        self.assertEqual(next(e["id"] for e in entries if e.get("current")), "v0.10")

    def test_stable_without_rc_promotes_current(self):
        result, _, after = self.run_update("v0.11.0")
        self.assertEqual((result.returncode, result.stdout, result.stderr), (0, "", ""))
        self.assertEqual(next(e["id"] for e in json.loads(after[1]) if e.get("current")), "v0.11")

    def test_older_release_leaves_newer_pointers_unchanged(self):
        result, before, after = self.run_update("v0.9.9")
        self.assertEqual((result.returncode, result.stdout, result.stderr), (0, "", ""))
        self.assertEqual(before, after)

    def test_stable_beats_rc_and_rc_cannot_replace_stable(self):
        result, _, after = self.run_update("v0.11.0", current="0.11.0-rc.9")
        self.assertEqual(result.returncode, 0)
        self.assertIn('version = "0.11.0"', after[0])
        result, before, after = self.run_update("v0.10.2-rc.99")
        self.assertEqual(result.returncode, 0)
        self.assertEqual(before, after)

    def test_invalid_checksum_is_atomic(self):
        result, before, after = self.run_update("v0.11.0", broken=True)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout, "")
        self.assertEqual(result.stderr, "error: dbflux-linux-amd64.tar.gz.sha256 does not hold a SHA-256 hex digest\n")
        self.assertEqual(before, after)

    def test_invalid_tag_is_atomic(self):
        result, before, after = self.run_update("v0.11.0-dev.1")
        self.assertEqual((result.returncode, result.stdout, result.stderr),
                         (1, "", "error: expected vX.Y.Z or vX.Y.Z-rc.N\n"))
        self.assertEqual(before, after)

    def test_site_never_demotes_newer_current(self):
        entries = [
            {"id": "nightly", "ref": "main", "noindex": True},
            {"id": "v0.12", "ref": "release/v0.12", "current": True},
            {"id": "v0.11", "ref": "release/v0.11"},
        ]
        result, before, after = self.run_update("v0.11.1", registry=entries)
        self.assertEqual((result.returncode, result.stdout, result.stderr), (0, "", ""))
        self.assertEqual(before[1], after[1])
        self.assertIn('version = "0.11.1"', after[0])

    def test_site_promotion_independent_of_newer_nix(self):
        result, before, after = self.run_update("v0.11.0", current="0.12.0-rc.1")
        self.assertEqual(result.returncode, 0)
        self.assertEqual(before[0], after[0])
        self.assertEqual(next(e["id"] for e in json.loads(after[1]) if e.get("current")), "v0.11")


if __name__ == "__main__":
    unittest.main()
