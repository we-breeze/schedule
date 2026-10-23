"""Exercise interrupted-release behavior without contacting the registry."""
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import urllib.error

spec = importlib.util.spec_from_file_location("publisher", Path(__file__).with_name("publish-packages.py"))
publisher = importlib.util.module_from_spec(spec)
spec.loader.exec_module(publisher)


class PublishTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        old = Path.cwd()
        os.chdir(temporary.name)
        self.addCleanup(os.chdir, old)
        Path("Cargo.toml").write_text('[package]\nname = "test-package"\nversion = "0.0.1"\n')
        Path("target/package").mkdir(parents=True)
        Path("target/package/test-package-0.0.1.crate").write_bytes(b"test")

    def test_matching_existing_upload_is_skipped(self):
        response = io.BytesIO((json.dumps({"vers":"0.0.1","cksum":hashlib.sha256(b"test").hexdigest()}) + "\n").encode())
        with patch.object(publisher, "run") as run, patch.object(publisher.urllib.request, "urlopen", return_value=response):
            publisher.publish("Cargo.toml")
        self.assertEqual(run.call_count, 1)
        self.assertIn("--dry-run", run.call_args.args)

    def test_different_existing_upload_is_rejected(self):
        response = io.BytesIO(b'{"vers":"0.0.1","cksum":"different"}\n')
        with patch.object(publisher, "run") as run, patch.object(publisher.urllib.request, "urlopen", return_value=response):
            with self.assertRaisesRegex(SystemExit, "different contents"):
                publisher.publish("Cargo.toml")
        self.assertEqual(run.call_count, 1)

    def test_registry_failure_does_not_publish(self):
        error = urllib.error.HTTPError("https://example.com", 503, "Unavailable", {}, None)
        with patch.object(publisher, "run") as run, patch.object(publisher.urllib.request, "urlopen", side_effect=error):
            with self.assertRaises(urllib.error.HTTPError):
                publisher.publish("Cargo.toml")
        self.assertEqual(run.call_count, 1)


if __name__ == "__main__":
    unittest.main()
