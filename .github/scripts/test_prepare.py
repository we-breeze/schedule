"""The first release can already carry its intended package version."""
import importlib.util
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location(
    "prepare_release", Path(__file__).with_name("prepare-release.py")
)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class PrepareTests(unittest.TestCase):
    def test_first_tag_accepts_manifests_that_already_have_that_version(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            adapter = root / "providers/redis/Cargo.toml"
            adapter.parent.mkdir(parents=True)
            package = '[package]\nname="example"\nversion = "0.0.1"\nlicense="MIT"\ndescription="fixture"\n'
            core = root / "Cargo.toml"
            core.write_text(package)
            adapter.write_text(package + '\n[dependencies]\nbrz-schedule = { path = "../..", version = "=0.0.1" }\n')
            output = root / "output"
            calls = []

            def git(*args):
                calls.append(args)
                if args[:1] == ("rev-parse",):
                    return "same-head"
                return ""

            with patch.object(MODULE, "MANIFESTS", [core, adapter]), patch.object(
                MODULE, "git", side_effect=git
            ), patch.object(MODULE.subprocess, "run"), patch.dict(
                os.environ, DEFAULT_BRANCH="main", RETRY_TAG="", GITHUB_OUTPUT=str(output)
            ):
                MODULE.main()
            self.assertEqual(output.read_text(), "tag=v0.0.1\n")
            self.assertIn(("commit", "--allow-empty", "-m", "chore: release v0.0.1"), calls)
            self.assertIn('version = "=0.0.1"', adapter.read_text())
