"""Prepare both package versions locally; the workflow pushes after checks."""
import os
from pathlib import Path
import re
import subprocess
import tomllib

MANIFESTS = [Path("Cargo.toml"), Path("providers/redis/Cargo.toml")]
SEMVER = re.compile(r"v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)")


def git(*args):
    return subprocess.check_output(["git", *args], text=True).strip()


def package(path):
    return tomllib.loads(path.read_text())["package"]


def main():
    branch = os.environ["DEFAULT_BRANCH"]
    retry = os.environ.get("RETRY_TAG", "")
    git("fetch", "origin", branch, "--tags")
    if retry:
        if not SEMVER.fullmatch(retry):
            raise SystemExit("retry_tag must be a semantic version tag")
        git("merge-base", "--is-ancestor", retry, f"origin/{branch}")
        git("checkout", "--detach", retry)
        if any(package(path)["version"] != retry[1:] for path in MANIFESTS):
            raise SystemExit("Tag and package versions differ")
        tag = retry
    else:
        if git("rev-parse", "HEAD") != git("rev-parse", f"origin/{branch}"):
            raise SystemExit("Default branch advanced; start a new Publish run")
        prefix = "v" + package(MANIFESTS[0])["version"].rsplit(".", 1)[0] + "."
        patches = [int(tag.rsplit(".", 1)[1]) for tag in git("tag", "--list", prefix + "*").splitlines()
                   if SEMVER.fullmatch(tag) and tag.startswith(prefix)]
        tag = prefix + str(max(patches, default=0) + 1)
        for path in MANIFESTS:
            manifest = package(path)
            if not manifest.get("license") or not manifest.get("description"):
                raise SystemExit("Both packages require license and description")
            text, count = re.subn(r'(?m)^version = "[^"]+"$', f'version = "{tag[1:]}"', path.read_text(), count=1)
            if count != 1:
                raise SystemExit(f"Missing package version in {path}")
            if path != MANIFESTS[0]:
                text, count = re.subn(r'(brz-schedule = \{[^\n]*version = ")[^"]+', rf'\g<1>={tag[1:]}', text)
                if count == 0:
                    raise SystemExit("Missing adapter dependency version")
            path.write_text(text)
        subprocess.run(["cargo", "check", "--workspace"], check=True)
        git("config", "user.name", "github-actions[bot]")
        git("config", "user.email", "41898282+github-actions[bot]@users.noreply.github.com")
        git("add", *(str(path) for path in MANIFESTS), "Cargo.lock")
        git("commit", "--allow-empty", "-m", f"chore: release {tag}")
    with open(os.environ["GITHUB_OUTPUT"], "a") as output:
        output.write(f"tag={tag}\n")


if __name__ == "__main__":
    main()
