"""Publish in dependency order; resume only an identical previously uploaded tag."""
import hashlib
import json
from pathlib import Path
import subprocess
import tomllib
import urllib.error
import urllib.request


def run(*args):
    subprocess.run(args, check=True)


def publish(manifest):
    package = tomllib.loads(Path(manifest).read_text())["package"]
    name, version = package["name"], package["version"]
    run("cargo", "publish", "-p", name, "--locked", "--dry-run", "--registry", "crates-io")
    request = urllib.request.Request(
        f"https://index.crates.io/{name[:2]}/{name[2:4]}/{name}",
        headers={"User-Agent": "we-breeze-release (https://github.com/we-breeze)"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            existing = next((entry for line in response
                             if (entry := json.loads(line))["vers"] == version), None)
    except urllib.error.HTTPError as error:
        if error.code != 404:
            raise
        existing = None
    if existing is not None:
        artifact = Path("target/package") / f"{name}-{version}.crate"
        if existing.get("cksum") != hashlib.sha256(artifact.read_bytes()).hexdigest():
            raise SystemExit(f"{name} {version} already exists with different contents")
        print(f"{name} {version} already published with identical contents")
        return
    run("cargo", "publish", "-p", name, "--locked", "--registry", "crates-io")


if __name__ == "__main__":
    publish("Cargo.toml")
    publish("providers/redis/Cargo.toml")
