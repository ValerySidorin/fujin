#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FILES = (
    "Cargo.toml",
    "Cargo.lock",
    ".github/workflows/release.yml",
    "deploy/docker/Cargo.lock",
    "deploy/docker/fujin.build.toml",
    "deploy/helm/fujin/Chart.yaml",
    "deploy/helm/fujin/values.yaml",
    "examples/deployment/docker-compose.yaml",
    "resources/Cargo.kafka.lock",
    "resources/docker-compose.fujin-kafka.yaml",
    "resources/fujin.kafka.build.toml",
    "README.md",
    "CONTRIBUTING.md",
    "sdk/go/embed/README.md",
)
SEMVER = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?$")
WORKSPACE_VERSION = re.compile(
    r'(?ms)^\[workspace\.package\]\n(?:(?!^\[).)*?^version = "([^"]+)"$'
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Update every Fujin release-coupled version")
    parser.add_argument("version", help="unprefixed semantic version, for example 0.6.0-alpha.2")
    parser.add_argument("--dry-run", action="store_true", help="validate replacements without writing")
    args = parser.parse_args()
    version = args.version
    if not SEMVER.fullmatch(version):
        parser.error("version must be unprefixed semantic version")

    root_manifest = (ROOT / "Cargo.toml").read_text()
    match = WORKSPACE_VERSION.search(root_manifest)
    if match is None:
        raise SystemExit("workspace package version not found")
    previous = match.group(1)
    if previous == version:
        raise SystemExit(f"workspace already uses {version}")

    for relative in FILES:
        path = ROOT / relative
        content = path.read_text()
        updated = content.replace(previous, version)
        if updated == content:
            raise SystemExit(f"expected {previous} in {relative}")
        if not args.dry_run:
            path.write_text(updated)

    action = "would update" if args.dry_run else "updated"
    print(f"{action} Fujin release version: {previous} -> {version}")


if __name__ == "__main__":
    main()
