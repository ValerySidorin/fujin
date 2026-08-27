#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SEMVER = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?$")
VERSION_FILES = (
    "Cargo.toml",
    ".github/workflows/release.yml",
    "deploy/docker/fujin.build.toml",
    "Cargo.lock",
    "deploy/helm/fujin/Chart.yaml",
    "deploy/helm/fujin/values.yaml",
    "deploy/docker/Cargo.lock",
    "examples/deployment/docker-compose.yaml",
    "resources/docker-compose.fujin-kafka.yaml",
    "resources/fujin.kafka.build.toml",
    "README.md",
    "CONTRIBUTING.md",
    "sdk/go/embed/README.md",
    "resources/Cargo.kafka.lock",
)


def cargo_metadata() -> dict:
    output = subprocess.check_output(
        ["cargo", "metadata", "--no-deps", "--format-version", "1"], cwd=ROOT
    )
    return json.loads(output)


def release_waves() -> list[list[str]]:
    waves = []
    for line in (ROOT / "scripts/release_crates.txt").read_text().splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            waves.append(stripped.split())
    return waves


def fail(message: str) -> None:
    raise SystemExit(message)


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate one Fujin release commit")
    parser.add_argument("version", help="unprefixed semantic version")
    parser.add_argument("--tag", help="expected checked-out Git tag")
    args = parser.parse_args()
    version = args.version
    if not SEMVER.fullmatch(version):
        parser.error("version must be unprefixed semantic version")

    metadata = cargo_metadata()
    packages = {package["name"]: package for package in metadata["packages"]}
    waves = release_waves()
    wave_by_crate: dict[str, int] = {}
    for wave_number, wave in enumerate(waves):
        for crate in wave:
            if crate in wave_by_crate:
                fail(f"duplicate release crate: {crate}")
            wave_by_crate[crate] = wave_number

    publishable = {
        name for name, package in packages.items() if package.get("publish") != []
    }
    if set(wave_by_crate) != publishable:
        missing = sorted(publishable - set(wave_by_crate))
        extra = sorted(set(wave_by_crate) - publishable)
        fail(f"crate release list mismatch: missing={missing}, extra={extra}")

    for crate, package in packages.items():
        if crate not in publishable:
            continue
        if package["version"] != version:
            fail(f"{crate} has version {package['version']}, expected {version}")
        for dependency in package["dependencies"]:
            dependency_name = dependency["name"]
            if dependency.get("path") is None or dependency_name not in publishable:
                continue
            if dependency["req"] != f"={version}":
                fail(
                    f"{crate} dependency {dependency_name} uses {dependency['req']}, "
                    f"expected ={version}"
                )
            if wave_by_crate[dependency_name] >= wave_by_crate[crate]:
                fail(f"{dependency_name} must be published before {crate}")

    prefixed = f"v{version}"
    for relative in VERSION_FILES:
        content = (ROOT / relative).read_text()
        if version not in content and prefixed not in content:
            fail(f"release version missing from {relative}")

    if args.tag:
        expected = f"fujin/v{version}"
        if args.tag != expected:
            fail(f"tag {args.tag} does not match {expected}")

    print(f"release metadata valid for {version}")


if __name__ == "__main__":
    main()
