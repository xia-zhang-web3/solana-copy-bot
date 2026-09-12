#!/usr/bin/env python3
import argparse
import json
import subprocess
import sys


def load_metadata():
    raw = subprocess.check_output(
        ["cargo", "metadata", "--locked", "--format-version=1", "--no-deps"],
        text=True,
    )
    return json.loads(raw)


def default_features(package):
    """Resolve package-local features for the default artifact build only."""
    features = package.get("features", {})
    pending = ["default"] if "default" in features else []
    enabled = set()
    while pending:
        feature = pending.pop()
        if feature in enabled:
            continue
        enabled.add(feature)
        pending.extend(features.get(feature, []))
    return enabled


def package_bins(metadata, package_name):
    for package in metadata.get("packages", []):
        if package.get("name") != package_name:
            continue
        enabled = default_features(package)
        bins = sorted(
            target.get("name")
            for target in package.get("targets", [])
            if "bin" in target.get("kind", []) and target.get("name")
            and set(target.get("required-features", [])).issubset(enabled)
        )
        return bins
    return None


def main():
    parser = argparse.ArgumentParser(
        description="List package bin targets enabled by default for artifact builds"
    )
    parser.add_argument("--package", required=True)
    args = parser.parse_args()

    bins = package_bins(load_metadata(), args.package)
    if bins is None:
        print(f"unknown package: {args.package}", file=sys.stderr)
        return 1
    if not bins:
        print(f"package has no bin targets: {args.package}", file=sys.stderr)
        return 1
    for bin_name in bins:
        print(bin_name)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
