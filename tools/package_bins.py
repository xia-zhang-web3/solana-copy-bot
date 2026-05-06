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


def package_bins(metadata, package_name):
    for package in metadata.get("packages", []):
        if package.get("name") != package_name:
            continue
        bins = sorted(
            target.get("name")
            for target in package.get("targets", [])
            if "bin" in target.get("kind", []) and target.get("name")
        )
        return bins
    return None


def main():
    parser = argparse.ArgumentParser(description="List all Cargo bin targets for a package")
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
