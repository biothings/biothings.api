#!/usr/bin/env python3
"""Resolve a BioThings release intent to a semantic version and branch."""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass

VERSION_PATTERN = re.compile(r"(?<![0-9A-Za-z])v?(\d+)\.(\d+)(?:\.(\d+))?(?![0-9A-Za-z.-])", re.IGNORECASE)
LEVEL_PATTERN = re.compile(r"\b(major|minor|patch)\b", re.IGNORECASE)


@dataclass(frozen=True, order=True)
class Version:
    major: int
    minor: int
    patch: int

    @classmethod
    def parse(cls, value: str, *, allow_shorthand: bool) -> "Version":
        pattern = r"v?(\d+)\.(\d+)(?:\.(\d+))?" if allow_shorthand else r"v?(\d+)\.(\d+)\.(\d+)"
        match = re.fullmatch(pattern, value.strip(), re.IGNORECASE)
        if not match:
            expected = "MAJOR.MINOR or MAJOR.MINOR.PATCH" if allow_shorthand else "MAJOR.MINOR.PATCH"
            raise ValueError(f"Invalid version {value!r}; expected {expected}.")
        major, minor, patch = match.groups()
        return cls(int(major), int(minor), int(patch or 0))

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}"


def bump(current: Version, level: str) -> Version:
    if level == "major":
        return Version(current.major + 1, 0, 0)
    if level == "minor":
        return Version(current.major, current.minor + 1, 0)
    if level == "patch":
        return Version(current.major, current.minor, current.patch + 1)
    raise ValueError(f"Unknown release level: {level}")


def classify(current: Version, target: Version) -> str:
    if target == bump(current, "major"):
        return "major"
    if target == bump(current, "minor"):
        return "minor"
    if target == bump(current, "patch"):
        return "patch"
    return "explicit"


def resolve(current: Version, intent: str) -> dict[str, object]:
    version_matches = list(VERSION_PATTERN.finditer(intent))
    explicit_versions = {
        Version(int(match.group(1)), int(match.group(2)), int(match.group(3) or 0)) for match in version_matches
    }
    levels = {match.group(1).lower() for match in LEVEL_PATTERN.finditer(intent)}

    if len(explicit_versions) > 1:
        found = ", ".join(str(version) for version in sorted(explicit_versions))
        raise ValueError(f"Ambiguous request: multiple target versions found ({found}).")
    if len(levels) > 1:
        raise ValueError(f"Ambiguous request: multiple release levels found ({', '.join(sorted(levels))}).")
    if not explicit_versions and not levels:
        raise ValueError("No release version or level found; specify major, minor, patch, or an explicit version.")

    explicit = next(iter(explicit_versions), None)
    level = next(iter(levels), None)

    if explicit is not None and level is not None:
        expected = bump(current, level)
        if explicit != expected:
            raise ValueError(
                f"Conflicting request: {level} from {current} resolves to {expected}, not explicit target {explicit}."
            )
        target = explicit
        resolution = level
    elif explicit is not None:
        target = explicit
        resolution = classify(current, target)
    else:
        target = bump(current, level or "")
        resolution = level or ""

    if target <= current:
        raise ValueError(f"Target version {target} must be newer than current published version {current}.")

    return {
        "current_version": str(current),
        "target_version": str(target),
        "release_level": resolution,
        "dev_branch": f"{target.major}.{target.minor}.x",
        "tag": f"v{target}",
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--current", required=True, help="Current published MAJOR.MINOR.PATCH version")
    parser.add_argument("--intent", required=True, help="User's release request or explicit version")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        current = Version.parse(args.current, allow_shorthand=False)
        result = resolve(current, args.intent)
    except ValueError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2

    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
