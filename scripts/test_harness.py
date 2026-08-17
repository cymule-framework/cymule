#!/usr/bin/env python3
"""Route repository changes to independent, reproducible verification suites."""

from __future__ import annotations

import argparse
import fnmatch
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import tomllib
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
MANIFEST = ROOT / "tests" / "harness" / "suites.toml"


def load_manifest(path: Path = MANIFEST) -> dict[str, Any]:
    """Load and validate the suite manifest."""
    with path.open("rb") as stream:
        manifest = tomllib.load(stream)
    return validate_manifest(manifest)


def validate_manifest(manifest: dict[str, Any]) -> dict[str, Any]:
    """Validate one already decoded suite and routing catalog."""
    if manifest.get("schema_version") != 1:
        raise ValueError("unsupported test harness manifest version")
    suites = manifest.get("suites")
    if not isinstance(suites, dict) or "full" not in suites:
        raise ValueError("test harness manifest must define suites and full")
    for name, suite in suites.items():
        if not isinstance(suite, dict) or not isinstance(suite.get("description"), str):
            raise ValueError(f"suite {name} has no description")
        if suite.get("abstract", False):
            if not suite.get("requires"):
                raise ValueError(f"abstract suite {name} has no requirements")
        elif not suite.get("commands"):
            raise ValueError(f"suite {name} has no commands")
    routes = manifest.get("routes")
    if not isinstance(routes, list) or not routes:
        raise ValueError("test harness manifest must define routes")
    for index, route in enumerate(routes):
        patterns = route.get("patterns") if isinstance(route, dict) else None
        selected = route.get("suites") if isinstance(route, dict) else None
        if (
            not isinstance(patterns, list)
            or not patterns
            or not all(isinstance(value, str) and value for value in patterns)
        ):
            raise ValueError(f"route {index} has invalid patterns")
        if (
            not isinstance(selected, list)
            or not selected
            or not all(isinstance(value, str) for value in selected)
        ):
            raise ValueError(f"route {index} has invalid suites")
        unknown = sorted(set(selected) - suites.keys())
        if unknown:
            raise ValueError(f"route {index} references unknown suites: {', '.join(unknown)}")
    return manifest


def matches(path: str, pattern: str) -> bool:
    """Match a normalized repository path against one route pattern."""
    return path == pattern or fnmatch.fnmatchcase(path, pattern)


def select_suites(
    paths: list[str], manifest: dict[str, Any] | None = None
) -> tuple[list[str], dict[str, list[str]]]:
    """Return the conservative suite union and path evidence for a change set."""
    if manifest is None:
        manifest = load_manifest()
    selected: set[str] = set()
    evidence: dict[str, list[str]] = {}
    for raw_path in sorted(set(paths)):
        path = raw_path.replace(os.sep, "/")
        path_suites: set[str] = set()
        for route in manifest["routes"]:
            if any(matches(path, pattern) for pattern in route["patterns"]):
                path_suites.update(route["suites"])
        if not path_suites:
            path_suites.add("full")
        for suite in path_suites:
            selected.add(suite)
            evidence.setdefault(suite, []).append(path)
    if "full" in selected:
        return ["full"], {"full": sorted(set(sum(evidence.values(), [])))}
    return sorted(selected), evidence


def expand_suites(names: list[str], manifest: dict[str, Any]) -> list[str]:
    """Expand abstract suite dependencies in deterministic order."""
    suites = manifest["suites"]
    result: list[str] = []
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(name: str) -> None:
        if name not in suites:
            raise ValueError(f"unknown suite {name}")
        if name in visiting:
            raise ValueError(f"suite dependency cycle at {name}")
        if name in visited:
            return
        visiting.add(name)
        for requirement in suites[name].get("requires", []):
            visit(requirement)
        visiting.remove(name)
        visited.add(name)
        if not suites[name].get("abstract", False):
            result.append(name)

    for name in names:
        visit(name)
    return result


def git(*args: str) -> str:
    """Run one read-only Git query without optional repository locks."""
    environment = dict(os.environ)
    environment["GIT_OPTIONAL_LOCKS"] = "0"
    result = subprocess.run(
        ["git", "-C", str(ROOT), *args],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=environment,
    )
    return result.stdout.strip()


def changed_paths(base: str, head: str, include_worktree: bool) -> tuple[list[str], dict[str, str]]:
    """Resolve committed and optional worktree changes against a unique merge base."""
    base_sha = git("rev-parse", "--verify", f"{base}^{{commit}}")
    head_sha = git("rev-parse", "--verify", f"{head}^{{commit}}")
    merge_bases = git("merge-base", "--all", base_sha, head_sha).splitlines()
    if len(merge_bases) != 1:
        raise ValueError(f"base and head require one merge base; found {len(merge_bases)}")
    merge_base = merge_bases[0]
    paths = set(filter(None, git("diff", "--name-only", "--diff-filter=ACDMRTUXB", f"{merge_base}..{head_sha}").splitlines()))
    if include_worktree:
        paths.update(filter(None, git("diff", "--name-only", "--diff-filter=ACDMRTUXB").splitlines()))
        paths.update(filter(None, git("diff", "--cached", "--name-only", "--diff-filter=ACDMRTUXB").splitlines()))
        paths.update(filter(None, git("ls-files", "--others", "--exclude-standard").splitlines()))
    return sorted(paths), {"base": base_sha, "head": head_sha, "merge_base": merge_base}


def ci_matrix(names: list[str], manifest: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Group selected suites into independently executable CI lanes."""
    suites = manifest["suites"]
    lanes: dict[str, dict[str, Any]] = {}
    for name in expand_suites(names, manifest):
        suite = suites[name]
        lane_name = suite["lane"]
        lane = lanes.setdefault(lane_name, {"lane": lane_name, "suites": [], "tools": set()})
        lane["suites"].append(name)
        lane["tools"].update(suite.get("tools", []))
    include = []
    for lane_name in sorted(lanes):
        lane = lanes[lane_name]
        tools = lane.pop("tools")
        include.append(
            {
                **lane,
                "suite_args": " ".join(lane["suites"]),
                "rust": "rust" in tools,
                "node": "node" in tools,
                "uv": "uv" in tools,
                "go": "go" in tools,
            }
        )
    return {"include": include}


def write_report(report: dict[str, Any], path: Path) -> None:
    """Atomically publish one machine-readable harness report."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as stream:
        json.dump(report, stream, indent=2, sort_keys=True)
        stream.write("\n")
        temporary = Path(stream.name)
    os.replace(temporary, path)


def run_suites(names: list[str], manifest: dict[str, Any], keep_going: bool, report_path: Path | None) -> int:
    """Execute selected leaf suites and retain structured timing evidence."""
    expanded = expand_suites(names, manifest)
    if not expanded:
        raise ValueError("no suites selected")
    if report_path is None:
        stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        report_path = ROOT / ".cache" / "test-harness" / f"{stamp}-{os.getpid()}.json"
    report: dict[str, Any] = {
        "schema_version": 1,
        "repository": str(ROOT),
        "head": git("rev-parse", "HEAD"),
        "requested_suites": names,
        "expanded_suites": expanded,
        "started_at_unix_ms": int(time.time() * 1000),
        "results": [],
    }
    failed = False
    try:
        for name in expanded:
            suite = manifest["suites"][name]
            print(f"\n== {name}: {suite['description']} ==", flush=True)
            suite_result: dict[str, Any] = {"suite": name, "commands": []}
            suite_started = time.monotonic()
            for command in suite["commands"]:
                if not isinstance(command, list) or not all(isinstance(value, str) for value in command):
                    raise ValueError(f"suite {name} has an invalid command")
                print("+ " + " ".join(command), flush=True)
                command_started = time.monotonic()
                result = subprocess.run(command, cwd=ROOT, check=False)
                elapsed_ms = round((time.monotonic() - command_started) * 1000)
                suite_result["commands"].append(
                    {"argv": command, "exit_code": result.returncode, "duration_ms": elapsed_ms}
                )
                if result.returncode != 0:
                    failed = True
                    break
            suite_result["duration_ms"] = round((time.monotonic() - suite_started) * 1000)
            suite_result["status"] = "failed" if any(command["exit_code"] != 0 for command in suite_result["commands"]) else "passed"
            report["results"].append(suite_result)
            if failed and not keep_going:
                break
    finally:
        report["finished_at_unix_ms"] = int(time.time() * 1000)
        report["status"] = "failed" if failed else "passed"
        write_report(report, report_path)
        print(f"\nHarness report: {report_path}", flush=True)
    return 1 if failed else 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse the stable harness CLI."""
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("list", help="list available suites")

    plan = subparsers.add_parser("plan", help="select suites for a Git change set")
    plan.add_argument("--base", required=True)
    plan.add_argument("--head", default="HEAD")
    plan.add_argument("--no-worktree", action="store_true")
    plan.add_argument("--full-if-empty", action="store_true")
    plan.add_argument("--format", choices=("text", "json", "github-matrix"), default="text")
    plan.add_argument("--report", type=Path)

    run = subparsers.add_parser("run", help="run one or more named suites")
    run.add_argument("suites", nargs="+")
    run.add_argument("--keep-going", action="store_true")
    run.add_argument("--report", type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the harness CLI."""
    arguments = parse_args(sys.argv[1:] if argv is None else argv)
    manifest = load_manifest()
    if arguments.command == "list":
        for name, suite in manifest["suites"].items():
            print(f"{name:24} {suite['description']}")
        return 0
    if arguments.command == "plan":
        paths, revisions = changed_paths(arguments.base, arguments.head, not arguments.no_worktree)
        names, evidence = select_suites(paths, manifest)
        if not names and arguments.full_if_empty:
            names, evidence = ["full"], {"full": []}
        payload = {
            "schema_version": 1,
            "revisions": revisions,
            "paths": paths,
            "selected_suites": names,
            "expanded_suites": expand_suites(names, manifest) if names else [],
            "evidence": evidence,
        }
        if arguments.report is not None:
            write_report(payload, arguments.report)
        if arguments.format == "json":
            print(json.dumps(payload, indent=2, sort_keys=True))
        elif arguments.format == "github-matrix":
            print(json.dumps(ci_matrix(names, manifest), separators=(",", ":")))
        else:
            print(f"base: {revisions['base']}")
            print(f"head: {revisions['head']}")
            print("paths:")
            for path in paths:
                print(f"  {path}")
            print("suites:")
            for name in names:
                print(f"  {name}")
        return 0
    return run_suites(arguments.suites, manifest, arguments.keep_going, arguments.report)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, subprocess.CalledProcessError, ValueError) as error:
        print(f"test harness failed: {error}", file=sys.stderr)
        raise SystemExit(2) from error
