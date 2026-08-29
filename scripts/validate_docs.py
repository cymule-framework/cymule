#!/usr/bin/env python3
"""Fail when a repository-local Markdown link points at a missing path."""

from __future__ import annotations

from pathlib import Path
import re
import subprocess
import sys
from urllib.parse import unquote


ROOT = Path(__file__).resolve().parent.parent
LINK = re.compile(r"!?\[[^\]]*\]\((?P<target><[^>]+>|[^)\s]+)(?:\s+[\"'][^\"']*[\"'])?\)")
EXTERNAL_SCHEMES = ("http://", "https://", "mailto:", "data:")


def markdown_files() -> list[Path]:
    """Return authored Markdown files while excluding build and dependency trees."""
    ignored = {".git", ".cache", "target", "node_modules", ".venv"}
    return sorted(path for path in ROOT.rglob("*.md") if not ignored.intersection(path.relative_to(ROOT).parts))


def missing_links(path: Path) -> list[tuple[int, str]]:
    """Find missing file targets in one Markdown document."""
    missing: list[tuple[int, str]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        for match in LINK.finditer(line):
            raw_target = match.group("target").strip("<>")
            if not raw_target or raw_target.startswith("#") or raw_target.startswith(EXTERNAL_SCHEMES):
                continue
            target_text = unquote(raw_target.split("#", 1)[0])
            if not target_text:
                continue
            target = (path.parent / target_text).resolve()
            try:
                target.relative_to(ROOT)
            except ValueError:
                missing.append((line_number, raw_target))
                continue
            if not target.exists():
                missing.append((line_number, raw_target))
    return missing


def main() -> int:
    """Validate every authored Markdown file."""
    failures = []
    files = markdown_files()
    for path in files:
        for line_number, target in missing_links(path):
            failures.append(f"{path.relative_to(ROOT)}:{line_number}: missing local link {target}")
    if failures:
        print("\n".join(failures), file=sys.stderr)
        return 1
    subprocess.run(
        [sys.executable, str(ROOT / "scripts/version_domains.py"), "check-docs"],
        cwd=ROOT,
        check=True,
    )
    print(f"validated local links in {len(files)} Markdown files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
