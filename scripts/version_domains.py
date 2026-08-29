#!/usr/bin/env python3
"""Validate Cymule version authority and materialize its derived projections."""

from __future__ import annotations

import argparse
import ast
import base64
import functools
import fnmatch
import hashlib
import heapq
import json
import os
import pathlib
import re
import stat
import subprocess
import sys
import tomllib
import urllib.parse
from typing import Any


CONTROL_ROOT = pathlib.Path(__file__).resolve().parents[1]
CONFIGURED_RELEASE_WORKSPACE = os.environ.get("CYMULE_RELEASE_WORKSPACE")
if (
    CONFIGURED_RELEASE_WORKSPACE is not None
    and not pathlib.Path(CONFIGURED_RELEASE_WORKSPACE).is_absolute()
):
    raise ValueError("CYMULE_RELEASE_WORKSPACE must be an absolute path")
ROOT = (
    pathlib.Path(CONFIGURED_RELEASE_WORKSPACE).resolve()
    if CONFIGURED_RELEASE_WORKSPACE is not None
    else CONTROL_ROOT
)
TABLE_PATH = ROOT / "docs" / "version-domains.md"
SHA_PATTERN = re.compile(r"[0-9a-f]{40}")
STABLE_VERSION_PATTERN = re.compile(
    r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
)
VERSION_PATTERN = re.compile(
    r"(?<![a-z0-9._/-])cymule\.[a-z0-9.-]+/[1-9][0-9]*(?![a-z0-9._/-])"
)
RUST_CFG_TEST_MODULE = re.compile(
    r"#\s*\[\s*cfg\s*\(\s*test\s*\)\s*\]\s*"
    r"(?:#\s*\[[^\]]*\]\s*)*"
    r"(?:pub(?:\s*\([^)]*\))?\s+)?mod\s+[A-Za-z_][A-Za-z0-9_]*\s*\{",
    re.MULTILINE,
)
PUBLIC_RUST_VERSION = re.compile(
    r"pub(?:\([^)]*\))?\s+const\s+([A-Z][A-Z0-9_]*)\s*:[^=]+?=\s*"
    r'"(cymule\.[a-z0-9.-]+/[1-9][0-9]*)"',
    re.DOTALL,
)
DIRECT_CONTENT_ID_SOURCE = re.compile(
    r"(?<![A-Za-z0-9_])(?:cymule_core::)?content_id\s*\(\s*"
    r"(?:[A-Za-z_][A-Za-z0-9_]*\s*::\s*)*([A-Z][A-Z0-9_]*)\b"
)
RUST_RAW_STRING_START = re.compile(r'(?:br|cr|r)(?P<hashes>#{0,255})"')
NON_CARGO_PACKAGES = {
    "release-governance",
    "schema-governance",
    "sdk-go",
    "sdk-python",
    "sdk-typescript",
}
MAX_EXACT_INTEGER = 9_007_199_254_740_991
REGISTRY_VERSION = "cymule.version-domain-registry/3"
RELEASE_BOM_VERSION = "cymule.release-bom/2"
NPM_REGISTRY = "https://registry.npmjs.org/"
CRATES_REGISTRY = "https://crates.io/"
NPM_SIGSTORE_CERTIFICATE_IDENTITY = (
    "https://github.com/cymule-framework/cymule/"
    ".github/workflows/publish-npm-controller.yml@refs/heads/main"
)
NPM_SIGSTORE_CERTIFICATE_ISSUER = "https://token.actions.githubusercontent.com"
NPM_SLSA_PROVENANCE = "https://slsa.dev/provenance/v1"
PUBLIC_SOURCE_SNAPSHOT_VERSION = "cymule.public-source-snapshot/1"
PUBLIC_SOURCE_EXCLUDED_PATHS = frozenset(
    {
        ".gitlab-ci.yml",
        ".github/workflows/mirror.yml",
        "versioning/version-domains.json",
    }
)
PUBLIC_SOURCE_EXCLUDED_PREFIXES = (".gitlab/",)
PRODUCTION_IDENTITY_GLOBS = (
    ".github/workflows/*.yml",
    "Cargo.toml",
    "crates/*/Cargo.toml",
    "crates/*/src/**/*.rs",
    "plugins/*/Cargo.toml",
    "plugins/*/src/**/*.rs",
    "scripts/*.toml",
    "sdk/go/go.mod",
    "sdk/go/**/*.go",
    "sdk/python/pyproject.toml",
    "sdk/python/src/**/*.py",
    "sdk/typescript/package.json",
    "sdk/typescript/src/**/*.ts",
    "schemas/*.schema.json",
    "plugins/*/schemas/**/*.schema.json",
    "scripts/crates_release.py",
    "scripts/npm_release.py",
    "scripts/version_domains.py",
    "scripts/validate_schemas.py",
)
CANONICAL_ENGINE_RECEIPT_SOURCE_SYMBOLS = frozenset(
    {
        "EFFECT_RESOLUTION_RECEIPT_VERSION",
        "RUN_CANCELLATION_RECEIPT_VERSION",
    }
)
CONTENT_ID_SOURCE_ROLES = frozenset({"catalog_namespace", "content_id_domain"})


def _validate_i_json_string(value: str) -> None:
    try:
        value.encode("utf-8", errors="strict")
        value.encode("utf-16-be", errors="strict")
    except UnicodeEncodeError as error:
        raise ValueError("version authority JSON contains an unpaired surrogate") from error


def _utf16_sort_key(value: str) -> bytes:
    _validate_i_json_string(value)
    return value.encode("utf-16-be")


def canonical_bytes(value: Any) -> bytes:
    """Encode the no-float I-JSON authority subset as RFC 8785 bytes."""

    def encode(item: Any) -> bytes:
        if item is None:
            return b"null"
        if item is True:
            return b"true"
        if item is False:
            return b"false"
        if isinstance(item, int):
            if abs(item) > MAX_EXACT_INTEGER:
                raise ValueError(
                    "version authority JSON integer exceeds the exact I-JSON range"
                )
            return str(item).encode("ascii")
        if isinstance(item, float):
            raise ValueError("version authority JSON must not contain floating-point values")
        if isinstance(item, str):
            _validate_i_json_string(item)
            return json.dumps(item, ensure_ascii=False, separators=(",", ":")).encode(
                "utf-8"
            )
        if isinstance(item, list):
            return b"[" + b",".join(encode(child) for child in item) + b"]"
        if isinstance(item, dict):
            if not all(isinstance(key, str) for key in item):
                raise ValueError("version authority JSON object keys must be strings")
            members = []
            for key in sorted(item, key=_utf16_sort_key):
                members.append(encode(key) + b":" + encode(item[key]))
            return b"{" + b",".join(members) + b"}"
        raise ValueError(
            f"version authority JSON contains unsupported {type(item).__name__}"
        )

    return encode(value)


def digest_bytes(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def digest_json(value: Any) -> str:
    return digest_bytes(canonical_bytes(value))


def _reject_duplicate_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, child in pairs:
        if key in value:
            raise ValueError(f"version authority JSON repeats object member {key!r}")
        value[key] = child
    return value


def load_json_bytes(value: bytes, *, label: str) -> Any:
    try:
        decoded = value.decode("utf-8", errors="strict")
        loaded = json.loads(
            decoded,
            object_pairs_hook=_reject_duplicate_object,
            parse_float=lambda _value: (_ for _ in ()).throw(
                ValueError("version authority JSON must not contain floating-point values")
            ),
            parse_constant=lambda constant: (_ for _ in ()).throw(
                ValueError(f"version authority JSON contains invalid number {constant}")
            ),
        )
        canonical_bytes(loaded)
        return loaded
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
        raise ValueError(f"{label} is not strict I-JSON: {error}") from error


def load_json(path: pathlib.Path) -> Any:
    return load_json_bytes(path.read_bytes(), label=str(path))


REGISTRY_CLOSED_OBJECT_DEFS = (
    "sourceGeneration",
    "defaults",
    "migration",
    "schema",
    "source",
    "consumers",
    "domain",
)


def registry_closed_object_fields(
    registry_schema: dict[str, Any], definition: str | None
) -> tuple[set[str], set[str]]:
    """Read one fixed closed-object contract from the registry schema."""

    node = registry_schema if definition is None else registry_schema["$defs"][definition]
    if (
        node.get("type") != "object"
        or node.get("additionalProperties") is not False
        or not isinstance(node.get("properties"), dict)
        or not isinstance(node.get("required"), list)
        or not all(isinstance(field, str) for field in node["required"])
    ):
        label = "document" if definition is None else definition
        raise ValueError(f"registry schema {label} is not a fixed closed object")
    properties = set(node["properties"])
    required = set(node["required"])
    if not required.issubset(properties):
        label = "document" if definition is None else definition
        raise ValueError(f"registry schema {label} requires an unknown property")
    return properties, required


def validate_registry_closed_shape(
    value: dict[str, Any], root: pathlib.Path = ROOT
) -> None:
    """Reject unknown or missing object members using the authored registry schema."""

    registry_schema = load_json(root / "schemas/version-domain-registry.schema.json")
    if not isinstance(registry_schema, dict) or not isinstance(
        registry_schema.get("$defs"), dict
    ):
        raise ValueError("registry schema has no fixed definitions")
    definitions = registry_schema["$defs"]
    missing_definitions = set(REGISTRY_CLOSED_OBJECT_DEFS) - set(definitions)
    if missing_definitions:
        raise ValueError(
            f"registry schema omits closed definitions {sorted(missing_definitions)}"
        )

    expected_refs = (
        (registry_schema["properties"]["source_generation"], "sourceGeneration"),
        (registry_schema["properties"]["defaults"], "defaults"),
        (registry_schema["properties"]["domains"]["items"], "domain"),
        (definitions["defaults"]["properties"]["migration"], "migration"),
        (definitions["domain"]["properties"]["schemas"]["items"], "schema"),
        (definitions["domain"]["properties"]["sources"]["items"], "source"),
        (definitions["domain"]["properties"]["consumers"], "consumers"),
    )
    for node, definition in expected_refs:
        if node.get("$ref") != f"#/$defs/{definition}":
            raise ValueError(
                f"registry schema no longer routes the closed {definition} object"
            )
    migration_union = definitions["domain"]["properties"]["migration"].get("oneOf")
    if (
        not isinstance(migration_union, list)
        or len(migration_union) != 2
        or {json.dumps(item, sort_keys=True) for item in migration_union}
        != {
            json.dumps({"$ref": "#/$defs/migration"}, sort_keys=True),
            json.dumps({"type": "null"}, sort_keys=True),
        }
    ):
        raise ValueError("registry schema no longer routes the nullable migration object")

    for definition in (None, *REGISTRY_CLOSED_OBJECT_DEFS):
        registry_closed_object_fields(registry_schema, definition)

    def closed_object(item: Any, definition: str | None, label: str) -> dict[str, Any]:
        if not isinstance(item, dict):
            raise ValueError(f"{label} must be an object")
        properties, required = registry_closed_object_fields(
            registry_schema, definition
        )
        unknown = set(item) - properties
        missing = required - set(item)
        if unknown:
            raise ValueError(f"{label} has unknown fields {sorted(unknown)}")
        if missing:
            raise ValueError(f"{label} omits required fields {sorted(missing)}")
        return item

    document = closed_object(value, None, "version-domain registry")
    closed_object(
        document["source_generation"],
        "sourceGeneration",
        "version-domain registry source_generation",
    )
    defaults = closed_object(
        document["defaults"], "defaults", "version-domain registry defaults"
    )
    closed_object(
        defaults["migration"],
        "migration",
        "version-domain registry defaults.migration",
    )
    if not isinstance(document["domains"], list):
        raise ValueError("version-domain registry domains must be an array")
    for index, raw_domain in enumerate(document["domains"]):
        label = f"version-domain registry domains[{index}]"
        domain = closed_object(raw_domain, "domain", label)
        if not isinstance(domain["sources"], list) or not isinstance(
            domain["schemas"], list
        ):
            raise ValueError(f"{label} sources and schemas must be arrays")
        for source_index, source in enumerate(domain["sources"]):
            closed_object(source, "source", f"{label}.sources[{source_index}]")
        for schema_index, schema in enumerate(domain["schemas"]):
            closed_object(schema, "schema", f"{label}.schemas[{schema_index}]")
        closed_object(domain["consumers"], "consumers", f"{label}.consumers")
        if isinstance(domain["migration"], dict):
            closed_object(domain["migration"], "migration", f"{label}.migration")


def validate_registry_schema(
    value: dict[str, Any], root: pathlib.Path = ROOT
) -> None:
    """Validate the complete authored registry contract as Draft 2020-12."""

    try:
        from jsonschema import Draft202012Validator, FormatChecker
    except ImportError as error:
        raise ValueError(
            "version-domain verification requires jsonschema 4.26.0; "
            "run it through the pinned sdk/python uv environment"
        ) from error
    registry_schema = load_json(root / "schemas/version-domain-registry.schema.json")
    Draft202012Validator.check_schema(registry_schema)
    validator = Draft202012Validator(
        registry_schema,
        format_checker=FormatChecker(),
    )
    failures = sorted(
        validator.iter_errors(value),
        key=lambda item: (
            tuple(str(part) for part in item.absolute_path),
            tuple(str(part) for part in item.absolute_schema_path),
        ),
    )
    if failures:
        failure = failures[0]
        path = "/" + "/".join(str(item) for item in failure.absolute_path)
        raise ValueError(
            f"version-domain registry violates Draft 2020-12 at {path}: "
            f"{failure.message}"
        )


def load_registry(root: pathlib.Path = ROOT) -> dict[str, Any]:
    value = load_json(root / "versioning/version-domains.json")
    if not isinstance(value, dict):
        raise ValueError("version-domain registry must be an object")
    validate_registry_closed_shape(value, root)
    return value


def git_output(
    arguments: list[str], *, text: bool = True, root: pathlib.Path = ROOT
) -> str | bytes:
    result = subprocess.run(
        ["git", *arguments],
        cwd=root,
        check=True,
        capture_output=True,
        text=text,
    )
    return result.stdout


def git_blob(
    source_sha: str, relative: str, *, root: pathlib.Path = ROOT
) -> bytes | None:
    result = subprocess.run(
        ["git", "show", f"{source_sha}:{relative}"],
        cwd=root,
        check=False,
        capture_output=True,
    )
    return result.stdout if result.returncode == 0 else None


def registry_digest(root: pathlib.Path = ROOT) -> str:
    """Return the one canonical identity of a source generation's registry."""

    return digest_json(load_registry(root))


def public_source_path(relative: str) -> bool:
    return relative not in PUBLIC_SOURCE_EXCLUDED_PATHS and not relative.startswith(
        PUBLIC_SOURCE_EXCLUDED_PREFIXES
    )


def source_snapshot_digest(entries: list[tuple[str, str, str, bytes]]) -> str:
    """Hash one ordered public export over Git paths, modes, types, and bytes."""

    normalized = sorted(entries, key=lambda item: item[0].encode("utf-8"))
    paths = [path for path, _mode, _kind, _payload in normalized]
    if paths != sorted(set(paths), key=lambda item: item.encode("utf-8")):
        raise ValueError("public source snapshot paths must be unique")
    digest = hashlib.sha256()

    def add(value: bytes) -> None:
        digest.update(len(value).to_bytes(8, "big"))
        digest.update(value)

    add(PUBLIC_SOURCE_SNAPSHOT_VERSION.encode("ascii"))
    digest.update(len(normalized).to_bytes(8, "big"))
    for relative, mode, kind, payload in normalized:
        if mode not in {"100644", "100755", "120000"} or kind != "blob":
            raise ValueError(
                f"public source snapshot contains unsupported Git entry "
                f"{mode} {kind} at {relative}"
            )
        add(relative.encode("utf-8"))
        add(mode.encode("ascii"))
        add(kind.encode("ascii"))
        add(payload)
    return "sha256:" + digest.hexdigest()


def candidate_git_paths(root: pathlib.Path = ROOT) -> list[str]:
    """Return every tracked or unignored untracked path in the source candidate."""

    encoded_paths = bytes(
        git_output(
            ["ls-files", "--cached", "--others", "--exclude-standard", "-z"],
            text=False,
            root=root,
        )
    ).split(b"\0")
    return [
        encoded.decode("utf-8", errors="strict")
        for encoded in encoded_paths
        if encoded
    ]


def current_source_snapshot_digest(root: pathlib.Path = ROOT) -> str:
    entries: list[tuple[str, str, str, bytes]] = []
    for relative in candidate_git_paths(root):
        if not public_source_path(relative):
            continue
        path = root / relative
        if path.is_symlink():
            mode = "120000"
            payload = os.readlink(path).encode("utf-8")
        elif path.is_file():
            mode = "100755" if path.stat().st_mode & stat.S_IXUSR else "100644"
            payload = path.read_bytes()
        else:
            # A tracked deletion is absent from the candidate snapshot.
            continue
        entries.append((relative, mode, "blob", payload))
    return source_snapshot_digest(entries)


def _commit_tree(root: pathlib.Path, commit: str) -> list[tuple[str, str, str, str]]:
    raw = bytes(
        git_output(
            ["ls-tree", "-r", "-z", "--full-tree", commit],
            text=False,
            root=root,
        )
    )
    entries: list[tuple[str, str, str, str]] = []
    for encoded in raw.split(b"\0"):
        if not encoded:
            continue
        metadata, encoded_path = encoded.split(b"\t", 1)
        mode, kind, encoded_oid = metadata.split(b" ", 2)
        relative = encoded_path.decode("utf-8", errors="strict")
        if not public_source_path(relative):
            continue
        decoded_mode = mode.decode("ascii")
        decoded_kind = kind.decode("ascii")
        if decoded_mode not in {"100644", "100755", "120000"} or kind != b"blob":
            raise ValueError(
                f"public source snapshot contains unsupported Git entry "
                f"{decoded_mode} {decoded_kind} at {relative}"
            )
        entries.append(
            (relative, decoded_mode, decoded_kind, encoded_oid.decode("ascii"))
        )
    return entries


def _git_blobs(root: pathlib.Path, object_ids: set[str]) -> dict[str, bytes]:
    ordered = sorted(object_ids)
    if not ordered:
        return {}
    result = subprocess.run(
        ["git", "cat-file", "--batch"],
        cwd=root,
        check=True,
        input=("\n".join(ordered) + "\n").encode("ascii"),
        capture_output=True,
    ).stdout
    cursor = 0
    blobs: dict[str, bytes] = {}
    for expected in ordered:
        line_end = result.find(b"\n", cursor)
        if line_end < 0:
            raise ValueError("git cat-file returned a truncated header")
        header = result[cursor:line_end].decode("ascii").split()
        cursor = line_end + 1
        if len(header) != 3 or header[0] != expected or header[1] != "blob":
            raise ValueError(f"git cat-file returned invalid object {header}")
        size = int(header[2])
        payload = result[cursor : cursor + size]
        cursor += size
        if len(payload) != size or result[cursor : cursor + 1] != b"\n":
            raise ValueError("git cat-file returned truncated blob bytes")
        cursor += 1
        blobs[expected] = payload
    if cursor != len(result):
        raise ValueError("git cat-file returned trailing bytes")
    return blobs


def source_snapshot_history(
    root: pathlib.Path = ROOT,
) -> list[tuple[str, str]]:
    head = str(git_output(["rev-parse", "HEAD"], root=root)).strip()
    return list(_source_snapshot_history(root.resolve(), head))


@functools.lru_cache(maxsize=4)
def _source_snapshot_history(
    root: pathlib.Path, head: str,
) -> tuple[tuple[str, str], ...]:
    commits = str(git_output(["rev-list", head], root=root)).splitlines()
    trees = {commit: _commit_tree(root, commit) for commit in commits}
    blobs = _git_blobs(
        root,
        {
            object_id
            for entries in trees.values()
            for _relative, _mode, _kind, object_id in entries
        },
    )
    return tuple(
        (
            commit,
            source_snapshot_digest(
                [
                    (relative, mode, kind, blobs[object_id])
                    for relative, mode, kind, object_id in trees[commit]
                ]
            ),
        )
        for commit in commits
    )


@functools.lru_cache(maxsize=16)
def rust_code_mask(source: str) -> str:
    """Mask Rust comments and literals while retaining code offsets and newlines."""

    masked = list(source)

    def erase(start: int, end: int) -> None:
        for index in range(start, end):
            if masked[index] != "\n":
                masked[index] = " "

    index = 0
    while index < len(source):
        if source.startswith("//", index):
            end = source.find("\n", index + 2)
            end = len(source) if end == -1 else end
            erase(index, end)
            index = end
            continue
        if source.startswith("/*", index):
            start = index
            depth = 1
            index += 2
            while index < len(source) and depth:
                if source.startswith("/*", index):
                    depth += 1
                    index += 2
                elif source.startswith("*/", index):
                    depth -= 1
                    index += 2
                else:
                    index += 1
            if depth:
                raise ValueError("unterminated Rust block comment")
            erase(start, index)
            continue

        raw = RUST_RAW_STRING_START.match(source, index)
        if raw is not None and (
            index == 0 or not (source[index - 1].isalnum() or source[index - 1] == "_")
        ):
            start = index
            delimiter = '"' + raw.group("hashes")
            body = raw.end()
            end = source.find(delimiter, body)
            if end == -1:
                raise ValueError("unterminated Rust raw string")
            index = end + len(delimiter)
            erase(start, index)
            continue

        if source[index] == '"':
            start = index
            index += 1
            while index < len(source):
                if source[index] == "\\":
                    index += 2
                elif source[index] == '"':
                    index += 1
                    break
                else:
                    index += 1
            else:
                raise ValueError("unterminated Rust string")
            erase(start, index)
            continue

        if source[index] == "'":
            end: int | None = None
            if index + 2 < len(source) and source[index + 2] == "'":
                end = index + 3
            elif index + 2 < len(source) and source[index + 1] == "\\":
                cursor = index + 2
                while cursor < len(source) and source[cursor] != "\n":
                    if source[cursor] == "'" and source[cursor - 1] != "\\":
                        end = cursor + 1
                        break
                    cursor += 1
            if end is not None:
                erase(index, end)
                index = end
                continue
        index += 1
    return "".join(masked)


@functools.lru_cache(maxsize=16)
def rust_comment_mask(source: str) -> str:
    """Mask Rust comments while preserving literal bytes and source offsets."""

    masked = list(source)

    def erase(start: int, end: int) -> None:
        for offset in range(start, end):
            if masked[offset] != "\n":
                masked[offset] = " "

    index = 0
    while index < len(source):
        if source.startswith("//", index):
            end = source.find("\n", index + 2)
            end = len(source) if end == -1 else end
            erase(index, end)
            index = end
            continue
        if source.startswith("/*", index):
            start = index
            depth = 1
            index += 2
            while index < len(source) and depth:
                if source.startswith("/*", index):
                    depth += 1
                    index += 2
                elif source.startswith("*/", index):
                    depth -= 1
                    index += 2
                else:
                    index += 1
            if depth:
                raise ValueError("unterminated Rust block comment")
            erase(start, index)
            continue

        raw = RUST_RAW_STRING_START.match(source, index)
        if raw is not None and (
            index == 0 or not (source[index - 1].isalnum() or source[index - 1] == "_")
        ):
            delimiter = '"' + raw.group("hashes")
            body = raw.end()
            end = source.find(delimiter, body)
            if end == -1:
                raise ValueError("unterminated Rust raw string")
            index = end + len(delimiter)
            continue

        if source[index] == '"':
            index += 1
            while index < len(source):
                if source[index] == "\\":
                    index += 2
                elif source[index] == '"':
                    index += 1
                    break
                else:
                    index += 1
            else:
                raise ValueError("unterminated Rust string")
            continue

        if source[index] == "'":
            end: int | None = None
            if index + 2 < len(source) and source[index + 2] == "'":
                end = index + 3
            elif index + 2 < len(source) and source[index + 1] == "\\":
                cursor = index + 2
                while cursor < len(source) and source[cursor] != "\n":
                    if source[cursor] == "'" and source[cursor - 1] != "\\":
                        end = cursor + 1
                        break
                    cursor += 1
            if end is not None:
                index = end
                continue
        index += 1
    return "".join(masked)


def rust_string_literal_at(source: str, index: int) -> tuple[str, int] | None:
    """Decode the unescaped ASCII subset used by version authority literals."""

    if index > 0 and (source[index - 1].isalnum() or source[index - 1] == "_"):
        return None
    raw = RUST_RAW_STRING_START.match(source, index)
    if raw is not None:
        delimiter = '"' + raw.group("hashes")
        body_start = raw.end()
        body_end = source.find(delimiter, body_start)
        if body_end == -1:
            raise ValueError("unterminated Rust raw string")
        return source[body_start:body_end], body_end + len(delimiter)

    prefix = 2 if source.startswith(("b\"", "c\""), index) else 1
    if prefix == 1 and (index >= len(source) or source[index] != '"'):
        return None
    body_start = index + prefix
    cursor = body_start
    escaped = False
    while cursor < len(source):
        if source[cursor] == "\\":
            escaped = True
            cursor += 2
        elif source[cursor] == '"':
            body = source[body_start:cursor]
            return ("" if escaped else body), cursor + 1
        else:
            cursor += 1
    raise ValueError("unterminated Rust string")


def rust_skip_space_and_comments(source: str, index: int) -> int:
    """Advance over whitespace and nested comments between `=` and a literal."""

    while index < len(source):
        if source[index].isspace():
            index += 1
            continue
        if source.startswith("//", index):
            end = source.find("\n", index + 2)
            index = len(source) if end == -1 else end + 1
            continue
        if source.startswith("/*", index):
            depth = 1
            index += 2
            while index < len(source) and depth:
                if source.startswith("/*", index):
                    depth += 1
                    index += 2
                elif source.startswith("*/", index):
                    depth -= 1
                    index += 2
                else:
                    index += 1
            if depth:
                raise ValueError("unterminated Rust block comment")
            continue
        break
    return index


def rust_exact_literal_count(source: str, expected: str) -> int:
    """Count exact standalone Rust string/byte-string tokens outside comments."""

    count = 0
    index = 0
    while index < len(source):
        if source.startswith("//", index):
            end = source.find("\n", index + 2)
            index = len(source) if end == -1 else end + 1
            continue
        if source.startswith("/*", index):
            index = rust_skip_space_and_comments(source, index)
            continue
        literal = rust_string_literal_at(source, index)
        if literal is not None:
            value, index = literal
            count += value == expected
            continue
        index += 1
    return count


@functools.lru_cache(maxsize=16)
def rust_production_text(source: str) -> str:
    """Remove syntactically bounded `#[cfg(test)] mod` bodies from a Rust source."""

    masked = rust_code_mask(source)
    ranges: list[tuple[int, int]] = []
    covered_until = 0
    for match in RUST_CFG_TEST_MODULE.finditer(masked):
        if match.start() < covered_until:
            continue
        opening = masked.rfind("{", match.start(), match.end())
        depth = 0
        closing: int | None = None
        for index in range(opening, len(masked)):
            if masked[index] == "{":
                depth += 1
            elif masked[index] == "}":
                depth -= 1
                if depth == 0:
                    closing = index + 1
                    break
        if closing is None:
            raise ValueError("unterminated Rust #[cfg(test)] module")
        ranges.append((match.start(), closing))
        covered_until = closing

    if not ranges:
        return source
    production = list(source)
    for start, end in ranges:
        for index in range(start, end):
            if production[index] != "\n":
                production[index] = " "
    return "".join(production)


def rust_block_ranges(masked: str, declaration: re.Pattern[str]) -> list[tuple[int, int]]:
    ranges: list[tuple[int, int]] = []
    for match in declaration.finditer(masked):
        opening = masked.rfind("{", match.start(), match.end())
        depth = 0
        for index in range(opening, len(masked)):
            if masked[index] == "{":
                depth += 1
            elif masked[index] == "}":
                depth -= 1
                if depth == 0:
                    ranges.append((opening + 1, index))
                    break
        else:
            raise ValueError("unterminated Rust impl block")
    return ranges


def rust_source_anchor(text: str, symbol: str, version: str) -> bool:
    production = rust_production_text(text)
    if symbol == "$literal":
        return rust_exact_literal_count(production, version) == 1
    constant = symbol
    ranges = [(0, len(production))]
    masked = rust_code_mask(production)
    if "::" in symbol:
        owner, constant = symbol.rsplit("::", 1)
        declaration = re.compile(
            rf"\bimpl\s+{re.escape(owner)}\s*\{{",
            re.MULTILINE,
        )
        ranges = rust_block_ranges(masked, declaration)
        if not ranges:
            return False
    declaration = re.compile(
        rf"(?:pub(?:\s*\([^)]*\))?\s+)?const\s+{re.escape(constant)}"
        rf"\s*:[^=;]+?=",
        re.DOTALL,
    )
    matches = 0
    for start, end in ranges:
        for match in declaration.finditer(masked, start, end):
            cursor = rust_skip_space_and_comments(production, match.end())
            literal = rust_string_literal_at(production, cursor)
            if literal is not None and literal[0] == version:
                matches += 1
    return matches == 1


def python_source_anchor(text: str, symbol: str, version: str) -> bool:
    """Match one exact Python string assignment, never a textual prefix."""

    if not symbol.isidentifier():
        return False
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return False

    assignments: list[tuple[bool, ast.expr | None]] = []

    def binds_symbol(target: ast.expr) -> bool:
        if isinstance(target, ast.Name):
            return target.id == symbol
        if isinstance(target, (ast.List, ast.Tuple)):
            return any(binds_symbol(child) for child in target.elts)
        if isinstance(target, ast.Starred):
            return binds_symbol(target.value)
        return False

    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            if any(binds_symbol(target) for target in node.targets):
                exact_target = (
                    len(node.targets) == 1
                    and isinstance(node.targets[0], ast.Name)
                    and node.targets[0].id == symbol
                )
                assignments.append((exact_target, node.value))
        elif isinstance(node, ast.AnnAssign) and binds_symbol(node.target):
            assignments.append(
                (isinstance(node.target, ast.Name) and node.target.id == symbol, node.value)
            )
        elif isinstance(node, (ast.AugAssign, ast.NamedExpr)) and binds_symbol(
            node.target
        ):
            assignments.append((False, node.value))

    if len(assignments) != 1:
        return False
    exact_target, value = assignments[0]
    return (
        exact_target
        and isinstance(value, ast.Constant)
        and isinstance(value.value, str)
        and value.value == version
    )


def json_pointer(value: Any, pointer: str) -> Any:
    if pointer == "":
        return value
    if not pointer.startswith("/"):
        raise ValueError(f"JSON Pointer must start with '/': {pointer}")
    current = value
    for encoded in pointer[1:].split("/"):
        if re.search(r"~(?:[^01]|$)", encoded):
            raise ValueError(f"JSON Pointer has an invalid escape: {pointer}")
        token = encoded.replace("~1", "/").replace("~0", "~")
        if isinstance(current, dict) and token in current:
            current = current[token]
        elif isinstance(current, list) and token.isdigit() and int(token) < len(current):
            current = current[int(token)]
        else:
            raise ValueError(f"JSON Pointer does not resolve: {pointer}")
    return current


def source_anchor_matches(text: str, path: pathlib.Path, symbol: str, version: str) -> bool:
    if path.suffix == ".rs":
        return rust_source_anchor(text, symbol, version)
    if path.suffix == ".py":
        return python_source_anchor(text, symbol, version)
    if path.suffix == ".json":
        token_pointer = symbol.removeprefix("$token:")
        if not token_pointer.startswith("/"):
            return False
        try:
            anchored = json_pointer(
                load_json_bytes(text.encode("utf-8"), label=str(path)), token_pointer
            )
        except ValueError:
            return False
        if symbol.startswith("$token:"):
            return isinstance(anchored, str) and VERSION_PATTERN.findall(anchored) == [
                version
            ]
        return anchored == version
    return (
        text.splitlines().count(symbol) == 1
        and VERSION_PATTERN.findall(symbol) == [version]
    )


def production_identity_inventory(
    root: pathlib.Path = ROOT,
) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """Inventory production versions and named direct content-ID authorities."""

    paths = sorted(
        {
            path
            for pattern in PRODUCTION_IDENTITY_GLOBS
            for path in root.glob(pattern)
            if path.is_file() and not path.name.endswith("_test.go")
        }
    )
    found: dict[str, set[str]] = {}
    content_id_sources: dict[str, set[str]] = {}
    for path in paths:
        relative = path.relative_to(root).as_posix()
        source = path.read_text(encoding="utf-8")
        if path.suffix == ".rs":
            production = rust_production_text(source)
            for symbol in DIRECT_CONTENT_ID_SOURCE.findall(rust_code_mask(production)):
                content_id_sources.setdefault(symbol, set()).add(relative)
            source = rust_comment_mask(production)
        for version in VERSION_PATTERN.findall(source):
            found.setdefault(version, set()).add(relative)
    return (
        {version: sorted(locations) for version, locations in sorted(found.items())},
        {
            symbol: sorted(locations)
            for symbol, locations in sorted(content_id_sources.items())
        },
    )


def production_identity_literals(
    root: pathlib.Path = ROOT,
) -> dict[str, list[str]]:
    """Inventory current production version, Artifact-kind, and ID domains."""

    return production_identity_inventory(root)[0]


def validate_direct_content_id_sources(
    registry: dict[str, Any], usages: dict[str, list[str]]
) -> None:
    """Require named `content_id` separators to declare identity ownership."""

    for domain in registry["domains"]:
        for source in domain["sources"]:
            if (
                source["symbol"] in usages
                and source["role"] not in CONTENT_ID_SOURCE_ROLES
            ):
                raise ValueError(
                    f"{domain['version']} source {source['symbol']} is used directly by "
                    f"content_id at {usages[source['symbol']]} but has role "
                    f"{source['role']}"
                )


def cargo_metadata() -> dict[str, Any]:
    result = subprocess.run(
        ["cargo", "metadata", "--locked", "--format-version", "1", "--no-deps"],
        cwd=ROOT,
        check=True,
        text=True,
        capture_output=True,
    )
    return json.loads(result.stdout)


def cargo_graph(
    metadata: dict[str, Any] | None = None,
) -> tuple[dict[str, pathlib.Path], dict[str, set[str]]]:
    metadata = cargo_metadata() if metadata is None else metadata
    paths: dict[str, pathlib.Path] = {}
    direct: dict[str, set[str]] = {}
    for package in metadata["packages"]:
        name = package["name"]
        paths[name] = pathlib.Path(package["manifest_path"]).parent.resolve()
        direct[name] = {
            dependency["name"]
            for dependency in package["dependencies"]
            if dependency["name"].startswith("cymule")
        }
    return paths, direct


def deterministic_release_catalog_order(
    dependencies: dict[str, set[str]], preference: tuple[str, ...]
) -> tuple[str, ...]:
    """Return the stable dependency-first crate order or reject a cycle."""

    nodes = set(dependencies)
    unknown = set().union(*dependencies.values(), set()) - nodes
    if unknown:
        raise ValueError(
            f"release Cargo graph references unknown dependencies {sorted(unknown)}"
        )
    if len(preference) != len(nodes) or set(preference) != nodes:
        raise ValueError("release Cargo graph preference is incomplete")
    rank = {name: index for index, name in enumerate(preference)}
    remaining = {name: len(required) for name, required in dependencies.items()}
    dependents = {name: set() for name in nodes}
    for name, required in dependencies.items():
        for dependency in required:
            dependents[dependency].add(name)
    ready = [(rank[name], name) for name, count in remaining.items() if count == 0]
    heapq.heapify(ready)
    ordered: list[str] = []
    while ready:
        _, name = heapq.heappop(ready)
        ordered.append(name)
        for dependent in sorted(
            dependents[name], key=lambda value: (rank[value], value)
        ):
            remaining[dependent] -= 1
            if remaining[dependent] == 0:
                heapq.heappush(ready, (rank[dependent], dependent))
    if len(ordered) != len(nodes):
        cycle = sorted(name for name, count in remaining.items() if count > 0)
        raise ValueError(f"release Cargo publish graph contains a cycle: {cycle}")
    return tuple(ordered)


def resolved_manifest_dependency(
    dependency_name: str,
    dependency: object,
    workspace_dependencies: dict[str, Any],
) -> tuple[str, str | None]:
    """Resolve a manifest dependency's package and explicit-version presence."""

    resolved = dependency
    if isinstance(dependency, dict) and dependency.get("workspace") is True:
        resolved = workspace_dependencies.get(dependency_name)
        if resolved is None:
            raise ValueError(f"workspace dependency {dependency_name} is missing")
    if isinstance(resolved, str):
        return dependency_name, resolved
    if not isinstance(resolved, dict):
        raise ValueError(f"Cargo dependency {dependency_name} is malformed")
    package_name = resolved.get("package", dependency_name)
    if not isinstance(package_name, str) or not package_name:
        raise ValueError(f"Cargo dependency {dependency_name} has no package identity")
    version = resolved.get("version")
    if version is not None and not isinstance(version, str):
        raise ValueError(f"Cargo dependency {dependency_name} has a malformed version")
    return package_name, version


def manifest_publish_graph(
    root: pathlib.Path,
    catalog: list[dict[str, Any]],
    workspace_dependencies: dict[str, Any],
    workspace_version: str,
) -> dict[str, set[str]]:
    """Read the public edges Cargo retains in normalized package manifests."""

    public = {entry["name"] for entry in catalog}
    graph = {name: set() for name in public}
    table_kinds = (
        ("dependencies", "normal"),
        ("build-dependencies", "build"),
        ("dev-dependencies", "dev"),
    )
    for entry in catalog:
        manifest = tomllib.loads(
            (root / entry["path"] / "Cargo.toml").read_text(encoding="utf-8")
        )
        tables: list[tuple[str, object]] = [
            (kind, manifest.get(table_name, {})) for table_name, kind in table_kinds
        ]
        targets = manifest.get("target", {})
        if not isinstance(targets, dict):
            raise ValueError(f"Cargo targets are malformed for {entry['name']}")
        for target in targets.values():
            if not isinstance(target, dict):
                raise ValueError(f"Cargo target is malformed for {entry['name']}")
            tables.extend(
                (kind, target.get(table_name, {}))
                for table_name, kind in table_kinds
            )
        for kind, dependencies in tables:
            if not isinstance(dependencies, dict):
                raise ValueError(
                    f"Cargo dependencies are malformed for {entry['name']}"
                )
            for dependency_name, dependency in dependencies.items():
                package_name, version = resolved_manifest_dependency(
                    dependency_name, dependency, workspace_dependencies
                )
                if package_name not in public or (kind == "dev" and version is None):
                    continue
                if version != workspace_version:
                    raise ValueError(
                        f"{entry['name']} dependency {package_name} uses "
                        f"{version!r} instead of {workspace_version}"
                    )
                graph[entry["name"]].add(package_name)
    return graph


def metadata_dependency_is_published(dependency: dict[str, Any]) -> bool:
    """Match Cargo metadata edges retained by package normalization."""

    kind = dependency.get("kind")
    requirement = dependency.get("req")
    if kind not in (None, "build", "dev") or not isinstance(requirement, str):
        raise ValueError("Cargo metadata contains an unsupported dependency kind")
    return kind in (None, "build") or requirement != "*"


def release_catalog_entries(root: pathlib.Path = ROOT) -> list[dict[str, Any]]:
    """Load the ordered public crate catalog without executing payload tooling."""

    workspace_manifest = tomllib.loads(
        (root / "Cargo.toml").read_text(encoding="utf-8")
    )
    workspace = workspace_manifest.get("workspace")
    if not isinstance(workspace, dict):
        raise ValueError("release workspace manifest has no workspace authority")
    workspace_package = workspace.get("package")
    workspace_version = (
        workspace_package.get("version")
        if isinstance(workspace_package, dict)
        else None
    )
    if (
        not isinstance(workspace_version, str)
        or STABLE_VERSION_PATTERN.fullmatch(workspace_version) is None
    ):
        raise ValueError("release workspace has no exact stable Cargo version")
    members = workspace.get("members")
    if (
        not isinstance(members, list)
        or not members
        or not all(isinstance(member, str) for member in members)
        or len(members) != len(set(members))
        or workspace.get("exclude", []) != []
    ):
        raise ValueError("release workspace members must be one explicit closed list")
    member_manifests: dict[str, pathlib.Path] = {}
    member_paths: set[pathlib.Path] = set()
    published_manifests: dict[str, pathlib.Path] = {}
    for member in members:
        relative = pathlib.PurePosixPath(member)
        if (
            relative.is_absolute()
            or ".." in relative.parts
            or any(character in member for character in "*?[")
        ):
            raise ValueError(
                f"release workspace member is not one exact path: {member}"
            )
        manifest_path = root / relative / "Cargo.toml"
        if not manifest_path.is_file() or manifest_path.is_symlink():
            raise ValueError(f"workspace member manifest is missing for {member}")
        manifest = tomllib.loads(manifest_path.read_text(encoding="utf-8"))
        package = manifest.get("package")
        name = package.get("name") if isinstance(package, dict) else None
        if not isinstance(name, str) or not name or name in member_manifests:
            raise ValueError(
                f"workspace member has invalid or duplicate package at {member}"
            )
        resolved_path = manifest_path.parent.resolve()
        if resolved_path != root.resolve() / relative:
            raise ValueError(f"workspace member path is not canonical: {member}")
        member_manifests[name] = resolved_path
        member_paths.add(resolved_path)
        publish = package.get("publish")
        if publish is False or publish == []:
            continue
        published_manifests[name] = resolved_path

    workspace_dependencies = workspace.get("dependencies", {})
    if not isinstance(workspace_dependencies, dict):
        raise ValueError("release workspace dependencies are malformed")
    for name, dependency in workspace_dependencies.items():
        if not isinstance(name, str) or not isinstance(dependency, dict):
            continue
        dependency_path = dependency.get("path")
        if dependency_path is None:
            continue
        if not isinstance(dependency_path, str):
            raise ValueError(f"workspace dependency {name} has a malformed path")
        relative = pathlib.PurePosixPath(dependency_path)
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError(f"workspace dependency {name} escapes the workspace")
        if (root / relative).resolve() not in member_paths:
            raise ValueError(
                f"workspace dependency {name} is not an explicit workspace member"
            )
    dependency_table_names = (
        "dependencies",
        "build-dependencies",
        "dev-dependencies",
    )
    for member_name, member_path in member_manifests.items():
        manifest = tomllib.loads(
            (member_path / "Cargo.toml").read_text(encoding="utf-8")
        )
        dependency_tables = [
            manifest.get(table_name, {}) for table_name in dependency_table_names
        ]
        targets = manifest.get("target", {})
        if not isinstance(targets, dict):
            raise ValueError(f"Cargo targets are malformed for {member_name}")
        dependency_tables.extend(
            target.get(table_name, {})
            for target in targets.values()
            if isinstance(target, dict)
            for table_name in dependency_table_names
        )
        for dependencies in dependency_tables:
            if not isinstance(dependencies, dict):
                raise ValueError(
                    f"Cargo dependencies are malformed for {member_name}"
                )
            for dependency_name, dependency in dependencies.items():
                if not isinstance(dependency, dict) or "path" not in dependency:
                    continue
                dependency_path = dependency["path"]
                if not isinstance(dependency_path, str):
                    raise ValueError(
                        f"Cargo dependency {member_name}:{dependency_name} has a "
                        "malformed path"
                    )
                if (member_path / dependency_path).resolve() not in member_paths:
                    raise ValueError(
                        f"Cargo dependency {member_name}:{dependency_name} is not an "
                        "explicit workspace member"
                    )

    raw = tomllib.loads(
        (root / "scripts/crates-release.toml").read_text(encoding="utf-8")
    )
    if raw.get("schema") != 1 or not isinstance(raw.get("crate"), list):
        raise ValueError("unsupported or malformed crate release catalog")
    catalog: list[dict[str, Any]] = []
    seen: set[str] = set()
    for entry in raw["crate"]:
        if set(entry) != {"name", "path", "dependencies"}:
            raise ValueError("crate release catalog entry has unknown or missing fields")
        name = entry["name"]
        path = entry["path"]
        dependencies = entry["dependencies"]
        if (
            not isinstance(name, str)
            or not isinstance(path, str)
            or pathlib.PurePosixPath(path).is_absolute()
            or ".." in pathlib.PurePosixPath(path).parts
            or not isinstance(dependencies, list)
            or not all(isinstance(dependency, str) for dependency in dependencies)
            or dependencies != sorted(set(dependencies))
            or name in seen
        ):
            raise ValueError(f"invalid or duplicate public crate {name}")
        manifest_path = root / path / "Cargo.toml"
        if not manifest_path.is_file() or manifest_path.is_symlink():
            raise ValueError(f"catalog manifest is missing for {name}")
        manifest = tomllib.loads(manifest_path.read_text(encoding="utf-8"))
        package = manifest.get("package")
        if not isinstance(package, dict) or package.get("name") != name:
            raise ValueError(f"catalog path mismatch for {name}")
        if package.get("version") != {"workspace": True}:
            raise ValueError(f"catalog crate {name} must use the workspace version")
        if package.get("publish") != {"workspace": True}:
            raise ValueError(f"catalog crate {name} must use the workspace registry")
        seen.add(name)
        catalog.append({"name": name, "path": path, "dependencies": dependencies})
    if seen != set(published_manifests):
        raise ValueError(
            "crate release catalog mismatch: "
            f"missing={sorted(set(published_manifests) - seen)} "
            f"extra={sorted(seen - set(published_manifests))}"
        )
    for entry in catalog:
        if published_manifests[entry["name"]] != (root / entry["path"]).resolve():
            raise ValueError(f"catalog path mismatch for {entry['name']}")
    actual_graph = manifest_publish_graph(
        root, catalog, workspace_dependencies, workspace_version
    )
    catalog_order = tuple(entry["name"] for entry in catalog)
    actual_order = deterministic_release_catalog_order(actual_graph, catalog_order)
    if actual_order != catalog_order:
        raise ValueError(
            "crate catalog is not in Cargo dependency-first publish order: "
            f"expected {list(actual_order)}"
        )
    for entry in catalog:
        actual = sorted(actual_graph[entry["name"]])
        if actual != entry["dependencies"]:
            raise ValueError(
                f"catalog dependencies for {entry['name']} are "
                f"{entry['dependencies']}, Cargo manifests have {actual}"
            )
    return catalog


def release_catalog(metadata: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    metadata = cargo_metadata() if metadata is None else metadata
    packages = {package["name"]: package for package in metadata["packages"]}
    catalog = release_catalog_entries()
    seen = {entry["name"] for entry in catalog}
    for entry in catalog:
        name = entry["name"]
        if name not in packages:
            raise ValueError(f"public crate {name} is absent from Cargo metadata")
        package = packages[name]
        if pathlib.Path(package["manifest_path"]).parent.resolve() != (
            ROOT / entry["path"]
        ).resolve():
            raise ValueError(f"catalog path mismatch for {name}")
    published = {
        name
        for name, package in packages.items()
        if package.get("publish") == ["crates-io"]
    }
    if seen != published:
        raise ValueError(
            "crate release catalog mismatch: "
            f"missing={sorted(published - seen)} extra={sorted(seen - published)}"
        )
    actual_graph = {name: set() for name in seen}
    for entry in catalog:
        package = packages[entry["name"]]
        dependencies = package.get("dependencies")
        if not isinstance(dependencies, list):
            raise ValueError(f"Cargo metadata is malformed for {entry['name']}")
        for dependency in dependencies:
            if not isinstance(dependency, dict):
                raise ValueError(
                    f"Cargo metadata dependency is malformed for {entry['name']}"
                )
            dependency_name = dependency.get("name")
            if dependency_name in seen and metadata_dependency_is_published(
                dependency
            ):
                actual_graph[entry["name"]].add(dependency_name)
    catalog_order = tuple(entry["name"] for entry in catalog)
    actual_order = deterministic_release_catalog_order(actual_graph, catalog_order)
    if actual_order != catalog_order:
        raise ValueError(
            "crate catalog is not in Cargo dependency-first publish order: "
            f"expected {list(actual_order)}"
        )
    for entry in catalog:
        actual = sorted(actual_graph[entry["name"]])
        if actual != entry["dependencies"]:
            raise ValueError(
                f"catalog dependencies for {entry['name']} are {entry['dependencies']}, "
                f"Cargo has {actual}"
            )
    return catalog


def transitive_dependencies(direct: dict[str, set[str]], package: str) -> set[str]:
    pending = list(direct.get(package, set()))
    reached: set[str] = set()
    while pending:
        dependency = pending.pop()
        if dependency in reached:
            continue
        reached.add(dependency)
        pending.extend(direct.get(dependency, set()))
    return reached


def schema_fragment(record: dict[str, Any]) -> str:
    fragment = record.get("fragment", "#")
    if fragment == "#":
        if not record["root"]:
            raise ValueError(f"supporting schema {record['path']} requires an exact fragment")
        return fragment
    if record["root"]:
        raise ValueError(f"root schema {record['path']} must use the document fragment")
    return fragment


def schema_record_key(record: dict[str, Any]) -> tuple[str, str]:
    """Identify one owned contract fragment independently from its document."""

    return record["path"], schema_fragment(record)


def validate_schema_record_order(domain: dict[str, Any]) -> None:
    keys = [schema_record_key(record) for record in domain["schemas"]]
    if keys != sorted(set(keys)):
        raise ValueError(
            f"{domain['version']}.schemas must be sorted and (path, fragment)-unique"
        )


def schema_fragment_value(schema: dict[str, Any], fragment: str) -> Any:
    if fragment == "#":
        return schema
    if fragment.startswith("#/"):
        return json_pointer(schema, fragment[1:])
    if fragment.startswith("#") and len(fragment) > 1:
        anchor = fragment[1:]
        matches: list[Any] = []

        def find(value: Any) -> None:
            if isinstance(value, dict):
                if value.get("$anchor") == anchor:
                    matches.append(value)
                for child in value.values():
                    find(child)
            elif isinstance(value, list):
                for child in value:
                    find(child)

        find(schema)
        if len(matches) != 1:
            raise ValueError(f"schema anchor {fragment} does not resolve exactly once")
        return matches[0]
    raise ValueError(f"malformed schema fragment {fragment}")


def schema_fragment_projection(schema: dict[str, Any], fragment: str) -> dict[str, Any]:
    nodes: dict[str, Any] = {}

    def project(value: Any) -> Any:
        if isinstance(value, dict):
            output: dict[str, Any] = {}
            for key, child in value.items():
                if key in {"$defs", "definitions"}:
                    continue
                output[key] = project(child)
            reference = value.get("$ref")
            if isinstance(reference, str) and reference.startswith("#"):
                visit(reference)
            return output
        if isinstance(value, list):
            return [project(child) for child in value]
        return value

    def visit(target: str) -> None:
        if target in nodes:
            return
        nodes[target] = None
        nodes[target] = project(schema_fragment_value(schema, target))

    visit(fragment)
    return {"fragment": fragment, "nodes": nodes}


def schema_fragment_dependencies(
    schema: dict[str, Any],
    fragment: str,
    owners: list[tuple[str, str]],
    source_version: str,
) -> tuple[set[str], set[str]]:
    references: set[str] = set()
    versions: set[str] = set()
    visited: set[str] = set()

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            reference = value.get("$ref")
            if isinstance(reference, str):
                if reference.startswith("#"):
                    visit_fragment(reference, boundary=True)
                else:
                    references.add(reference)
            for key, child in value.items():
                if key not in {"$defs", "definitions", "$ref"}:
                    visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)
        elif isinstance(value, str):
            versions.update(VERSION_PATTERN.findall(value))

    def fragment_owner_versions(target: str) -> set[str]:
        exact = {
            version
            for owned, version in owners
            if owned != "#" and owned == target
        }
        if exact:
            return exact
        if target.startswith("#/"):
            ancestors = [
                (owned, version)
                for owned, version in owners
                if owned != "#" and target.startswith(owned + "/")
            ]
            if ancestors:
                longest = max(len(owned) for owned, _ in ancestors)
                return {
                    version for owned, version in ancestors if len(owned) == longest
                }
        return set()

    def visit_fragment(target: str, *, boundary: bool) -> None:
        if boundary:
            owned_versions = fragment_owner_versions(target)
            if owned_versions:
                versions.update(owned_versions - {source_version})
                return
        if target in visited:
            return
        visited.add(target)
        visit(schema_fragment_value(schema, target))

    visit_fragment(fragment, boundary=False)
    return references, versions


def require_sorted_unique(values: list[str], label: str) -> None:
    if values != sorted(set(values)):
        raise ValueError(f"{label} must be sorted and duplicate-free")


def package_for_path(
    path: pathlib.Path, cargo_paths: dict[str, pathlib.Path]
) -> str | None:
    resolved = path.resolve()
    package = next(
        (
            name
            for name, package_root in cargo_paths.items()
            if resolved.is_relative_to(package_root)
        ),
        None,
    )
    if package is not None:
        return package
    relative = resolved.relative_to(ROOT).as_posix()
    if relative.startswith("sdk/go/"):
        return "sdk-go"
    if relative.startswith("sdk/python/"):
        return "sdk-python"
    if relative.startswith("sdk/typescript/"):
        return "sdk-typescript"
    if (
        relative.startswith("schemas/")
        or "/schemas/" in relative
        or relative == "scripts/validate_schemas.py"
    ):
        return "schema-governance"
    if relative.startswith("scripts/"):
        return "release-governance"
    return None


def tracked_schema_paths(root: pathlib.Path = ROOT) -> list[pathlib.Path]:
    """Return every candidate root/plugin schema, including a pre-commit addition."""

    return sorted(
        {
            *root.glob("schemas/*.schema.json"),
            *root.glob("plugins/*/schemas/**/*.schema.json"),
        }
    )


REGISTRY_AUTHORITY_PATHS = (
    "versioning/version-domains.json",
    "schemas/version-domain-registry.schema.json",
    "scripts/version_domains.py",
)


def validate_registry_conformance(
    registry: dict[str, Any], root: pathlib.Path = ROOT
) -> None:
    """Bind registry evidence names and change routing to real harness leaves."""

    manifest_path = root / "tests/harness/suites.toml"
    try:
        manifest = tomllib.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise ValueError(f"cannot load harness suite authority {manifest_path}: {error}") from error
    suites = manifest.get("suites")
    routes = manifest.get("routes")
    if not isinstance(suites, dict) or not isinstance(routes, list):
        raise ValueError("harness suite authority omits suites or routes")
    leaves = {
        name
        for name, suite in suites.items()
        if isinstance(name, str)
        and isinstance(suite, dict)
        and not suite.get("abstract", False)
        and isinstance(suite.get("commands"), list)
        and bool(suite["commands"])
    }
    declared = {
        suite
        for domain in registry.get("domains", [])
        if isinstance(domain, dict)
        for suite in domain.get("conformance", [])
        if isinstance(suite, str)
    }
    unknown = sorted(declared - leaves)
    if unknown:
        raise ValueError(
            "version-domain conformance references non-leaf harness suites: "
            + ", ".join(unknown)
        )

    for authority_path in REGISTRY_AUTHORITY_PATHS:
        selected: set[str] = set()
        for route in routes:
            if not isinstance(route, dict):
                continue
            patterns = route.get("patterns")
            routed = route.get("suites")
            if not isinstance(patterns, list) or not isinstance(routed, list):
                continue
            if any(
                isinstance(pattern, str)
                and (
                    authority_path == pattern
                    or fnmatch.fnmatchcase(authority_path, pattern)
                )
                for pattern in patterns
            ):
                selected.update(suite for suite in routed if isinstance(suite, str))
        if "full" not in selected and not declared.issubset(selected):
            raise ValueError(
                f"registry authority route {authority_path} omits declared conformance "
                f"suites {sorted(declared - selected)} and does not select full"
            )


def validate_identity_source_dependencies(domain: dict[str, Any]) -> None:
    """Close canonical identity source roles over their JCS prerequisites."""

    version = domain["version"]
    roles = {source["role"] for source in domain["sources"]}
    if any(
        source["symbol"] in CANONICAL_ENGINE_RECEIPT_SOURCE_SYMBOLS
        for source in domain["sources"]
    ) and not (
        domain["kind"] == "receipt"
        and "content_id_domain" in roles
        and "cymule.jcs/1" in domain["depends_on"]
    ):
        raise ValueError(
            f"{version} canonical Engine receipt identity omits its "
            "content-ID source role or cymule.jcs/1"
        )
    if roles & CONTENT_ID_SOURCE_ROLES and "cymule.jcs/1" not in domain["depends_on"]:
        raise ValueError(f"{version} content identity omits cymule.jcs/1")
    if "artifact_kind" in roles and not {
        "cymule.artifact/2",
        "cymule.jcs/1",
    }.issubset(domain["depends_on"]):
        raise ValueError(f"{version} Artifact kind omits identity or canonical JSON")
    if "artifact_type_key" in roles and not {
        "cymule.artifact-type-contract/1",
        "cymule.jcs/1",
    }.issubset(domain["depends_on"]):
        raise ValueError(f"{version} Artifact type key omits its contract authority")


def validate_release_registry_closure(
    registry: dict[str, Any], root: pathlib.Path = ROOT
) -> None:
    """Validate the registry, schema, domain, and migration closure embedded by BOM/2."""

    validate_registry_closed_shape(registry, root)
    validate_registry_schema(registry, root)
    if registry.get("registry_version") != REGISTRY_VERSION:
        raise ValueError("unsupported version-domain registry")
    generation = registry["source_generation"]
    defaults = registry["defaults"]
    if generation["generation"] != defaults["introduced_release_generation"]:
        raise ValueError("registry defaults belong to another release generation")

    domains = registry["domains"]
    versions = [domain["version"] for domain in domains]
    if versions != sorted(versions) or len(versions) != len(set(versions)):
        raise ValueError("release registry domains are not one canonical unique order")
    by_version = {domain["version"]: domain for domain in domains}
    production_literals, content_id_sources = production_identity_inventory(root)
    validate_direct_content_id_sources(registry, content_id_sources)
    if set(by_version) != set(production_literals):
        raise ValueError(
            "release registry does not close the production domain set: "
            f"missing={sorted(set(production_literals) - set(by_version))} "
            f"extra={sorted(set(by_version) - set(production_literals))}"
        )

    schema_authorities: dict[pathlib.Path, tuple[str, str]] = {}
    for domain in domains:
        version = domain["version"]
        if (
            VERSION_PATTERN.fullmatch(version) is None
            or version.rsplit("/", 1)[0] != domain["domain"]
        ):
            raise ValueError(f"malformed release registry domain {version}")
        for field in (
            "literal_locations",
            "embeds",
            "depends_on",
            "generator_paths",
        ):
            require_sorted_unique(domain[field], f"{version}.{field}")
        if domain["literal_locations"] != production_literals[version]:
            raise ValueError(f"{version} release literal locations drifted from production")
        related = set(domain["embeds"]) | set(domain["depends_on"])
        missing_related = related - set(by_version)
        if missing_related:
            raise ValueError(
                f"{version} release dependency closure is missing {sorted(missing_related)}"
            )
        if version in related:
            raise ValueError(f"{version} references itself as a release dependency")
        if not set(domain["embeds"]).issubset(domain["depends_on"]):
            raise ValueError(f"{version} embeds a domain outside its release dependencies")
        validate_identity_source_dependencies(domain)

        for source in domain["sources"]:
            raw_path = root / source["path"]
            path = raw_path.resolve()
            if (
                not raw_path.is_file()
                or raw_path.is_symlink()
                or not path.is_relative_to(root.resolve())
            ):
                raise ValueError(f"{version} release source is missing: {source['path']}")
            if not source_anchor_matches(
                path.read_text(encoding="utf-8"), path, source["symbol"], version
            ):
                raise ValueError(
                    f"{version} release source anchor {source['path']}::{source['symbol']} drifted"
                )

        validate_schema_record_order(domain)
        for record in domain["schemas"]:
            raw_path = root / record["path"]
            path = raw_path.resolve()
            if (
                not raw_path.is_file()
                or raw_path.is_symlink()
                or not path.is_relative_to(root.resolve())
            ):
                raise ValueError(f"{version} release schema is missing: {record['path']}")
            schema = load_json(path)
            fragment = schema_fragment(record)
            schema_fragment_value(schema, fragment)
            identity = (record["id"], record["canonical_digest"])
            if schema.get("$id") != record["id"] or digest_json(schema) != identity[1]:
                raise ValueError(f"{version} release schema authority drifted")
            previous = schema_authorities.setdefault(path, identity)
            if previous != identity:
                raise ValueError(f"release schema {record['path']} has conflicting authorities")

        migration = effective(domain, defaults, "migration")
        runbook = migration["runbook"]
        if runbook is not None:
            raw_path = root / runbook
            path = raw_path.resolve()
            if (
                not raw_path.is_file()
                or raw_path.is_symlink()
                or not path.is_relative_to(root.resolve())
            ):
                raise ValueError(f"{version} release migration runbook is missing: {runbook}")

    tracked_schemas = {path.resolve() for path in tracked_schema_paths(root)}
    if set(schema_authorities) != tracked_schemas:
        raise ValueError(
            "release registry does not close the tracked schema set: "
            f"missing={sorted(path.relative_to(root).as_posix() for path in tracked_schemas - set(schema_authorities))} "
            f"extra={sorted(path.relative_to(root).as_posix() for path in set(schema_authorities) - tracked_schemas)}"
        )


def verify_source_candidate_closure(
    registry: dict[str, Any], root: pathlib.Path = ROOT
) -> None:
    """Close the current public candidate without traversing release ancestry."""

    validate_release_registry_closure(registry, root)
    generation = registry["source_generation"]
    if generation["state"] != "source":
        raise ValueError("the current public candidate is not a source generation")
    observed_snapshot = current_source_snapshot_digest(root)
    if generation["source_snapshot_digest"] != observed_snapshot:
        raise ValueError(
            "source generation snapshot drifted from the public-export candidate"
        )

    workspace_version = tomllib.loads(
        (root / "Cargo.toml").read_text(encoding="utf-8")
    )["workspace"]["package"]["version"]
    typescript_version = load_json(root / "sdk/typescript/package.json")["version"]
    python_version = tomllib.loads(
        (root / "sdk/python/pyproject.toml").read_text(encoding="utf-8")
    )["project"]["version"]
    if (
        generation["workspace_version"] != workspace_version
        or {workspace_version, typescript_version, python_version}
        != {workspace_version}
    ):
        raise ValueError("public package versions are not one source generation")


def verify_registry(registry: dict[str, Any]) -> None:
    validate_release_registry_closure(registry)
    generation = registry["source_generation"]
    defaults = registry["defaults"]
    validate_registry_conformance(registry)
    baseline_snapshot = generation["baseline_source_snapshot_digest"]
    if defaults.get("defined_at_source_snapshot_digest") != baseline_snapshot:
        raise ValueError("registry default source snapshot differs from its baseline")
    current_snapshot = current_source_snapshot_digest()
    if generation["source_snapshot_digest"] != current_snapshot:
        raise ValueError(
            "source generation snapshot drifted from the public-export candidate"
        )

    snapshot_history = source_snapshot_history()
    history_by_commit = dict(snapshot_history)
    if baseline_snapshot not in set(history_by_commit.values()):
        raise ValueError("baseline public source snapshot is not in HEAD ancestry")

    predecessor_registry_digest = generation["predecessor_registry_digest"]
    predecessor_source_snapshot = generation["predecessor_source_snapshot_digest"]
    predecessor_commit: str | None = None
    predecessor: dict[str, Any] | None = None
    if predecessor_registry_digest is None:
        foreign_ancestral_registries: list[str] = []
        for commit, _snapshot_digest in snapshot_history:
            blob = git_blob(commit, "versioning/version-domains.json")
            if blob is None:
                continue
            ancestor = load_json_bytes(blob, label=f"registry at {commit}")
            ancestor_generation = (
                ancestor.get("source_generation", {})
                if isinstance(ancestor, dict)
                else {}
            )
            predecessor_fields = (
                "predecessor_registry_digest",
                "predecessor_source_snapshot_digest",
                "predecessor_registry_version",
                "predecessor_source_generation",
            )
            if (
                not isinstance(ancestor, dict)
                or ancestor.get("registry_version") != registry["registry_version"]
                or ancestor_generation.get("generation") != generation["generation"]
                or any(
                    field not in ancestor_generation
                    or ancestor_generation[field] is not None
                    for field in predecessor_fields
                )
            ):
                foreign_ancestral_registries.append(commit)
        if foreign_ancestral_registries:
            raise ValueError(
                "genesis registry is invalid because HEAD ancestry contains another release generation"
            )
    else:
        predecessor_matches: list[tuple[str, dict[str, Any]]] = []
        for commit, snapshot_digest in snapshot_history:
            if snapshot_digest != predecessor_source_snapshot:
                continue
            blob = git_blob(commit, "versioning/version-domains.json")
            if blob is None:
                continue
            predecessor_value = load_json_bytes(
                blob,
                label=f"predecessor registry at public snapshot {snapshot_digest}",
            )
            if isinstance(predecessor_value, dict) and digest_json(
                predecessor_value
            ) == predecessor_registry_digest:
                predecessor_matches.append((commit, predecessor_value))
        if not predecessor_matches:
            raise ValueError(
                "predecessor registry digest and public source snapshot are not in HEAD ancestry"
            )
        predecessor_commit, predecessor = predecessor_matches[0]
        validate_registry_closed_shape(predecessor)
        validate_registry_schema(predecessor)
        if (
            predecessor.get("registry_version")
            != generation["predecessor_registry_version"]
            or predecessor.get("source_generation", {}).get("generation")
            != generation["predecessor_source_generation"]
        ):
            raise ValueError(
                "predecessor registry generation is not the declared authority"
            )

    metadata = cargo_metadata()
    catalog = release_catalog(metadata)
    catalog_packages = {entry["name"] for entry in catalog}
    cargo_paths, direct_dependencies = cargo_graph(metadata)
    candidate_paths = set(candidate_git_paths())
    known_packages = set(cargo_paths) | NON_CARGO_PACKAGES
    domains = registry["domains"]
    versions = [domain["version"] for domain in domains]
    if versions != sorted(versions):
        raise ValueError("version domains must be sorted by exact version")
    if len(versions) != len(set(versions)):
        raise ValueError("one exact version may have only one registry authority")
    by_version = {domain["version"]: domain for domain in domains}
    production_literals, content_id_sources = production_identity_inventory()
    validate_direct_content_id_sources(registry, content_id_sources)
    if set(by_version) != set(production_literals):
        raise ValueError(
            "current registry and production identities differ: "
            f"missing={sorted(set(production_literals) - set(by_version))} "
            f"extra={sorted(set(by_version) - set(production_literals))}"
        )
    for version, locations in production_literals.items():
        domain = by_version.get(version)
        if domain is None:
            raise ValueError(
                f"production identity {version} is unregistered at {locations}"
            )
        for location in locations:
            if location not in domain["literal_locations"]:
                raise ValueError(
                    f"{version} does not acknowledge production literal {location}"
                )
            package = package_for_path(ROOT / location, cargo_paths)
            if package is not None and package not in domain["consumers"]["packages"]:
                raise ValueError(
                    f"{version} omits literal consumer {package} at {location}"
                )
    schema_contract_mismatches: list[str] = []
    for domain in domains:
        expected_locations = production_literals.get(domain["version"], [])
        if domain["literal_locations"] != expected_locations:
            raise ValueError(
                f"{domain['version']} literal locations drifted from production"
            )

    schema_values: dict[pathlib.Path, dict[str, Any]] = {}
    schema_ids: dict[str, pathlib.Path] = {}
    schema_owners: dict[pathlib.Path, list[tuple[str, str]]] = {}
    provenance_mismatches: list[str] = []
    for domain in domains:
        version = domain["version"]
        if VERSION_PATTERN.fullmatch(version) is None:
            raise ValueError(f"malformed version domain {version}")
        if version.rsplit("/", 1)[0] != domain["domain"]:
            raise ValueError(f"domain name does not match {version}")
        for field in (
            "writers",
            "readers",
            "literal_locations",
            "embeds",
            "depends_on",
            "generator_paths",
            "conformance",
        ):
            require_sorted_unique(domain[field], f"{version}.{field}")
        for field in ("schemas", "types", "packages"):
            require_sorted_unique(
                domain["consumers"][field], f"{version}.consumers.{field}"
            )
        if domain["owner"] not in known_packages:
            raise ValueError(f"{version} has unknown owner {domain['owner']}")
        for package in domain["writers"] + domain["readers"] + domain["consumers"]["packages"]:
            if package not in known_packages:
                raise ValueError(f"{version} names unknown package {package}")
        for package in [domain["owner"], *domain["writers"]]:
            if package in cargo_paths and package not in catalog_packages:
                raise ValueError(f"{version} gives authority to unpublished Cargo package {package}")
        for related in domain["embeds"] + domain["depends_on"]:
            if related not in by_version:
                raise ValueError(f"{version} references unregistered domain {related}")
            if related == version:
                raise ValueError(f"{version} references itself as a version dependency")
        if not set(domain["embeds"]).issubset(domain["depends_on"]):
            raise ValueError(f"{version} embeds a domain outside its dependency closure")
        for source in domain["sources"]:
            path = ROOT / source["path"]
            if not path.is_file():
                raise ValueError(f"{version} source is missing: {source['path']}")
            text = path.read_text(encoding="utf-8")
            if not source_anchor_matches(text, path, source["symbol"], version):
                raise ValueError(
                    f"{version} source anchor {source['path']}::{source['symbol']} drifted"
                )
            source_package = package_for_path(path, cargo_paths)
            if source_package in cargo_paths and source_package not in catalog_packages:
                raise ValueError(
                    f"{version} source belongs to unpublished Cargo package {source_package}"
                )
            if (
                source_package is not None
                and source_package not in domain["consumers"]["packages"]
            ):
                raise ValueError(
                    f"{version} omits source package {source_package} from its consumers"
                )
            if (
                source_package in cargo_paths
                and source_package != domain["owner"]
                and source_package not in domain["writers"]
            ):
                raise ValueError(
                    f"{version} source package {source_package} is neither owner nor writer"
                )
        unique_identity_roles = {
            "artifact_kind",
            "artifact_type_key",
            "binary_hash_domain",
            "catalog_namespace",
            "content_id_domain",
        }
        for role in unique_identity_roles:
            if sum(source["role"] == role for source in domain["sources"]) > 1:
                raise ValueError(f"{version} has more than one {role} source authority")
        validate_identity_source_dependencies(domain)
        migration = effective(domain, defaults, "migration")
        if migration["runbook"] is not None:
            runbook = migration["runbook"]
            if runbook not in candidate_paths or not (ROOT / runbook).is_file():
                raise ValueError(
                    f"{version} migration runbook is not a source-candidate file: {runbook}"
                )
        for path_value in domain["generator_paths"] + domain["consumers"]["schemas"]:
            if not (ROOT / path_value).exists():
                raise ValueError(f"{version} path is missing: {path_value}")
        validate_schema_record_order(domain)
        schema_paths = [schema["path"] for schema in domain["schemas"]]
        owned_schema_paths = set(schema_paths)
        missing_schema_consumers = owned_schema_paths - set(
            domain["consumers"]["schemas"]
        )
        missing_schema_generators = owned_schema_paths - set(domain["generator_paths"])
        if missing_schema_consumers or missing_schema_generators:
            raise ValueError(
                f"{version} owned schemas are not closed: "
                f"consumers={sorted(missing_schema_consumers)} "
                f"generators={sorted(missing_schema_generators)}"
            )
        for schema_record in domain["schemas"]:
            path = (ROOT / schema_record["path"]).resolve()
            schema = schema_values.setdefault(path, load_json(path))
            fragment = schema_fragment(schema_record)
            schema_fragment_value(schema, fragment)
            if schema.get("$id") != schema_record["id"]:
                raise ValueError(f"{version} schema $id drifted")
            if digest_json(schema) != schema_record["canonical_digest"]:
                raise ValueError(f"{version} canonical schema digest drifted")
            existing = schema_ids.setdefault(schema_record["id"], path)
            if existing != path:
                raise ValueError(f"schema $id {schema_record['id']} is not unique")
            owner = (fragment, version)
            if owner in schema_owners.setdefault(path, []):
                raise ValueError(f"{version} repeats schema fragment {fragment}")
            schema_owners[path].append(owner)

    predecessor_by_version = (
        {}
        if predecessor is None
        else {domain["version"]: domain for domain in predecessor["domains"]}
    )

    def provenance_domain(
        domain: dict[str, Any], source_commit: str | None, current: dict[str, Any]
    ) -> dict[str, Any]:
        normalized = load_json_bytes(
            canonical_bytes(domain), label=f"normalized domain {domain['version']}"
        )
        normalized.pop("defined_at_source_snapshot_digest", None)
        current_records = {schema_record_key(record): record for record in current["schemas"]}
        for record in normalized["schemas"]:
            current_record = current_records.get(schema_record_key(record))
            current_record = record if current_record is None else current_record
            fragment = schema_fragment(current_record)
            blob = (
                (ROOT / record["path"]).read_bytes()
                if source_commit is None
                else git_blob(source_commit, record["path"])
            )
            if blob is None:
                record["canonical_digest"] = "missing"
            else:
                try:
                    projection = schema_fragment_projection(
                        load_json_bytes(
                            blob,
                            label=f"schema {record['path']} at {source_commit or 'candidate'}",
                        ),
                        fragment,
                    )
                    record["canonical_digest"] = digest_json(projection)
                except ValueError:
                    record["canonical_digest"] = "invalid"
            record["fragment"] = fragment
        return normalized

    changed_from_predecessor: dict[str, bool] = {}
    for domain in domains:
        version = domain["version"]
        previous = predecessor_by_version.get(version)
        changed_from_predecessor[version] = previous is None or provenance_domain(
            previous, predecessor_commit, domain
        ) != provenance_domain(domain, None, domain)

    propagated = True
    while propagated:
        propagated = False
        for domain in domains:
            version = domain["version"]
            if not changed_from_predecessor[version] and any(
                changed_from_predecessor[dependency]
                for dependency in domain["depends_on"]
            ):
                changed_from_predecessor[version] = True
                propagated = True

    for domain in domains:
        version = domain["version"]
        if changed_from_predecessor[version]:
            if domain["defined_at_source_snapshot_digest"] is not None:
                provenance_mismatches.append(
                    f"{version} requires explicit null current-source provenance"
                )
            continue
        if predecessor_source_snapshot is None:
            raise ValueError("genesis registry contains an inherited domain")
        previous = predecessor_by_version[version]
        if previous["defined_at_source_snapshot_digest"] is None:
            expected_provenance = predecessor_source_snapshot
        elif isinstance(previous["defined_at_source_snapshot_digest"], str):
            expected_provenance = previous["defined_at_source_snapshot_digest"]
        else:
            expected_provenance = baseline_snapshot
        current_value = domain["defined_at_source_snapshot_digest"]
        actual_provenance = current_snapshot if current_value is None else current_value
        if actual_provenance != expected_provenance:
            provenance_mismatches.append(
                f"{version} requires inherited source snapshot {expected_provenance}"
            )
    if provenance_mismatches:
        raise ValueError("source provenance drift: " + "; ".join(provenance_mismatches))

    for path in tracked_schema_paths():
        resolved = path.resolve()
        schema = schema_values.setdefault(resolved, load_json(resolved))
        schema_id = schema.get("$id")
        if not isinstance(schema_id, str):
            raise ValueError(f"schema {path.relative_to(ROOT)} has no $id")
        existing = schema_ids.setdefault(schema_id, resolved)
        if existing != resolved:
            raise ValueError(f"schema $id {schema_id} is not unique")
        owners = schema_owners.get(resolved, [])
        if not owners:
            raise ValueError(f"schema {path.relative_to(ROOT)} has no version-domain owner")
        roots = [version for fragment, version in owners if fragment == "#"]
        if len(roots) != 1:
            raise ValueError(
                f"schema {path.relative_to(ROOT)} requires exactly one root owner"
            )

    def owners_for_fragment(path: pathlib.Path, fragment: str) -> set[str]:
        owners = schema_owners.get(path, [])
        exact = {version for owned, version in owners if owned == fragment}
        if exact:
            return exact
        if fragment.startswith("#/"):
            ancestors = [
                (owned, version)
                for owned, version in owners
                if owned.startswith("#/") and fragment.startswith(owned + "/")
            ]
            if ancestors:
                longest = max(len(owned) for owned, _ in ancestors)
                return {version for owned, version in ancestors if len(owned) == longest}
        return {version for owned, version in owners if owned == "#"}

    for domain in domains:
        version = domain["version"]
        required_embeds: set[str] = set()
        for schema_record in domain["schemas"]:
            path = (ROOT / schema_record["path"]).resolve()
            schema = schema_values[path]
            fragment = schema_fragment(schema_record)
            references, literal_versions = schema_fragment_dependencies(
                schema,
                fragment,
                schema_owners[path],
                version,
            )
            required_embeds.update(literal_versions - {version})
            for reference in references:
                target = urllib.parse.urljoin(schema["$id"], reference)
                target_url, target_fragment = urllib.parse.urldefrag(target)
                target_path = schema_ids.get(target_url)
                if target_path is None:
                    raise ValueError(
                        f"schema {path.relative_to(ROOT)} references unknown $id {target_url}"
                    )
                try:
                    target_fragment = urllib.parse.unquote_to_bytes(
                        target_fragment
                    ).decode("utf-8", errors="strict")
                except UnicodeDecodeError as error:
                    raise ValueError(
                        f"schema reference has invalid UTF-8 fragment {reference}"
                    ) from error
                target_fragment = f"#{target_fragment}" if target_fragment else "#"
                schema_fragment_value(schema_values[target_path], target_fragment)
                target_versions = owners_for_fragment(target_path, target_fragment)
                if not target_versions:
                    raise ValueError(
                        f"schema target {target_url}{target_fragment} has no domain owner"
                    )
                required_embeds.update(target_versions - {version})
        missing_dependencies = required_embeds - set(domain["depends_on"])
        missing_embeds = required_embeds - set(domain["embeds"])
        if missing_dependencies:
            schema_contract_mismatches.append(
                f"{version} dependencies {sorted(missing_dependencies)}"
            )
        if missing_embeds:
            schema_contract_mismatches.append(
                f"{version} embeds {sorted(missing_embeds)}"
            )
    if schema_contract_mismatches:
        raise ValueError(
            "schema contract drift: " + "; ".join(schema_contract_mismatches)
        )

    for domain in domains:
        owner = domain["owner"]
        for related_version in domain["depends_on"]:
            related_owner = by_version[related_version]["owner"]
            cargo_writers = [
                writer for writer in domain["writers"] if writer in cargo_paths
            ]
            if related_owner in cargo_paths and not any(
                writer == related_owner
                or related_owner in transitive_dependencies(direct_dependencies, writer)
                for writer in cargo_writers
            ):
                raise ValueError(
                    f"{domain['version']} declares {related_version} without a Cargo path "
                    f"from one of {cargo_writers} to {related_owner}"
                )
        for reader in domain["readers"]:
            if (
                owner in cargo_paths
                and reader in cargo_paths
                and reader != owner
                and reader not in domain["writers"]
                and owner not in transitive_dependencies(direct_dependencies, reader)
            ):
                raise ValueError(
                    f"{reader} cannot read {domain['version']} without depending on {owner}"
                )

    for root in (ROOT / "crates", ROOT / "plugins"):
        for path in sorted(root.glob("*/src/**/*.rs")):
            relative = path.relative_to(ROOT).as_posix()
            text = rust_comment_mask(
                rust_production_text(path.read_text(encoding="utf-8"))
            )
            for symbol, version in PUBLIC_RUST_VERSION.findall(text):
                domain = by_version.get(version)
                if domain is None or relative not in domain["literal_locations"]:
                    raise ValueError(
                        f"public Rust version {relative}::{symbol}={version} is unregistered"
                    )

    declared_release_sources = {
        (source["path"], domain["version"])
        for domain in domains
        if domain["owner"] == "release-governance"
        for source in domain["sources"]
    }
    for relative in ("scripts/crates_release.py", "scripts/npm_release.py"):
        text = (ROOT / relative).read_text(encoding="utf-8")
        for version in set(VERSION_PATTERN.findall(text)):
            if ("-report/" in version or "-stage/" in version) and (
                relative,
                version,
            ) not in declared_release_sources:
                raise ValueError(
                    f"release receipt {relative}={version} is unregistered"
                )

    workspace_version = tomllib.loads((ROOT / "Cargo.toml").read_text())["workspace"][
        "package"
    ]["version"]
    if workspace_version != generation["workspace_version"]:
        raise ValueError("workspace version differs from the source generation")
    typescript_version = load_json(ROOT / "sdk/typescript/package.json")["version"]
    python_version = tomllib.loads((ROOT / "sdk/python/pyproject.toml").read_text())[
        "project"
    ]["version"]
    if {workspace_version, typescript_version, python_version} != {workspace_version}:
        raise ValueError("public package versions are not one release generation")


def effective(domain: dict[str, Any], defaults: dict[str, Any], field: str) -> Any:
    value = domain[field]
    return defaults[field] if value is None else value


def render_table(registry: dict[str, Any]) -> str:
    defaults = registry["defaults"]
    lines = [
        "# Version Domains",
        "",
        "Status: generated from `versioning/version-domains.json`; do not edit by hand.",
        "",
        "| Exact version | Kind | Owner | Compatibility | Schema | Conformance |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for domain in registry["domains"]:
        schema = (
            "—"
            if not domain["schemas"]
            else ", ".join(f"`{path}`" for path in sorted({item["path"] for item in domain["schemas"]}))
        )
        conformance = ", ".join(f"`{suite}`" for suite in domain["conformance"])
        lines.append(
            f"| `{domain['version']}` | {domain['kind']} | `{domain['owner']}` | "
            f"{effective(domain, defaults, 'compatibility_mode')} | {schema} | {conformance} |"
        )
    lines.extend(
        [
            "",
            "The registry also freezes writers, accepted readers, embedding and dependency "
            "edges, source anchors, canonical schema digests, migration status, removal gates, "
            "and release-generation ownership.",
            "",
        ]
    )
    return "\n".join(lines)


def validate_stable_release_version(version: object) -> str:
    if (
        not isinstance(version, str)
        or STABLE_VERSION_PATTERN.fullmatch(version) is None
    ):
        raise ValueError("release version must be one exact stable SemVer")
    return version


def exact_npm_registry_url(value: object, *, path_prefix: str) -> bool:
    """Return whether a BOM URL belongs to the exact npm registry origin."""

    if not isinstance(value, str):
        return False
    parsed = urllib.parse.urlsplit(value)
    return (
        parsed.scheme == "https"
        and parsed.netloc == "registry.npmjs.org"
        and parsed.username is None
        and parsed.password is None
        and not parsed.query
        and not parsed.fragment
        and parsed.path.startswith(path_prefix)
    )


def validate_publication(
    name: str,
    version: str,
    publication: object,
    source_sha: str,
    expected_kind: str,
) -> dict[str, Any]:
    if not isinstance(publication, dict):
        raise ValueError(f"publication evidence for {name} must be an object")
    if set(publication) != {
        "kind",
        "registry",
        "registry_identity",
        "content_digest",
        "provenance",
    }:
        raise ValueError(f"publication evidence for {name} has an open shape")
    kind = publication["kind"]
    if kind != expected_kind:
        raise ValueError(
            f"publication ecosystem for {name} is {kind}, expected {expected_kind}"
        )
    provenance = publication["provenance"]
    if not isinstance(provenance, dict):
        raise ValueError(f"publication provenance for {name} must be an object")
    if kind == "cargo":
        expected_identity = f"https://crates.io/crates/{name}/{version}"
        expected_download = (
            f"https://static.crates.io/crates/{name}/{name}-{version}.crate"
        )
        if (
            publication["registry"] != CRATES_REGISTRY
            or publication["registry_identity"] != expected_identity
            or not isinstance(publication["content_digest"], str)
            or re.fullmatch(
                r"sha256:[0-9a-f]{64}", publication["content_digest"]
            )
            is None
            or set(provenance) != {"kind", "checksum", "download_url"}
            or provenance.get("kind") != "registry-checksum"
            or provenance.get("checksum") != publication["content_digest"]
            or provenance.get("download_url") != expected_download
        ):
            raise ValueError(f"Cargo publication evidence is not exact for {name}")
    elif kind == "npm":
        encoded_name = urllib.parse.quote(name, safe="")
        encoded_version = urllib.parse.quote(version, safe="")
        expected_identity = (
            f"{NPM_REGISTRY.rstrip('/')}/{encoded_name}/{encoded_version}"
        )
        content_digest = publication["content_digest"]
        provenance_fields = {
            "kind",
            "sha1",
            "integrity",
            "tarball_url",
            "attestations_url",
            "bundle_digest",
            "statement_digest",
            "certificate_identity",
            "certificate_issuer",
            "predicate_type",
            "workflow_ref",
            "source_sha",
            "signer_ref",
            "signer_sha",
        }
        if (
            publication["registry"] != NPM_REGISTRY
            or publication["registry_identity"] != expected_identity
            or not isinstance(content_digest, str)
            or re.fullmatch(r"sha512:[0-9a-f]{128}", content_digest) is None
            or set(provenance) != provenance_fields
            or provenance.get("kind") != "sigstore"
            or not isinstance(provenance.get("sha1"), str)
            or re.fullmatch(r"sha1:[0-9a-f]{40}", provenance["sha1"]) is None
            or not isinstance(provenance.get("integrity"), str)
            or not exact_npm_registry_url(
                provenance.get("tarball_url"), path_prefix="/"
            )
            or not exact_npm_registry_url(
                provenance.get("attestations_url"), path_prefix="/-/"
            )
            or not isinstance(provenance.get("bundle_digest"), str)
            or re.fullmatch(r"sha256:[0-9a-f]{64}", provenance["bundle_digest"])
            is None
            or not isinstance(provenance.get("statement_digest"), str)
            or re.fullmatch(
                r"sha256:[0-9a-f]{64}", provenance["statement_digest"]
            )
            is None
            or provenance.get("certificate_identity")
            != NPM_SIGSTORE_CERTIFICATE_IDENTITY
            or provenance.get("certificate_issuer")
            != NPM_SIGSTORE_CERTIFICATE_ISSUER
            or provenance.get("predicate_type") != NPM_SLSA_PROVENANCE
            or provenance.get("workflow_ref")
            not in {"refs/heads/main", f"refs/tags/v{version}"}
            or provenance.get("source_sha") != source_sha
            or provenance.get("signer_ref")
            != NPM_SIGSTORE_CERTIFICATE_IDENTITY
            or not isinstance(provenance.get("signer_sha"), str)
            or SHA_PATTERN.fullmatch(provenance["signer_sha"]) is None
        ):
            raise ValueError(f"npm publication evidence is not exact for {name}")
        expected_integrity = "sha512-" + base64.b64encode(
            bytes.fromhex(content_digest.removeprefix("sha512:"))
        ).decode("ascii")
        if provenance["integrity"] != expected_integrity:
            raise ValueError(f"npm integrity does not match content digest for {name}")
    else:
        raise ValueError(f"unknown publication kind for {name}: {kind}")
    return publication


def publication_map(
    publications: object,
    expected_packages: dict[str, tuple[str, str]],
    version: str,
    source_sha: str,
) -> dict[str, dict[str, Any]]:
    if not isinstance(publications, list):
        raise ValueError("release publications must be an array")
    by_id: dict[str, dict[str, Any]] = {}
    for record in publications:
        if not isinstance(record, dict) or set(record) != {
            "package_id",
            "name",
            "version",
            "publication",
        }:
            raise ValueError("release publication record has an open shape")
        package_id = record["package_id"]
        name = record["name"]
        if (
            not isinstance(package_id, str)
            or not isinstance(name, str)
            or package_id in by_id
            or package_id not in expected_packages
            or name != expected_packages.get(package_id, (None, None))[1]
            or record["version"] != version
        ):
            raise ValueError(
                f"release publication identity is invalid for {package_id}:{name}"
            )
        expected_kind, _expected_name = expected_packages[package_id]
        by_id[package_id] = validate_publication(
            name,
            version,
            record["publication"],
            source_sha,
            expected_kind,
        )
    if set(by_id) != set(expected_packages):
        raise ValueError(
            "release publications do not close the published package set: "
            f"missing={sorted(set(expected_packages) - set(by_id))} "
            f"extra={sorted(set(by_id) - set(expected_packages))}"
        )
    return by_id


def package_records(
    publications: object,
    source_sha: str,
    *,
    catalog: list[dict[str, Any]] | None = None,
    root: pathlib.Path = ROOT,
) -> list[dict[str, Any]]:
    workspace = tomllib.loads((root / "Cargo.toml").read_text(encoding="utf-8"))
    version = validate_stable_release_version(
        workspace["workspace"]["package"]["version"]
    )
    catalog = release_catalog() if catalog is None else catalog
    published_packages = {
        **{
            f"cargo:{crate['name']}": ("cargo", crate["name"])
            for crate in catalog
        },
        "npm:cymule": ("npm", "cymule"),
        "npm:@cymule/sdk": ("npm", "@cymule/sdk"),
    }
    by_id = publication_map(
        publications, published_packages, version, source_sha
    )
    records: list[dict[str, Any]] = []
    for crate in catalog:
        manifest = pathlib.Path(crate["path"]) / "Cargo.toml"
        records.append(
            package_record(
                f"cargo:{crate['name']}",
                crate["name"],
                version,
                manifest,
                by_id[f"cargo:{crate['name']}"],
                root=root,
            )
        )
    records.extend(
        [
            package_record(
                "npm:cymule",
                "cymule",
                version,
                pathlib.Path("sdk/typescript/package.json"),
                by_id["npm:cymule"],
                root=root,
            ),
            package_record(
                "npm:@cymule/sdk",
                "@cymule/sdk",
                version,
                pathlib.Path("sdk/typescript/package.json"),
                by_id["npm:@cymule/sdk"],
                root=root,
            ),
            package_record(
                "python:cymule",
                "cymule",
                version,
                pathlib.Path("sdk/python/pyproject.toml"),
                None,
                root=root,
            ),
            package_record(
                "go:github.com/cymule-framework/cymule/sdk/go",
                "github.com/cymule-framework/cymule/sdk/go",
                version,
                pathlib.Path("sdk/go/go.mod"),
                None,
                root=root,
            ),
        ]
    )
    return sorted(records, key=lambda record: record["package_id"])


def package_record(
    package_id: str,
    name: str,
    version: str,
    manifest: pathlib.Path,
    publication: dict[str, Any] | None,
    *,
    root: pathlib.Path = ROOT,
) -> dict[str, Any]:
    return {
        "package_id": package_id,
        "name": name,
        "version": version,
        "manifest_path": manifest.as_posix(),
        "manifest_digest": digest_bytes((root / manifest).read_bytes()),
        "publication": publication,
    }


def validate_bom_package_order(packages: object) -> None:
    """Require one unique canonical package_id order in a release BOM."""

    if not isinstance(packages, list):
        raise ValueError("release BOM packages must be an array")
    package_ids = [
        package.get("package_id") if isinstance(package, dict) else None
        for package in packages
    ]
    if (
        any(not isinstance(package_id, str) for package_id in package_ids)
        or len(set(package_ids)) != len(package_ids)
        or package_ids != sorted(package_ids)
    ):
        raise ValueError("release BOM packages are not unique canonical package_id order")


def release_bom_projection(registry: dict[str, Any]) -> dict[str, Any]:
    """Derive every registry-owned BOM/2 projection from one exact registry."""

    defaults = registry["defaults"]
    schemas_by_path: dict[str, tuple[str, str]] = {}
    for domain in registry["domains"]:
        for schema in domain["schemas"]:
            identity = schema["id"], schema["canonical_digest"]
            previous = schemas_by_path.setdefault(schema["path"], identity)
            if previous != identity:
                raise ValueError(f"release schema {schema['path']} has conflicting authorities")
    schemas = [
        (path, schema_id, digest)
        for path, (schema_id, digest) in sorted(schemas_by_path.items())
    ]
    migrations = sorted(
        {
            migration["edge"]
            for domain in registry["domains"]
            if (migration := effective(domain, defaults, "migration"))["edge"]
            is not None
        }
    )
    return {
        "release_generation": registry["source_generation"]["generation"],
        "workspace_version": registry["source_generation"]["workspace_version"],
        "version_domain_registry_digest": digest_json(registry),
        "schemas": [
            {"path": path, "id": schema_id, "canonical_digest": digest}
            for path, schema_id, digest in schemas
        ],
        "domains": [
            {
                "version": domain["version"],
                "owner": domain["owner"],
                "compatibility_mode": effective(
                    domain, defaults, "compatibility_mode"
                ),
            }
            for domain in registry["domains"]
        ],
        "migration_edges": migrations,
    }


def validate_release_package_manifests(
    *, root: pathlib.Path, version: str, catalog: list[dict[str, Any]]
) -> None:
    """Bind BOM package identities to the exact tagged manifest semantics."""

    workspace = tomllib.loads((root / "Cargo.toml").read_text(encoding="utf-8"))
    workspace_package = workspace.get("workspace", {}).get("package", {})
    if workspace_package.get("version") != version:
        raise ValueError("release workspace version differs from the registry generation")
    if workspace_package.get("publish") != ["crates-io"]:
        raise ValueError("release workspace has no exact crates.io publication authority")
    if {entry["name"] for entry in catalog} == set():
        raise ValueError("release Cargo catalog must not be empty")
    typescript = load_json(root / "sdk/typescript/package.json")
    if (
        not isinstance(typescript, dict)
        or typescript.get("name") != "cymule"
        or typescript.get("version") != version
    ):
        raise ValueError("TypeScript release manifest is not the workspace generation")
    python = tomllib.loads(
        (root / "sdk/python/pyproject.toml").read_text(encoding="utf-8")
    ).get("project")
    if (
        not isinstance(python, dict)
        or python.get("name") != "cymule"
        or python.get("version") != version
    ):
        raise ValueError("Python release manifest is not the workspace generation")
    module_lines = [
        line.strip()
        for line in (root / "sdk/go/go.mod").read_text(encoding="utf-8").splitlines()
        if line.strip().startswith("module ")
    ]
    if module_lines != ["module github.com/cymule-framework/cymule/sdk/go"]:
        raise ValueError("Go release manifest has no exact module identity")


def validate_release_bom_projection(
    value: object,
    *,
    registry: dict[str, Any],
    source_sha: str,
    public_source_sha: str | None,
    catalog: list[dict[str, Any]] | None = None,
    root: pathlib.Path = ROOT,
) -> dict[str, Any]:
    """Validate an already-attested BOM against its exact registry and package bytes."""

    if not isinstance(value, dict):
        raise ValueError("release BOM is not an object")
    expected_fields = {
        "bom_version",
        "release_generation",
        "workspace_version",
        "source_sha",
        "public_source_sha",
        "version_domain_registry_digest",
        "schemas",
        "packages",
        "domains",
        "migration_edges",
    }
    if set(value) != expected_fields:
        raise ValueError("release BOM has an open or incomplete top-level shape")
    if SHA_PATTERN.fullmatch(source_sha) is None:
        raise ValueError("source SHA must be one exact lowercase Git commit")
    if public_source_sha is not None and SHA_PATTERN.fullmatch(public_source_sha) is None:
        raise ValueError("public source SHA must be one exact lowercase Git commit")
    validate_registry_closed_shape(registry, root)
    if value["bom_version"] != RELEASE_BOM_VERSION:
        raise ValueError("release BOM version is not supported")
    identities = {
        "source_sha": source_sha,
        "public_source_sha": public_source_sha,
    }
    for field, expected in identities.items():
        if value[field] != expected:
            raise ValueError(f"release BOM {field} belongs to another generation")
    projection = release_bom_projection(registry)
    for field, expected in projection.items():
        if value[field] != expected:
            raise ValueError(f"release BOM {field} is not the complete registry projection")

    authoritative_catalog = release_catalog_entries(root)
    if catalog is not None and catalog != authoritative_catalog:
        raise ValueError("release Cargo catalog differs from workspace authority")
    catalog = authoritative_catalog
    validate_release_package_manifests(
        root=root,
        version=projection["workspace_version"],
        catalog=catalog,
    )
    packages = value["packages"]
    validate_bom_package_order(packages)
    publications: list[dict[str, Any]] = []
    if not isinstance(packages, list):
        raise ValueError("release BOM packages must be an array")
    for package in packages:
        if not isinstance(package, dict) or set(package) != {
            "package_id",
            "name",
            "version",
            "manifest_path",
            "manifest_digest",
            "publication",
        }:
            raise ValueError("release BOM package record has an open or incomplete shape")
        if package["publication"] is not None:
            publications.append(
                {
                    "package_id": package["package_id"],
                    "name": package["name"],
                    "version": package["version"],
                    "publication": package["publication"],
                }
            )
    expected_packages = package_records(
        publications,
        source_sha,
        catalog=catalog,
        root=root,
    )
    if packages != expected_packages:
        raise ValueError("release BOM packages are not the complete exact package catalog")
    return value


def validate_release_bom(
    value: object,
    *,
    registry: dict[str, Any],
    source_sha: str,
    public_source_sha: str | None,
    catalog: list[dict[str, Any]] | None = None,
    root: pathlib.Path = ROOT,
) -> dict[str, Any]:
    """Validate the complete semantic closure of one release BOM/2."""

    if root.resolve() != ROOT.resolve():
        raise ValueError(
            "complete release BOM validation must use the configured release workspace"
        )
    verify_registry(registry)
    return validate_release_bom_projection(
        value,
        registry=registry,
        source_sha=source_sha,
        public_source_sha=public_source_sha,
        catalog=catalog,
        root=root,
    )


def build_bom(
    registry: dict[str, Any],
    source_sha: str,
    public_source_sha: str | None,
    publications: object,
    *,
    catalog: list[dict[str, Any]] | None = None,
    root: pathlib.Path = ROOT,
) -> dict[str, Any]:
    validate_registry_closed_shape(registry, root)
    validate_registry_schema(registry, root)
    if SHA_PATTERN.fullmatch(source_sha) is None:
        raise ValueError("source SHA must be one exact lowercase Git commit")
    if public_source_sha is not None and SHA_PATTERN.fullmatch(public_source_sha) is None:
        raise ValueError("public source SHA must be one exact lowercase Git commit")
    validate_stable_release_version(registry["source_generation"]["workspace_version"])
    projection = release_bom_projection(registry)
    resolved_catalog = release_catalog() if catalog is None else catalog
    packages = package_records(
        publications,
        source_sha,
        catalog=resolved_catalog,
        root=root,
    )
    validate_bom_package_order(packages)
    value = {
        "bom_version": RELEASE_BOM_VERSION,
        "source_sha": source_sha,
        "public_source_sha": public_source_sha,
        "packages": packages,
        **projection,
    }
    return validate_release_bom(
        value,
        registry=registry,
        source_sha=source_sha,
        public_source_sha=public_source_sha,
        catalog=resolved_catalog,
        root=root,
    )


def changelog_sections(root: pathlib.Path = ROOT) -> dict[str, list[str]]:
    """Parse exact second-level Keep-a-Changelog sections without prefix matching."""

    sections: dict[str, list[str]] = {}
    current: str | None = None
    heading = re.compile(r"^## \[([^]]+)\](?: - .+)?$")
    for line in (root / "CHANGELOG.md").read_text(encoding="utf-8").splitlines():
        match = heading.fullmatch(line)
        if match is not None:
            current = match.group(1)
            if current in sections:
                raise ValueError(f"CHANGELOG repeats section {current}")
            sections[current] = []
        elif current is not None:
            sections[current].append(line)
    return sections


def release_notes(version: str, root: pathlib.Path = ROOT) -> str:
    """Return normalized notes from one exact non-empty version section."""

    validate_stable_release_version(version)
    sections = changelog_sections(root)
    if version not in sections or not any(line.strip() for line in sections[version]):
        raise ValueError(f"CHANGELOG has no non-empty release section for {version}")
    return "\n".join(sections[version]).strip() + "\n"


def verify_release_changelog(version: str, root: pathlib.Path = ROOT) -> None:
    """Require one complete version section and an empty Unreleased queue."""

    sections = changelog_sections(root)
    if "Unreleased" not in sections:
        raise ValueError("CHANGELOG omits the Unreleased section")
    if any(line.strip() for line in sections["Unreleased"]):
        raise ValueError(
            "CHANGELOG Unreleased is non-empty; move its entries into the release section"
        )
    release_notes(version, root)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("verify")
    subparsers.add_parser("verify-source-closure")
    subparsers.add_parser("check-docs")
    subparsers.add_parser("source-snapshot")
    subparsers.add_parser("generate-docs")
    digest = subparsers.add_parser("digest")
    digest.add_argument("--bare", action="store_true")
    bom = subparsers.add_parser("bom")
    bom.add_argument("--source-sha", required=True)
    bom.add_argument("--public-source-sha")
    bom.add_argument(
        "--publications",
        dest="publication_paths",
        type=pathlib.Path,
        action="append",
        required=True,
    )
    bom.add_argument("--output", type=pathlib.Path)
    release = subparsers.add_parser("verify-release")
    release.add_argument("--version", required=True)
    notes = subparsers.add_parser("release-notes")
    notes.add_argument("--version", required=True)
    notes.add_argument("--output", type=pathlib.Path, required=True)
    return parser.parse_args()


def main() -> int:
    arguments = parse_arguments()
    if arguments.command == "source-snapshot":
        print(current_source_snapshot_digest())
        return 0
    registry = load_registry()
    if arguments.command == "check-docs":
        expected = render_table(registry)
        if not TABLE_PATH.is_file() or TABLE_PATH.read_text(encoding="utf-8") != expected:
            raise ValueError(
                "docs/version-domains.md drifted; run scripts/version_domains.py generate-docs"
            )
        print("verified generated version-domain documentation")
        return 0
    if arguments.command == "verify-source-closure":
        verify_source_candidate_closure(registry)
        print(f"verified source closure for {len(registry['domains'])} version domains")
        return 0
    verify_registry(registry)
    if arguments.command == "verify":
        expected = render_table(registry)
        if not TABLE_PATH.is_file() or TABLE_PATH.read_text(encoding="utf-8") != expected:
            raise ValueError(
                "docs/version-domains.md drifted; run scripts/version_domains.py generate-docs"
            )
        print(f"verified {len(registry['domains'])} version domains")
    elif arguments.command == "generate-docs":
        TABLE_PATH.write_text(render_table(registry), encoding="utf-8")
        print(f"generated {TABLE_PATH.relative_to(ROOT)}")
    elif arguments.command == "digest":
        value = digest_json(registry)
        print(value.removeprefix("sha256:") if arguments.bare else value)
    elif arguments.command == "bom":
        publications: list[Any] = []
        for path in arguments.publication_paths:
            if not path.is_absolute() or path.is_symlink() or not path.is_file():
                raise ValueError(
                    "release publications must be absolute regular files"
                )
            value = load_json(path)
            if isinstance(value, list):
                publications.extend(value)
            else:
                publications.append(value)
        bom = build_bom(
            registry,
            arguments.source_sha,
            arguments.public_source_sha,
            publications,
        )
        encoded = json.dumps(bom, indent=2, sort_keys=True) + "\n"
        if arguments.output is None:
            sys.stdout.write(encoded)
        else:
            arguments.output.write_text(encoded, encoding="utf-8")
            print(arguments.output)
    elif arguments.command == "verify-release":
        verify_release_changelog(arguments.version)
        print(f"verified release changelog for {arguments.version}")
    elif arguments.command == "release-notes":
        arguments.output.write_text(release_notes(arguments.version), encoding="utf-8")
        print(f"materialized exact release notes for {arguments.version}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (KeyError, OSError, subprocess.CalledProcessError, ValueError) as error:
        raise SystemExit(f"version-domain validation failed: {error}") from error
