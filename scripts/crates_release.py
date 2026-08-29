#!/usr/bin/env python3
"""Verify and publish the ordered public Cymule crate set."""

from __future__ import annotations

import argparse
import dataclasses
import email.utils
import heapq
import hashlib
import json
import math
import os
import pathlib
import re
import shutil
import struct
import subprocess
import sys
import tarfile
import tempfile
import time
import tomllib
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable


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
sys.path.insert(0, str(CONTROL_ROOT / "scripts"))
import version_domains  # noqa: E402

CATALOG_PATH = ROOT / "scripts" / "crates-release.toml"
USER_AGENT = "cymule-release/1 (https://github.com/cymule-framework/cymule)"
REGISTRY_API = "https://crates.io/api/v1"
STATIC_REGISTRY = "https://static.crates.io/crates"
MAX_CRATE_BYTES = 10 * 1024 * 1024
MAX_REGISTRY_UPLOAD_BYTES = 2 * MAX_CRATE_BYTES + 8
MAX_NEW_CRATE_RATE_LIMIT_WAIT_SECONDS = 15 * 60
MAX_NEW_CRATE_RATE_LIMIT_RETRIES = 2
MAX_REGISTRY_CHECKSUM_WAIT_SECONDS = 300
REGISTRY_CHECKSUM_POLL_SECONDS = 5
GIT_SHA_PATTERN = re.compile(r"[0-9a-f]{40}")
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
PREFIXED_SHA256_PATTERN = re.compile(r"sha256:[0-9a-f]{64}")
STABLE_VERSION_PATTERN = re.compile(
    r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
)
CRATE_NAME_PATTERN = re.compile(r"[a-z][a-z0-9_-]*")
NEW_CRATE_RATE_LIMIT_MARKERS = (
    "status 429 Too Many Requests",
    "You have published too many new crates in a short period of time.",
)
NEW_CRATE_RETRY_PATTERN = re.compile(
    r"Please try again after "
    r"([A-Za-z]{3}, \d{1,2} [A-Za-z]{3} \d{4} \d{2}:\d{2}:\d{2} GMT)"
)
CRATES_PACKAGE_REPORT_VERSION = "cymule.crates-package-report/1"
CRATES_PUBLISH_REPORT_VERSION = "cymule.crates-publish-report/1"
CRATES_RELEASE_STAGE_VERSION = "cymule.crates-release-stage/3"


class RegistryChecksumMissing(TimeoutError):
    """The registry remained reachable but never exposed the requested version."""


class CratePublishOutcomeAmbiguous(ValueError):
    """A PUT response was lost and exact registry readback was unavailable."""


def version_registry_digest() -> str:
    return version_domains.registry_digest(ROOT)


def load_i_json(value: bytes, *, label: str) -> object:
    """Decode release authority through the shared strict I-JSON contract."""

    return version_domains.load_json_bytes(value, label=label)


@dataclasses.dataclass(frozen=True)
class PublicCrate:
    """One public crate and its direct public Cymule dependencies."""

    name: str
    path: pathlib.Path
    dependencies: tuple[str, ...]


@dataclasses.dataclass(frozen=True)
class ClosedCrate:
    """One independently closed archive and its exact registry upload body."""

    archive_sha256: str
    upload: pathlib.Path
    upload_sha256: str


def deterministic_publish_order(
    dependencies: dict[str, set[str]],
    *,
    preference: tuple[str, ...] | None = None,
) -> tuple[str, ...]:
    """Return one stable dependency-first order and reject incomplete graphs."""

    nodes = set(dependencies)
    unknown = set().union(*dependencies.values(), set()) - nodes
    if unknown:
        raise ValueError(
            f"public crate graph references unknown dependencies {sorted(unknown)}"
        )
    if preference is None:
        preference = tuple(sorted(nodes))
    if len(preference) != len(nodes) or set(preference) != nodes:
        raise ValueError("public crate graph preference does not cover every crate")
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
        raise ValueError(f"public crate publish graph contains a cycle: {cycle}")
    return tuple(ordered)


def cargo_dependency_is_published(dependency: dict[str, object]) -> bool:
    """Match the dependency kinds retained by Cargo's normalized package."""

    kind = dependency.get("kind")
    requirement = dependency.get("req")
    if kind not in (None, "build", "dev") or not isinstance(requirement, str):
        raise ValueError("Cargo metadata contains an unsupported dependency kind")
    return kind in (None, "build") or requirement != "*"


def cargo_publish_graph(
    packages: dict[str, object], catalog_names: set[str]
) -> dict[str, set[str]]:
    """Project Cargo metadata to the public edges retained for publication."""

    graph = {name: set() for name in catalog_names}
    for name in sorted(catalog_names):
        package = packages.get(name)
        if not isinstance(package, dict) or not isinstance(
            package.get("dependencies"), list
        ):
            raise ValueError(f"Cargo metadata is missing public crate {name}")
        for dependency in package["dependencies"]:
            if not isinstance(dependency, dict):
                raise ValueError(f"Cargo metadata dependency is malformed for {name}")
            dependency_name = dependency.get("name")
            if dependency_name not in catalog_names:
                continue
            if cargo_dependency_is_published(dependency):
                graph[name].add(dependency_name)
    return graph


def run(
    args: list[str],
    *,
    cwd: pathlib.Path = ROOT,
    env: dict[str, str] | None = None,
    capture: bool = False,
) -> subprocess.CompletedProcess[str]:
    """Run one visible release command and fail on the first error."""

    print("+", " ".join(args), flush=True)
    return subprocess.run(
        args,
        cwd=cwd,
        env=env,
        check=True,
        text=True,
        capture_output=capture,
    )


def load_catalog() -> list[PublicCrate]:
    """Load and validate the single ordered crate publication catalog."""

    with CATALOG_PATH.open("rb") as handle:
        raw = tomllib.load(handle)
    if (
        set(raw) != {"schema", "crate"}
        or raw.get("schema") != 1
        or not isinstance(raw.get("crate"), list)
    ):
        raise ValueError("unsupported or malformed crate release catalog")
    crates: list[PublicCrate] = []
    catalog_projection: list[dict[str, object]] = []
    seen: set[str] = set()
    for entry in raw["crate"]:
        if (
            not isinstance(entry, dict)
            or set(entry) != {"name", "path", "dependencies"}
            or not isinstance(entry.get("name"), str)
            or CRATE_NAME_PATTERN.fullmatch(entry["name"]) is None
            or not isinstance(entry.get("path"), str)
            or not isinstance(entry.get("dependencies"), list)
            or any(not isinstance(value, str) for value in entry["dependencies"])
            or len(set(entry["dependencies"])) != len(entry["dependencies"])
            or entry["dependencies"] != sorted(entry["dependencies"])
        ):
            raise ValueError("malformed public crate catalog entry")
        relative = pathlib.PurePosixPath(entry["path"])
        if (
            relative.is_absolute()
            or relative.as_posix() != entry["path"]
            or not relative.parts
            or ".." in relative.parts
            or pathlib.PureWindowsPath(entry["path"]).is_absolute()
            or "\\" in entry["path"]
        ):
            raise ValueError(f"invalid public crate path {entry['path']}")
        unresolved_path = ROOT.joinpath(*relative.parts)
        if any(
            ROOT.joinpath(*relative.parts[:index]).is_symlink()
            for index in range(1, len(relative.parts) + 1)
        ):
            raise ValueError(f"public crate path is a symlink for {entry['name']}")
        crate_path = unresolved_path.resolve(strict=True)
        try:
            crate_path.relative_to(ROOT.resolve(strict=True))
        except ValueError as error:
            raise ValueError(f"public crate path escapes payload for {entry['name']}") from error
        manifest = crate_path / "Cargo.toml"
        crate = PublicCrate(
            name=entry["name"],
            path=crate_path,
            dependencies=tuple(entry["dependencies"]),
        )
        if (
            crate.name in seen
            or manifest.is_symlink()
            or not manifest.is_file()
        ):
            raise ValueError(f"invalid or duplicate public crate {crate.name}")
        seen.add(crate.name)
        crates.append(crate)
        catalog_projection.append(
            {
                "name": entry["name"],
                "path": entry["path"],
                "dependencies": entry["dependencies"],
            }
        )
    catalog_order = tuple(crate.name for crate in crates)
    declared_graph = {crate.name: set(crate.dependencies) for crate in crates}
    ordered = deterministic_publish_order(
        declared_graph,
        preference=catalog_order,
    )
    if ordered != catalog_order:
        raise ValueError(
            "crate catalog is not in dependency-first publish order: "
            f"expected {list(ordered)}"
        )
    authoritative = version_domains.release_catalog_entries(ROOT)
    if catalog_projection != authoritative:
        raise ValueError(
            "crate catalog differs from Cargo's normalized publication graph"
        )
    return crates


def validate_stable_version(version: object) -> str:
    if not isinstance(version, str) or STABLE_VERSION_PATTERN.fullmatch(version) is None:
        raise ValueError("crate release version must be one exact stable SemVer")
    return version


def cargo_metadata() -> dict[str, object]:
    """Read current workspace package authority from Cargo."""

    result = run(
        ["cargo", "metadata", "--format-version", "1", "--no-deps"],
        capture=True,
    )
    metadata = load_i_json(
        result.stdout.encode("utf-8"), label="cargo metadata output"
    )
    if not isinstance(metadata, dict):
        raise ValueError("cargo metadata returned a non-object")
    return metadata


def validate_workspace(crates: list[PublicCrate], requested: str | None = None) -> str:
    """Validate catalog coverage, versions, metadata, and dependency edges."""

    metadata = cargo_metadata()
    if not isinstance(metadata.get("packages"), list):
        raise ValueError("Cargo metadata omits its package inventory")
    packages = {
        package["name"]: package
        for package in metadata["packages"]
        if isinstance(package, dict) and isinstance(package.get("name"), str)
    }
    catalog_names = {crate.name for crate in crates}
    published = {
        name
        for name, package in packages.items()
        if name.startswith("cymule") and package.get("publish") == ["crates-io"]
    }
    if published != catalog_names:
        raise ValueError(
            "crate catalog mismatch: "
            f"missing={sorted(published - catalog_names)} "
            f"extra={sorted(catalog_names - published)}"
        )
    declared_graph = {crate.name: set(crate.dependencies) for crate in crates}
    actual_graph = cargo_publish_graph(packages, catalog_names)
    catalog_order = tuple(crate.name for crate in crates)
    actual_order = deterministic_publish_order(
        actual_graph,
        preference=catalog_order,
    )
    if actual_order != catalog_order:
        raise ValueError(
            "crate catalog is not in Cargo dependency-first publish order: "
            f"expected {list(actual_order)}"
        )
    for crate in crates:
        actual = actual_graph[crate.name]
        declared = declared_graph[crate.name]
        if actual != declared:
            raise ValueError(
                f"catalog dependencies for {crate.name} are {sorted(declared)}, "
                f"Cargo publishes {sorted(actual)}"
            )
    versions = {packages[crate.name]["version"] for crate in crates}
    if len(versions) != 1:
        raise ValueError(f"public crate versions diverge: {sorted(versions)}")
    version = validate_stable_version(versions.pop())
    if requested is not None and version != requested:
        raise ValueError(f"requested {requested}, manifests contain {version}")
    typescript_manifest = load_i_json(
        ROOT.joinpath("sdk/typescript/package.json").read_bytes(),
        label="sdk/typescript/package.json",
    )
    if not isinstance(typescript_manifest, dict):
        raise ValueError("TypeScript package manifest must be an object")
    typescript_version = typescript_manifest["version"]
    if typescript_version != version:
        raise ValueError(
            f"TypeScript package {typescript_version} does not match Rust {version}"
        )
    python_version = tomllib.loads(
        ROOT.joinpath("sdk/python/pyproject.toml").read_text(encoding="utf-8")
    )["project"]["version"]
    if python_version != version:
        raise ValueError(
            f"Python package {python_version} does not match Rust {version}"
        )
    for crate in crates:
        package = packages[crate.name]
        if pathlib.Path(package["manifest_path"]).parent != crate.path:
            raise ValueError(f"catalog path mismatch for {crate.name}")
        if package.get("repository") != "https://github.com/cymule-framework/cymule":
            raise ValueError(f"public repository metadata is missing for {crate.name}")
        if package.get("readme") is None:
            raise ValueError(f"README metadata is missing for {crate.name}")
        for dependency in package["dependencies"]:
            if (
                dependency["name"] in catalog_names
                and cargo_dependency_is_published(dependency)
            ):
                if dependency["req"] != f"^{version}":
                    raise ValueError(
                        f"{crate.name} dependency {dependency['name']} uses "
                        f"{dependency['req']} instead of ^{version}"
                    )
    return version


def sha256(path: pathlib.Path) -> str:
    """Hash one immutable archive without loading it into memory."""

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def stage_file(directory: pathlib.Path, basename: object) -> pathlib.Path:
    """Resolve one regular, non-symlink stage file owned by ``directory``."""

    if (
        not isinstance(basename, str)
        or not basename
        or pathlib.PurePosixPath(basename).name != basename
        or pathlib.PureWindowsPath(basename).name != basename
    ):
        raise ValueError("crate release stage file must be one basename")
    path = directory / basename
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"crate release stage file {basename} is not regular non-symlink")
    resolved = path.resolve(strict=True)
    if resolved.parent != directory:
        raise ValueError(f"crate release stage file {basename} escapes its stage")
    return resolved


def inspect_crate_members(
    archive: tarfile.TarFile, crate: str, version: str
) -> dict[tuple[str, ...], tarfile.TarInfo]:
    """Close the complete Cargo archive namespace before reading any member."""

    root = (f"{crate}-{version}",)
    members: dict[tuple[str, ...], tarfile.TarInfo] = {}
    for member in archive.getmembers():
        path = pathlib.PurePosixPath(member.name)
        if (
            path.is_absolute()
            or not path.parts
            or ".." in path.parts
            or path.parts[:1] != root
            or member.issym()
            or member.islnk()
            or not (member.isfile() or member.isdir())
            or path.parts in members
        ):
            raise ValueError(f"unsafe archive member in {crate}: {member.name}")
        members[path.parts] = member
    manifest_path = (*root, "Cargo.toml")
    manifest = members.get(manifest_path)
    if manifest is None or not manifest.isfile():
        raise ValueError(f"archive for {crate} omits normalized Cargo.toml")
    return members


def publish_metadata(archive_path: pathlib.Path, crate: str, version: str) -> bytes:
    """Derive Cargo's documented registry metadata from normalized archive bytes."""

    root = pathlib.PurePosixPath(f"{crate}-{version}")
    with tarfile.open(archive_path, "r:gz") as archive:
        members = inspect_crate_members(archive, crate, version)
        manifest_member = members[(*root.parts, "Cargo.toml")]
        manifest_stream = archive.extractfile(manifest_member)
        if manifest_stream is None:
            raise ValueError(f"archive for {crate} omits normalized Cargo.toml")
        manifest = tomllib.loads(manifest_stream.read().decode("utf-8"))
        package = manifest.get("package", {})
        if package.get("name") != crate or package.get("version") != version:
            raise ValueError(f"normalized archive identity is wrong for {crate}")
        readme_file = package.get("readme")
        readme = None
        if readme_file is not None:
            if not isinstance(readme_file, str):
                raise ValueError(f"normalized readme path is invalid for {crate}")
            readme_path = pathlib.PurePosixPath(readme_file)
            if (
                readme_path.is_absolute()
                or not readme_path.parts
                or ".." in readme_path.parts
                or readme_path.as_posix() != readme_file
            ):
                raise ValueError(f"normalized readme path is invalid for {crate}")
            readme_member = members.get((*root.parts, *readme_path.parts))
            if readme_member is None or not readme_member.isfile():
                raise ValueError(f"archive for {crate} omits {readme_file}")
            readme_stream = archive.extractfile(readme_member)
            if readme_stream is None:
                raise ValueError(f"archive for {crate} omits {readme_file}")
            readme = readme_stream.read().decode("utf-8")

    dependencies: list[dict[str, object]] = []

    def append_dependencies(
        table: object, kind: str, target: str | None = None
    ) -> None:
        if not isinstance(table, dict):
            return
        for explicit_name, raw in sorted(table.items()):
            specification = {"version": raw} if isinstance(raw, str) else raw
            if not isinstance(specification, dict):
                raise ValueError(f"invalid normalized dependency {explicit_name}")
            package_name = specification.get("package", explicit_name)
            dependencies.append(
                {
                    "name": package_name,
                    "version_req": specification.get("version", "*"),
                    "features": specification.get("features", []),
                    "optional": specification.get("optional", False),
                    "default_features": specification.get("default-features", True),
                    "target": target,
                    "kind": kind,
                    "registry": specification.get("registry"),
                    "explicit_name_in_toml": (
                        explicit_name if package_name != explicit_name else None
                    ),
                }
            )

    append_dependencies(manifest.get("dependencies"), "normal")
    append_dependencies(manifest.get("build-dependencies"), "build")
    append_dependencies(manifest.get("dev-dependencies"), "dev")
    targets = manifest.get("target", {})
    if isinstance(targets, dict):
        for target, tables in sorted(targets.items()):
            if not isinstance(tables, dict):
                continue
            append_dependencies(tables.get("dependencies"), "normal", target)
            append_dependencies(tables.get("build-dependencies"), "build", target)
            append_dependencies(tables.get("dev-dependencies"), "dev", target)
    metadata = {
        "name": crate,
        "vers": version,
        "deps": dependencies,
        "features": manifest.get("features", {}),
        "authors": package.get("authors", []),
        "description": package.get("description"),
        "documentation": package.get("documentation"),
        "homepage": package.get("homepage"),
        "readme": readme,
        "readme_file": readme_file,
        "keywords": package.get("keywords", []),
        "categories": package.get("categories", []),
        "license": package.get("license"),
        "license_file": package.get("license-file"),
        "repository": package.get("repository"),
        "badges": manifest.get("badges", {}),
        "links": package.get("links"),
        "rust_version": package.get("rust-version"),
    }
    return json.dumps(metadata, separators=(",", ":"), sort_keys=True).encode("utf-8")


def write_upload_body(
    archive: pathlib.Path, crate: str, version: str, output: pathlib.Path
) -> None:
    """Write the exact Cargo registry publish body around one closed archive."""

    metadata = publish_metadata(archive, crate, version)
    crate_bytes = archive.read_bytes()
    output.write_bytes(
        struct.pack("<I", len(metadata))
        + metadata
        + struct.pack("<I", len(crate_bytes))
        + crate_bytes
    )


def inspect_upload_body(
    body: pathlib.Path, archive: pathlib.Path, crate: str, version: str
) -> None:
    """Authenticate a staged upload body against its archive and closed identity."""

    if body.stat().st_size > MAX_REGISTRY_UPLOAD_BYTES:
        raise ValueError(f"registry upload body is oversized for {crate}")
    raw = body.read_bytes()
    if len(raw) < 8:
        raise ValueError(f"registry upload body is truncated for {crate}")
    metadata_length = struct.unpack("<I", raw[:4])[0]
    metadata_end = 4 + metadata_length
    if metadata_end + 4 > len(raw):
        raise ValueError(f"registry metadata length is invalid for {crate}")
    metadata = load_i_json(
        raw[4:metadata_end], label=f"registry upload metadata for {crate}"
    )
    if not isinstance(metadata, dict):
        raise ValueError(f"registry upload metadata is not an object for {crate}")
    crate_length = struct.unpack("<I", raw[metadata_end : metadata_end + 4])[0]
    crate_bytes = raw[metadata_end + 4 :]
    if crate_length != len(crate_bytes) or crate_bytes != archive.read_bytes():
        raise ValueError(f"registry upload body does not contain exact {crate} bytes")
    if metadata.get("name") != crate or metadata.get("vers") != version:
        raise ValueError(f"registry upload metadata identity is wrong for {crate}")
    if raw[4:metadata_end] != publish_metadata(archive, crate, version):
        raise ValueError(f"registry upload metadata is not canonical for {crate}")


def package_archive(
    crate: PublicCrate,
    version: str,
    target: pathlib.Path,
    *,
    allow_dirty: bool,
    verify: bool,
) -> pathlib.Path:
    """Create one Cargo archive and return its exact path."""

    command = ["cargo", "package", "--package", crate.name, "--target-dir", str(target)]
    if allow_dirty:
        command.append("--allow-dirty")
    if not verify:
        command.append("--no-verify")
    run(command)
    archive = target / "package" / f"{crate.name}-{version}.crate"
    if not archive.is_file() or archive.stat().st_size > MAX_CRATE_BYTES:
        raise ValueError(f"missing or oversized archive for {crate.name}")
    return archive


def package_workspace(
    crates: list[PublicCrate],
    version: str,
    target: pathlib.Path,
    *,
    allow_dirty: bool,
) -> dict[str, pathlib.Path]:
    """Package the complete workspace release set in one dependency-aware run."""

    command = ["cargo", "package", "--target-dir", str(target), "--no-verify"]
    for crate in crates:
        command.extend(["--package", crate.name])
    if allow_dirty:
        command.append("--allow-dirty")
    run(command)
    archives = {
        crate.name: target / "package" / f"{crate.name}-{version}.crate"
        for crate in crates
    }
    for name, archive in archives.items():
        if not archive.is_file() or archive.stat().st_size > MAX_CRATE_BYTES:
            raise ValueError(f"missing or oversized archive for {name}")
    return archives


def cargo_publish_dry_run(crates: list[PublicCrate], allow_dirty: bool) -> None:
    """Run Cargo's own dependency-aware workspace publication simulation."""

    patch = (
        "[patch.crates-io]\n"
        + "\n".join(
            f"{json.dumps(crate.name)} = {{ path = {json.dumps(str(crate.path))} }}"
            for crate in crates
        )
        + "\n"
    )
    with tempfile.TemporaryDirectory(prefix="cymule-publish-dry-run-") as directory:
        config = pathlib.Path(directory) / "config.toml"
        config.write_text(patch, encoding="utf-8")
        command = ["cargo", "publish", "--dry-run", "--config", str(config)]
        for crate in crates:
            command.extend(["--package", crate.name])
        if allow_dirty:
            command.append("--allow-dirty")
        run(command)


def write_packaged_workspace(
    staging: pathlib.Path,
    crates: list[PublicCrate],
    version: str,
    archives: dict[str, pathlib.Path],
) -> None:
    """Build a local registry-shaped workspace from normalized package bytes."""

    for crate in crates:
        extracted = staging / "extracted" / crate.name
        extracted.mkdir(parents=True)
        with tarfile.open(archives[crate.name], "r:gz") as archive:
            inspect_crate_members(archive, crate.name, version)
            archive.extractall(extracted, filter="data")
        source = extracted / f"{crate.name}-{version}"
        destination = staging / "packages" / crate.name
        shutil.copytree(source, destination)
        normalized = destination / "Cargo.toml"
        with normalized.open("rb") as handle:
            manifest = tomllib.load(handle)
        dependency_tables = [
            manifest.get("dependencies", {}),
            manifest.get("dev-dependencies", {}),
            manifest.get("build-dependencies", {}),
        ]
        for target in manifest.get("target", {}).values():
            dependency_tables.extend(
                [
                    target.get("dependencies", {}),
                    target.get("dev-dependencies", {}),
                    target.get("build-dependencies", {}),
                ]
            )
        if any(
            isinstance(specification, dict) and "path" in specification
            for table in dependency_tables
            for specification in table.values()
        ):
            raise ValueError(
                f"normalized manifest leaked a dependency path for {crate.name}"
            )
        if manifest["package"].get("publish") != ["crates-io"]:
            raise ValueError(f"normalized publish policy is wrong for {crate.name}")
        if not destination.joinpath("README.md").is_file():
            raise ValueError(f"packaged README is missing for {crate.name}")
    consumer = staging / "consumer"
    consumer.mkdir()
    consumer.joinpath("src").mkdir()
    consumer.joinpath("Cargo.toml").write_text(
        """[package]
name = "cymule-registry-consumer"
version = "0.0.0"
edition = "2024"
publish = false

[dependencies]
cymule = "={version}"
cymule-agent = "={version}"
cymule-directory-store = "={version}"
serde_json = "1.0.149"
""".format(
            version=version
        ),
        encoding="utf-8",
    )
    consumer.joinpath("src/lib.rs").write_text(
        """use cymule::{Expression, FlowBuilder, PlanCandidate};

pub fn build_flow() -> PlanCandidate {
    FlowBuilder::new("registry_consumer", serde_json::json!({}), serde_json::json!({}))
        .finish(Expression::Input)
}
""",
        encoding="utf-8",
    )

    members = [f'  "packages/{crate.name}",' for crate in crates]
    members.append('  "consumer",')
    patches = [
        f'{json.dumps(crate.name)} = {{ path = "packages/{crate.name}" }}'
        for crate in crates
    ]
    staging.joinpath("Cargo.toml").write_text(
        '[workspace]\nresolver = "3"\nmembers = [\n'
        + "\n".join(members)
        + "\n]\n\n[patch.crates-io]\n"
        + "\n".join(patches)
        + "\n",
        encoding="utf-8",
    )
    run(["cargo", "check", "--workspace", "--lib", "--bins"], cwd=staging)


def verify_packages(allow_dirty: bool) -> dict[str, object]:
    """Prove deterministic archives and compile their normalized manifests."""

    crates = load_catalog()
    version = validate_workspace(crates)
    cargo_publish_dry_run(crates, allow_dirty)
    staging = ROOT / ".cache" / "crates-package" / version
    expected_parent = ROOT / ".cache" / "crates-package"
    if staging.parent != expected_parent:
        raise ValueError("unsafe crate staging path")
    shutil.rmtree(staging, ignore_errors=True)
    target = staging / "cargo-target"
    target.mkdir(parents=True)
    first = package_workspace(crates, version, target, allow_dirty=allow_dirty)
    first_hashes = {name: sha256(archive) for name, archive in first.items()}
    second = package_workspace(crates, version, target, allow_dirty=allow_dirty)
    hashes = {name: sha256(archive) for name, archive in second.items()}
    for crate in crates:
        if first_hashes[crate.name] != hashes[crate.name]:
            raise ValueError(f"archive for {crate.name} is not deterministic")
    upload_hashes: dict[str, str] = {}
    upload_directory = staging / "uploads"
    upload_directory.mkdir()
    for crate in crates:
        upload = upload_directory / f"{crate.name}-{version}.publish"
        write_upload_body(second[crate.name], crate.name, version, upload)
        inspect_upload_body(upload, second[crate.name], crate.name, version)
        upload_hashes[crate.name] = sha256(upload)
    write_packaged_workspace(staging, crates, version, second)
    report = {
        "schema": CRATES_PACKAGE_REPORT_VERSION,
        "version": version,
        "crates": [
            {
                "name": crate.name,
                "sha256": hashes[crate.name],
                "upload_sha256": upload_hashes[crate.name],
            }
            for crate in crates
        ],
    }
    report_path = staging / "report.json"
    report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"verified {len(crates)} deterministic crate archives at {report_path}")
    return report


def request_json(path: str) -> dict[str, object] | None:
    """Read one crates.io API object; return None only for an exact 404."""

    request = urllib.request.Request(
        f"{REGISTRY_API}{path}", headers={"User-Agent": USER_AGENT}
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = load_i_json(response.read(), label=request.full_url)
        if not isinstance(payload, dict):
            raise ValueError(f"crates.io returned a non-object from {request.full_url}")
        return payload
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return None
        raise


def registry_checksum(crate: str, version: str) -> str | None:
    """Return the exact registry checksum for one version when it exists."""

    name = urllib.parse.quote(crate, safe="")
    release = urllib.parse.quote(version, safe="")
    payload = request_json(f"/crates/{name}/{release}")
    if payload is None:
        return None
    record = payload.get("version")
    checksum = record.get("checksum") if isinstance(record, dict) else None
    if not isinstance(checksum, str) or SHA256_PATTERN.fullmatch(checksum) is None:
        raise ValueError(f"crates.io returned a malformed checksum for {crate}@{version}")
    return checksum


def wait_for_checksum(crate: str, version: str, expected: str) -> None:
    """Wait a bounded interval for the registry index to expose exact bytes."""

    deadline = time.monotonic() + MAX_REGISTRY_CHECKSUM_WAIT_SECONDS
    last_observation = "unavailable"
    last_error: BaseException | None = None
    while True:
        try:
            observed = registry_checksum(crate, version)
        except (OSError, TimeoutError, urllib.error.URLError) as error:
            last_observation = "unavailable"
            last_error = error
        else:
            last_error = None
            if observed is not None:
                if observed != expected:
                    raise ValueError(
                        f"registry checksum mismatch for {crate}@{version}: {observed}"
                    )
                return
            last_observation = "missing"
        if time.monotonic() >= deadline:
            if last_observation == "missing":
                raise RegistryChecksumMissing(
                    f"registry did not index {crate}@{version} within "
                    f"{MAX_REGISTRY_CHECKSUM_WAIT_SECONDS} seconds"
                )
            raise CratePublishOutcomeAmbiguous(
                "crate_publish_outcome_ambiguous: exact registry checksum "
                f"readback unavailable for {crate}@{version}"
            ) from last_error
        time.sleep(REGISTRY_CHECKSUM_POLL_SECONDS)


def crate_download_url(crate: str, version: str) -> str:
    return f"{STATIC_REGISTRY}/{crate}/{crate}-{version}.crate"


def verify_download(crate: str, version: str, expected: str) -> str:
    """Download published bytes and verify the registry checksum independently."""

    url = crate_download_url(crate, version)
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    digest = hashlib.sha256()
    with urllib.request.urlopen(request, timeout=60) as response:
        for chunk in iter(lambda: response.read(1024 * 1024), b""):
            digest.update(chunk)
    observed = digest.hexdigest()
    if observed != expected:
        raise ValueError(f"downloaded archive checksum mismatch for {crate}@{version}")
    return observed


def stage_registry_evidence(
    directory: pathlib.Path, version: str, release_sha: str
) -> list[dict[str, object]]:
    """Bind one authenticated stage to fresh crates.io checksum and byte readback."""

    version = validate_stable_version(version)
    crates = load_catalog()
    closed = load_stage(directory, crates, version, release_sha)
    evidence: list[dict[str, object]] = []
    for crate in crates:
        expected = closed[crate.name].archive_sha256
        observed = registry_checksum(crate.name, version)
        if observed is None:
            raise ValueError(f"missing {crate.name}@{version} from crates.io")
        if observed != expected:
            raise ValueError(
                f"immutable registry version {crate.name}@{version} has other bytes"
            )
        downloaded = verify_download(crate.name, version, expected)
        evidence.append(
            {
                "package_id": f"cargo:{crate.name}",
                "name": crate.name,
                "version": version,
                "publication": {
                    "kind": "cargo",
                    "registry": "https://crates.io/",
                    "registry_identity": (
                        f"https://crates.io/crates/{crate.name}/{version}"
                    ),
                    "content_digest": f"sha256:{downloaded}",
                    "provenance": {
                        "kind": "registry-checksum",
                        "checksum": f"sha256:{observed}",
                        "download_url": crate_download_url(crate.name, version),
                    },
                },
            }
        )
    return evidence


def write_publication_evidence(
    output: pathlib.Path, evidence: list[dict[str, object]]
) -> None:
    """Materialize job-local BOM inputs after exact stage/registry convergence."""

    if (
        not output.is_absolute()
        or output.parent.is_symlink()
        or not output.parent.is_dir()
        or output.exists()
    ):
        raise ValueError("crate publication evidence output must be one new absolute file")
    with output.open("x", encoding="utf-8") as handle:
        handle.write(json.dumps(evidence, indent=2, sort_keys=True) + "\n")


def require_release_checkout(version: str) -> None:
    """Require a clean checkout of the exact immutable public release tag."""

    if run(["git", "status", "--porcelain"], capture=True).stdout:
        raise ValueError("crate publication requires a clean checkout")
    tag = run(
        ["git", "describe", "--exact-match", "--tags"], capture=True
    ).stdout.strip()
    if tag != f"v{version}":
        raise ValueError(
            f"crate publication requires exact tag v{version}, found {tag}"
        )


def stage_packages(version: str, release_sha: str, output: pathlib.Path) -> None:
    """Stage exact Cargo archives before any registry identity is granted."""

    version = validate_stable_version(version)
    if GIT_SHA_PATTERN.fullmatch(release_sha) is None:
        raise ValueError("release SHA must be one exact lowercase Git commit identity")
    crates = load_catalog()
    validate_workspace(crates, version)
    require_release_checkout(version)
    head = run(["git", "rev-parse", "HEAD"], capture=True).stdout.strip()
    if head != release_sha:
        raise ValueError(
            f"release checkout {head} does not match verified {release_sha}"
        )
    output.mkdir(parents=True, exist_ok=False)
    with tempfile.TemporaryDirectory(prefix="cymule-crates-stage-") as temp:
        archives = package_workspace(
            crates,
            version,
            pathlib.Path(temp),
            allow_dirty=False,
        )
        entries = []
        for crate in crates:
            destination = output / archives[crate.name].name
            shutil.copyfile(archives[crate.name], destination)
            upload = output / f"{crate.name}-{version}.publish"
            write_upload_body(destination, crate.name, version, upload)
            entries.append(
                {
                    "name": crate.name,
                    "archive": destination.name,
                    "sha256": sha256(destination),
                    "upload": upload.name,
                    "upload_sha256": sha256(upload),
                }
            )
    output.joinpath("manifest.json").write_text(
        json.dumps(
            {
                "schema": CRATES_RELEASE_STAGE_VERSION,
                "version": version,
                "release_sha": release_sha,
                "version_domain_registry_digest": version_registry_digest(),
                "crates": entries,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    print(f"staged {len(crates)} exact crate archives for {release_sha}")


def load_stage(
    directory: pathlib.Path,
    crates: list[PublicCrate],
    version: str,
    release_sha: str,
) -> dict[str, ClosedCrate]:
    """Authenticate the no-OIDC stage consumed by the publisher."""

    version = validate_stable_version(version)
    if GIT_SHA_PATTERN.fullmatch(release_sha) is None:
        raise ValueError("crate release stage requires one exact release SHA")
    directory = directory.resolve(strict=True)
    if not directory.is_dir():
        raise ValueError("crate release stage is not a directory")
    manifest_path = stage_file(directory, "manifest.json")
    manifest = load_i_json(manifest_path.read_bytes(), label=str(manifest_path))
    if not isinstance(manifest, dict):
        raise ValueError("malformed crate release stage manifest")
    if set(manifest) != {
        "schema",
        "version",
        "release_sha",
        "version_domain_registry_digest",
        "crates",
    }:
        raise ValueError("malformed crate release stage manifest")
    if manifest["schema"] != CRATES_RELEASE_STAGE_VERSION:
        raise ValueError("unsupported crate release stage manifest")
    if manifest["version"] != version or manifest["release_sha"] != release_sha:
        raise ValueError("crate release stage belongs to another version or commit")
    if (
        not isinstance(manifest["version_domain_registry_digest"], str)
        or PREFIXED_SHA256_PATTERN.fullmatch(
            manifest["version_domain_registry_digest"]
        )
        is None
        or manifest["version_domain_registry_digest"] != version_registry_digest()
    ):
        raise ValueError("crate release stage belongs to another version-domain generation")
    expected_names = [crate.name for crate in crates]
    entries = manifest["crates"]
    if not isinstance(entries, list) or len(entries) != len(expected_names):
        raise ValueError("crate release stage does not match the ordered catalog")
    for entry, expected_name in zip(entries, expected_names, strict=True):
        if not isinstance(entry, dict) or entry.get("name") != expected_name:
            raise ValueError("crate release stage does not match the ordered catalog")

    expected_files = {"manifest.json"}
    for entry in entries:
        if set(entry) != {
            "name",
            "archive",
            "sha256",
            "upload",
            "upload_sha256",
        }:
            raise ValueError("malformed crate release stage entry")
        if (
            not isinstance(entry["sha256"], str)
            or SHA256_PATTERN.fullmatch(entry["sha256"]) is None
            or not isinstance(entry["upload_sha256"], str)
            or SHA256_PATTERN.fullmatch(entry["upload_sha256"]) is None
        ):
            raise ValueError("malformed crate release stage digest")
        expected_archive = f"{entry['name']}-{version}.crate"
        expected_upload = f"{entry['name']}-{version}.publish"
        if entry["archive"] != expected_archive or entry["upload"] != expected_upload:
            raise ValueError("crate release stage file must be one expected basename")
        expected_files.update((expected_archive, expected_upload))
    observed_files = {path.name for path in directory.iterdir()}
    if observed_files != expected_files:
        raise ValueError("crate release stage does not contain its exact file set")

    closed: dict[str, ClosedCrate] = {}
    for entry in entries:
        expected_archive = f"{entry['name']}-{version}.crate"
        archive = stage_file(directory, entry["archive"])
        if archive.stat().st_size > MAX_CRATE_BYTES:
            raise ValueError(f"staged archive is oversized for {entry['name']}")
        observed = sha256(archive)
        if observed != entry["sha256"]:
            raise ValueError(f"staged archive digest changed for {entry['name']}")
        expected_upload = f"{entry['name']}-{version}.publish"
        upload = stage_file(directory, entry["upload"])
        if upload.stat().st_size > MAX_REGISTRY_UPLOAD_BYTES:
            raise ValueError(f"staged upload body is oversized for {entry['name']}")
        upload_sha256 = sha256(upload)
        if upload_sha256 != entry["upload_sha256"]:
            raise ValueError(f"staged upload body changed for {entry['name']}")
        inspect_upload_body(upload, archive, entry["name"], version)
        closed[entry["name"]] = ClosedCrate(observed, upload, upload_sha256)
    return closed


def compare_stages(candidate: pathlib.Path, reference: pathlib.Path) -> None:
    """Require independently packaged crate stages to be byte-identical."""

    candidate_path = candidate / "manifest.json"
    candidate_manifest = load_i_json(
        candidate_path.read_bytes(), label=str(candidate_path)
    )
    if not isinstance(candidate_manifest, dict):
        raise ValueError("candidate crate stage manifest must be an object")
    version = candidate_manifest.get("version")
    release_sha = candidate_manifest.get("release_sha")
    if not isinstance(version, str) or not isinstance(release_sha, str):
        raise ValueError("candidate crate stage omits version or release SHA")
    crates = load_catalog()
    candidate_closed = load_stage(candidate, crates, version, release_sha)
    reference_closed = load_stage(reference, crates, version, release_sha)
    reference_path = reference / "manifest.json"
    reference_manifest = load_i_json(
        reference_path.read_bytes(), label=str(reference_path)
    )
    if candidate_manifest != reference_manifest:
        raise ValueError("independent crate stage manifests differ")
    for crate in crates:
        left = candidate_closed[crate.name]
        right = reference_closed[crate.name]
        if left.archive_sha256 != right.archive_sha256:
            raise ValueError(f"independent archives differ for {crate.name}")
        if left.upload_sha256 != right.upload_sha256:
            raise ValueError(f"independent upload bodies differ for {crate.name}")


def new_crate_rate_limit_delay(output: str, *, now: float | None = None) -> int | None:
    """Return a bounded retry delay only for crates.io's explicit new-name limit."""

    if not all(marker in output for marker in NEW_CRATE_RATE_LIMIT_MARKERS):
        return None
    match = NEW_CRATE_RETRY_PATTERN.search(output)
    if match is None:
        raise ValueError("crates.io new-crate rate limit omitted a retry timestamp")
    retry_at = email.utils.parsedate_to_datetime(match.group(1))
    current = time.time() if now is None else now
    delay = max(5, math.ceil(retry_at.timestamp() - current) + 5)
    if delay > MAX_NEW_CRATE_RATE_LIMIT_WAIT_SECONDS:
        raise ValueError(
            f"crates.io requested an excessive new-crate retry delay of {delay} seconds"
        )
    return delay


def verify_remote_release_authority(
    controller_sha: str, release_sha: str, release_tag: str
) -> None:
    """Re-read the exact remote main and peeled release tag before one write."""

    main_ref = "refs/heads/main"
    tag_ref = f"refs/tags/{release_tag}^{{}}"
    result = run(
        ["git", "ls-remote", "origin", main_ref, tag_ref],
        cwd=CONTROL_ROOT,
        capture=True,
    )
    observed: dict[str, str] = {}
    for line in result.stdout.splitlines():
        fields = line.split("\t")
        if (
            len(fields) != 2
            or GIT_SHA_PATTERN.fullmatch(fields[0]) is None
            or fields[1] in observed
        ):
            raise ValueError("remote release authority returned malformed refs")
        observed[fields[1]] = fields[0]
    expected = {main_ref: controller_sha, tag_ref: release_sha}
    if observed != expected:
        raise ValueError(
            "remote release authority moved before crates.io publication"
        )


def publish_crate(
    crate: str,
    version: str,
    expected: str,
    upload: pathlib.Path,
    token: str,
    verify_authority: Callable[[], None],
) -> None:
    """Upload one already-closed body, retrying only the exact new-name limit."""

    body = upload.read_bytes()
    for attempt in range(MAX_NEW_CRATE_RATE_LIMIT_RETRIES + 1):
        request = urllib.request.Request(
            f"{REGISTRY_API}/crates/new",
            data=body,
            method="PUT",
            headers={
                "Accept": "application/json",
                "Authorization": token,
                "Content-Type": "application/octet-stream",
                "User-Agent": USER_AGENT,
            },
        )
        verify_authority()
        print(f"+ PUT exact closed bytes for {crate}", flush=True)
        put_error: BaseException | None = None
        rate_limit_output: str | None = None
        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                payload = load_i_json(
                    response.read(), label=f"crates.io publish response for {crate}"
                )
            if not isinstance(payload, dict) or payload.get("errors"):
                raise ValueError(f"crates.io rejected {crate}: {payload}")
        except urllib.error.HTTPError as error:
            try:
                error_body = error.read().decode("utf-8", errors="replace")
            finally:
                error.close()
            output = f"status {error.code} {error.reason}: {error_body}"
            put_error = error
            if error.code == 429:
                rate_limit_output = output
        except (OSError, TimeoutError, urllib.error.URLError, ValueError) as error:
            put_error = error
        try:
            wait_for_checksum(crate, version, expected)
            return
        except RegistryChecksumMissing as missing:
            if rate_limit_output is None:
                raise ValueError(
                    f"crates.io did not publish {crate}@{version}: {put_error}"
                ) from put_error or missing
        delay = new_crate_rate_limit_delay(rate_limit_output)
        if delay is None or attempt == MAX_NEW_CRATE_RATE_LIMIT_RETRIES:
            raise ValueError(f"crates.io rejected {crate}: {rate_limit_output}")
        print(
            f"crates.io limited new crate names; retrying {crate} in {delay} seconds",
            flush=True,
        )
        time.sleep(delay)


def publish(version: str) -> None:
    """Publish or exactly replay every crate in dependency order."""

    version = validate_stable_version(version)
    token = os.environ.get("CARGO_REGISTRY_TOKEN")
    if not token:
        raise ValueError("CARGO_REGISTRY_TOKEN is required")
    crates = load_catalog()
    require_release_checkout(version)
    release_sha = run(["git", "rev-parse", "HEAD"], capture=True).stdout.strip()
    controller_sha = os.environ.get("CONTROLLER_SHA", "")
    expected_release_sha = os.environ.get("RELEASE_SHA", "")
    release_tag = os.environ.get("RELEASE_TAG", "")
    if (
        GIT_SHA_PATTERN.fullmatch(controller_sha) is None
        or GIT_SHA_PATTERN.fullmatch(expected_release_sha) is None
        or expected_release_sha != release_sha
        or release_tag != f"v{version}"
    ):
        raise ValueError("crate publication release authority is missing or mismatched")
    local_controller_sha = run(
        ["git", "rev-parse", "HEAD"], cwd=CONTROL_ROOT, capture=True
    ).stdout.strip()
    if local_controller_sha != controller_sha:
        raise ValueError("crate publication controller checkout does not match main authority")
    stage_directory = os.environ.get("CYMULE_CRATES_STAGE")
    if stage_directory is None or not pathlib.Path(stage_directory).is_absolute():
        raise ValueError("CYMULE_CRATES_STAGE must name the absolute no-OIDC stage")
    closed = load_stage(pathlib.Path(stage_directory), crates, version, release_sha)
    report_dir = ROOT / ".cache" / "crates-release" / version
    report_dir.mkdir(parents=True, exist_ok=True)
    outcomes: list[dict[str, str]] = []
    for crate in crates:
        expected = closed[crate.name].archive_sha256
        observed = registry_checksum(crate.name, version)
        if observed is None:
            publish_crate(
                crate.name,
                version,
                expected,
                closed[crate.name].upload,
                token,
                lambda: verify_remote_release_authority(
                    controller_sha, release_sha, release_tag
                ),
            )
            outcome = "published"
        elif observed == expected:
            outcome = "retained"
        else:
            raise ValueError(
                f"immutable registry version {crate.name}@{version} has other bytes"
            )
        verify_download(crate.name, version, expected)
        outcomes.append({"name": crate.name, "sha256": expected, "outcome": outcome})
        report_dir.joinpath("publish.json").write_text(
            json.dumps(
                {
                    "schema": CRATES_PUBLISH_REPORT_VERSION,
                    "version": version,
                    "crates": outcomes,
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
    print(f"published or retained {len(crates)} exact crate versions")


def verify_registry(version: str) -> None:
    """Verify registry bytes against the exact tag and compile fresh consumers."""

    version = validate_stable_version(version)
    crates = load_catalog()
    validate_workspace(crates, version)
    require_release_checkout(version)
    with tempfile.TemporaryDirectory(prefix="cymule-crates-finalize-") as temp:
        target = pathlib.Path(temp)
        for crate in crates:
            archive = package_archive(
                crate, version, target, allow_dirty=False, verify=True
            )
            expected = sha256(archive)
            checksum = registry_checksum(crate.name, version)
            if checksum is None:
                raise ValueError(f"missing {crate.name}@{version} from crates.io")
            if checksum != expected:
                raise ValueError(
                    f"immutable registry version {crate.name}@{version} has other bytes"
                )
            verify_download(crate.name, version, expected)
    with tempfile.TemporaryDirectory(prefix="cymule-crates-consumer-") as temp:
        consumer = pathlib.Path(temp)
        consumer.joinpath("src").mkdir()
        consumer.joinpath("Cargo.toml").write_text(
            """[package]
name = "cymule-published-consumer"
version = "0.0.0"
edition = "2024"

[dependencies]
cymule = "={version}"
cymule-agent = "={version}"
cymule-directory-store = "={version}"
serde_json = "1.0.149"
""".format(
                version=version
            ),
            encoding="utf-8",
        )
        consumer.joinpath("src/lib.rs").write_text(
            """use cymule::{Expression, FlowBuilder, PlanCandidate};

pub fn build_flow() -> PlanCandidate {
    FlowBuilder::new("published_consumer", serde_json::json!({}), serde_json::json!({}))
        .finish(Expression::Input)
}
""",
            encoding="utf-8",
        )
        run(["cargo", "generate-lockfile"], cwd=consumer)
        run(["cargo", "check", "--locked"], cwd=consumer)
        install_root = consumer / "install"
        run(
            [
                "cargo",
                "install",
                "cymule-cli",
                "--version",
                f"={version}",
                "--locked",
                "--root",
                str(install_root),
            ],
            cwd=consumer,
        )
        if not install_root.joinpath("bin", "cymule").is_file():
            raise ValueError("published cymule CLI binary is missing")
    print(f"verified {len(crates)} crates.io versions and fresh consumers")


def parse_args() -> argparse.Namespace:
    """Parse the closed release command surface."""

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    verify_parser = subparsers.add_parser("verify")
    verify_parser.add_argument("--allow-dirty", action="store_true")
    for command in ("publish", "verify-registry"):
        command_parser = subparsers.add_parser(command)
        command_parser.add_argument("--version", required=True)
    stage_parser = subparsers.add_parser("stage")
    stage_parser.add_argument("--version", required=True)
    stage_parser.add_argument("--release-sha", required=True)
    stage_parser.add_argument("--output", type=pathlib.Path, required=True)
    compare_parser = subparsers.add_parser("compare-stages")
    compare_parser.add_argument("--candidate", type=pathlib.Path, required=True)
    compare_parser.add_argument("--reference", type=pathlib.Path, required=True)
    evidence_parser = subparsers.add_parser("registry-evidence")
    evidence_parser.add_argument("--version", required=True)
    evidence_parser.add_argument("--release-sha", required=True)
    evidence_parser.add_argument("--stage", type=pathlib.Path, required=True)
    evidence_parser.add_argument("--output", type=pathlib.Path, required=True)
    subparsers.add_parser("list")
    return parser.parse_args()


def main() -> int:
    """Run one verified release operation."""

    args = parse_args()
    if args.command == "verify":
        verify_packages(args.allow_dirty)
    elif args.command == "publish":
        publish(args.version)
    elif args.command == "verify-registry":
        verify_registry(args.version)
    elif args.command == "stage":
        stage_packages(args.version, args.release_sha, args.output)
    elif args.command == "compare-stages":
        compare_stages(args.candidate, args.reference)
        print("verified independent crate stage equality")
    elif args.command == "registry-evidence":
        evidence = stage_registry_evidence(
            args.stage, args.version, args.release_sha
        )
        write_publication_evidence(args.output, evidence)
        print(f"materialized {len(evidence)} crate publication records")
    else:
        for crate in load_catalog():
            print(crate.name)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, subprocess.CalledProcessError, TimeoutError) as error:
        print(f"crate release failed: {error}", file=sys.stderr)
        sys.exit(1)
