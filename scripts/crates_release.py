#!/usr/bin/env python3
"""Verify and publish the ordered public Cymule crate set."""

from __future__ import annotations

import argparse
import dataclasses
import email.utils
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
CATALOG_PATH = ROOT / "scripts" / "crates-release.toml"
USER_AGENT = "cymule-release/1 (https://github.com/cymule-framework/cymule)"
REGISTRY_API = "https://crates.io/api/v1"
STATIC_REGISTRY = "https://static.crates.io/crates"
MAX_CRATE_BYTES = 10 * 1024 * 1024
MAX_NEW_CRATE_RATE_LIMIT_WAIT_SECONDS = 15 * 60
MAX_NEW_CRATE_RATE_LIMIT_RETRIES = 2
GIT_SHA_PATTERN = re.compile(r"[0-9a-f]{40}")
NEW_CRATE_RATE_LIMIT_MARKERS = (
    "status 429 Too Many Requests",
    "You have published too many new crates in a short period of time.",
)
NEW_CRATE_RETRY_PATTERN = re.compile(
    r"Please try again after "
    r"([A-Za-z]{3}, \d{1,2} [A-Za-z]{3} \d{4} \d{2}:\d{2}:\d{2} GMT)"
)


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
    if raw.get("schema") != 1 or not isinstance(raw.get("crate"), list):
        raise ValueError("unsupported or malformed crate release catalog")
    crates: list[PublicCrate] = []
    seen: set[str] = set()
    for entry in raw["crate"]:
        crate = PublicCrate(
            name=entry["name"],
            path=ROOT / entry["path"],
            dependencies=tuple(entry["dependencies"]),
        )
        if crate.name in seen or not crate.path.joinpath("Cargo.toml").is_file():
            raise ValueError(f"invalid or duplicate public crate {crate.name}")
        missing = set(crate.dependencies) - seen
        if missing:
            raise ValueError(
                f"crate {crate.name} precedes dependencies {sorted(missing)}"
            )
        seen.add(crate.name)
        crates.append(crate)
    return crates


def cargo_metadata() -> dict[str, object]:
    """Read current workspace package authority from Cargo."""

    result = run(
        ["cargo", "metadata", "--format-version", "1", "--no-deps"],
        capture=True,
    )
    return json.loads(result.stdout)


def validate_workspace(crates: list[PublicCrate], requested: str | None = None) -> str:
    """Validate catalog coverage, versions, metadata, and dependency edges."""

    metadata = cargo_metadata()
    packages = {package["name"]: package for package in metadata["packages"]}
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
    versions = {packages[crate.name]["version"] for crate in crates}
    if len(versions) != 1:
        raise ValueError(f"public crate versions diverge: {sorted(versions)}")
    version = versions.pop()
    if requested is not None and version != requested:
        raise ValueError(f"requested {requested}, manifests contain {version}")
    typescript_version = json.loads(
        ROOT.joinpath("sdk/typescript/package.json").read_text(encoding="utf-8")
    )["version"]
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
        actual_dependencies = {
            dependency["name"]
            for dependency in package["dependencies"]
            if dependency["name"] in catalog_names and dependency["kind"] is None
        }
        if actual_dependencies != set(crate.dependencies):
            raise ValueError(
                f"catalog dependencies for {crate.name} are "
                f"{sorted(crate.dependencies)}, Cargo has {sorted(actual_dependencies)}"
            )
        for dependency in package["dependencies"]:
            if dependency["name"] in catalog_names and dependency["kind"] is None:
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


def publish_metadata(archive_path: pathlib.Path, crate: str, version: str) -> bytes:
    """Derive Cargo's documented registry metadata from normalized archive bytes."""

    root = pathlib.PurePosixPath(f"{crate}-{version}")
    with tarfile.open(archive_path, "r:gz") as archive:
        manifest_member = archive.getmember(str(root / "Cargo.toml"))
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
            readme_member = archive.getmember(str(root / readme_file))
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

    raw = body.read_bytes()
    if len(raw) < 8:
        raise ValueError(f"registry upload body is truncated for {crate}")
    metadata_length = struct.unpack("<I", raw[:4])[0]
    metadata_end = 4 + metadata_length
    if metadata_end + 4 > len(raw):
        raise ValueError(f"registry metadata length is invalid for {crate}")
    metadata = json.loads(raw[4:metadata_end])
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
            root = pathlib.PurePosixPath(f"{crate.name}-{version}")
            for member in archive.getmembers():
                path = pathlib.PurePosixPath(member.name)
                if (
                    path.is_absolute()
                    or ".." in path.parts
                    or path.parts[:1] != root.parts
                    or member.issym()
                    or member.islnk()
                ):
                    raise ValueError(
                        f"unsafe archive member in {crate.name}: {member.name}"
                    )
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
        "schema": "cymule.crates-package-report/1",
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
            return json.load(response)
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
    return payload["version"]["checksum"]


def wait_for_checksum(crate: str, version: str, expected: str) -> None:
    """Wait a bounded interval for the registry index to expose exact bytes."""

    deadline = time.monotonic() + 300
    while time.monotonic() < deadline:
        observed = registry_checksum(crate, version)
        if observed is not None:
            if observed != expected:
                raise ValueError(
                    f"registry checksum mismatch for {crate}@{version}: {observed}"
                )
            return
        time.sleep(5)
    raise TimeoutError(f"registry did not index {crate}@{version} within 300 seconds")


def verify_download(crate: str, version: str, expected: str) -> None:
    """Download published bytes and verify the registry checksum independently."""

    url = f"{STATIC_REGISTRY}/{crate}/{crate}-{version}.crate"
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    digest = hashlib.sha256()
    with urllib.request.urlopen(request, timeout=60) as response:
        for chunk in iter(lambda: response.read(1024 * 1024), b""):
            digest.update(chunk)
    if digest.hexdigest() != expected:
        raise ValueError(f"downloaded archive checksum mismatch for {crate}@{version}")


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
                "schema": "cymule.crates-release-stage/2",
                "version": version,
                "release_sha": release_sha,
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

    manifest = json.loads(
        directory.joinpath("manifest.json").read_text(encoding="utf-8")
    )
    if set(manifest) != {"schema", "version", "release_sha", "crates"}:
        raise ValueError("malformed crate release stage manifest")
    if manifest["schema"] != "cymule.crates-release-stage/2":
        raise ValueError("unsupported crate release stage manifest")
    if manifest["version"] != version or manifest["release_sha"] != release_sha:
        raise ValueError("crate release stage belongs to another version or commit")
    expected_names = [crate.name for crate in crates]
    entries = manifest["crates"]
    if (
        not isinstance(entries, list)
        or [entry.get("name") for entry in entries] != expected_names
    ):
        raise ValueError("crate release stage does not match the ordered catalog")
    closed: dict[str, ClosedCrate] = {}
    for entry in entries:
        if set(entry) != {
            "name",
            "archive",
            "sha256",
            "upload",
            "upload_sha256",
        }:
            raise ValueError("malformed crate release stage entry")
        archive = directory / entry["archive"]
        expected_archive = f"{entry['name']}-{version}.crate"
        if archive.name != expected_archive or not archive.is_file():
            raise ValueError(f"missing staged archive {expected_archive}")
        observed = sha256(archive)
        if observed != entry["sha256"]:
            raise ValueError(f"staged archive digest changed for {entry['name']}")
        upload = directory / entry["upload"]
        expected_upload = f"{entry['name']}-{version}.publish"
        if upload.name != expected_upload or not upload.is_file():
            raise ValueError(f"missing staged upload body {expected_upload}")
        upload_sha256 = sha256(upload)
        if upload_sha256 != entry["upload_sha256"]:
            raise ValueError(f"staged upload body changed for {entry['name']}")
        inspect_upload_body(upload, archive, entry["name"], version)
        closed[entry["name"]] = ClosedCrate(observed, upload, upload_sha256)
    return closed


def compare_stages(candidate: pathlib.Path, reference: pathlib.Path) -> None:
    """Require independently packaged crate stages to be byte-identical."""

    candidate_manifest = json.loads(
        candidate.joinpath("manifest.json").read_text(encoding="utf-8")
    )
    version = candidate_manifest.get("version")
    release_sha = candidate_manifest.get("release_sha")
    if not isinstance(version, str) or not isinstance(release_sha, str):
        raise ValueError("candidate crate stage omits version or release SHA")
    crates = load_catalog()
    candidate_closed = load_stage(candidate, crates, version, release_sha)
    reference_closed = load_stage(reference, crates, version, release_sha)
    if candidate_manifest != json.loads(
        reference.joinpath("manifest.json").read_text(encoding="utf-8")
    ):
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


def publish_crate(crate: str, upload: pathlib.Path, token: str) -> None:
    """Upload one already-closed body, retrying only the exact new-name limit."""

    for attempt in range(MAX_NEW_CRATE_RATE_LIMIT_RETRIES + 1):
        request = urllib.request.Request(
            f"{REGISTRY_API}/crates/new",
            data=upload.read_bytes(),
            method="PUT",
            headers={
                "Accept": "application/json",
                "Authorization": token,
                "Content-Type": "application/octet-stream",
                "User-Agent": USER_AGENT,
            },
        )
        print(f"+ PUT exact closed bytes for {crate}", flush=True)
        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                payload = json.load(response)
            if not isinstance(payload, dict) or payload.get("errors"):
                raise ValueError(f"crates.io rejected {crate}: {payload}")
            return
        except urllib.error.HTTPError as error:
            body = error.read().decode("utf-8", errors="replace")
            output = f"status {error.code} {error.reason}: {body}"
            if error.code != 429:
                raise
        delay = new_crate_rate_limit_delay(output)
        if delay is None or attempt == MAX_NEW_CRATE_RATE_LIMIT_RETRIES:
            raise ValueError(f"crates.io rejected {crate}: {output}")
        print(
            f"crates.io limited new crate names; retrying {crate} in {delay} seconds",
            flush=True,
        )
        time.sleep(delay)


def publish(version: str) -> None:
    """Publish or exactly replay every crate in dependency order."""

    token = os.environ.get("CARGO_REGISTRY_TOKEN")
    if not token:
        raise ValueError("CARGO_REGISTRY_TOKEN is required")
    crates = load_catalog()
    require_release_checkout(version)
    release_sha = run(["git", "rev-parse", "HEAD"], capture=True).stdout.strip()
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
            publish_crate(crate.name, closed[crate.name].upload, token)
            outcome = "published"
        elif observed == expected:
            outcome = "retained"
        else:
            raise ValueError(
                f"immutable registry version {crate.name}@{version} has other bytes"
            )
        wait_for_checksum(crate.name, version, expected)
        verify_download(crate.name, version, expected)
        outcomes.append({"name": crate.name, "sha256": expected, "outcome": outcome})
        report_dir.joinpath("publish.json").write_text(
            json.dumps(
                {
                    "schema": "cymule.crates-publish-report/1",
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
