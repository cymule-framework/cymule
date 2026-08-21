#!/usr/bin/env python3
"""Stage and verify exact npm release archives and their provenance."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import pathlib
import re
import subprocess
import sys
import tarfile
import time
import urllib.error
import urllib.parse
import urllib.request


REGISTRY = "https://registry.npmjs.org"
REPOSITORY = "https://github.com/cymule-framework/cymule"
WORKFLOW = ".github/workflows/publish-npm.yml"
SLSA_PROVENANCE = "https://slsa.dev/provenance/v1"
SHA_PATTERN = re.compile(r"[0-9a-f]{40}")


def digest(path: pathlib.Path, algorithm: str) -> str:
    value = hashlib.new(algorithm)
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def archive_identity(path: pathlib.Path) -> dict[str, str]:
    sha1 = digest(path, "sha1")
    sha512_bytes = bytes.fromhex(digest(path, "sha512"))
    return {
        "sha1": sha1,
        "sha512": sha512_bytes.hex(),
        "integrity": "sha512-" + base64.b64encode(sha512_bytes).decode("ascii"),
    }


def validate_release_sha(value: str) -> str:
    if SHA_PATTERN.fullmatch(value) is None:
        raise ValueError("release SHA must be one exact lowercase Git commit identity")
    return value


def inspect_archive(path: pathlib.Path, package: str, version: str) -> None:
    if not path.is_file() or path.stat().st_size == 0:
        raise ValueError(f"missing npm archive {path}")
    with tarfile.open(path, "r:gz") as archive:
        members = archive.getmembers()
        for member in members:
            member_path = pathlib.PurePosixPath(member.name)
            if (
                member_path.is_absolute()
                or ".." in member_path.parts
                or member_path.parts[:1] != ("package",)
                or member.issym()
                or member.islnk()
            ):
                raise ValueError(f"unsafe npm archive member {member.name}")
        package_json = archive.extractfile("package/package.json")
        if package_json is None:
            raise ValueError("npm archive omits package/package.json")
        manifest = json.load(package_json)
    if manifest.get("name") != package or manifest.get("version") != version:
        raise ValueError(
            f"npm archive identity {manifest.get('name')}@{manifest.get('version')} "
            f"does not match {package}@{version}"
        )


def stage(
    package_directory: pathlib.Path,
    package: str,
    version: str,
    release_sha: str,
    output: pathlib.Path,
) -> pathlib.Path:
    release_sha = validate_release_sha(release_sha)
    output.mkdir(parents=True, exist_ok=False)
    result = subprocess.run(
        [
            "npm",
            "pack",
            "--json",
            "--pack-destination",
            str(output),
            str(package_directory),
        ],
        check=True,
        text=True,
        capture_output=True,
    )
    packed = json.loads(result.stdout)
    if not isinstance(packed, list) or len(packed) != 1:
        raise ValueError("npm pack did not describe exactly one archive")
    archive = output / packed[0]["filename"]
    inspect_archive(archive, package, version)
    identity = archive_identity(archive)
    manifest = {
        "schema": "cymule.npm-release-stage/1",
        "package": package,
        "version": version,
        "release_sha": release_sha,
        "archive": archive.name,
        **identity,
    }
    manifest_path = output / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return manifest_path


def load_stage(
    manifest_path: pathlib.Path, release_sha: str
) -> tuple[dict[str, str], pathlib.Path]:
    release_sha = validate_release_sha(release_sha)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    required = {
        "schema",
        "package",
        "version",
        "release_sha",
        "archive",
        "sha1",
        "sha512",
        "integrity",
    }
    if set(manifest) != required or manifest["schema"] != "cymule.npm-release-stage/1":
        raise ValueError("unsupported or malformed npm release manifest")
    if manifest["release_sha"] != release_sha:
        raise ValueError("staged npm archive belongs to another verified commit")
    archive = manifest_path.parent / manifest["archive"]
    inspect_archive(archive, manifest["package"], manifest["version"])
    if archive_identity(archive) != {
        key: manifest[key] for key in ("sha1", "sha512", "integrity")
    }:
        raise ValueError("staged npm archive digest does not match its manifest")
    return manifest, archive


def compare_stages(candidate_path: pathlib.Path, reference_path: pathlib.Path) -> None:
    """Require two independently built stages to contain identical release bytes."""

    candidate_sha = json.loads(candidate_path.read_text(encoding="utf-8")).get(
        "release_sha"
    )
    if not isinstance(candidate_sha, str):
        raise ValueError("candidate npm stage omits its release SHA")
    candidate, candidate_archive = load_stage(candidate_path, candidate_sha)
    reference, reference_archive = load_stage(reference_path, candidate_sha)
    if candidate != reference:
        raise ValueError(
            "independent npm stages do not describe identical release bytes"
        )
    if candidate_archive.read_bytes() != reference_archive.read_bytes():
        raise ValueError("independent npm archives differ despite their manifests")


def request_json(url: str) -> dict[str, object]:
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": "cymule-release/2"},
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        value = json.load(response)
    if not isinstance(value, dict):
        raise ValueError(f"registry returned a non-object from {url}")
    return value


def wait_for_registry(package: str, version: str) -> dict[str, object]:
    encoded_package = urllib.parse.quote(package, safe="")
    encoded_version = urllib.parse.quote(version, safe="")
    url = f"{REGISTRY}/{encoded_package}/{encoded_version}"
    deadline = time.monotonic() + 300
    while True:
        try:
            return request_json(url)
        except urllib.error.HTTPError as error:
            if error.code != 404 or time.monotonic() >= deadline:
                raise
            time.sleep(5)


def verify_provenance(
    attestations_url: str,
    package: str,
    version: str,
    sha512: str,
    release_sha: str,
) -> None:
    if not attestations_url.startswith(f"{REGISTRY}/-/"):
        raise ValueError("npm provenance URL is outside the registry authority")
    payload = request_json(attestations_url)
    attestations = payload.get("attestations")
    if not isinstance(attestations, list):
        raise ValueError("npm registry omitted provenance attestations")
    for attestation in attestations:
        if (
            not isinstance(attestation, dict)
            or attestation.get("predicateType") != SLSA_PROVENANCE
        ):
            continue
        envelope = attestation.get("bundle", {}).get("dsseEnvelope", {})
        encoded = envelope.get("payload")
        if not isinstance(encoded, str):
            continue
        statement = json.loads(base64.b64decode(encoded, validate=True))
        subjects = statement.get("subject", [])
        predicate = statement.get("predicate", {})
        build = predicate.get("buildDefinition", {})
        workflow = build.get("externalParameters", {}).get("workflow", {})
        dependencies = build.get("resolvedDependencies", [])
        if workflow != {
            "ref": "refs/heads/main",
            "repository": REPOSITORY,
            "path": WORKFLOW,
        }:
            continue
        if not any(
            isinstance(dependency, dict)
            and dependency.get("digest", {}).get("gitCommit") == release_sha
            for dependency in dependencies
        ):
            continue
        expected_purl = f"pkg:npm/{urllib.parse.quote(package, safe='/')}@{version}"
        if not any(
            isinstance(subject, dict)
            and subject.get("name") == expected_purl
            and subject.get("digest", {}).get("sha512") == sha512
            for subject in subjects
        ):
            continue
        return
    raise ValueError(
        f"npm omitted exact {WORKFLOW} provenance for {package}@{version} at {release_sha}"
    )


def verify_registry(manifest_path: pathlib.Path, release_sha: str) -> None:
    manifest, archive = load_stage(manifest_path, release_sha)
    metadata = wait_for_registry(manifest["package"], manifest["version"])
    dist = metadata.get("dist")
    if not isinstance(dist, dict):
        raise ValueError("npm registry omitted the immutable distribution record")
    if (
        dist.get("shasum") != manifest["sha1"]
        or dist.get("integrity") != manifest["integrity"]
    ):
        raise ValueError(
            f"immutable npm version {manifest['package']}@{manifest['version']} has other bytes"
        )
    tarball = dist.get("tarball")
    if not isinstance(tarball, str) or not tarball.startswith(f"{REGISTRY}/"):
        raise ValueError("npm tarball URL is outside the registry authority")
    request = urllib.request.Request(
        tarball, headers={"User-Agent": "cymule-release/2"}
    )
    downloaded = hashlib.sha512()
    with urllib.request.urlopen(request, timeout=60) as response:
        for chunk in iter(lambda: response.read(1024 * 1024), b""):
            downloaded.update(chunk)
    if downloaded.hexdigest() != manifest["sha512"]:
        raise ValueError("downloaded npm archive does not match the staged bytes")
    provenance = dist.get("attestations")
    if not isinstance(provenance, dict):
        raise ValueError("npm distribution record omits provenance")
    if provenance.get("provenance", {}).get("predicateType") != SLSA_PROVENANCE:
        raise ValueError("npm distribution record omits SLSA v1 provenance")
    url = provenance.get("url")
    if not isinstance(url, str):
        raise ValueError("npm distribution record omits the attestation URL")
    verify_provenance(
        url,
        manifest["package"],
        manifest["version"],
        manifest["sha512"],
        release_sha,
    )
    print(
        f"verified npm bytes and provenance for "
        f"{manifest['package']}@{manifest['version']} at {release_sha}"
    )


def registry_status(manifest_path: pathlib.Path, release_sha: str) -> str:
    manifest, _archive = load_stage(manifest_path, release_sha)
    encoded_package = urllib.parse.quote(manifest["package"], safe="")
    encoded_version = urllib.parse.quote(manifest["version"], safe="")
    try:
        request_json(f"{REGISTRY}/{encoded_package}/{encoded_version}")
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return "missing"
        raise
    return "exists"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    stage_parser = subparsers.add_parser("stage")
    stage_parser.add_argument("--package-dir", type=pathlib.Path, required=True)
    stage_parser.add_argument("--package-name", required=True)
    stage_parser.add_argument("--version", required=True)
    stage_parser.add_argument("--release-sha", required=True)
    stage_parser.add_argument("--output", type=pathlib.Path, required=True)
    for command in ("verify-staged", "verify-registry", "registry-status"):
        verify_parser = subparsers.add_parser(command)
        verify_parser.add_argument("--manifest", type=pathlib.Path, required=True)
        verify_parser.add_argument("--release-sha", required=True)
    compare_parser = subparsers.add_parser("compare-stages")
    compare_parser.add_argument("--candidate", type=pathlib.Path, required=True)
    compare_parser.add_argument("--reference", type=pathlib.Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.command == "stage":
        print(
            stage(
                args.package_dir,
                args.package_name,
                args.version,
                args.release_sha,
                args.output,
            )
        )
    elif args.command == "verify-staged":
        load_stage(args.manifest, args.release_sha)
        print(f"verified staged npm archive {args.manifest}")
    elif args.command == "registry-status":
        print(registry_status(args.manifest, args.release_sha))
    elif args.command == "compare-stages":
        compare_stages(args.candidate, args.reference)
        print("verified independent npm stage equality")
    else:
        verify_registry(args.manifest, args.release_sha)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (
        OSError,
        ValueError,
        subprocess.CalledProcessError,
        tarfile.TarError,
        urllib.error.URLError,
    ) as error:
        print(f"npm release verification failed: {error}", file=sys.stderr)
        sys.exit(1)
