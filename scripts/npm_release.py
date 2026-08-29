#!/usr/bin/env python3
"""Stage and verify exact npm release archives and their provenance."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
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
PUBLISH_REGISTRY = "https://registry.npmjs.org/"
PUBLISH_DIST_TAG = "latest"
GITHUB_API = "https://api.github.com"
GITHUB_API_VERSION = "2026-03-10"
MAX_GITHUB_IDENTITY_RESPONSE_BYTES = 1024 * 1024
PINNED_NODE_VERSION = "v26.7.0"
PINNED_NPM_VERSION = "11.19.0"
REPOSITORY = "https://github.com/cymule-framework/cymule"
REPOSITORY_ID = "1336620232"
REPOSITORY_OWNER_ID = "317745091"
WORKFLOW = ".github/workflows/publish-npm-release.yml"
CONTROLLER_WORKFLOW = ".github/workflows/publish-npm-controller.yml"
SIGSTORE_CERTIFICATE_ISSUER = "https://token.actions.githubusercontent.com"
SIGSTORE_CERTIFICATE_IDENTITY = (
    f"{REPOSITORY}/{CONTROLLER_WORKFLOW}@refs/heads/main"
)
FULCIO_BUILD_SIGNER_URI_OID = "1.3.6.1.4.1.57264.1.9"
FULCIO_BUILD_SIGNER_DIGEST_OID = "1.3.6.1.4.1.57264.1.10"
INTOTO_STATEMENT = "https://in-toto.io/Statement/v1"
INTOTO_PAYLOAD = "application/vnd.in-toto+json"
SLSA_PROVENANCE = "https://slsa.dev/provenance/v1"
SLSA_GITHUB_BUILD_TYPE = (
    "https://slsa-framework.github.io/github-actions-buildtypes/workflow/v1"
)
SLSA_GITHUB_BUILDER = "https://github.com/actions/runner/github-hosted"
SHA_PATTERN = re.compile(r"[0-9a-f]{40}")
STABLE_VERSION_PATTERN = re.compile(
    r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
)
GITHUB_APP_SLUG_PATTERN = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,98}[a-z0-9])?")
NPM_RELEASE_STAGE_VERSION = "cymule.npm-release-stage/3"
PUBLIC_PACKAGES = frozenset({"cymule", "@cymule/sdk"})
REGISTRY_READBACK_TIMEOUT_SECONDS = 300
REGISTRY_READBACK_INTERVAL_SECONDS = 5


def require_registry_url(value: object, *, path_prefix: str, label: str) -> str:
    """Require one HTTPS URL under the exact npm registry origin."""

    if not isinstance(value, str):
        raise ValueError(f"{label} is missing")
    parsed = urllib.parse.urlsplit(value)
    if (
        parsed.scheme != "https"
        or parsed.netloc != "registry.npmjs.org"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or not parsed.path.startswith(path_prefix)
    ):
        raise ValueError(f"{label} is outside the registry authority")
    return value
NPM_PUBLISH_TIMEOUT_SECONDS = 600
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


SIGSTORE_VERIFY_PROGRAM = r"""
const fs = require("node:fs");
const path = require("node:path");

const npmRoot = process.argv[1];
const certificateIdentity = process.argv[2];
const certificateIssuer = process.argv[3];
const sigstorePath = path.join(npmRoot, "npm", "node_modules", "sigstore");
const sigstore = require(sigstorePath);
const bundle = JSON.parse(fs.readFileSync(0, "utf8"));
const escapeRegExp = (value) => value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

sigstore.verify(bundle, {
  certificateIdentityURI: new RegExp(`^${escapeRegExp(certificateIdentity)}$`),
  certificateIssuer,
  ctLogThreshold: 1,
  tlogThreshold: 1,
}).catch((error) => {
  process.stderr.write(`${error.message}\n`);
  process.exitCode = 1;
});
"""


def version_registry_digest() -> str:
    return version_domains.registry_digest(ROOT)


def load_i_json(value: bytes, *, label: str) -> object:
    """Decode release authority through the shared strict I-JSON contract."""

    return version_domains.load_json_bytes(value, label=label)


def digest(path: pathlib.Path, algorithm: str) -> str:
    value = hashlib.new(algorithm)
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def stage_file(directory: pathlib.Path, basename: object) -> pathlib.Path:
    """Resolve one regular, non-symlink stage file owned by ``directory``."""

    if (
        not isinstance(basename, str)
        or not basename
        or pathlib.PurePosixPath(basename).name != basename
        or pathlib.PureWindowsPath(basename).name != basename
    ):
        raise ValueError("npm release stage file must be one basename")
    path = directory / basename
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"npm release stage file {basename} is not regular non-symlink")
    resolved = path.resolve(strict=True)
    if resolved.parent != directory:
        raise ValueError(f"npm release stage file {basename} escapes its stage")
    return resolved


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
        member_paths: set[tuple[str, ...]] = set()
        package_manifest: tarfile.TarInfo | None = None
        for member in members:
            member_path = pathlib.PurePosixPath(member.name)
            if (
                member_path.is_absolute()
                or ".." in member_path.parts
                or member_path.parts[:1] != ("package",)
                or member.issym()
                or member.islnk()
                or not (member.isfile() or member.isdir())
                or member_path.parts in member_paths
            ):
                raise ValueError(f"unsafe npm archive member {member.name}")
            member_paths.add(member_path.parts)
            if member_path.parts == ("package", "package.json"):
                if not member.isfile():
                    raise ValueError("npm archive package/package.json is not a file")
                package_manifest = member
        if package_manifest is None:
            raise ValueError("npm archive omits package/package.json")
        package_json = archive.extractfile(package_manifest)
        if package_json is None:
            raise ValueError("npm archive omits package/package.json")
        manifest = load_i_json(
            package_json.read(), label=f"{path}:package/package.json"
        )
    if not isinstance(manifest, dict):
        raise ValueError("npm archive package/package.json must be an object")
    if manifest.get("name") != package or manifest.get("version") != version:
        raise ValueError(
            f"npm archive identity {manifest.get('name')}@{manifest.get('version')} "
            f"does not match {package}@{version}"
        )
    publish_config = manifest.get("publishConfig")
    if (
        not isinstance(publish_config, dict)
        or set(publish_config) != {"access", "provenance", "registry", "tag"}
        or publish_config["access"] != "public"
        or publish_config["provenance"] is not True
        or publish_config["registry"] != PUBLISH_REGISTRY
        or publish_config["tag"] != PUBLISH_DIST_TAG
    ):
        raise ValueError("npm archive publishConfig is not the closed public target")


def stage(
    package_directory: pathlib.Path,
    package: str,
    version: str,
    release_sha: str,
    output: pathlib.Path,
) -> pathlib.Path:
    if package not in PUBLIC_PACKAGES:
        raise ValueError(f"unsupported public npm package {package}")
    parse_stable_version(version)
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
    packed = load_i_json(result.stdout.encode("utf-8"), label="npm pack output")
    if not isinstance(packed, list) or len(packed) != 1:
        raise ValueError("npm pack did not describe exactly one archive")
    archive = output / packed[0]["filename"]
    inspect_archive(archive, package, version)
    identity = archive_identity(archive)
    manifest = {
        "schema": NPM_RELEASE_STAGE_VERSION,
        "package": package,
        "version": version,
        "dist_tag": PUBLISH_DIST_TAG,
        "release_sha": release_sha,
        "version_domain_registry_digest": version_registry_digest(),
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
    if manifest_path.name != "manifest.json":
        raise ValueError("npm release stage manifest must be manifest.json")
    directory = manifest_path.parent.resolve(strict=True)
    if not directory.is_dir():
        raise ValueError("npm release stage is not a directory")
    manifest_path = stage_file(directory, "manifest.json")
    manifest = load_i_json(manifest_path.read_bytes(), label=str(manifest_path))
    if not isinstance(manifest, dict):
        raise ValueError("unsupported or malformed npm release manifest")
    required = {
        "schema",
        "package",
        "version",
        "dist_tag",
        "release_sha",
        "version_domain_registry_digest",
        "archive",
        "sha1",
        "sha512",
        "integrity",
    }
    if set(manifest) != required or manifest["schema"] != NPM_RELEASE_STAGE_VERSION:
        raise ValueError("unsupported or malformed npm release manifest")
    if manifest["dist_tag"] != PUBLISH_DIST_TAG:
        raise ValueError("staged npm archive belongs to another distribution tag")
    if manifest["package"] not in PUBLIC_PACKAGES:
        raise ValueError("staged npm archive belongs to an unsupported package")
    parse_stable_version(manifest["version"])
    if manifest["release_sha"] != release_sha:
        raise ValueError("staged npm archive belongs to another verified commit")
    if manifest["version_domain_registry_digest"] != version_registry_digest():
        raise ValueError("staged npm archive belongs to another version-domain generation")
    archive_name = manifest["archive"]
    if (
        not isinstance(archive_name, str)
        or not archive_name
        or pathlib.PurePosixPath(archive_name).name != archive_name
        or pathlib.PureWindowsPath(archive_name).name != archive_name
    ):
        raise ValueError("npm release stage file must be one basename")
    if {path.name for path in directory.iterdir()} != {"manifest.json", archive_name}:
        raise ValueError("npm release stage does not contain its exact file set")
    archive = stage_file(directory, archive_name)
    inspect_archive(archive, manifest["package"], manifest["version"])
    if archive_identity(archive) != {
        key: manifest[key] for key in ("sha1", "sha512", "integrity")
    }:
        raise ValueError("staged npm archive digest does not match its manifest")
    return manifest, archive


def compare_stages(candidate_path: pathlib.Path, reference_path: pathlib.Path) -> None:
    """Require two independently built stages to contain identical release bytes."""

    candidate_manifest = load_i_json(
        candidate_path.read_bytes(), label=str(candidate_path)
    )
    candidate_sha = (
        candidate_manifest.get("release_sha")
        if isinstance(candidate_manifest, dict)
        else None
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
        value = load_i_json(response.read(), label=url)
    if not isinstance(value, dict):
        raise ValueError(f"registry returned a non-object from {url}")
    return value


class _RejectRedirects(urllib.request.HTTPRedirectHandler):
    """Keep a GitHub App token on the one reviewed API origin and path."""

    def redirect_request(
        self,
        _request: urllib.request.Request,
        _file_pointer: object,
        _code: int,
        _message: str,
        _headers: object,
        _new_url: str,
    ) -> None:
        raise ValueError("GitHub App identity API redirected outside its exact request")


def github_api_json(path: str, token: str) -> dict[str, object]:
    """Read one bounded, non-redirecting GitHub App identity object."""

    if not token:
        raise ValueError("GitHub App identity readback requires its installation token")
    if not path.startswith("/") or "?" in path or "#" in path:
        raise ValueError("GitHub App identity API path is not exact")
    url = f"{GITHUB_API}{path}"
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "User-Agent": "cymule-release/2",
            "X-GitHub-Api-Version": GITHUB_API_VERSION,
        },
    )
    opener = urllib.request.build_opener(_RejectRedirects())
    with opener.open(request, timeout=30) as response:
        if response.geturl() != url:
            raise ValueError("GitHub App identity API returned another URL")
        payload = response.read(MAX_GITHUB_IDENTITY_RESPONSE_BYTES + 1)
    if len(payload) > MAX_GITHUB_IDENTITY_RESPONSE_BYTES:
        raise ValueError("GitHub App identity API response is oversized")
    value = load_i_json(payload, label=url)
    if not isinstance(value, dict):
        raise ValueError("GitHub App identity API returned a non-object")
    return value


def github_app_bot_user_id(app_slug: str, app_id: int, token: str) -> int:
    """Bind one minted App slug and App ID to its distinct bot user ID."""

    if GITHUB_APP_SLUG_PATTERN.fullmatch(app_slug) is None:
        raise ValueError("GitHub App slug is not canonical")
    if (
        type(app_id) is not int
        or app_id <= 0
        or app_id > version_domains.MAX_EXACT_INTEGER
    ):
        raise ValueError("GitHub App ID is not one positive exact integer")
    encoded_slug = urllib.parse.quote(app_slug, safe="")
    app = github_api_json(f"/apps/{encoded_slug}", token)
    if app.get("id") != app_id or app.get("slug") != app_slug:
        raise ValueError("GitHub App slug and App ID do not name one authority")

    bot_login = f"{app_slug}[bot]"
    encoded_login = urllib.parse.quote(bot_login, safe="")
    bot = github_api_json(f"/users/{encoded_login}", token)
    bot_user_id = bot.get("id")
    if (
        bot.get("login") != bot_login
        or bot.get("type") != "Bot"
        or bot.get("site_admin") is not False
        or type(bot_user_id) is not int
        or bot_user_id <= 0
        or bot_user_id > version_domains.MAX_EXACT_INTEGER
    ):
        raise ValueError("GitHub App bot user identity is not exact")
    return bot_user_id


def parse_stable_version(value: object) -> tuple[int, int, int]:
    if not isinstance(value, str):
        raise ValueError("npm stable release version must be a string")
    match = STABLE_VERSION_PATTERN.fullmatch(value)
    if match is None:
        raise ValueError(f"npm stable release version is not exact semver: {value}")
    return tuple(int(part) for part in match.groups())


def packument_url(package: str) -> str:
    return f"{REGISTRY}/{urllib.parse.quote(package, safe='')}"


def inspect_stable_packument(
    packument: dict[str, object], package: str, version: str
) -> tuple[bool, str, tuple[int, int, int]]:
    """Return target presence, current tag, and highest published stable."""

    target = parse_stable_version(version)
    versions = packument.get("versions")
    tags = packument.get("dist-tags")
    if (
        packument.get("name") != package
        or not isinstance(versions, dict)
        or not isinstance(tags, dict)
    ):
        raise ValueError("npm registry returned a malformed package packument")
    stable_versions: list[tuple[int, int, int]] = []
    for published in versions:
        match = STABLE_VERSION_PATTERN.fullmatch(published)
        if match is not None:
            stable_versions.append(tuple(int(part) for part in match.groups()))
    if not stable_versions:
        raise ValueError("npm registry packument contains no stable version")
    latest = tags.get(PUBLISH_DIST_TAG)
    parse_stable_version(latest)
    if latest not in versions:
        raise ValueError("npm latest tag does not select a published stable version")
    return version in versions, latest, max(stable_versions)


def latest_publish_status(package: str, version: str) -> str:
    """Admit one monotonic stable publication against the complete packument."""

    parse_stable_version(version)
    try:
        packument = request_json(packument_url(package))
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return "missing"
        raise
    target = parse_stable_version(version)
    target_exists, _latest, highest = inspect_stable_packument(
        packument, package, version
    )
    if highest > target:
        raise ValueError("npm registry already contains a higher stable version")
    if parse_stable_version(_latest) != highest:
        raise ValueError("npm latest tag does not select the highest stable version")
    return "exists" if target_exists else "missing"


def wait_for_latest_tag(package: str, version: str) -> None:
    """Read back the package-wide stable head after immutable version admission."""

    deadline = time.monotonic() + REGISTRY_READBACK_TIMEOUT_SECONDS
    while True:
        try:
            packument = request_json(packument_url(package))
        except urllib.error.HTTPError as error:
            if error.code != 404 or time.monotonic() >= deadline:
                raise
        else:
            target_exists, latest, highest = inspect_stable_packument(
                packument, package, version
            )
            if highest > parse_stable_version(version):
                raise ValueError("npm registry advanced to a higher stable version")
            if target_exists and latest == version:
                return
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"npm registry did not expose {package}@{version} as latest"
            )
        time.sleep(REGISTRY_READBACK_INTERVAL_SECONDS)


def verify_existing_stable_state(package: str, version: str) -> None:
    """Require latest only when the verified version is the stable head."""

    packument = request_json(packument_url(package))
    target_exists, latest, highest = inspect_stable_packument(
        packument, package, version
    )
    if not target_exists:
        raise ValueError("npm packument omits the verified immutable version")
    if parse_stable_version(latest) != highest:
        raise ValueError("npm latest tag does not select the highest stable version")


def wait_for_registry(package: str, version: str) -> dict[str, object]:
    encoded_package = urllib.parse.quote(package, safe="")
    encoded_version = urllib.parse.quote(version, safe="")
    url = f"{REGISTRY}/{encoded_package}/{encoded_version}"
    deadline = time.monotonic() + REGISTRY_READBACK_TIMEOUT_SECONDS
    while True:
        try:
            return request_json(url)
        except urllib.error.HTTPError as error:
            if error.code != 404 or time.monotonic() >= deadline:
                raise
            time.sleep(REGISTRY_READBACK_INTERVAL_SECONDS)


def pinned_npm_root() -> pathlib.Path:
    """Resolve the Sigstore implementation shipped by the pinned npm CLI."""

    node_version = subprocess.run(
        ["node", "--version"],
        cwd=CONTROL_ROOT,
        check=True,
        text=True,
        capture_output=True,
    ).stdout.strip()
    npm_version = subprocess.run(
        ["npm", "--version"],
        cwd=CONTROL_ROOT,
        check=True,
        text=True,
        capture_output=True,
    ).stdout.strip()
    if node_version != PINNED_NODE_VERSION or npm_version != PINNED_NPM_VERSION:
        raise ValueError("npm Sigstore verification requires the pinned Node/npm toolchain")
    npm_root_result = subprocess.run(
        ["npm", "root", "--global"],
        cwd=CONTROL_ROOT,
        check=True,
        text=True,
        capture_output=True,
    )
    npm_root = pathlib.Path(npm_root_result.stdout.strip())
    sigstore_package = npm_root / "npm" / "node_modules" / "sigstore" / "package.json"
    if not npm_root.is_absolute() or not sigstore_package.is_file():
        raise ValueError("pinned npm installation omits its Sigstore verifier")
    return npm_root


def run_sigstore_verifier(
    bundle: dict[str, object],
    npm_root: pathlib.Path,
    certificate_identity: str,
    certificate_issuer: str,
) -> None:
    """Run npm's verifier with one exact Fulcio workload identity."""

    try:
        subprocess.run(
            [
                "node",
                "-e",
                SIGSTORE_VERIFY_PROGRAM,
                str(npm_root),
                certificate_identity,
                certificate_issuer,
            ],
            cwd=CONTROL_ROOT,
            check=True,
            text=True,
            input=json.dumps(bundle, separators=(",", ":")),
            capture_output=True,
            timeout=120,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
        detail = getattr(error, "stderr", "")
        raise ValueError(f"npm Sigstore bundle verification failed: {detail}") from error


def der_element(data: bytes, offset: int = 0) -> tuple[int, bytes, int]:
    """Read one definite-length DER element and reject non-canonical lengths."""

    if offset < 0 or offset + 2 > len(data):
        raise ValueError("Fulcio certificate contains truncated DER")
    tag = data[offset]
    if tag & 0x1F == 0x1F:
        raise ValueError("Fulcio certificate uses an unsupported DER tag")
    first_length = data[offset + 1]
    cursor = offset + 2
    if first_length < 0x80:
        length = first_length
    else:
        length_octets = first_length & 0x7F
        if (
            length_octets == 0
            or length_octets > 4
            or cursor + length_octets > len(data)
            or data[cursor] == 0
        ):
            raise ValueError("Fulcio certificate contains invalid DER length")
        length = int.from_bytes(data[cursor : cursor + length_octets], "big")
        if length < 0x80:
            raise ValueError("Fulcio certificate contains non-canonical DER length")
        cursor += length_octets
    end = cursor + length
    if end > len(data):
        raise ValueError("Fulcio certificate contains truncated DER value")
    return tag, data[cursor:end], end


def der_children(data: bytes) -> list[tuple[int, bytes]]:
    children: list[tuple[int, bytes]] = []
    offset = 0
    while offset < len(data):
        tag, value, offset = der_element(data, offset)
        children.append((tag, value))
    return children


def decode_der_oid(value: bytes) -> str:
    if not value:
        raise ValueError("Fulcio certificate contains an empty extension OID")
    first = value[0]
    arcs = [min(first // 40, 2), first - min(first // 40, 2) * 40]
    current = 0
    open_arc = False
    for octet in value[1:]:
        if not open_arc and octet == 0x80:
            raise ValueError("Fulcio certificate contains non-canonical OID")
        current = (current << 7) | (octet & 0x7F)
        open_arc = bool(octet & 0x80)
        if not open_arc:
            arcs.append(current)
            current = 0
    if open_arc:
        raise ValueError("Fulcio certificate contains a truncated OID")
    return ".".join(str(arc) for arc in arcs)


def fulcio_extensions(certificate: bytes) -> dict[str, bytes]:
    certificate_tag, certificate_value, certificate_end = der_element(certificate)
    if certificate_tag != 0x30 or certificate_end != len(certificate):
        raise ValueError("Fulcio certificate is not one exact DER sequence")
    certificate_fields = der_children(certificate_value)
    if not certificate_fields or certificate_fields[0][0] != 0x30:
        raise ValueError("Fulcio certificate omits its signed body")
    extensions_fields = [
        value for tag, value in der_children(certificate_fields[0][1]) if tag == 0xA3
    ]
    if len(extensions_fields) != 1:
        raise ValueError("Fulcio certificate omits its exact extension set")
    explicit = der_children(extensions_fields[0])
    if len(explicit) != 1 or explicit[0][0] != 0x30:
        raise ValueError("Fulcio certificate extensions are malformed")
    extensions: dict[str, bytes] = {}
    for tag, extension_value in der_children(explicit[0][1]):
        if tag != 0x30:
            raise ValueError("Fulcio certificate extension is malformed")
        fields = der_children(extension_value)
        if len(fields) not in (2, 3) or fields[0][0] != 0x06:
            raise ValueError("Fulcio certificate extension shape is malformed")
        value_index = 1
        if len(fields) == 3:
            if fields[1][0] != 0x01:
                raise ValueError("Fulcio certificate extension critical flag is malformed")
            value_index = 2
        if fields[value_index][0] != 0x04:
            raise ValueError("Fulcio certificate extension value is malformed")
        oid = decode_der_oid(fields[0][1])
        if oid in extensions:
            raise ValueError(f"Fulcio certificate repeats extension {oid}")
        extensions[oid] = fields[value_index][1]
    return extensions


def extract_fulcio_signer(
    bundle: dict[str, object],
    expected_signer_ref: str = SIGSTORE_CERTIFICATE_IDENTITY,
) -> tuple[str, str]:
    """Extract GitHub's signed workflow URI and exact workflow commit."""

    material = bundle.get("verificationMaterial")
    certificate_record = (
        material.get("certificate") if isinstance(material, dict) else None
    )
    raw = (
        certificate_record.get("rawBytes")
        if isinstance(certificate_record, dict)
        else None
    )
    if not isinstance(raw, str):
        raise ValueError("npm Sigstore bundle omits its Fulcio certificate")
    try:
        certificate = base64.b64decode(raw, validate=True)
    except ValueError as error:
        raise ValueError("npm Sigstore bundle has invalid Fulcio certificate bytes") from error
    extensions = fulcio_extensions(certificate)
    values: list[str] = []
    for oid in (FULCIO_BUILD_SIGNER_URI_OID, FULCIO_BUILD_SIGNER_DIGEST_OID):
        encoded = extensions.get(oid)
        if encoded is None:
            raise ValueError(f"Fulcio certificate omits required extension {oid}")
        tag, value, end = der_element(encoded)
        if tag != 0x0C or end != len(encoded):
            raise ValueError(f"Fulcio certificate extension {oid} is not exact UTF8String")
        try:
            values.append(value.decode("utf-8", errors="strict"))
        except UnicodeDecodeError as error:
            raise ValueError(f"Fulcio certificate extension {oid} is not UTF-8") from error
    signer_ref, signer_sha = values
    if signer_ref != expected_signer_ref:
        raise ValueError("Fulcio build signer URI is not the npm controller")
    if SHA_PATTERN.fullmatch(signer_sha) is None:
        raise ValueError("Fulcio build signer digest is not one exact Git commit")
    return signer_ref, signer_sha


def verify_signer_commit(signer_sha: str) -> None:
    """Require the signed workflow commit to be retained on current public main."""

    checks = (
        ["git", "cat-file", "-e", f"{signer_sha}^{{commit}}"],
        ["git", "merge-base", "--is-ancestor", signer_sha, "HEAD"],
        ["git", "cat-file", "-e", f"{signer_sha}:{CONTROLLER_WORKFLOW}"],
    )
    for command in checks:
        result = subprocess.run(
            command,
            cwd=CONTROL_ROOT,
            check=False,
            text=True,
            capture_output=True,
        )
        if result.returncode != 0:
            raise ValueError("npm provenance signer is not a retained controller commit")


def verify_sigstore_bundle(bundle: dict[str, object]) -> tuple[str, str]:
    """Verify DSSE, Fulcio identity, SCT, Rekor inclusion, and signed payload."""

    run_sigstore_verifier(
        bundle,
        pinned_npm_root(),
        SIGSTORE_CERTIFICATE_IDENTITY,
        SIGSTORE_CERTIFICATE_ISSUER,
    )
    signer_ref, signer_sha = extract_fulcio_signer(bundle)
    verify_signer_commit(signer_sha)
    return signer_ref, signer_sha


def verify_provenance(
    attestations_url: str,
    package: str,
    version: str,
    sha512: str,
    release_sha: str,
) -> dict[str, str]:
    require_registry_url(
        attestations_url,
        path_prefix="/-/",
        label="npm provenance URL",
    )
    payload = request_json(attestations_url)
    attestations = payload.get("attestations")
    if not isinstance(attestations, list):
        raise ValueError("npm registry omitted provenance attestations")
    matching = [
        attestation
        for attestation in attestations
        if isinstance(attestation, dict)
        and attestation.get("predicateType") == SLSA_PROVENANCE
    ]
    if len(matching) != 1 or set(matching[0]) != {"predicateType", "bundle"}:
        raise ValueError("npm registry must expose one exact SLSA v1 attestation")
    bundle = matching[0]["bundle"]
    if not isinstance(bundle, dict):
        raise ValueError("npm registry returned a malformed Sigstore bundle")
    signer_ref, signer_sha = verify_sigstore_bundle(bundle)
    envelope = bundle.get("dsseEnvelope")
    if not isinstance(envelope, dict) or envelope.get("payloadType") != INTOTO_PAYLOAD:
        raise ValueError("npm provenance omitted the exact in-toto DSSE payload type")
    encoded = envelope.get("payload")
    if not isinstance(encoded, str):
        raise ValueError("npm provenance omitted its signed statement")
    statement_bytes = base64.b64decode(encoded, validate=True)
    statement = load_i_json(
        statement_bytes, label="npm SLSA provenance statement"
    )
    if (
        not isinstance(statement, dict)
        or set(statement) != {"_type", "subject", "predicateType", "predicate"}
        or statement.get("_type") != INTOTO_STATEMENT
        or statement.get("predicateType") != SLSA_PROVENANCE
    ):
        raise ValueError("npm provenance is not one exact SLSA v1 statement")
    subjects = statement["subject"]
    predicate = statement["predicate"]
    if (
        not isinstance(subjects, list)
        or len(subjects) != 1
        or not isinstance(subjects[0], dict)
        or set(subjects[0]) != {"name", "digest"}
        or not isinstance(subjects[0]["digest"], dict)
        or set(subjects[0]["digest"]) != {"sha512"}
    ):
        raise ValueError("npm provenance must contain one exact package subject")
    expected_purl = f"pkg:npm/{urllib.parse.quote(package, safe='/')}@{version}"
    if subjects[0] != {"name": expected_purl, "digest": {"sha512": sha512}}:
        raise ValueError("npm provenance subject does not match the staged package")
    if (
        not isinstance(predicate, dict)
        or set(predicate) != {"buildDefinition", "runDetails"}
        or not isinstance(predicate["runDetails"], dict)
    ):
        raise ValueError("npm provenance predicate is not the closed SLSA v1 shape")
    build = predicate["buildDefinition"]
    if (
        not isinstance(build, dict)
        or set(build)
        != {
            "buildType",
            "externalParameters",
            "internalParameters",
            "resolvedDependencies",
        }
        or build["buildType"] != SLSA_GITHUB_BUILD_TYPE
        or not isinstance(build["internalParameters"], dict)
    ):
        raise ValueError("npm provenance has another GitHub Actions build definition")
    if build["internalParameters"] != {
        "github": {
            "event_name": "workflow_dispatch",
            "repository_id": REPOSITORY_ID,
            "repository_owner_id": REPOSITORY_OWNER_ID,
        }
    }:
        raise ValueError("npm provenance has another GitHub source repository")
    run_details = predicate["runDetails"]
    if (
        set(run_details) != {"builder", "metadata"}
        or run_details.get("builder") != {"id": SLSA_GITHUB_BUILDER}
        or not isinstance(run_details.get("metadata"), dict)
        or set(run_details["metadata"]) != {"invocationId"}
        or not isinstance(run_details["metadata"]["invocationId"], str)
        or re.fullmatch(
            rf"{re.escape(REPOSITORY)}/actions/runs/[1-9][0-9]*/attempts/[1-9][0-9]*",
            run_details["metadata"]["invocationId"],
        )
        is None
    ):
        raise ValueError("npm provenance has another GitHub-hosted invocation")
    external = build["externalParameters"]
    dependencies = build["resolvedDependencies"]
    if (
        not isinstance(external, dict)
        or set(external) != {"workflow"}
        or not isinstance(external["workflow"], dict)
        or not isinstance(dependencies, list)
        or len(dependencies) != 1
        or not isinstance(dependencies[0], dict)
    ):
        raise ValueError("npm provenance has another workflow or dependency set")
    workflow = external["workflow"]
    expected_workflow_refs = {"refs/heads/main", f"refs/tags/v{version}"}
    if (
        set(workflow) != {"ref", "repository", "path"}
        or workflow.get("ref") not in expected_workflow_refs
        or workflow.get("repository") != REPOSITORY
        or workflow.get("path") != WORKFLOW
    ):
        raise ValueError("npm provenance has another workflow authority")
    workflow_ref = workflow["ref"]
    expected_dependency = {
        "uri": f"git+{REPOSITORY}@{workflow_ref}",
        "digest": {"gitCommit": release_sha},
    }
    if dependencies[0] != expected_dependency:
        raise ValueError("npm provenance does not resolve the exact release source")
    return {
        "attestations_url": attestations_url,
        "bundle_digest": version_domains.digest_json(bundle),
        "statement_digest": f"sha256:{hashlib.sha256(statement_bytes).hexdigest()}",
        "certificate_identity": SIGSTORE_CERTIFICATE_IDENTITY,
        "certificate_issuer": SIGSTORE_CERTIFICATE_ISSUER,
        "predicate_type": SLSA_PROVENANCE,
        "workflow_ref": workflow_ref,
        "source_sha": release_sha,
        "signer_ref": signer_ref,
        "signer_sha": signer_sha,
    }


def verify_registry(
    manifest_path: pathlib.Path, release_sha: str, *, require_latest: bool = False
) -> dict[str, object]:
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
    tarball = require_registry_url(
        dist.get("tarball"),
        path_prefix="/",
        label="npm tarball URL",
    )
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
    provenance_record = provenance.get("provenance")
    if (
        not isinstance(provenance_record, dict)
        or provenance_record.get("predicateType") != SLSA_PROVENANCE
    ):
        raise ValueError("npm distribution record omits SLSA v1 provenance")
    url = provenance.get("url")
    if not isinstance(url, str):
        raise ValueError("npm distribution record omits the attestation URL")
    provenance_evidence = verify_provenance(
        url,
        manifest["package"],
        manifest["version"],
        manifest["sha512"],
        release_sha,
    )
    if require_latest:
        wait_for_latest_tag(manifest["package"], manifest["version"])
    else:
        verify_existing_stable_state(manifest["package"], manifest["version"])
    print(
        f"verified npm bytes, provenance, and stable tag state for "
        f"{manifest['package']}@{manifest['version']} at {release_sha}"
    )
    encoded_package = urllib.parse.quote(manifest["package"], safe="")
    encoded_version = urllib.parse.quote(manifest["version"], safe="")
    return {
        "package_id": f"npm:{manifest['package']}",
        "name": manifest["package"],
        "version": manifest["version"],
        "publication": {
            "kind": "npm",
            "registry": PUBLISH_REGISTRY,
            "registry_identity": f"{REGISTRY}/{encoded_package}/{encoded_version}",
            "content_digest": f"sha512:{manifest['sha512']}",
            "provenance": {
                "kind": "sigstore",
                "sha1": f"sha1:{manifest['sha1']}",
                "integrity": manifest["integrity"],
                "tarball_url": tarball,
                **provenance_evidence,
            },
        },
    }


def write_publication_evidence(
    output: pathlib.Path, evidence: dict[str, object]
) -> None:
    """Materialize one job-local BOM input without granting it receipt authority."""

    if (
        not output.is_absolute()
        or output.parent.is_symlink()
        or not output.parent.is_dir()
        or output.exists()
    ):
        raise ValueError("npm publication evidence output must be one new absolute file")
    with output.open("x", encoding="utf-8") as handle:
        handle.write(json.dumps(evidence, indent=2, sort_keys=True) + "\n")


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


def publication_admission(manifest_path: pathlib.Path, release_sha: str) -> str:
    """Admit publication or historical recovery of one exact immutable version."""

    status = registry_status(manifest_path, release_sha)
    if status == "exists":
        verify_registry(manifest_path, release_sha)
        return "retained"
    if status != "missing":
        raise ValueError(f"unknown npm registry status {status}")
    manifest, _archive = load_stage(manifest_path, release_sha)
    latest_status = latest_publish_status(manifest["package"], manifest["version"])
    if latest_status == "exists":
        verify_registry(manifest_path, release_sha)
        return "retained"
    if latest_status != "missing":
        raise ValueError(f"unknown npm latest publication status {latest_status}")
    return "missing"


def tag_creation_admission(manifest_path: pathlib.Path, release_sha: str) -> str:
    """Authorize a missing tag only at the current stable registry frontier."""

    status = registry_status(manifest_path, release_sha)
    if status == "exists":
        verify_registry(manifest_path, release_sha)
    elif status != "missing":
        raise ValueError(f"unknown npm registry status {status}")

    manifest, _archive = load_stage(manifest_path, release_sha)
    latest_status = latest_publish_status(manifest["package"], manifest["version"])
    if latest_status == "exists":
        if status == "missing":
            verify_registry(manifest_path, release_sha)
        return "retained"
    if latest_status != "missing":
        raise ValueError(f"unknown npm latest publication status {latest_status}")
    if status == "exists":
        raise ValueError("npm version endpoint is absent from the stable package frontier")
    return "missing"


def required_publish_authority(version: str, release_sha: str) -> tuple[str, str]:
    """Bind this controller and payload checkout to the requested release refs."""

    parse_stable_version(version)
    controller_sha = os.environ.get("CONTROLLER_SHA", "")
    expected_release_sha = os.environ.get("RELEASE_SHA", "")
    release_tag = os.environ.get("RELEASE_TAG", "")
    if (
        SHA_PATTERN.fullmatch(controller_sha) is None
        or expected_release_sha != release_sha
        or release_tag != f"v{version}"
    ):
        raise ValueError("npm publication release authority is missing or mismatched")
    controller_head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=CONTROL_ROOT,
        check=True,
        text=True,
        capture_output=True,
    ).stdout.strip()
    payload_head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        check=True,
        text=True,
        capture_output=True,
    ).stdout.strip()
    if controller_head != controller_sha or payload_head != release_sha:
        raise ValueError("npm publication checkout does not match release authority")
    return controller_sha, release_tag


def verify_remote_release_authority(
    controller_sha: str, release_sha: str, release_tag: str
) -> None:
    """Re-read the exact remote main and peeled tag before one npm write."""

    main_ref = "refs/heads/main"
    tag_ref = f"refs/tags/{release_tag}^{{}}"
    result = subprocess.run(
        ["git", "ls-remote", "origin", main_ref, tag_ref],
        cwd=CONTROL_ROOT,
        check=True,
        text=True,
        capture_output=True,
    )
    observed: dict[str, str] = {}
    for line in result.stdout.splitlines():
        fields = line.split("\t")
        if (
            len(fields) != 2
            or SHA_PATTERN.fullmatch(fields[0]) is None
            or fields[1] in observed
        ):
            raise ValueError("remote npm release authority returned malformed refs")
        observed[fields[1]] = fields[0]
    if observed != {main_ref: controller_sha, tag_ref: release_sha}:
        raise ValueError("remote npm release authority moved before publication")


def run_npm_publish(archive: pathlib.Path) -> None:
    """Grant npm identity to one already authenticated archive."""

    subprocess.run(
        [
            "npm",
            "publish",
            str(archive),
            f"--registry={PUBLISH_REGISTRY}",
            "--access=public",
            "--provenance",
            f"--tag={PUBLISH_DIST_TAG}",
            "--ignore-scripts",
        ],
        cwd=CONTROL_ROOT,
        check=True,
        timeout=NPM_PUBLISH_TIMEOUT_SECONDS,
    )


def publish(manifest_path: pathlib.Path, release_sha: str) -> str:
    """Read registry state and fence the only missing-version mutation."""

    admission = publication_admission(manifest_path, release_sha)
    if admission == "retained":
        return "retained"
    if admission != "missing":
        raise ValueError(f"unknown npm publication admission {admission}")
    manifest, archive = load_stage(manifest_path, release_sha)
    controller_sha, release_tag = required_publish_authority(
        manifest["version"], release_sha
    )
    latest_status = latest_publish_status(manifest["package"], manifest["version"])
    if latest_status == "exists":
        verify_registry(manifest_path, release_sha)
        return "retained"
    if latest_status != "missing":
        raise ValueError(f"unknown npm latest publication status {latest_status}")
    verify_remote_release_authority(controller_sha, release_sha, release_tag)
    try:
        run_npm_publish(archive)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        # npm's response is not publication authority; exact registry state is.
        pass
    try:
        verify_registry(manifest_path, release_sha, require_latest=True)
    except (TimeoutError, urllib.error.URLError) as error:
        raise ValueError(
            "npm_publish_outcome_ambiguous: exact registry readback unavailable"
        ) from error
    return "published"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    stage_parser = subparsers.add_parser("stage")
    stage_parser.add_argument("--package-dir", type=pathlib.Path, required=True)
    stage_parser.add_argument("--package-name", required=True)
    stage_parser.add_argument("--version", required=True)
    stage_parser.add_argument("--release-sha", required=True)
    stage_parser.add_argument("--output", type=pathlib.Path, required=True)
    for command in (
        "verify-staged",
        "registry-status",
        "publication-admission",
        "tag-creation-admission",
        "publish",
    ):
        verify_parser = subparsers.add_parser(command)
        verify_parser.add_argument("--manifest", type=pathlib.Path, required=True)
        verify_parser.add_argument("--release-sha", required=True)
    registry_parser = subparsers.add_parser("verify-registry")
    registry_parser.add_argument("--manifest", type=pathlib.Path, required=True)
    registry_parser.add_argument("--release-sha", required=True)
    registry_parser.add_argument("--publication-output", type=pathlib.Path)
    compare_parser = subparsers.add_parser("compare-stages")
    compare_parser.add_argument("--candidate", type=pathlib.Path, required=True)
    compare_parser.add_argument("--reference", type=pathlib.Path, required=True)
    app_identity_parser = subparsers.add_parser("github-app-bot-user-id")
    app_identity_parser.add_argument("--app-slug", required=True)
    app_identity_parser.add_argument("--app-id", type=int, required=True)
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
    elif args.command == "publication-admission":
        print(publication_admission(args.manifest, args.release_sha))
    elif args.command == "tag-creation-admission":
        print(tag_creation_admission(args.manifest, args.release_sha))
    elif args.command == "publish":
        print(publish(args.manifest, args.release_sha))
    elif args.command == "compare-stages":
        compare_stages(args.candidate, args.reference)
        print("verified independent npm stage equality")
    elif args.command == "github-app-bot-user-id":
        token = os.environ.get("GITHUB_APP_TOKEN", "")
        print(github_app_bot_user_id(args.app_slug, args.app_id, token))
    else:
        evidence = verify_registry(args.manifest, args.release_sha)
        if args.publication_output is not None:
            write_publication_evidence(args.publication_output, evidence)
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
