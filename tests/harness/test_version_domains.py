import copy
import base64
import hashlib
import importlib.util
import inspect
import json
import os
import pathlib
import re
import subprocess
import tempfile
import unittest
import urllib.parse
from unittest import mock


ROOT = pathlib.Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "version_domains", ROOT / "scripts/version_domains.py"
)
assert SPEC is not None and SPEC.loader is not None
version_domains = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(version_domains)


# Reviewed wire/semantic fragment owners, not a naming convention. Core
# Definition belongs to the IR contract even when embedded without an outer
# candidate discriminator. Unversioned wrappers are expanded through their
# actual Serde fields until another listed boundary is reached.
RUST_WIRE_BOUNDARIES = {
    "ArtifactRecord": "cymule.artifact/2",
    "ArtifactRef": "cymule.artifact/2",
    "ClockObservation": "cymule.clock-observation/2",
    "ClockObservationRef": "cymule.clock-observation/2",
    "Continuation": "cymule.continuation-state/1",
    "Definition": "cymule.ir/3",
    "EvolutionCommand": "cymule.evolution-control/5",
    "EvolutionCurrent": "cymule.evolution-current/2",
    "EvolutionPersistenceCommand": "cymule.evolution-persistence-command/4",
    "EvolutionPersistenceReceipt": "cymule.evolution-persistence-receipt/4",
    "LiveEvolutionCommand": "cymule.live-evolution-control/6",
    "PlanCandidate": "cymule.ir/3",
    "RegionMigrationCommand": "cymule.virtual-region-migration-control/3",
    "RegionMigrationPlan": "cymule.virtual-region-migration/3",
    "ResourceCandidate": "cymule.resource/4",
    "ResourceHandle": "cymule.resource/4",
    "ResourceLocatorSet": "cymule.resource-locators/2",
    "ResourceManifestDescriptor": "cymule.resource-manifest/3",
    "ResourcePinReceipt": "cymule.resource-pin-receipt/3",
    "ResourceReleaseReceipt": "cymule.resource-release-receipt/3",
    "ResourceRetentionFamily": "cymule.resource-retention-family/1",
    "ResourceRetentionSubject": "cymule.resource-retention-subject/1",
    "SealedPlan": "cymule.plan/1",
    "SubflowRevision": "cymule.subflow-revision/2",
    "VirtualActivationCommand": "cymule.virtual-activation-control/1",
    "VirtualActiveRegionCurrent": "cymule.virtual-active-region-current/1",
    "VirtualArchiveCommandIndexProof": "cymule.virtual-archive-command-index-proof/1",
    "VirtualArchiveManifest": "cymule.virtual-archive-manifest/2",
    "VirtualArchiveRetirementCommand": "cymule.virtual-archive-retirement-control/1",
    "VirtualArchiveWorkProof": "cymule.virtual-archive-work-proof/1",
    "VirtualCertificateCurrent": "cymule.virtual-certificate-current/1",
    "VirtualClaimCommand": "cymule.virtual-claim-control/4",
    "VirtualCompactionCertificate": "cymule.virtual-compaction-certificate/4",
    "VirtualCompactionCommand": "cymule.virtual-compaction-control/1",
    "VirtualCurrent": "cymule.virtual-current/3",
    "VirtualCurrentBody": "cymule.virtual-current-body/2",
    "VirtualInitializationCommand": "cymule.virtual-initialization-control/2",
    "VirtualLeaseRenewalCommand": "cymule.virtual-lease-renewal-control/2",
    "VirtualMaterializationCommand": "cymule.virtual-materialization-control/2",
    "VirtualMigrationCurrent": "cymule.virtual-migration-current/1",
    "VirtualMutationSet": "cymule.virtual-mutation-set/2",
    "VirtualOccurrenceCurrent": "cymule.virtual-occurrence-current/1",
    "VirtualParkedCurrent": "cymule.virtual-parked-current/1",
    "VirtualParkedIndexPage": "cymule.virtual-parked-index-page/1",
    "VirtualPersistenceCommand": "cymule.virtual-persistence-command/2",
    "VirtualPersistenceReceipt": "cymule.virtual-persistence-receipt/3",
    "VirtualRecoveryCommand": "cymule.virtual-recovery-control/2",
    "VirtualRegionCurrent": "cymule.virtual-region-current/1",
    "VirtualRehydrationCommand": "cymule.virtual-rehydration-control/1",
    "VirtualRunCurrent": "cymule.virtual-run-current/1",
    "VirtualRunWeightCommand": "cymule.virtual-run-weight-control/1",
    "VirtualWorkCurrent": "cymule.virtual-work-current/1",
    "WaitActivationReceipt": "cymule.wait-activation-receipt/3",
    "WorkOccurrence": "cymule.virtual-work-occurrence/3",
    "WorkResolutionCommand": "cymule.virtual-work-control/2",
}

# These are identity references, not embedded copies. VirtualReduction::finish
# seals the independent body, then its receipt, then the current wrapper; the
# prior-current link comes from reduce_virtual's exact loaded parent.
RUST_VIRTUAL_REFERENCE_DEPENDENCIES = {
    "cymule.virtual-current-body/2": {"cymule.virtual-state-root/1"},
    "cymule.virtual-current/3": {"cymule.virtual-persistence-receipt/3"},
    "cymule.virtual-persistence-receipt/3": {
        "cymule.virtual-current-body/2", "cymule.virtual-current/3",
    },
}


def rust_wire_source_dependencies() -> dict[str, set[str]]:
    """Read direct field edges for the current Rust-only receipt contracts."""

    paths = [
        ROOT / "crates/cymule-core/src/model.rs",
        ROOT / "crates/cymule-profile-protocol/src/virtual_work.rs",
        ROOT / "crates/cymule-profile-protocol/src/resource.rs",
        ROOT / "crates/cymule-profile-protocol/src/evolution.rs",
        *sorted((ROOT / "crates/cymule-profile-protocol/src/evolution").glob("*.rs")),
    ]
    definitions: dict[str, tuple[str, str]] = {}
    for path in paths:
        source = version_domains.rust_code_mask(version_domains.rust_production_text(path.read_text()))
        for match in re.finditer(r"\bpub\s+(struct|enum)\s+([A-Z]\w*)\s*\{", source):
            start, end, depth = match.end(), match.end(), 1
            while depth:
                depth += (source[end] == "{") - (source[end] == "}")
                end += 1
            name = match.group(2)
            if path == ROOT / "crates/cymule-core/src/model.rs" and name != "ReplayAvailability":
                continue
            if name in definitions:
                raise ValueError(f"wire inventory repeats type {name}")
            definitions[name] = (match.group(1), source[start:end - 1])

    def split_fields(source: str) -> list[str]:
        parts, start, depth = [], 0, 0
        for index, character in enumerate(source):
            if character in "([{<":
                depth += 1
            elif character in ")]}>":
                depth -= 1
            elif character == "," and depth == 0:
                parts.append(source[start:index])
                start = index + 1
        parts.append(source[start:])
        if depth:
            raise ValueError("wire field inventory contains an unbalanced type")
        return parts

    def type_tokens(name: str) -> set[str]:
        kind, body = definitions[name]
        if re.search(r"#\s*\[\s*serde\s*\([^)]*\bskip\b", body):
            raise ValueError(f"wire inventory must explicitly account for {name}'s skipped fields")
        tokens = set()
        for field in split_fields(body):
            # Attribute string contents are already masked. Remove attributes so
            # a tuple variant is identified by its actual field group, never a
            # serde option or a unit variant with the name of another Rust type.
            field = re.sub(r"#\s*\[[^\]]*\]", "", field).strip()
            if not field:
                continue
            if kind == "struct":
                match = re.fullmatch(r"(?:pub(?:\([^)]*\))?\s+)?[a-z][a-z0-9_]*\s*:\s*(.*)", field, re.S)
                if match is None:
                    raise ValueError(f"unsupported wire field in {name}: {field}")
                type_source = match.group(1)
            elif "{" in field:
                type_source = " ".join(
                    member.split(":", 1)[1]
                    for member in split_fields(field.split("{", 1)[1].rsplit("}", 1)[0])
                    if member.strip()
                )
            elif "(" in field:
                type_source = field.split("(", 1)[1].rsplit(")", 1)[0]
            else:
                continue
            tokens.update(re.findall(r"\b[A-Z][A-Za-z0-9_]*\b", type_source))
        return tokens

    standard = {"BTreeMap", "BTreeSet", "Box", "Option", "String", "Value", "Vec", "VecDeque"}
    requirements: dict[str, set[str]] = {}
    for root_type, version in RUST_WIRE_BOUNDARIES.items():
        if root_type not in definitions:
            continue  # External discriminator boundaries are leaves of this audit.
        required = requirements.setdefault(version, set())
        pending, visited = [root_type], set()
        while pending:
            name = pending.pop()
            if name in visited:
                continue
            visited.add(name)
            for target in type_tokens(name) - standard:
                if target in RUST_WIRE_BOUNDARIES:
                    dependency = RUST_WIRE_BOUNDARIES[target]
                    if dependency != version:
                        required.add(dependency)
                elif target in definitions:
                    pending.append(target)
                else:
                    raise ValueError(f"unmapped public wire field {name} -> {target}")
    return requirements


def npm_publication(name: str, source_sha: str, version: str = "0.2.0") -> dict:
    sha512 = "d" * 128
    integrity = "sha512-" + base64.b64encode(bytes.fromhex(sha512)).decode()
    encoded_name = urllib.parse.quote(name, safe="")
    return {
        "package_id": f"npm:{name}",
        "name": name,
        "version": version,
        "publication": {
            "kind": "npm",
            "registry": version_domains.NPM_REGISTRY,
            "registry_identity": (
                f"{version_domains.NPM_REGISTRY.rstrip('/')}/{encoded_name}/{version}"
            ),
            "content_digest": f"sha512:{sha512}",
            "provenance": {
                "kind": "sigstore",
                "sha1": "sha1:" + "c" * 40,
                "integrity": integrity,
                "tarball_url": f"{version_domains.NPM_REGISTRY}{encoded_name}/-/package.tgz",
                "attestations_url": f"{version_domains.NPM_REGISTRY}-/attestations",
                "bundle_digest": "sha256:" + "1" * 64,
                "statement_digest": "sha256:" + "2" * 64,
                "certificate_identity": version_domains.NPM_SIGSTORE_CERTIFICATE_IDENTITY,
                "certificate_issuer": version_domains.NPM_SIGSTORE_CERTIFICATE_ISSUER,
                "predicate_type": version_domains.NPM_SLSA_PROVENANCE,
                "workflow_ref": "refs/heads/main",
                "source_sha": source_sha,
                "signer_ref": version_domains.NPM_SIGSTORE_CERTIFICATE_IDENTITY,
                "signer_sha": "e" * 40,
            },
        },
    }


def cargo_publication(name: str, version: str = "0.2.0") -> dict:
    digest = "sha256:" + "a" * 64
    return {
        "package_id": f"cargo:{name}",
        "name": name,
        "version": version,
        "publication": {
            "kind": "cargo",
            "registry": version_domains.CRATES_REGISTRY,
            "registry_identity": f"https://crates.io/crates/{name}/{version}",
            "content_digest": digest,
            "provenance": {
                "kind": "registry-checksum",
                "checksum": digest,
                "download_url": (
                    f"https://static.crates.io/crates/{name}/{name}-{version}.crate"
                ),
            },
        },
    }


def publication_fixture(
    catalog: list[dict], source_sha: str, version: str = "0.2.0"
) -> list[dict]:
    return [
        *(cargo_publication(entry["name"], version) for entry in catalog),
        npm_publication("cymule", source_sha, version),
        npm_publication("@cymule/sdk", source_sha, version),
    ]


class VersionDomainTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.registry = version_domains.load_registry()

    def test_current_registry_and_generated_specification_table_are_exact(self) -> None:
        version_domains.verify_registry(self.registry)
        self.assertEqual(
            (ROOT / "docs/version-domains.md").read_text(),
            version_domains.render_table(self.registry),
        )

    def test_strict_i_json_and_rfc8785_utf16_order_are_exact(self) -> None:
        value = {"\ue000": 1, "😀": 2}
        expected = '{"😀":2,"\ue000":1}'.encode()
        self.assertEqual(version_domains.canonical_bytes(value), expected)
        self.assertEqual(
            version_domains.digest_json(value),
            "sha256:" + hashlib.sha256(expected).hexdigest(),
        )
        with tempfile.TemporaryDirectory() as temporary:
            duplicate = pathlib.Path(temporary) / "duplicate.json"
            duplicate.write_text('{"owner":"first","owner":"second"}')
            with self.assertRaisesRegex(ValueError, "repeats object member"):
                version_domains.load_json(duplicate)
        for malformed in [1.5, 9_007_199_254_740_992, "\ud800"]:
            with self.subTest(malformed=repr(malformed)):
                with self.assertRaises(ValueError):
                    version_domains.canonical_bytes(malformed)

    def test_current_controller_reads_release_authority_from_exact_payload_workspace(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            workspace = pathlib.Path(temporary).resolve()
            workspace.joinpath("scripts").mkdir()
            workspace.joinpath("scripts/version_domains.py").write_text(
                'raise RuntimeError("payload controller executed")\n',
                encoding="utf-8",
            )
            workspace.joinpath("schemas").mkdir()
            workspace.joinpath(
                "schemas/version-domain-registry.schema.json"
            ).write_bytes(
                ROOT.joinpath(
                    "schemas/version-domain-registry.schema.json"
                ).read_bytes()
            )
            workspace.joinpath("versioning").mkdir()
            payload_registry = copy.deepcopy(self.registry)
            payload_registry["source_generation"]["generation"] = (
                "payload-release-generation"
            )
            workspace.joinpath("versioning/version-domains.json").write_text(
                json.dumps(payload_registry), encoding="utf-8"
            )
            workspace.joinpath("CHANGELOG.md").write_text(
                "# Changelog\n\n## [Unreleased]\n\n"
                "## [0.2.0]\n\n- Payload release notes\n",
                encoding="utf-8",
            )
            workspace.joinpath("sdk/typescript").mkdir(parents=True)
            workspace.joinpath("sdk/python").mkdir(parents=True)
            workspace.joinpath("sdk/go").mkdir(parents=True)
            workspace.joinpath("packages/cymule-core").mkdir(parents=True)
            workspace.joinpath("scripts").mkdir(parents=True, exist_ok=True)
            payload_manifests = {
                pathlib.Path("sdk/typescript/package.json"): (
                    b'{"name":"cymule","version":"0.2.0"}\n'
                ),
                pathlib.Path("sdk/python/pyproject.toml"): (
                    b'[project]\nname = "cymule"\nversion = "0.2.0"\n'
                ),
                pathlib.Path("sdk/go/go.mod"): (
                    b"module github.com/cymule-framework/cymule/sdk/go\n"
                ),
                pathlib.Path("packages/cymule-core/Cargo.toml"): (
                    b'[package]\nname = "cymule-core"\n'
                    b"version.workspace = true\npublish.workspace = true\n"
                ),
            }
            for relative, value in payload_manifests.items():
                workspace.joinpath(relative).write_bytes(value)
            workspace.joinpath("Cargo.toml").write_text(
                '[workspace]\nmembers = ["packages/cymule-core"]\n'
                '[workspace.package]\nversion = "0.2.0"\n'
                'publish = ["crates-io"]\n',
                encoding="utf-8",
            )
            workspace.joinpath("scripts/crates-release.toml").write_text(
                'schema = 1\n\n[[crate]]\nname = "cymule-core"\n'
                'path = "packages/cymule-core"\ndependencies = []\n',
                encoding="utf-8",
            )

            specification = importlib.util.spec_from_file_location(
                "version_domains_payload_test", ROOT / "scripts/version_domains.py"
            )
            assert specification is not None and specification.loader is not None
            controller = importlib.util.module_from_spec(specification)
            with mock.patch.dict(
                os.environ, {"CYMULE_RELEASE_WORKSPACE": str(workspace)}
            ):
                specification.loader.exec_module(controller)

            self.assertEqual(controller.CONTROL_ROOT, ROOT)
            self.assertEqual(controller.ROOT, workspace)
            self.assertEqual(
                controller.registry_digest(), controller.digest_json(payload_registry)
            )
            self.assertEqual(
                controller.release_notes("0.2.0"), "- Payload release notes\n"
            )
            catalog = controller.release_catalog_entries()
            with mock.patch.object(controller, "verify_registry"):
                bom = controller.build_bom(
                    payload_registry,
                    "a" * 40,
                    "b" * 40,
                    publication_fixture(catalog, "b" * 40),
                    catalog=catalog,
                )
            package_manifests = {
                record["package_id"]: record["manifest_digest"]
                for record in bom["packages"]
            }
            expected_manifests = {
                "cargo:cymule-core": pathlib.Path(
                    "packages/cymule-core/Cargo.toml"
                ),
                "npm:cymule": pathlib.Path("sdk/typescript/package.json"),
                "python:cymule": pathlib.Path("sdk/python/pyproject.toml"),
                "go:github.com/cymule-framework/cymule/sdk/go": pathlib.Path(
                    "sdk/go/go.mod"
                ),
            }
            for package, manifest in expected_manifests.items():
                self.assertEqual(
                    package_manifests[package],
                    controller.digest_bytes(payload_manifests[manifest]),
                )
            workspace.joinpath("scripts/crates-release.toml").write_text(
                "schema = 1\ncrate = []\n", encoding="utf-8"
            )
            with self.assertRaisesRegex(ValueError, "catalog mismatch"):
                controller.release_catalog_entries()

    def test_release_workspace_must_be_absolute_for_version_authority(self) -> None:
        specification = importlib.util.spec_from_file_location(
            "version_domains_relative_workspace_test",
            ROOT / "scripts/version_domains.py",
        )
        assert specification is not None and specification.loader is not None
        controller = importlib.util.module_from_spec(specification)
        with mock.patch.dict(
            os.environ, {"CYMULE_RELEASE_WORKSPACE": "relative/tag"}
        ):
            with self.assertRaisesRegex(
                ValueError, "CYMULE_RELEASE_WORKSPACE must be an absolute path"
            ):
                specification.loader.exec_module(controller)

    def test_source_anchors_ignore_comments_and_require_exact_tokens(self) -> None:
        comment = '// pub const VERSION: &str = "cymule.fake/1";\n'
        self.assertFalse(
            version_domains.rust_source_anchor(comment, "VERSION", "cymule.fake/1")
        )
        self.assertFalse(
            version_domains.rust_source_anchor(
                'const TEXT: &str = r#"pub const VERSION: &str = '
                '\"cymule.fake/1\";"#;',
                "VERSION",
                "cymule.fake/1",
            )
        )
        self.assertEqual(
            version_domains.VERSION_PATTERN.findall("cymule.engine/4evil"), []
        )
        self.assertTrue(
            version_domains.rust_source_anchor(
                'let _ = b"cymule.binary-domain/1";',
                "$literal",
                "cymule.binary-domain/1",
            )
        )
        self.assertFalse(
            version_domains.rust_source_anchor(
                'r#"b\\"cymule.binary-domain/1\\""#; // b"cymule.binary-domain/1"',
                "$literal",
                "cymule.binary-domain/1",
            )
        )
        with tempfile.TemporaryDirectory() as temporary:
            path = pathlib.Path(temporary) / "schema.json"
            path.write_text('{"title":"Protocol cymule.agent/2"}')
            self.assertTrue(
                version_domains.source_anchor_matches(
                    path.read_text(), path, "$token:/title", "cymule.agent/2"
                )
            )
            path.write_text('{"title":"Protocol cymule.agent/2legacy"}')
            self.assertFalse(
                version_domains.source_anchor_matches(
                    path.read_text(), path, "$token:/title", "cymule.agent/2"
                )
            )

        python_version = "cymule.release-stage/1"
        self.assertTrue(
            version_domains.python_source_anchor(
                f'RELEASE_VERSION: str = "{python_version}"\n',
                "RELEASE_VERSION",
                python_version,
            )
        )
        for malformed in (
            f'RELEASE_VERSION = "{python_version}" + "-suffix"\n',
            f'RELEASE_VERSION = "{python_version}"\nRELEASE_VERSION = "other"\n',
            f'RELEASE_VERSION = OTHER = "{python_version}"\n',
            f'"RELEASE_VERSION = \\"{python_version}\\""\n',
        ):
            with self.subTest(malformed=malformed):
                self.assertFalse(
                    version_domains.python_source_anchor(
                        malformed, "RELEASE_VERSION", python_version
                    )
                )

    def test_public_source_snapshot_excludes_registry_and_private_mirror_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            subprocess.run(["git", "init", "-b", "main"], cwd=root, check=True)
            root.joinpath("README.md").write_text("public\n")
            root.joinpath(".gitlab").mkdir()
            root.joinpath(".gitlab/private.yml").write_text("private one\n")
            root.joinpath("versioning").mkdir()
            root.joinpath("versioning/version-domains.json").write_text("{}\n")
            subprocess.run(["git", "add", "."], cwd=root, check=True)
            first = version_domains.current_source_snapshot_digest(root)
            root.joinpath(".gitlab/private.yml").write_text("private two\n")
            root.joinpath("versioning/version-domains.json").write_text('{"changed":true}\n')
            self.assertEqual(first, version_domains.current_source_snapshot_digest(root))
            root.joinpath("README.md").write_text("public changed\n")
            self.assertNotEqual(first, version_domains.current_source_snapshot_digest(root))

    def test_source_candidate_includes_unignored_new_files(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            subprocess.run(["git", "init", "-b", "main"], cwd=root, check=True)
            root.joinpath("tracked.txt").write_text("tracked\n")
            root.joinpath(".gitignore").write_text("ignored.txt\n")
            subprocess.run(["git", "add", "."], cwd=root, check=True)
            root.joinpath("candidate.txt").write_text("candidate\n")
            root.joinpath("ignored.txt").write_text("ignored\n")

            self.assertEqual(
                set(version_domains.candidate_git_paths(root)),
                {".gitignore", "candidate.txt", "tracked.txt"},
            )

    def test_required_ci_source_closure_is_exact_and_does_not_walk_history(self) -> None:
        expected = self.registry["source_generation"]["source_snapshot_digest"]
        with (
            mock.patch.object(
                version_domains, "validate_release_registry_closure"
            ) as validate_closure,
            mock.patch.object(
                version_domains,
                "current_source_snapshot_digest",
                return_value=expected,
            ),
            mock.patch.object(
                version_domains,
                "source_snapshot_history",
                side_effect=AssertionError("required CI must not walk release ancestry"),
            ),
        ):
            version_domains.verify_source_candidate_closure(self.registry)
        validate_closure.assert_called_once_with(self.registry, version_domains.ROOT)

        with (
            mock.patch.object(
                version_domains, "validate_release_registry_closure"
            ),
            mock.patch.object(
                version_domains,
                "current_source_snapshot_digest",
                return_value="sha256:" + "0" * 64,
            ),
            self.assertRaisesRegex(ValueError, "snapshot drifted"),
        ):
            version_domains.verify_source_candidate_closure(self.registry)

        archived = copy.deepcopy(self.registry)
        archived["source_generation"]["state"] = "archived"
        with (
            mock.patch.object(
                version_domains, "validate_release_registry_closure"
            ),
            self.assertRaisesRegex(ValueError, "not a source generation"),
        ):
            version_domains.verify_source_candidate_closure(archived)

    def test_public_source_snapshot_binds_git_mode_type_and_history(self) -> None:
        regular = version_domains.source_snapshot_digest(
            [("tool", "100644", "blob", b"same")]
        )
        executable = version_domains.source_snapshot_digest(
            [("tool", "100755", "blob", b"same")]
        )
        self.assertNotEqual(regular, executable)
        with self.assertRaisesRegex(ValueError, "unsupported Git entry"):
            version_domains.source_snapshot_digest(
                [("nested", "040000", "tree", b"same")]
            )

        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            subprocess.run(["git", "init", "-b", "main"], cwd=root, check=True)
            subprocess.run(
                ["git", "config", "user.name", "Cymule Test"], cwd=root, check=True
            )
            subprocess.run(
                ["git", "config", "user.email", "test@example.com"],
                cwd=root,
                check=True,
            )
            root.joinpath("README.md").write_text("public\n")
            tool = root / "tool.sh"
            tool.write_text("#!/bin/sh\nexit 0\n")
            tool.chmod(0o644)
            root.joinpath("tool-link").symlink_to("tool.sh")
            subprocess.run(["git", "add", "."], cwd=root, check=True)
            subprocess.run(["git", "commit", "-m", "Add public tree"], cwd=root, check=True)
            head = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=root,
                check=True,
                text=True,
                capture_output=True,
            ).stdout.strip()
            current = version_domains.current_source_snapshot_digest(root)
            self.assertEqual(
                version_domains.commit_source_snapshot_digest(head, root=root),
                current,
            )
            self.assertEqual(
                dict(version_domains.source_snapshot_history(root))[head], current
            )
            tool.chmod(0o755)
            executable_snapshot = version_domains.current_source_snapshot_digest(root)
            self.assertNotEqual(current, executable_snapshot)
            self.assertEqual(
                version_domains.commit_source_snapshot_digest(head, root=root),
                current,
            )
            tool.chmod(0o644)
            root.joinpath("tool-link").unlink()
            root.joinpath("tool-link").write_text("tool.sh")
            self.assertNotEqual(
                current, version_domains.current_source_snapshot_digest(root)
            )
            subprocess.run(["git", "add", "."], cwd=root, check=True)
            subprocess.run(["git", "commit", "-m", "Replace symlink with explicit file"], cwd=root, check=True)
            next_head = str(version_domains.git_output(["rev-parse", "HEAD"], root=root)).strip()
            history = version_domains.source_snapshot_history(root)
            self.assertNotEqual(next_head, head)
            self.assertEqual(history[0][0], next_head)
            self.assertEqual(dict(history)[next_head], version_domains.current_source_snapshot_digest(root))
            self.assertEqual(
                version_domains.commit_source_snapshot_digest(next_head, root=root),
                version_domains.current_source_snapshot_digest(root),
            )
            with self.assertRaisesRegex(ValueError, "exact lowercase Git commit"):
                version_domains.commit_source_snapshot_digest("HEAD", root=root)
            history.clear()
            self.assertEqual(version_domains.source_snapshot_history(root)[0][0], next_head)

    def test_conformance_names_and_registry_routes_bind_real_harness_leaves(self) -> None:
        version_domains.validate_registry_conformance(self.registry)
        malformed = copy.deepcopy(self.registry)
        malformed["domains"][0]["conformance"] = ["full"]
        with self.assertRaisesRegex(ValueError, "non-leaf harness suites"):
            version_domains.validate_registry_conformance(malformed)

        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            manifest = root / "tests/harness/suites.toml"
            manifest.parent.mkdir(parents=True)
            manifest.write_text(
                """
[suites.protocol]
commands = [["protocol"]]
[suites.rust-core]
commands = [["rust-core"]]
[suites.full]
abstract = true
requires = ["protocol", "rust-core"]
[[routes]]
patterns = ["**"]
suites = ["protocol"]
""",
                encoding="utf-8",
            )
            registry = {
                "domains": [
                    {"conformance": ["protocol", "rust-core"]},
                ]
            }
            with self.assertRaisesRegex(ValueError, "does not select full"):
                version_domains.validate_registry_conformance(registry, root)
            manifest.write_text(
                manifest.read_text(encoding="utf-8").replace(
                    'suites = ["protocol"]', 'suites = ["full"]'
                ),
                encoding="utf-8",
            )
            version_domains.validate_registry_conformance(registry, root)

    def test_release_changelog_requires_exact_section_and_empty_queue(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            root.joinpath("CHANGELOG.md").write_text(
                "# Changelog\n\n## [Unreleased]\n\n"
                "## [0.2.0-rc.1]\n\n- Candidate\n\n"
                "## [0.2.0] - 2026-08-21\n\n- Stable\n",
                encoding="utf-8",
            )
            version_domains.verify_release_changelog("0.2.0", root)
            self.assertEqual(version_domains.release_notes("0.2.0", root), "- Stable\n")
            for malformed in ("0.2.0-rc.1", "0.2.0\ncontroller_sha=attacker"):
                with self.subTest(malformed=malformed):
                    with self.assertRaisesRegex(ValueError, "stable SemVer"):
                        version_domains.release_notes(malformed, root)
            root.joinpath("CHANGELOG.md").write_text(
                "# Changelog\n\n## [Unreleased]\n\n- Pending\n\n"
                "## [0.2.0]\n\n- Stable\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "Unreleased is non-empty"):
                version_domains.verify_release_changelog("0.2.0", root)

    def test_registry_schema_drives_every_closed_object_boundary(self) -> None:
        mutations = []

        malformed = copy.deepcopy(self.registry)
        malformed["fallback_generation"] = "forbidden"
        mutations.append(malformed)

        malformed = copy.deepcopy(self.registry)
        malformed["source_generation"]["fallback_generation"] = "forbidden"
        mutations.append(malformed)

        malformed = copy.deepcopy(self.registry)
        malformed["defaults"]["fallback"] = True
        mutations.append(malformed)

        malformed = copy.deepcopy(self.registry)
        malformed["defaults"]["migration"]["fallback"] = True
        mutations.append(malformed)

        malformed = copy.deepcopy(self.registry)
        domain = malformed["domains"][0]
        domain["fallback_reader"] = True
        mutations.append(malformed)

        malformed = copy.deepcopy(self.registry)
        domain = malformed["domains"][0]
        domain["sources"][0]["fallback"] = True
        mutations.append(malformed)

        malformed = copy.deepcopy(self.registry)
        domain = next(item for item in malformed["domains"] if item["schemas"])
        domain["schemas"][0]["fallback"] = True
        mutations.append(malformed)

        malformed = copy.deepcopy(self.registry)
        domain = malformed["domains"][0]
        domain["consumers"]["fallback"] = []
        mutations.append(malformed)

        malformed = copy.deepcopy(self.registry)
        domain = malformed["domains"][0]
        domain["migration"] = {
            **self.registry["defaults"]["migration"],
            "fallback": True,
        }
        mutations.append(malformed)

        for malformed in mutations:
            with self.subTest(malformed=malformed):
                with self.assertRaisesRegex(ValueError, "unknown fields"):
                    version_domains.validate_registry_closed_shape(malformed)

        malformed_registry = mutations[0]
        with self.assertRaisesRegex(ValueError, "unknown fields"):
            version_domains.verify_registry(malformed_registry)
        with self.assertRaisesRegex(ValueError, "unknown fields"):
            version_domains.build_bom(
                malformed_registry, "a" * 40, "b" * 40, []
            )

        registry_schema = version_domains.load_json(
            ROOT / "schemas/version-domain-registry.schema.json"
        )

        def load_with_unknown_field(path: pathlib.Path):
            if path.name == "version-domains.json":
                return malformed_registry
            return registry_schema

        with mock.patch.object(
            version_domains, "load_json", side_effect=load_with_unknown_field
        ):
            with self.assertRaisesRegex(ValueError, "unknown fields"):
                version_domains.load_registry()

        malformed_schema = copy.deepcopy(
            registry_schema
        )
        malformed_schema["$defs"]["domain"]["additionalProperties"] = True
        with mock.patch.object(
            version_domains, "load_json", return_value=malformed_schema
        ):
            with self.assertRaisesRegex(ValueError, "not a fixed closed object"):
                version_domains.validate_registry_closed_shape(self.registry)

    def test_complete_registry_schema_closes_enums_and_migration_modes(self) -> None:
        for field, value in [
            (("source_generation", "state"), "invented"),
            (("domains", 0, "kind"), "invented"),
            (("domains", 0, "sources", 0, "role"), "invented"),
        ]:
            malformed = copy.deepcopy(self.registry)
            target = malformed
            for part in field[:-1]:
                target = target[part]
            target[field[-1]] = value
            with self.subTest(field=field):
                with self.assertRaisesRegex(ValueError, "violates Draft 2020-12"):
                    version_domains.validate_registry_schema(malformed)
        for migration in [
            {"mode": "none", "edge": "old-to-new", "runbook": None},
            {"mode": "offline", "edge": None, "runbook": None},
            {
                "mode": "unsupported",
                "edge": "forbidden-edge",
                "runbook": "docs/migrations/pre-segmented-store-generations.md",
            },
        ]:
            malformed = copy.deepcopy(self.registry)
            malformed["defaults"]["migration"] = migration
            with self.subTest(migration=migration):
                with self.assertRaisesRegex(ValueError, "violates Draft 2020-12"):
                    version_domains.validate_registry_schema(malformed)

    def test_schema_reference_requires_registered_dependency(self) -> None:
        malformed = copy.deepcopy(self.registry)
        engine = next(
            domain
            for domain in malformed["domains"]
            if domain["version"] == "cymule.engine/5"
        )
        engine["depends_on"].remove("cymule.resource/4")
        engine["embeds"].remove("cymule.resource/4")
        with self.assertRaisesRegex(ValueError, "schema contract drift"):
            version_domains.verify_registry(malformed)

    def test_root_schema_owned_fragments_require_direct_dependency_edges(self) -> None:
        by_version = {domain["version"]: domain for domain in self.registry["domains"]}
        engine = by_version["cymule.engine/5"]
        direct = {
            "cymule.durable-state/7",
            "cymule.effect-intent/2",
            "cymule.wait/2",
        }
        self.assertTrue(direct.issubset(engine["embeds"]))
        self.assertTrue(direct.issubset(engine["depends_on"]))

        malformed = copy.deepcopy(self.registry)
        engine = next(
            domain
            for domain in malformed["domains"]
            if domain["version"] == "cymule.engine/5"
        )
        engine["embeds"].remove("cymule.wait/2")
        engine["depends_on"].remove("cymule.wait/2")
        with self.assertRaisesRegex(ValueError, "schema contract drift"):
            version_domains.verify_registry(malformed)

    def test_exact_recursive_dependencies_allow_sccs_but_not_self_edges(self) -> None:
        by_version = {domain["version"]: domain for domain in self.registry["domains"]}
        self.assertIn(
            "cymule.resource-manifest/3",
            by_version["cymule.resource/4"]["depends_on"],
        )
        self.assertIn(
            "cymule.resource/4",
            by_version["cymule.resource-manifest-leaf/2"]["depends_on"],
        )
        malformed = copy.deepcopy(self.registry)
        canary = next(
            domain for domain in malformed["domains"] if domain["version"] == "cymule.canary/2"
        )
        canary["depends_on"].append("cymule.canary/2")
        canary["depends_on"].sort()
        with self.assertRaisesRegex(ValueError, "references itself"):
            version_domains.verify_registry(malformed)

    def test_every_production_literal_is_registered(self) -> None:
        registered = {domain["version"] for domain in self.registry["domains"]}
        production = version_domains.production_identity_literals(ROOT)
        self.assertEqual(set(production), registered)
        self.assertIn("cymule.component-occurrence/4", production)
        self.assertIn("cymule.virtual-archive-command-proof/2", production)
        self.assertIn("cymule.authenticated-map-node/1", production)
        self.assertIn("cymule.run-query-indexes/3", production)
        self.assertNotIn("cymule.artifact/1", production)
        self.assertNotIn("cymule.engine/2", production)
        self.assertNotIn("cymule.engine/3", production)
        self.assertNotIn("cymule.live-evolution-checkpoint/4", production)
        self.assertNotIn("cymule.resource-lifecycle/1", production)
        self.assertIn(".github/workflows/*.yml", version_domains.PRODUCTION_IDENTITY_GLOBS)
        self.assertIn(
            "scripts/release_contracts.py", version_domains.PRODUCTION_IDENTITY_GLOBS
        )
        self.assertNotIn(
            "scripts/finalize_release.py", version_domains.PRODUCTION_IDENTITY_GLOBS
        )
        self.assertNotIn(
            "scripts/verify_github_release_settings.py",
            version_domains.PRODUCTION_IDENTITY_GLOBS,
        )
        self.assertFalse(
            any(pattern.startswith(".gitlab") for pattern in version_domains.PRODUCTION_IDENTITY_GLOBS)
        )
        self.assertIn("Cargo.toml", version_domains.PRODUCTION_IDENTITY_GLOBS)

    def test_unregistered_production_literal_fails_closed(self) -> None:
        with mock.patch.object(
            version_domains,
            "production_identity_inventory",
            return_value=(
                {"cymule.unregistered-production/1": ["crates/example/src/lib.rs"]},
                {},
            ),
        ):
            with self.assertRaisesRegex(ValueError, "release registry does not close the production domain set"):
                version_domains.verify_registry(self.registry)

    def test_rust_cfg_test_module_literals_are_not_production(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            source = root / "crates/example/src/lib.rs"
            source.parent.mkdir(parents=True)
            source.write_text(
                '''
pub const CURRENT: &str = "cymule.artifact/2";
fn current_id() { let _ = content_id(CURRENT, &()); }

#[cfg(test)]
#[allow(clippy::needless_raw_string_hashes)]
mod tests {
    pub const LEGACY: &str = "cymule.artifact/1";
    const RAW: &str = r###"} cymule.effect-input/1 { content_id(FAKE, &())"###;
    // } cymule.engine/2 {
    /* outer { cymule.block-test/1 /* inner } */ } */
    fn nested() { if true { let _ = content_id(LEGACY, &()); assert_eq!(LEGACY, "cymule.artifact/1"); let _ = '{'; } }
}

pub const LATER: &str = "cymule.engine/4";
''',
                encoding="utf-8",
            )
            self.assertEqual(
                version_domains.production_identity_literals(root),
                {
                    "cymule.artifact/2": ["crates/example/src/lib.rs"],
                    "cymule.engine/4": ["crates/example/src/lib.rs"],
                },
            )
            _, content_id_sources = version_domains.production_identity_inventory(root)
            self.assertEqual(
                content_id_sources,
                {"CURRENT": ["crates/example/src/lib.rs"]},
            )
            production_text = version_domains.rust_production_text(source.read_text())
            self.assertNotIn(
                "cymule.artifact/1",
                {
                    version
                    for _, version in version_domains.PUBLIC_RUST_VERSION.findall(
                        production_text
                    )
                },
            )

    def test_unterminated_rust_cfg_test_module_fails_closed(self) -> None:
        with self.assertRaisesRegex(ValueError, "unterminated Rust #\\[cfg\\(test\\)\\] module"):
            version_domains.rust_production_text(
                '#[cfg(test)]\nmod tests {\nconst LEGACY: &str = "cymule.artifact/1";\n'
            )

    def test_rust_parser_cache_never_reuses_file_mtime_as_authority(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            path = root / "crates/example/src/lib.rs"
            path.parent.mkdir(parents=True)
            path.write_text('pub const CURRENT: &str = "cymule.cache-probe/1";\n')
            original_stat = path.stat()
            self.assertEqual(
                set(version_domains.production_identity_literals(root)),
                {"cymule.cache-probe/1"},
            )
            path.write_text('pub const CURRENT: &str = "cymule.cache-probe/2";\n')
            os.utime(path, ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns))
            self.assertEqual(
                set(version_domains.production_identity_literals(root)),
                {"cymule.cache-probe/2"},
            )
        for parser in (
            version_domains.rust_code_mask,
            version_domains.rust_comment_mask,
            version_domains.rust_production_text,
        ):
            self.assertEqual(parser.cache_info().maxsize, 16)

    def test_rust_byte_character_quotes_do_not_open_string_literals(self) -> None:
        source = '''
pub const CURRENT: &str = "cymule.artifact/2";
fn quotes(bytes: &[u8]) -> bool {
    bytes.first() == Some(&b'"') && bytes.last() == Some(&b'"')
}
'''
        self.assertEqual(
            version_domains.VERSION_PATTERN.findall(
                version_domains.rust_comment_mask(source)
            ),
            ["cymule.artifact/2"],
        )

    def test_qualified_content_id_constant_still_requires_identity_role(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            source = root / "crates/example/src/lib.rs"
            source.parent.mkdir(parents=True)
            source.write_text(
                'fn derive() { let _ = cymule_core::content_id(cymule_durable_protocol::CONTINUATION_STATE_VERSION, &()); }\n'
            )
            _, usages = version_domains.production_identity_inventory(root)
            self.assertEqual(usages, {"CONTINUATION_STATE_VERSION": ["crates/example/src/lib.rs"]})
            domain = {
                "version": "cymule.continuation-state/1",
                "sources": [{"symbol": "CONTINUATION_STATE_VERSION", "role": "persistence_discriminator"}],
            }
            with self.assertRaisesRegex(ValueError, "used directly by content_id"):
                version_domains.validate_direct_content_id_sources({"domains": [domain]}, usages)
            domain["sources"][0]["role"] = "content_id_domain"
            version_domains.validate_direct_content_id_sources({"domains": [domain]}, usages)

    def test_virtual_claim_public_surface_has_one_exact_outcome_authority(self) -> None:
        self.assertFalse((ROOT / "crates/cymule-sdk/src/control.rs").exists())
        rust_control = (ROOT / "crates/cymule-durable/src/control.rs").read_text()
        rust_exports = (ROOT / "crates/cymule-sdk/src/lib.rs").read_text()
        self.assertIn(
            "-> DurableResult<virtual_protocol::VirtualClaimOutcome>",
            rust_control,
        )
        self.assertIn("impl<S: DurableStore> DurableVirtualControl", rust_control)
        self.assertNotRegex(rust_exports, r"\bmod\s+control\s*;")
        self.assertIn("VirtualClaimOutcome", rust_exports)
        for removed in (
            "DurableControl",
            "EvolutionControl",
            "LiveEvolutionControl",
            "VirtualWorkControl",
            "VirtualSchedulingControl",
        ):
            self.assertNotRegex(rust_exports, rf"\b{removed}\b")
        builder_markers = {
            "sdk/go/cymule.go": "func ClaimVirtualWork(",
            "sdk/python/src/cymule/__init__.py": "class VirtualSchedulingControlBuilder:",
            "sdk/typescript/src/index.ts": "export class VirtualSchedulingControlBuilder",
        }
        for relative, builder_marker in builder_markers.items():
            source = (ROOT / relative).read_text()
            self.assertNotIn("VirtualSchedulingControl interface", source)
            self.assertNotIn("class VirtualSchedulingControl(Protocol)", source)
            self.assertNotIn("interface VirtualSchedulingControl", source)
            self.assertIn(builder_marker, source)
            for removed in (
                "DurableControl",
                "EvolutionControl",
                "LiveEvolutionControl",
                "VirtualClaimReceipt",
                "CoupledJournalRecord",
                "CoupledJournalManifest",
                "VirtualCompactionReceipt",
                "VirtualWorkControl",
                "RegionMigrator",
                "VirtualArchive",
            ):
                self.assertNotRegex(source, rf"\b{removed}\b")

    def test_rust_only_receipt_dependencies_follow_real_serde_fields(self) -> None:
        requirements = rust_wire_source_dependencies()
        domains = {domain["version"]: domain for domain in self.registry["domains"]}
        self.assertEqual(requirements["cymule.evolution-current/2"], set())
        self.assertEqual(requirements["cymule.resource-retention-family/1"], set())
        self.assertEqual(
            requirements["cymule.virtual-current/3"],
            {"cymule.virtual-current-body/2"},
        )
        self.assertEqual(len(requirements["cymule.virtual-mutation-set/2"]), 9)
        for version, dependencies in requirements.items():
            with self.subTest(version=version):
                self.assertLessEqual(dependencies, set(domains[version]["embeds"]))
                self.assertLessEqual(dependencies, set(domains[version]["depends_on"]))
        for version, dependencies in RUST_VIRTUAL_REFERENCE_DEPENDENCIES.items():
            with self.subTest(identity_references=version):
                self.assertLessEqual(dependencies, set(domains[version]["depends_on"]))
                self.assertTrue(dependencies.isdisjoint(domains[version]["embeds"]))
        facade = (ROOT / "crates/cymule-sdk/src/lib.rs").read_text()
        exports = set(re.findall(r"\b[A-Z][A-Za-z0-9_]*\b", facade))
        for name, version in RUST_WIRE_BOUNDARIES.items():
            if name in exports and version in requirements:
                with self.subTest(public_field=name):
                    self.assertIn("cymule", domains[version]["readers"])
                    self.assertIn("cymule", domains[version]["consumers"]["packages"])
                    self.assertIn("sdk-rust", domains[version]["conformance"])

    def test_retired_schema_versions_are_exact_fixture_values(self) -> None:
        source = (ROOT / "scripts/validate_schemas.py").read_text()
        self.assertNotIn("superseded_version(", source)
        legacy = version_domains.load_json(
            ROOT / "tests/fixtures/legacy-protocol-versions.json"
        )
        self.assertEqual(list(legacy), sorted(legacy))
        current = {domain["version"] for domain in self.registry["domains"]}
        self.assertTrue(set(legacy.values()).isdisjoint(current))
        for version in legacy.values():
            self.assertIsNotNone(version_domains.VERSION_PATTERN.fullmatch(version))

    def test_schema_fragment_pairs_are_unique_and_bom_documents_are_not_duplicated(self) -> None:
        relative = "schemas/durable-storage.schema.json"
        schema = version_domains.load_json(ROOT / relative)
        records = [
            {
                "path": relative,
                "id": schema["$id"],
                "canonical_digest": version_domains.digest_json(schema),
                "fragment": fragment,
                "root": False,
            }
            for fragment in ("#/$defs/state_map_node", "#/$defs/state_map_root")
        ]
        domain = {"version": "fixture", "schemas": records}
        version_domains.validate_schema_record_order(domain)
        for invalid in ([*records, records[0]], list(reversed(records))):
            with self.assertRaisesRegex(ValueError, "path, fragment"):
                version_domains.validate_schema_record_order({**domain, "schemas": invalid})

        candidate = copy.deepcopy(self.registry)
        map_domain = next(
            domain for domain in candidate["domains"]
            if domain["version"] == "cymule.authenticated-map-node/1"
        )
        map_domain["schemas"] = records
        projected = version_domains.release_bom_projection(candidate)
        self.assertEqual(sum(item["path"] == relative for item in projected["schemas"]), 1)
        map_domain["schemas"][1] = {**records[1], "canonical_digest": "sha256:" + "0" * 64}
        with self.assertRaisesRegex(ValueError, "conflicting authorities"):
            version_domains.release_bom_projection(candidate)

    def test_core_and_durable_share_the_exact_collection_root_owner(self) -> None:
        domains = {domain["version"]: domain for domain in self.registry["domains"]}
        for version, fragments in (
            ("cymule.authenticated-map-node/1", {"#/$defs/state_map_node", "#/$defs/state_map_root"}),
            ("cymule.authenticated-log-node/1", {"#/$defs/state_log_node", "#/$defs/state_log_root"}),
            ("cymule.machine-authority-frontier/3", {"#/$defs/machine_authority_frontier"}),
        ):
            self.assertTrue(fragments.issubset({record.get("fragment", "#") for record in domains[version]["schemas"]}))
        self.assertIn("cymule.authenticated-map-node/1", domains["cymule.machine-authority-frontier/3"]["depends_on"])
        storage_schema = version_domains.load_json(ROOT / "schemas/durable-storage.schema.json")
        self.assertNotIn("machine_map_root", storage_schema["$defs"])
        self.assertNotIn("machine_log_root", storage_schema["$defs"])
        self.assertIn("cymule.authenticated-map-node/1", domains["cymule.durable-state-root/4"]["depends_on"])
        self.assertIn("cymule.continuation-state/1", domains["cymule.evolution-persistence-receipt/4"]["depends_on"])
        self.assertNotIn("cymule.durable-state/7", domains["cymule.evolution-persistence-receipt/4"]["depends_on"])

        malformed = copy.deepcopy(self.registry)
        by_version = {domain["version"]: domain for domain in malformed["domains"]}
        owner = by_version["cymule.continuation-state/1"]
        root_record = next(record for record in owner["schemas"] if record["path"] == "schemas/engine-protocol.schema.json")
        owner["schemas"].remove(root_record)
        wrong_owner = by_version["cymule.durable-state/7"]
        wrong_owner["schemas"].append(root_record)
        wrong_owner["schemas"].sort(key=version_domains.schema_record_key)
        with self.assertRaisesRegex(ValueError, "schema contract drift"):
            version_domains.verify_registry(malformed)

        malformed = copy.deepcopy(self.registry)
        owner = next(domain for domain in malformed["domains"] if domain["version"] == "cymule.authenticated-map-node/1")
        owner["schemas"][0]["fragment"] = "#/$defs/state_map_node/missing"
        owner["schemas"].sort(key=version_domains.schema_record_key)
        with self.assertRaises((ValueError, KeyError)):
            version_domains.validate_release_registry_closure(malformed)

    def test_unacknowledged_literal_location_fails_closed(self) -> None:
        malformed = copy.deepcopy(self.registry)
        canary = next(
            domain
            for domain in malformed["domains"]
            if domain["version"] == "cymule.canary/2"
        )
        canary["literal_locations"].remove(
            "crates/cymule-profile-protocol/src/evolution/persistence.rs"
        )
        with self.assertRaisesRegex(ValueError, "release literal locations drifted from production"):
            version_domains.verify_registry(malformed)

    def test_source_anchor_must_be_the_exact_constant_definition(self) -> None:
        malformed = copy.deepcopy(self.registry)
        canary = next(
            domain
            for domain in malformed["domains"]
            if domain["version"] == "cymule.canary/2"
        )
        canary["sources"][0]["symbol"] = "EvolutionController"
        with self.assertRaisesRegex(ValueError, "source anchor"):
            version_domains.verify_registry(malformed)

    def test_json_and_associated_rust_source_anchors_are_exact(self) -> None:
        by_version = {domain["version"]: domain for domain in self.registry["domains"]}
        self.assertEqual(
            by_version["cymule.machine-delta/6"]["sources"][0]["symbol"],
            "MachineDelta::VERSION",
        )
        self.assertEqual(
            by_version["cymule.machine-snapshot/11"]["sources"][0]["symbol"],
            "MachineSnapshot::VERSION",
        )
        self.assertEqual(
            by_version["cymule.agent/7"]["sources"][0]["symbol"],
            "$token:/title",
        )
        malformed = copy.deepcopy(self.registry)
        agent = next(
            domain for domain in malformed["domains"] if domain["version"] == "cymule.agent/7"
        )
        agent["sources"][0]["symbol"] = "/type"
        with self.assertRaisesRegex(ValueError, "source anchor"):
            version_domains.verify_registry(malformed)

    def test_current_registry_rejects_a_historical_extra_domain(self) -> None:
        malformed = copy.deepcopy(self.registry)
        historical = copy.deepcopy(
            next(
                domain
                for domain in malformed["domains"]
                if domain["version"] == "cymule.durable-state/7"
            )
        )
        historical["version"] = "cymule.durable-state/3"
        historical["defined_at_source_snapshot_digest"] = None
        historical["literal_locations"] = []
        malformed["domains"].append(historical)
        malformed["domains"].sort(key=lambda domain: domain["version"])
        with self.assertRaisesRegex(ValueError, "release registry does not close the production domain set"):
            version_domains.verify_registry(malformed)

    def test_source_provenance_is_bound_to_git_authority(self) -> None:
        malformed = copy.deepcopy(self.registry)
        malformed["defaults"]["defined_at_source_snapshot_digest"] = "sha256:" + "a" * 64
        with self.assertRaisesRegex(ValueError, "default source snapshot"):
            version_domains.verify_registry(malformed)

        malformed = copy.deepcopy(self.registry)
        malformed["source_generation"]["predecessor_registry_digest"] = (
            "sha256:" + "b" * 64
        )
        with self.assertRaisesRegex(ValueError, "violates Draft 2020-12"):
            version_domains.verify_registry(malformed)

        malformed = copy.deepcopy(self.registry)
        malformed["source_generation"].update(
            {
                "predecessor_registry_digest": "sha256:" + "b" * 64,
                "predecessor_source_snapshot_digest": "sha256:" + "c" * 64,
                "predecessor_registry_version": "cymule.version-domain-registry/2",
                "predecessor_source_generation": "fabricated-generation",
            }
        )
        with self.assertRaisesRegex(ValueError, "not in HEAD ancestry"):
            version_domains.verify_registry(malformed)

        malformed = copy.deepcopy(self.registry)
        engine = next(
            domain for domain in malformed["domains"] if domain["version"] == "cymule.engine/5"
        )
        del engine["defined_at_source_snapshot_digest"]
        with self.assertRaisesRegex(
            ValueError, r"omits required fields \['defined_at_source_snapshot_digest'\]"
        ):
            version_domains.verify_registry(malformed)

        malformed = copy.deepcopy(self.registry)
        engine = next(
            domain for domain in malformed["domains"] if domain["version"] == "cymule.engine/5"
        )
        engine["defined_at_source_snapshot_digest"] = malformed["source_generation"][
            "baseline_source_snapshot_digest"
        ]
        with self.assertRaisesRegex(ValueError, "engine/5 requires explicit null"):
            version_domains.verify_registry(malformed)

    def test_tracked_plugin_schema_requires_fragment_owned_digest(self) -> None:
        malformed = copy.deepcopy(self.registry)
        agent = next(
            domain for domain in malformed["domains"] if domain["version"] == "cymule.agent/7"
        )
        agent["schemas"] = []
        with self.assertRaisesRegex(ValueError, "agent-protocol.schema.json requires exactly one root owner"):
            version_domains.verify_registry(malformed)

        malformed = copy.deepcopy(self.registry)
        gc = next(
            domain
            for domain in malformed["domains"]
            if domain["version"] == "cymule.durable-gc-receipt/2"
        )
        del gc["schemas"][0]["fragment"]
        with self.assertRaisesRegex(ValueError, "'fragment' is a required property"):
            version_domains.verify_registry(malformed)

        for field in ("root", "canonical_digest"):
            with self.subTest(field=field):
                malformed = copy.deepcopy(self.registry)
                gc = next(
                    domain
                    for domain in malformed["domains"]
                    if domain["version"] == "cymule.durable-gc-receipt/2"
                )
                del gc["schemas"][0][field]
                with self.assertRaisesRegex(
                    ValueError, rf"omits required fields \['{field}'\]"
                ):
                    version_domains.verify_registry(malformed)

    def test_owned_schema_fragments_are_dependency_boundaries(self) -> None:
        schema = {
            "$defs": {
                "rootValue": {"$ref": "#/$defs/supporting"},
                "supporting": {"$ref": "#/$defs/leaf"},
                "leaf": {"const": "cymule.artifact/2"},
            },
            "$ref": "#/$defs/rootValue",
        }
        owners = [
            ("#", "cymule.root/1"),
            ("#/$defs/supporting", "cymule.supporting/1"),
            ("#/$defs/leaf", "cymule.leaf/1"),
        ]
        references, versions = version_domains.schema_fragment_dependencies(
            schema,
            "#/$defs/supporting",
            owners,
            "cymule.supporting/1",
        )
        self.assertEqual(references, set())
        self.assertEqual(versions, {"cymule.leaf/1"})

        references, versions = version_domains.schema_fragment_dependencies(
            schema,
            "#",
            owners,
            "cymule.root/1",
        )
        self.assertEqual(references, set())
        self.assertEqual(versions, {"cymule.supporting/1"})

        references, versions = version_domains.schema_fragment_dependencies(
            schema,
            "#",
            [("#", "cymule.root/1"), ("#/$defs/supporting", "cymule.root/1")],
            "cymule.root/1",
        )
        self.assertEqual(references, set())
        self.assertEqual(versions, set())

        with self.assertRaisesRegex(ValueError, "invalid escape"):
            version_domains.json_pointer({"bad~2escape": True}, "/bad~2escape")

    def test_owned_schemas_are_closed_over_consumers_and_generators(self) -> None:
        plan = next(
            domain for domain in self.registry["domains"] if domain["version"] == "cymule.plan/1"
        )
        self.assertIn("schemas/sealed-plan.schema.json", plan["consumers"]["schemas"])
        self.assertIn("schemas/sealed-plan.schema.json", plan["generator_paths"])

        for field in ("consumers", "generator_paths"):
            malformed = copy.deepcopy(self.registry)
            plan = next(
                domain
                for domain in malformed["domains"]
                if domain["version"] == "cymule.plan/1"
            )
            if field == "consumers":
                plan["consumers"]["schemas"].remove("schemas/sealed-plan.schema.json")
            else:
                plan["generator_paths"].remove("schemas/sealed-plan.schema.json")
            with self.subTest(field=field):
                with self.assertRaisesRegex(ValueError, "owned schemas are not closed"):
                    version_domains.verify_registry(malformed)

    def test_content_id_authorities_are_declared_with_jcs(self) -> None:
        for domain in self.registry["domains"]:
            roles = {source["role"] for source in domain["sources"]}
            if roles & version_domains.CONTENT_ID_SOURCE_ROLES:
                with self.subTest(version=domain["version"]):
                    self.assertIn("cymule.jcs/1", domain["depends_on"])
                    version_domains.validate_identity_source_dependencies(domain)

        _, direct_sources = version_domains.production_identity_inventory(ROOT)
        audited_symbols = {"COMMAND_VERSION", "RESOURCE_MANIFEST_VERSION"}
        self.assertTrue(audited_symbols.issubset(direct_sources))
        audited_sources = {
            symbol: direct_sources[symbol] for symbol in audited_symbols
        }
        version_domains.validate_direct_content_id_sources(
            self.registry, audited_sources
        )

        malformed = copy.deepcopy(self.registry)
        command = next(
            domain
            for domain in malformed["domains"]
            if domain["version"] == "cymule.command/6"
        )
        command["sources"][0]["role"] = "semantic_selector"
        with self.assertRaisesRegex(ValueError, "used directly by content_id"):
            version_domains.validate_direct_content_id_sources(
                malformed, audited_sources
            )

    def test_canonical_engine_receipt_ids_have_content_sources_and_jcs(self) -> None:
        by_version = {domain["version"]: domain for domain in self.registry["domains"]}
        for version in (
            "cymule.effect-resolution-receipt/1",
            "cymule.run-cancellation-receipt/1",
        ):
            domain = by_version[version]
            self.assertIn(
                "content_id_domain", {source["role"] for source in domain["sources"]}
            )
            self.assertIn("cymule.jcs/1", domain["depends_on"])
            version_domains.validate_identity_source_dependencies(domain)

            wrong_role = copy.deepcopy(domain)
            wrong_role["sources"][0]["role"] = "receipt_discriminator"
            with self.assertRaisesRegex(ValueError, "canonical Engine receipt identity"):
                version_domains.validate_identity_source_dependencies(wrong_role)

            missing_jcs = copy.deepcopy(domain)
            missing_jcs["depends_on"].remove("cymule.jcs/1")
            with self.assertRaisesRegex(ValueError, "canonical Engine receipt identity"):
                version_domains.validate_identity_source_dependencies(missing_jcs)

    def test_agent_session_and_stream_identities_are_registered(self) -> None:
        by_version = {domain["version"]: domain for domain in self.registry["domains"]}
        expected = {
            "cymule.agent-command-id/1": (
                "crates/cymule-profile-protocol/src/agent.rs",
                "AGENT_COMMAND_ID_DOMAIN",
            ),
            "cymule.agent-stream-final-update-id/1": (
                "crates/cymule-profile-protocol/src/agent.rs",
                "AGENT_STREAM_FINAL_UPDATE_ID_DOMAIN",
            ),
            "cymule.agent-stream-finalization-coupling-id/1": (
                "crates/cymule-profile-protocol/src/agent.rs",
                "AGENT_STREAM_FINALIZATION_COUPLING_ID_DOMAIN",
            ),
            "cymule.agent-stream-key/1": (
                "crates/cymule-profile-protocol/src/agent.rs",
                "AGENT_STREAM_KEY_DOMAIN",
            ),
        }
        for version, (path, symbol) in expected.items():
            domain = by_version[version]
            self.assertEqual(
                domain["sources"],
                [{"path": path, "symbol": symbol, "role": "content_id_domain"}],
            )
            version_domains.validate_identity_source_dependencies(domain)

        publication = by_version["cymule.agent-stream-publication/1"]
        self.assertEqual(
            publication["sources"],
            [
                {
                    "path": "crates/cymule-profile-protocol/src/agent.rs",
                    "symbol": "AGENT_STREAM_PUBLICATION_NAMESPACE",
                    "role": "content_id_domain",
                }
            ],
        )
        version_domains.validate_identity_source_dependencies(publication)

        reservation = by_version["cymule.agent-stream-publication-reservation/1"]
        reservation_dependencies = {
            "cymule.agent-stream-publication-intent/1",
            "cymule.resource-pin-receipt/3",
            "cymule.resource-retention-family/1",
            "cymule.resource-retention-subject/1",
        }
        self.assertTrue(reservation_dependencies.issubset(reservation["embeds"]))
        self.assertTrue(reservation_dependencies.issubset(reservation["depends_on"]))

    def test_virtual_archive_work_index_domains_are_registered(self) -> None:
        by_version = {domain["version"]: domain for domain in self.registry["domains"]}
        expected_roles = {
            "cymule.virtual-archive-work-empty-leaf/1": "binary_hash_domain",
            "cymule.virtual-archive-work-index-node/2": "catalog_namespace",
            "cymule.virtual-archive-work-key/1": "binary_hash_domain",
            "cymule.virtual-archive-work-leaf/1": "binary_hash_domain",
            "cymule.virtual-archive-work-node/1": "binary_hash_domain",
            "cymule.virtual-archive-work-proof/1": "receipt_discriminator",
        }
        for version, role in expected_roles.items():
            domain = by_version[version]
            self.assertEqual(domain["sources"][0]["role"], role)
            expected_path = (
                "crates/cymule-virtual/src/archive.rs"
                if version == "cymule.virtual-archive-work-index-node/2"
                else "crates/cymule-profile-protocol/src/virtual_work.rs"
            )
            self.assertEqual(domain["sources"][0]["path"], expected_path)
            version_domains.validate_identity_source_dependencies(domain)
        self.assertEqual(by_version["cymule.virtual-archive-work-proof/1"]["schemas"], [])
        source = (ROOT / "crates/cymule-profile-protocol/src/virtual_work.rs").read_text()
        self.assertIn("pub struct VirtualArchiveWorkProof", source)
        self.assertIn("proof_version: WORK_INDEX_PROOF_VERSION.to_owned()", source)
        self.assertIn("VirtualArchiveWorkProof", (ROOT / "crates/cymule-sdk/src/lib.rs").read_text())

    def test_resource_retention_and_physical_generations_are_registered(self) -> None:
        by_version = {domain["version"]: domain for domain in self.registry["domains"]}
        retention = by_version["cymule.resource-retention-key/1"]
        self.assertEqual(retention["sources"][0]["role"], "content_id_domain")
        self.assertTrue(
            {
                "cymule.jcs/1",
                "cymule.resource-locators/2",
                "cymule.resource/4",
            }.issubset(retention["depends_on"])
        )
        for version in (
            "cymule.resource-delete-intent/3",
            "cymule.resource-delete-receipt/3",
            "cymule.resource-gc-receipt/3",
            "cymule.resource-pin-receipt/3",
            "cymule.resource-release-receipt/3",
        ):
            self.assertIn("cymule.resource-retention-key/1", by_version[version]["depends_on"])
        expected_sources = {
            "cymule.resource-fs-layout/2": ("PHYSICAL_LAYOUT_VERSION", "persistence_discriminator"),
            "cymule.resource-object-store-content/1": ("OBJECT_INDEX_NAMESPACE", "catalog_namespace"),
            "cymule.resource-object-store-layout/2": ("PHYSICAL_LAYOUT_VERSION", "persistence_discriminator"),
        }
        for version, (symbol, role) in expected_sources.items():
            self.assertEqual(by_version[version]["sources"][0]["symbol"], symbol)
            self.assertEqual(by_version[version]["sources"][0]["role"], role)
            version_domains.validate_identity_source_dependencies(by_version[version])

    def test_agent_command_receipt_registers_resource_lifecycle_receipts(self) -> None:
        by_version = {domain["version"]: domain for domain in self.registry["domains"]}
        receipt = by_version["cymule.agent-command-receipt/3"]
        lifecycle_receipts = {
            "cymule.resource-pin-receipt/3",
            "cymule.resource-release-receipt/3",
        }
        self.assertTrue(lifecycle_receipts.issubset(receipt["embeds"]))
        self.assertTrue(lifecycle_receipts.issubset(receipt["depends_on"]))

    def test_coupled_checkpoint_receipt_and_key_are_registered(self) -> None:
        by_version = {domain["version"]: domain for domain in self.registry["domains"]}
        key = by_version["cymule.coupled-checkpoint-key/1"]
        receipt = by_version["cymule.coupled-checkpoint-receipt/3"]
        self.assertEqual(key["sources"][0]["role"], "content_id_domain")
        self.assertEqual(receipt["sources"][0]["role"], "content_id_domain")
        self.assertIn("cymule.jcs/1", key["depends_on"])
        self.assertIn("cymule.jcs/1", receipt["depends_on"])
        self.assertEqual(
            receipt["schemas"][0]["fragment"],
            "#/$defs/coupled_checkpoint_receipt",
        )
        self.assertIn(
            "cymule.coupled-checkpoint-receipt/3",
            by_version["cymule.durable-state/7"]["depends_on"],
        )

    def test_incremental_machine_state_root_and_store_domains_are_exact(self) -> None:
        by_version = {domain["version"]: domain for domain in self.registry["domains"]}
        expected_sources = {
            "cymule.machine-authority-root/2": (
                "crates/cymule-core/src/machine.rs",
                "MACHINE_AUTHORITY_ROOT_DOMAIN",
                "content_id_domain",
            ),
            "cymule.machine-root-delta/3": (
                "crates/cymule-core/src/machine.rs",
                "MachineRootDelta::VERSION",
                "persistence_discriminator",
            ),
            "cymule.machine-root-parts/3": (
                "crates/cymule-core/src/machine.rs",
                "MachineRootParts::VERSION",
                "persistence_discriminator",
            ),
            "cymule.projection-root-event/1": (
                "crates/cymule-core/src/machine.rs",
                "PROJECTION_ROOT_EVENT_DOMAIN",
                "content_id_domain",
            ),
            "cymule.projection-root-genesis/1": (
                "crates/cymule-core/src/machine.rs",
                "PROJECTION_ROOT_GENESIS_DOMAIN",
                "content_id_domain",
            ),
            "cymule.durable-physical-token/2": (
                "crates/cymule-durable/src/store.rs",
                "PHYSICAL_TOKEN_VERSION",
                "content_id_domain",
            ),
            "cymule.durable-revision/3": (
                "crates/cymule-durable/src/state_root.rs",
                "DURABLE_REVISION_VERSION",
                "content_id_domain",
            ),
            "cymule.authenticated-log-node/1": (
                "crates/cymule-authenticated-collections/src/log.rs",
                "LOG_NODE_VERSION",
                "binary_hash_domain",
            ),
            "cymule.authenticated-map-node/1": (
                "crates/cymule-authenticated-collections/src/map.rs",
                "MAP_NODE_VERSION",
                "binary_hash_domain",
            ),
            "cymule.durable-state-root/4": (
                "crates/cymule-durable/src/state_root.rs",
                "STATE_ROOT_MANIFEST_VERSION",
                "content_id_domain",
            ),
            "cymule.durable-state-value/4": (
                "crates/cymule-durable/src/state_root.rs",
                "STATE_ROOT_VALUE_VERSION",
                "content_id_domain",
            ),
        }
        for version, (path, symbol, role) in expected_sources.items():
            with self.subTest(version=version):
                self.assertEqual(
                    by_version[version]["sources"],
                    [{"path": path, "symbol": symbol, "role": role}],
                )
                version_domains.validate_identity_source_dependencies(
                    by_version[version]
                )

        self.assertTrue(
            {
                "cymule.authenticated-log-node/1",
                "cymule.authenticated-map-node/1",
                "cymule.durable-state-value/4",
                "cymule.machine-authority-frontier/3",
            }.issubset(by_version["cymule.durable-state-root/4"]["depends_on"])
        )
        for provider in ("cymule.directory-store/5", "cymule.sqlite-store/6"):
            self.assertTrue(
                {
                    "cymule.durable-gc-receipt/2",
                    "cymule.durable-head/2",
                    "cymule.durable-state-root/4",
                }.issubset(by_version[provider]["depends_on"])
            )

    def test_removed_segment_checkpoint_and_legacy_generations_are_not_current(self) -> None:
        versions = {domain["version"] for domain in self.registry["domains"]}
        legacy = version_domains.load_json(
            ROOT / "tests/harness/fixtures/legacy-state-root-domains.json"
        )["legacy_versions"]
        self.assertEqual(legacy, sorted(set(legacy)))
        self.assertTrue(set(legacy).isdisjoint(versions))
        self.assertFalse(
            any(
                version.startswith("cymule.durable-checkpoint/")
                or version.startswith("cymule.durable-segment/")
                for version in versions
            )
        )

    def test_durable_storage_supporting_fragments_keep_core_ownership(self) -> None:
        by_version = {domain["version"]: domain for domain in self.registry["domains"]}
        expected = {
            "cymule.artifact/2": (
                "schemas/engine-protocol.schema.json",
                "#/$defs/artifactRef",
            ),
            "cymule.command-archive-segment/4": (
                "schemas/durable-storage.schema.json",
                "#/$defs/command_archive_header",
            ),
            "cymule.history-compaction/2": (
                "schemas/durable-storage.schema.json",
                "#/$defs/history_compaction",
            ),
            "cymule.machine-snapshot/11": (
                "schemas/durable-storage.schema.json",
                "#/$defs/machine_snapshot",
            ),
        }
        for version, (path, fragment) in expected.items():
            self.assertIn(
                {"path": path, "fragment": fragment},
                [
                    {"path": schema["path"], "fragment": schema.get("fragment", "#")}
                    for schema in by_version[version]["schemas"]
                ],
            )
        self.assertNotIn(
            "cymule.engine/5",
            by_version["cymule.machine-snapshot/11"]["depends_on"],
        )

    def test_source_package_must_own_or_write_its_domain(self) -> None:
        malformed = copy.deepcopy(self.registry)
        canary = next(
            domain for domain in malformed["domains"] if domain["version"] == "cymule.canary/2"
        )
        canary["owner"] = "cymule-core"
        canary["writers"] = ["cymule-core"]
        with self.assertRaisesRegex(ValueError, "neither owner nor writer"):
            version_domains.verify_registry(malformed)

    def test_release_catalog_is_complete_before_bom_materialization(self) -> None:
        metadata = version_domains.cargo_metadata()
        with mock.patch.object(
            version_domains,
            "release_catalog_entries",
            return_value=[],
        ):
            with self.assertRaisesRegex(ValueError, "crate release catalog mismatch"):
                version_domains.release_catalog(metadata)

    def test_physical_store_and_release_bom_ownership_is_exact(self) -> None:
        by_version = {
            domain["version"]: domain for domain in self.registry["domains"]
        }
        sqlite = by_version["cymule.sqlite-store/6"]
        self.assertEqual(sqlite["owner"], "cymule-store-sqlite")
        self.assertEqual(sqlite["writers"], ["cymule-runtime", "cymule-store-sqlite"])
        self.assertIn(
            {
                "path": "plugins/store-sqlite/src/lib.rs",
                "symbol": "SCHEMA",
                "role": "persistence_discriminator",
            },
            sqlite["sources"],
        )
        directory = by_version["cymule.directory-store/5"]
        self.assertEqual(directory["kind"], "binding")
        self.assertEqual(directory["owner"], "cymule-directory-store")
        self.assertEqual(directory["compatibility_mode"], "exact-reject")
        self.assertEqual(
            directory["sources"],
            [
                {
                    "path": "crates/cymule-runtime/src/protocol.rs",
                    "symbol": "ENGINE_DIRECTORY_STORE_PROVIDER",
                    "role": "protocol_discriminator",
                },
                {
                    "path": "plugins/directory-store/src/lib.rs",
                    "symbol": "DIRECTORY_SCHEMA_VERSION",
                    "role": "persistence_discriminator",
                }
            ],
        )
        self.assertEqual(sqlite["compatibility_mode"], "exact-reject")
        self.assertEqual(by_version["cymule.release-bom/3"]["kind"], "receipt")
        self.assertEqual(
            by_version["cymule.durable-gc-receipt/2"]["schemas"][0]["fragment"],
            "#/$defs/gc_receipt",
        )
        self.assertNotIn(
            "cymule.engine/5",
            by_version["cymule.durable-gc-receipt/2"]["depends_on"],
        )
        self.assertIn(
            "cymule.resource-manifest/3",
            by_version["cymule.resource/4"]["embeds"],
        )
        self.assertIn(
            "cymule.resource/4",
            by_version["cymule.resource-manifest-leaf/2"]["depends_on"],
        )
        self.assertEqual(
            by_version["cymule.resource-handoff/5"]["embeds"],
            [
                "cymule.artifact/2",
                "cymule.component-occurrence/4",
                "cymule.framework-resource-handle/4",
            ],
        )
        execution_binding = by_version["cymule.execution-binding/2"]
        self.assertIn("cymule.runtime-composition/1", execution_binding["embeds"])
        self.assertIn("cymule.runtime-composition/1", execution_binding["depends_on"])
        occurrence = by_version["cymule.component-occurrence/4"]
        self.assertEqual(
            occurrence["sources"],
            [
                {
                    "path": "crates/cymule-durable/src/model.rs",
                    "symbol": "COMPONENT_OCCURRENCE_VERSION",
                    "role": "content_id_domain",
                }
            ],
        )

    def test_release_control_receipts_have_one_registered_source_authority(self) -> None:
        by_version = {
            domain["version"]: domain for domain in self.registry["domains"]
        }
        expected_symbols = {
            "cymule.github-release-control-plane-receipt/2":
                "CONTROL_PLANE_RECEIPT_VERSION",
            "cymule.github-release-settings-snapshot/2":
                "CONTROL_PLANE_SETTINGS_VERSION",
            "cymule.public-mirror-receipt/2": "MIRROR_RECEIPT_VERSION",
            "cymule.release-finalization-stage/3": "FINALIZATION_STAGE_VERSION",
        }
        for version, symbol in expected_symbols.items():
            with self.subTest(version=version):
                domain = by_version[version]
                self.assertEqual(domain["owner"], "release-governance")
                self.assertEqual(domain["compatibility_mode"], "exact-reject")
                self.assertEqual(
                    domain["sources"],
                    [{
                        "path": "scripts/release_contracts.py",
                        "symbol": symbol,
                        "role": "release_receipt",
                    }],
                )
                self.assertEqual(
                    domain["literal_locations"], ["scripts/release_contracts.py"]
                )
                self.assertEqual(domain["conformance"], ["release-workflows"])
        self.assertEqual(
            by_version["cymule.github-release-control-plane-receipt/2"]["embeds"],
            ["cymule.github-release-settings-snapshot/2"],
        )
        self.assertEqual(
            by_version["cymule.public-mirror-receipt/2"]["migration"],
            {
                "mode": "unsupported",
                "edge": None,
                "runbook": "docs/migrations/public-mirror-receipt-carrier-v1.md",
            },
        )

    def test_terminal_source_generation_has_no_intermediate_domains(self) -> None:
        self.assertEqual(
            self.registry["source_generation"]["generation"],
            "source-0.2.0-unreleased-generation-3",
        )
        self.assertTrue(
            all(
                self.registry["source_generation"][field] is None
                for field in (
                    "predecessor_registry_digest",
                    "predecessor_source_snapshot_digest",
                    "predecessor_registry_version",
                    "predecessor_source_generation",
                )
            )
        )
        self.assertTrue(
            all(
                domain["defined_at_source_snapshot_digest"] is None
                for domain in self.registry["domains"]
            )
        )
        by_version = {
            domain["version"]: domain for domain in self.registry["domains"]
        }
        versions = set(by_version)
        self.assertIn("cymule.agent/7", versions)
        self.assertTrue(
            {
                "cymule.agent-command/3",
                "cymule.agent-command-receipt/3",
                "cymule.agent-session-current/2",
                "cymule.agent-stream-publication-reservation/1",
                "cymule.agent-stream-key/1",
                "cymule.agent-stream-chunk-key/1",
            }.issubset(versions)
        )
        self.assertIn("cymule.canary/2", versions)
        self.assertIn("cymule.coupled-checkpoint-receipt/3", versions)
        self.assertIn("cymule.durable-control/4", versions)
        self.assertIn("cymule.durable-state/7", versions)
        self.assertIn("cymule.durable-state-root/4", versions)
        self.assertIn("cymule.durable-state-value/4", versions)
        self.assertIn("cymule.durable-revision/3", versions)
        self.assertIn("cymule.effect-provider-attempt/1", versions)
        self.assertIn("cymule.engine/5", versions)
        self.assertIn("cymule.evolution-control/5", versions)
        self.assertIn("cymule.evolution-plugin/3", versions)
        self.assertIn("cymule.ir/3", versions)
        self.assertIn("cymule.live-evolution-control/6", versions)
        self.assertIn("cymule.plugin/3", versions)
        self.assertIn("cymule.resource-handoff/5", versions)
        self.assertIn("cymule.resource-locators/2", versions)
        self.assertIn("cymule.resource-catalog-record/2", versions)
        self.assertIn("cymule.version-domain-registry/3", versions)
        self.assertIn("cymule.machine-base/4", versions)
        self.assertIn("cymule.machine-delta/6", versions)
        self.assertIn("cymule.machine-snapshot/11", versions)
        self.assertIn("cymule.command/6", versions)
        self.assertIn("cymule.event/8", versions)
        self.assertIn("cymule.command-admission/3", versions)
        self.assertIn("cymule.directory-store/5", versions)
        self.assertIn("cymule.sqlite-store/6", versions)
        self.assertTrue(
            {
                "cymule.activation-http-spool/2",
                "cymule.activation-timer-store/3",
                "cymule.framework-resource-handle/4",
                "cymule.resource-lifecycle-receipt-ref/3",
                "cymule.resource-pin-current/2",
                "cymule.resource/4",
                "cymule.run-query-indexes/3",
            }.issubset(versions)
        )
        rebuild_only = {
            "mode": "unsupported",
            "edge": None,
            "runbook": "docs/migrations/pre-segmented-store-generations.md",
        }
        activation_hard_cuts = {
            "cymule.activation-http-spool/2": "docs/migrations/activation-http-spool-generation-2.md",
            "cymule.activation-timer-store/3": "docs/migrations/activation-timer-store-generation-3.md",
        }
        for version, runbook in activation_hard_cuts.items():
            self.assertEqual(by_version[version]["compatibility_mode"], "exact-reject")
            self.assertEqual(
                by_version[version]["migration"],
                {"mode": "unsupported", "edge": None, "runbook": runbook},
            )
        for version in (
            "cymule.agent-command-receipt/3",
            "cymule.agent-command/3",
            "cymule.agent/7",
            "cymule.durable-state-root/4",
            "cymule.durable-state-value/4",
            "cymule.resource-lifecycle-receipt-ref/3",
            "cymule.resource-pin-current/2",
            "cymule.run-query-indexes/3",
        ):
            self.assertEqual(by_version[version]["migration"], rebuild_only)
        for removed in (
            "cymule.virtual-checkpoint/4",
            "cymule.virtual-journal-base/2",
            "cymule.virtual-journal-base-certificate/2",
        ):
            self.assertNotIn(removed, versions)
        self.assertIn("cymule.virtual-claim-control/4", versions)
        self.assertIn("cymule.virtual-lease-renewal-control/2", versions)
        self.assertIn("cymule.virtual-recovery-control/2", versions)
        self.assertIn("cymule.virtual-work-control/2", versions)
        self.assertIn("cymule.wait-activation-receipt/3", versions)
        self.assertIn("cymule.wait-activation/2", versions)
        self.assertTrue(
            {
                "cymule.agent-stream/1",
                "cymule.agent-stream/2",
                "cymule.agent-update/1",
                "cymule.agent-host-occurrence/2",
                "cymule.agent-occurrence-journal-base/1",
                "cymule.agent-session-journal-base/1",
                "cymule.agent/1",
                "cymule.agent/3",
                "cymule.agent/6",
                "cymule.agent-command/2",
                "cymule.agent-command-receipt/2",
                "cymule.activation-http-spool/1",
                "cymule.activation-timer-store/1",
                "cymule.activation-timer-store/2",
                "cymule.canary/1",
                "cymule.coupled-checkpoint-receipt/2",
                "cymule.command/5",
                "cymule.command-admission/2",
                "cymule.durable-control/2",
                "cymule.durable-control/3",
                "cymule.durable-revision/2",
                "cymule.durable-state-root/2",
                "cymule.durable-state-root/3",
                "cymule.durable-state-value/2",
                "cymule.durable-state-value/3",
                "cymule.durable-state/3",
                "cymule.durable-state/4",
                "cymule.durable-state/5",
                "cymule.engine/2",
                "cymule.engine/3",
                "cymule.event/7",
                "cymule.evolution-control/4",
                "cymule.evolution-plugin/2",
                "cymule.ir/2",
                "cymule.live-evolution-checkpoint/4",
                "cymule.live-evolution-control/4",
                "cymule.live-evolution-control/5",
                "cymule.machine-base/2",
                "cymule.machine-delta/2",
                "cymule.machine-delta/4",
                "cymule.machine-prefix/2",
                "cymule.machine-snapshot/7",
                "cymule.machine-snapshot/9",
                "cymule.plugin/2",
                "cymule.framework-resource-handle/3",
                "cymule.resource-handoff/3",
                "cymule.resource-handoff/4",
                "cymule.resource-lifecycle-receipt-ref/2",
                "cymule.resource-lifecycle/1",
                "cymule.resource-locators/1",
                "cymule.resource-catalog-record/1",
                "cymule.resource-pin-current/1",
                "cymule.resource/3",
                "cymule.run-query-indexes/2",
                "cymule.version-domain-registry/1",
                "cymule.version-domain-registry/2",
                "cymule.directory-store/2",
                "cymule.directory-store/4",
                "cymule.sqlite-store/3",
                "cymule.sqlite-store/5",
                "cymule.virtual-checkpoint/3",
                "cymule.virtual-claim-control/3",
                "cymule.virtual-lease-renewal-control/1",
                "cymule.virtual-recovery-control/1",
                "cymule.virtual-work-control/1",
                "cymule.wait-activation-receipt/2",
                "cymule.wait-activation/1",
            }.isdisjoint(versions)
        )

    def test_release_bom_binds_source_registry_schemas_and_packages(self) -> None:
        source_sha = "a" * 40
        public_source_sha = "b" * 40
        catalog = version_domains.release_catalog()
        bom = version_domains.build_bom(
            self.registry,
            source_sha,
            public_source_sha,
            publication_fixture(catalog, public_source_sha),
        )
        for invalid_public_source in (None, source_sha):
            with self.subTest(
                invalid_public_source=invalid_public_source
            ), self.assertRaisesRegex(ValueError, "distinct exact rewritten"):
                version_domains.build_bom(
                    self.registry,
                    source_sha,
                    invalid_public_source,
                    publication_fixture(catalog, source_sha),
                )
        self.assertEqual(bom["bom_version"], "cymule.release-bom/3")
        self.assertNotIn("controller_sha", bom)
        with self.assertRaisesRegex(ValueError, "open or incomplete top-level shape"):
            version_domains.validate_release_bom_projection(
                {**bom, "controller_sha": "c" * 40},
                registry=self.registry,
                source_sha=source_sha,
                public_source_sha=public_source_sha,
                catalog=catalog,
            )
        self.assertEqual(
            bom["version_domain_registry_digest"],
            version_domains.digest_json(self.registry),
        )
        self.assertTrue(bom["schemas"])
        schema_paths = {schema["path"] for schema in bom["schemas"]}
        self.assertIn("schemas/sealed-plan.schema.json", schema_paths)
        self.assertIn("schemas/wait-condition.schema.json", schema_paths)
        self.assertTrue(bom["packages"])
        by_package = {record["package_id"]: record for record in bom["packages"]}
        self.assertEqual(
            [record["package_id"] for record in bom["packages"]],
            sorted(by_package),
        )
        self.assertEqual(by_package["cargo:cymule"]["publication"]["kind"], "cargo")
        self.assertEqual(by_package["npm:cymule"]["publication"]["kind"], "npm")
        self.assertIsNone(by_package["python:cymule"]["publication"])
        self.assertIsNone(
            by_package["go:github.com/cymule-framework/cymule/sdk/go"][
                "publication"
            ]
        )
        self.assertEqual(
            [domain["version"] for domain in bom["domains"]],
            sorted(domain["version"] for domain in self.registry["domains"]),
        )

        swapped = publication_fixture(catalog, public_source_sha)
        cargo_cymule = next(
            item for item in swapped if item["package_id"] == "cargo:cymule"
        )
        npm_cymule = next(
            item for item in swapped if item["package_id"] == "npm:cymule"
        )
        cargo_cymule["publication"], npm_cymule["publication"] = (
            npm_cymule["publication"],
            cargo_cymule["publication"],
        )
        with self.assertRaisesRegex(ValueError, "ecosystem"):
            version_domains.build_bom(
                self.registry,
                source_sha,
                public_source_sha,
                swapped,
            )
        with self.assertRaisesRegex(ValueError, "canonical package_id order"):
            version_domains.validate_bom_package_order(
                [bom["packages"][1], bom["packages"][0], *bom["packages"][2:]]
            )
        with self.assertRaisesRegex(ValueError, "canonical package_id order"):
            version_domains.validate_bom_package_order(
                [*bom["packages"], bom["packages"][0]]
            )

        projection_mutations = []
        for field in (
            "release_generation",
            "version_domain_registry_digest",
            "schemas",
            "domains",
            "migration_edges",
        ):
            malformed = copy.deepcopy(bom)
            if isinstance(malformed[field], list):
                malformed[field] = [*malformed[field], "forged"]
            else:
                malformed[field] = "forged"
            projection_mutations.append((field, malformed))
        for field, malformed in projection_mutations:
            with self.subTest(field=field), self.assertRaisesRegex(
                ValueError, f"release BOM {field} is not the complete registry projection"
            ):
                version_domains.validate_release_bom(
                    malformed,
                    registry=self.registry,
                    source_sha=source_sha,
                    public_source_sha=public_source_sha,
                    catalog=catalog,
                )

        missing_package = copy.deepcopy(bom)
        missing_package["packages"].pop()
        with self.assertRaisesRegex(ValueError, "complete exact package catalog"):
            version_domains.validate_release_bom(
                missing_package,
                registry=self.registry,
                source_sha=source_sha,
                public_source_sha=public_source_sha,
                catalog=catalog,
            )

        wrong_manifest = copy.deepcopy(bom)
        wrong_manifest["packages"][0]["manifest_digest"] = "sha256:" + "0" * 64
        with self.assertRaisesRegex(ValueError, "complete exact package catalog"):
            version_domains.validate_release_bom(
                wrong_manifest,
                registry=self.registry,
                source_sha=source_sha,
                public_source_sha=public_source_sha,
                catalog=catalog,
            )
        with self.assertRaisesRegex(ValueError, "differs from workspace authority"):
            version_domains.validate_release_bom(
                bom,
                registry=self.registry,
                source_sha=source_sha,
                public_source_sha=public_source_sha,
                catalog=catalog[:-1],
            )

        malformed_registry = copy.deepcopy(self.registry)
        malformed_registry["domains"][0]["depends_on"] = ["cymule.missing/1"]
        with self.assertRaisesRegex(ValueError, "dependency closure is missing"):
            version_domains.validate_release_bom(
                bom,
                registry=malformed_registry,
                source_sha=source_sha,
                public_source_sha=public_source_sha,
                catalog=catalog,
            )

        malformed_registry = copy.deepcopy(self.registry)
        schema_domain = next(domain for domain in malformed_registry["domains"] if domain["schemas"])
        schema_domain["schemas"][0]["canonical_digest"] = "sha256:" + "0" * 64
        with self.assertRaisesRegex(ValueError, "release schema authority drifted"):
            version_domains.validate_release_bom(
                bom,
                registry=malformed_registry,
                source_sha=source_sha,
                public_source_sha=public_source_sha,
                catalog=catalog,
            )

        malformed_registry = copy.deepcopy(self.registry)
        migration_domain = next(
            domain
            for domain in malformed_registry["domains"]
            if version_domains.effective(
                domain, malformed_registry["defaults"], "migration"
            )["runbook"]
            is not None
        )
        migration_domain["migration"] = {
            **version_domains.effective(
                migration_domain, malformed_registry["defaults"], "migration"
            ),
            "runbook": "docs/migrations/missing-release-runbook.md",
        }
        with self.assertRaisesRegex(ValueError, "release migration runbook is missing"):
            version_domains.validate_release_bom(
                bom,
                registry=malformed_registry,
                source_sha=source_sha,
                public_source_sha=public_source_sha,
                catalog=catalog,
            )

    def test_bom_interface_has_no_mutable_controller_identity_or_alias(self) -> None:
        for function in (
            version_domains.build_bom,
            version_domains.validate_release_bom,
            version_domains.validate_release_bom_projection,
        ):
            parameters = inspect.signature(function).parameters
            self.assertNotIn("controller_sha", parameters)
            self.assertFalse(
                any(
                    parameter.kind == inspect.Parameter.VAR_KEYWORD
                    for parameter in parameters.values()
                )
            )
        process = subprocess.run(
            [
                os.sys.executable,
                str(ROOT / "scripts/version_domains.py"),
                "bom",
                "--source-sha", "a" * 40,
                "--public-source-sha", "b" * 40,
                "--publications", "/unused-publications.json",
                "--controller-sha", "b" * 40,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(process.returncode, 2)
        self.assertIn("unrecognized arguments: --controller-sha", process.stderr)


if __name__ == "__main__":
    unittest.main()
