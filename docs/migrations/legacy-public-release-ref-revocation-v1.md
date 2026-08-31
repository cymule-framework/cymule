# Legacy public release ref revocation

State: **executed 2026-08-31; terminal readback complete**

Owner: Cymule public-repository administrators

## Purpose

The first four internal-test GitHub Releases retained annotated tags whose
peeled histories still contained the retired private-source mirror controller.
The canonical public-history rewrite had already produced clean equivalents in
public `main`, but moving a stable tag would silently change both its raw tag
identity and release payload identity. The terminal migration therefore
revoked the legacy GitHub Release projections and deleted their exact tag refs.

This migration does not retract or mutate package-registry bytes. It retires
only the four GitHub Release/tag identities, and those version names must never
be recreated or reused.

## Frozen preflight

Before mutation, GitHub reported each Release as published, mutable, and with
zero assets. The exact ref inventory was:

| Tag | Release ID | Raw annotated-tag SHA | Peeled commit SHA |
| --- | ---: | --- | --- |
| `v0.1.1` | `372223881` | `4c2af8329b5fd73b0701d703f3b29dde130ae677` | `86d2073650aca55ca0dc3f25438b96692d80421c` |
| `v0.1.2` | `372262944` | `ae4308dee2a77790cd2c179141b884a2d13156a4` | `ab79baab88477e261f86011191b0b543ccc34de6` |
| `v0.1.3` | `372304190` | `d56332423b7680d2241b6bf6cd192e21474652e6` | `af4e7ed2a7a713e16131721087f22365dddc046f` |
| `v0.1.4` | `372504005` | `ad97e17481276f0207d58079618a675d82844257` | `49aa6baed706203a74386d329c81e1f2a8a4309b` |

The affected commits were unreachable from public `main`, the active pull
request head, and its merge ancestry. Only these four tag refs retained them.

## Stop conditions

Stop without mutation if any Release ID, asset count, raw tag SHA, peeled
commit SHA, public `main`, or active candidate differs from the frozen
preflight. Delete no tag unless all four exact ref leases can commit atomically.
Never move a legacy tag to its clean rewritten commit.

## Execution record

The terminal execution completed by `2026-08-31T18:43Z`:

1. Cancelled superseded public Required CI attempts so they could not project
   evidence from the pre-migration ref set or precede the committed execution
   record.
2. Deleted GitHub Release objects `372223881`, `372262944`, `372304190`, and
   `372504005` through the authenticated GitHub API.
3. Deleted `refs/tags/v0.1.1` through `refs/tags/v0.1.4` in one atomic Git push,
   with each deletion bound to the raw annotated-tag SHA listed above.

No credential, token, or API response body is retained in this record.

## Verification

Terminal readback proved:

- every Release lookup by the four tag names returns absent;
- `git ls-remote` returns neither raw nor peeled refs for the four tags;
- a fresh public clone followed by a complete ref/tag fetch contains none of
  the retired tags;
- `git log --all -- .github/workflows/mirror.yml` is empty;
- public `main` remained
  `83d77ec13c27252f02a1754bffb96d3a35335d5d`; and
- the reviewed candidate branch remained
  `457b345a18f90e42fc32b5b931214ad96aff364b` during the migration.

Required CI and final public-`main` promotion remain separate post-migration
gates; this execution record does not project either as complete.

## Rollback boundary

The original GitHub Release publication timestamps and identities cannot be
reconstructed exactly, so there is no lossless rollback. Do not recreate the
four tags or Release objects. A future public release must use a new canonical
version and the current immutable release workflow.
