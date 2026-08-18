"""Unit tests for fail-closed crates.io publication recovery."""

from __future__ import annotations

import datetime
import importlib.util
import pathlib
import sys
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "crates_release", ROOT / "scripts" / "crates_release.py"
)
assert SPEC is not None and SPEC.loader is not None
crates_release = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = crates_release
SPEC.loader.exec_module(crates_release)


class NewCrateRateLimitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.now = datetime.datetime(
            2026, 8, 18, 16, 0, 0, tzinfo=datetime.timezone.utc
        ).timestamp()

    def test_explicit_new_crate_limit_uses_server_time_plus_grace(self) -> None:
        output = (
            "status 429 Too Many Requests: "
            "You have published too many new crates in a short period of time. "
            "Please try again after Tue, 18 Aug 2026 16:02:13 GMT"
        )
        self.assertEqual(
            crates_release.new_crate_rate_limit_delay(output, now=self.now), 138
        )

    def test_unrelated_failure_is_never_retried(self) -> None:
        self.assertIsNone(
            crates_release.new_crate_rate_limit_delay(
                "status 403 Forbidden: token is invalid", now=self.now
            )
        )

    def test_rate_limit_without_server_timestamp_fails_closed(self) -> None:
        output = (
            "status 429 Too Many Requests: "
            "You have published too many new crates in a short period of time."
        )
        with self.assertRaisesRegex(ValueError, "omitted a retry timestamp"):
            crates_release.new_crate_rate_limit_delay(output, now=self.now)

    def test_excessive_server_delay_fails_closed(self) -> None:
        output = (
            "status 429 Too Many Requests: "
            "You have published too many new crates in a short period of time. "
            "Please try again after Tue, 18 Aug 2026 16:30:00 GMT"
        )
        with self.assertRaisesRegex(ValueError, "excessive"):
            crates_release.new_crate_rate_limit_delay(output, now=self.now)


if __name__ == "__main__":
    unittest.main()
