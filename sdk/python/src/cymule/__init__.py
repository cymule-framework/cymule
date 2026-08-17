"""Python authoring and engine client SDK for Cymule."""

from __future__ import annotations

import copy
import json
import subprocess
from pathlib import Path
from typing import Any

Json = None | bool | int | float | str | list["Json"] | dict[str, "Json"]


class FlowBuilder:
    """Small code-first builder for the frozen language-neutral IR."""

    def __init__(self, name: str, input_schema: Json, output_schema: Json) -> None:
        self._candidate: dict[str, Any] = {
            "ir_version": "cymule.ir/1",
            "name": name,
            "entry": "main",
            "components": [],
            "effects": [],
            "definitions": [
                {
                    "id": "main",
                    "input_schema": input_schema,
                    "output_schema": output_schema,
                    "body": {"steps": [], "result": {"kind": "literal", "value": None}},
                }
            ],
            "metadata": {},
        }

    def component(self, operation_id: str, input_schema: Json, output_schema: Json) -> FlowBuilder:
        self._candidate["components"].append(
            {
                "id": operation_id,
                "input_schema": input_schema,
                "output_schema": output_schema,
                "requirements": {},
            }
        )
        return self

    def effect_contract(
        self,
        operation_id: str,
        input_schema: Json,
        output_schema: Json,
        profile: dict[str, Json],
    ) -> FlowBuilder:
        self._candidate["effects"].append(
            {
                "id": operation_id,
                "input_schema": input_schema,
                "output_schema": output_schema,
                "profile": profile,
                "requirements": {},
            }
        )
        return self

    def call(self, site: str, component: str, expression: dict[str, Json], bind: str) -> FlowBuilder:
        self._steps().append(
            {"id": site, "op": "call", "component": component, "input": expression, "bind": bind}
        )
        return self

    def effect(
        self,
        site: str,
        effect: str,
        expression: dict[str, Json],
        occurrence: str,
    ) -> FlowBuilder:
        self._steps().append(
            {
                "id": site,
                "op": "effect",
                "effect": effect,
                "input": expression,
                "occurrence": occurrence,
            }
        )
        return self

    def wait(self, site: str, wait: dict[str, Json]) -> FlowBuilder:
        """Append a durable suspension boundary."""
        self._steps().append({"id": site, "op": "wait", "wait": wait})
        return self

    def scope(
        self,
        site: str,
        mode: str,
        body: dict[str, Json],
        bind: str,
    ) -> FlowBuilder:
        """Append a structured transactional or speculative scope."""
        self._steps().append(
            {"id": site, "op": "scope", "mode": mode, "body": body, "bind": bind}
        )
        return self

    def finish(self, result: dict[str, Json]) -> dict[str, Any]:
        self._entry()["body"]["result"] = result
        return copy.deepcopy(self._candidate)

    def _entry(self) -> dict[str, Any]:
        return self._candidate["definitions"][0]

    def _steps(self) -> list[dict[str, Any]]:
        return self._entry()["body"]["steps"]


class ResourceBuilder:
    """Provider-neutral resource candidates sealed only by the Rust engine."""

    @staticmethod
    def text(text: str, annotations: dict[str, str] | None = None) -> dict[str, Any]:
        return {
            "resource_version": "cymule.resource/1",
            "shape": "inline",
            "media_type": "text/plain;charset=utf-8",
            "inline": {"encoding": "utf8", "text": text},
            "integrity": {"kind": "inline"},
            "annotations": dict(annotations or {}),
        }

    @staticmethod
    def json(value: Json, annotations: dict[str, str] | None = None) -> dict[str, Any]:
        return {
            "resource_version": "cymule.resource/1",
            "shape": "inline",
            "media_type": "application/json",
            "inline": {"encoding": "json", "value": value},
            "integrity": {"kind": "inline"},
            "annotations": dict(annotations or {}),
        }

    @staticmethod
    def external(
        shape: str,
        media_type: str,
        integrity: dict[str, Json],
        locations: list[dict[str, str]],
        annotations: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        if shape == "inline":
            raise ValueError("external resource shape cannot be inline")
        return {
            "resource_version": "cymule.resource/1",
            "shape": shape,
            "media_type": media_type,
            "integrity": copy.deepcopy(integrity),
            "locations": copy.deepcopy(locations),
            "annotations": dict(annotations or {}),
        }

    @staticmethod
    def handoff(
        transfer_id: str,
        from_run: str,
        to_run: str,
        slot: str,
        resource: dict[str, Any],
    ) -> dict[str, Any]:
        """Create one M1 Run-to-Run resource handoff record."""
        return {
            "handoff_version": "cymule.resource-handoff/1",
            "transfer_id": transfer_id,
            "from_run": from_run,
            "to_run": to_run,
            "slot": slot,
            "resource": copy.deepcopy(resource),
        }


class CliEngine:
    """CLI-backed Engine transport."""

    def __init__(self, executable: str | Path) -> None:
        self.executable = str(executable)

    def seal(self, candidate: dict[str, Any]) -> dict[str, Any]:
        response = self._request({"type": "seal", "candidate": candidate})
        if response.get("type") != "sealed":
            raise RuntimeError(f"unexpected engine response: {response!r}")
        return response["plan"]

    def seal_resource(self, candidate: dict[str, Any]) -> dict[str, Any]:
        """Validate and seal a Resource Candidate with the Rust engine."""
        response = self._request({"type": "seal_resource", "candidate": candidate})
        if response.get("type") != "sealed_resource":
            raise RuntimeError(f"unexpected engine response: {response!r}")
        return response["resource"]

    def verify_agent_stream(self, records: list[dict[str, Any]]) -> dict[str, Any]:
        """Validate and reduce Agent stream records with the Rust engine."""
        response = self._request({"type": "verify_agent_stream", "records": records})
        if response.get("type") != "verified_agent_stream":
            raise RuntimeError(f"unexpected engine response: {response!r}")
        return response["stream"]

    def run(
        self,
        plan: dict[str, Any],
        input_value: Json,
        plugin: str | Path,
        run_id: str,
    ) -> dict[str, Any]:
        response = self._request(
            {
                "type": "run",
                "plan": plan,
                "input": input_value,
                "plugin": str(plugin),
                "run_id": run_id,
            }
        )
        if response.get("type") != "executed":
            raise RuntimeError(f"unexpected engine response: {response!r}")
        return response["result"]

    def _request(self, request: dict[str, Any]) -> dict[str, Any]:
        completed = subprocess.run(
            [self.executable, "rpc"],
            input=json.dumps(request, separators=(",", ":")),
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
        if completed.returncode != 0:
            raise RuntimeError(completed.stderr.strip() or f"engine exited {completed.returncode}")
        return json.loads(completed.stdout)


__all__ = ["CliEngine", "FlowBuilder", "Json", "ResourceBuilder"]
