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


class CliEngine:
    """CLI-backed Engine transport."""

    def __init__(self, executable: str | Path) -> None:
        self.executable = str(executable)

    def seal(self, candidate: dict[str, Any]) -> dict[str, Any]:
        response = self._request({"type": "seal", "candidate": candidate})
        if response.get("type") != "sealed":
            raise RuntimeError(f"unexpected engine response: {response!r}")
        return response["plan"]

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


__all__ = ["CliEngine", "FlowBuilder", "Json"]
