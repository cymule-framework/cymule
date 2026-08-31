#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"
mkdir -p "$ROOT/.cache"

cargo run --quiet -p cymule-example-hello-world -- Ada > "$ROOT/.cache/hello-world-result.json"
python3 -c 'import json, pathlib, sys; result = json.loads(pathlib.Path(sys.argv[1]).read_text()); assert result["run_id"] == "run:hello-world"; assert result["value"] == {"message": "Hello, Ada!"}; assert len(result["effects"]) == 1' "$ROOT/.cache/hello-world-result.json"

cargo run --quiet -p cymule-example-hello-world -- Ada --unknown-once > "$ROOT/.cache/hello-world-unknown-result.json"
python3 -c 'import json, pathlib, sys; result = json.loads(pathlib.Path(sys.argv[1]).read_text()); assert result["value"] == {"message": "Hello, Ada!"}; assert len(result["effects"]) == 1' "$ROOT/.cache/hello-world-unknown-result.json"

cargo test --quiet --profile conformance \
  -p cymule-example-durable-evaluation-campaign -- --test-threads=1
