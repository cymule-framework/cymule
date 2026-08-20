#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
CONSUMER=$(mktemp -d "${TMPDIR:-/tmp}/cymule-go-package.XXXXXX")
trap 'rm -rf "$CONSUMER"' EXIT HUP INT TERM

cd "$CONSUMER"
go mod init example.com/cymule-package-check
go mod edit -replace github.com/cymule-framework/cymule/sdk/go="$ROOT/sdk/go"
go mod edit -require=github.com/cymule-framework/cymule/sdk/go@v0.2.0
printf '%s\n' \
  'package packagecheck' \
  '' \
  'import cymule "github.com/cymule-framework/cymule/sdk/go"' \
  '' \
  'func Engine() cymule.CliEngine { return cymule.CliEngine{} }' \
  > package_test.go
go test ./...
