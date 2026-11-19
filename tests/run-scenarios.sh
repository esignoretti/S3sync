#!/usr/bin/env bash
# Cross-compile the scenario suite, ship it to a Linux host, and run it there.
#
#   ./tests/run-scenarios.sh [tier] [host]
#
# tier: fast (behaviour only, minutes), full (default; adds 5 GiB compositions),
#       all (adds the 6 GiB and 500-object scenarios).
# host: any SSH destination with ~10 GB of free disk. Defaults to $SCENARIO_HOST.
#
# Credentials come from the caller's environment and are passed inline over SSH;
# they are never written to a file on either side.
set -euo pipefail

# Keepalives: a silent network drop otherwise leaves the local ssh waiting
# forever while the remote test process is already gone, so the run looks hung
# instead of failing.
ssh_opts=(-o ServerAliveInterval=30 -o ServerAliveCountMax=6 -o ConnectTimeout=30)

tier="${1:-full}"
host="${2:-${SCENARIO_HOST:-}}"

if [[ -z "$host" ]]; then
  echo "usage: $0 [fast|full|all] <user@host>" >&2
  echo "  (or set SCENARIO_HOST)" >&2
  exit 2
fi
for v in S3_TEST_ENDPOINT S3_TEST_ACCESS_KEY S3_TEST_SECRET_KEY; do
  if [[ -z "${!v:-}" ]]; then
    echo "missing $v in the environment" >&2
    exit 2
  fi
done

cd "$(dirname "$0")/.."
test_bin=/tmp/scenario-linux
cli_bin=/tmp/s3sync-linux
log=/tmp/scenario-$(date +%Y%m%d-%H%M%S).log

echo "==> cross-compiling (GOOS=linux GOARCH=amd64)"
GOOS=linux GOARCH=amd64 go test -c -tags=integration -o "$test_bin" ./tests/scenario/
GOOS=linux GOARCH=amd64 go build -o "$cli_bin" ./

echo "==> shipping to $host"
# Remove the previous copies first: scp cannot overwrite a binary that an
# earlier, still-running invocation is executing ("text file busy").
ssh "${ssh_opts[@]}" "$host" "rm -f /root/scenario-linux /root/s3sync-linux"
scp -q "${ssh_opts[@]}" "$test_bin" "$cli_bin" "$host":/root/

echo "==> running tier=$tier on $host (log: $log)"
ssh "${ssh_opts[@]}" "$host" \
  "SCENARIO_TIER='$tier' SCENARIO_CLI_BIN=/root/s3sync-linux \
   S3_TEST_ENDPOINT='$S3_TEST_ENDPOINT' \
   S3_TEST_ACCESS_KEY='$S3_TEST_ACCESS_KEY' \
   S3_TEST_SECRET_KEY='$S3_TEST_SECRET_KEY' \
   /root/scenario-linux -test.v -test.timeout 60m" | tee "$log"

echo "==> done; log at $log"
