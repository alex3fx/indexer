#!/usr/bin/env bash
set -eu

require_env() {
  name="$1"
  eval "value=\${$name:-}"
  if [ -z "$value" ]; then
    echo "sync.sh: missing required env $name" >&2
    exit 1
  fi
}

require_env EVM_CHAIN_ID
require_env SCYLLA_DB_HOST
require_env SCYLLA_DB_PORT
require_env SCYLLA_DB_USERNAME
require_env SCYLLA_DB_PASSWORD

case "$EVM_CHAIN_ID" in
  ''|*[!0-9]*)
    echo "sync.sh: EVM_CHAIN_ID must be a positive integer" >&2
    exit 1
    ;;
esac

case "$EVM_CHAIN_ID" in
  *[1-9]*) ;;
  *)
    echo "sync.sh: EVM_CHAIN_ID must be greater than or equal to 1" >&2
    exit 1
    ;;
esac

case "$SCYLLA_DB_PORT" in
  ''|*[!0-9]*)
    echo "sync.sh: SCYLLA_DB_PORT must be an integer from 0 to 65535" >&2
    exit 1
    ;;
esac

if [ "$SCYLLA_DB_PORT" -gt 65535 ]; then
  echo "sync.sh: SCYLLA_DB_PORT must be an integer from 0 to 65535" >&2
  exit 1
fi

keyspace="${SCYLLA_DB_KEYSPACE:-i${EVM_CHAIN_ID}}"
init_query="CREATE KEYSPACE IF NOT EXISTS \"$keyspace\" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"

cqlsh_args=(
  "$SCYLLA_DB_HOST"
  "$SCYLLA_DB_PORT"
  -u "$SCYLLA_DB_USERNAME"
  -p "$SCYLLA_DB_PASSWORD"
)

models=(
  "db/models/minified/blocks.cql"
  "db/models/minified/transactions.cql"
  "db/models/minified/logs.cql"
  "db/models/minified/internal_transactions.cql"
  "db/models/minified/contracts.cql"
  "db/models/minified/block_completions.cql"
  "db/models/lookups/contracts.cql"
  "db/models/lookups/erc20_tokens.cql"
  "db/models/lookups/erc20_total_supplies.cql"
  "db/models/lookups/erc20_owners.cql"
  "db/models/lookups/erc20_self_destructed.cql"
  "db/models/lookups/bytecode_store.cql"
  "db/models/lookups/contracts_by_bytecode_hash.cql"
  "db/models/lookups/bytecode_collision_registry.cql"
)

echo "sync.sh: initializing keyspace $keyspace"
cqlsh "${cqlsh_args[@]}" -e "$init_query"

for model in "${models[@]}"; do
  if [ ! -f "$model" ]; then
    echo "sync.sh: model file not found: $model" >&2
    exit 1
  fi

  echo "sync.sh: applying $model to keyspace $keyspace"
  cqlsh "${cqlsh_args[@]}" -k "$keyspace" -f "$model"
done
