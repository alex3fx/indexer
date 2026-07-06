# ETH ERC-20 Indexer — Data Description

## Overview

ETH ERC-20 indexer on branch `alex_erc20`. Indexes Ethereum mainnet (chain_id=1), blocks 0 → HEAD.

Collects:
- All deployed contracts with bytecode, deployer, factory, tx metadata
- ERC-20 token detection via Transfer/Approval log patterns + multicall3 name/symbol/decimals resolution
- Deduplicated bytecode storage with Bloom-filter deduplication

Reference detection logic: `~/lotos/task1/try_discover_erc_twenty_tokens.ts`

---

## Scylla Keyspace: `eth`

### Core contract tables

#### `contracts_by_address_v2`
Primary lookup for deployed contracts.

| Column | Type | Semantics |
|--------|------|-----------|
| `address` | text | Contract address (hex, lowercase) |
| `block_number` | bigint | Block in which the contract was deployed |
| `deployer` | text | **EOA** — `tx.from_address` (the outer tx sender). For factory deploys: the wallet that called the factory, NOT the factory itself |
| `contract_factory` | text | Factory contract address if the CREATE was issued from a contract; NULL for direct EOA deploys |
| `tx_hash` | text | Transaction hash that triggered the deploy |
| `bytecode_hash` | blob | sha256 of deployed bytecode |
| `bytecode_seq` | tinyint | Collision-resolution seq (0 = no collision) |
| `creation_hash` | blob | sha256 of creation (init) bytecode |
| `creation_seq` | tinyint | As above for creation bytecode |
| `block_timestamp_s` | bigint | Block timestamp (seconds) |
| `block_timestamp_ms` | bigint | Block timestamp (milliseconds) |
| `trace_index` | int | Index within block trace array |
| `transaction_index` | int | Transaction index in block |
| `creation_method` | tinyint | 0=CREATE, 1=CREATE2, 255=UNKNOWN |

**PK:** `(address, block_number DESC)` — allows multiple deployments of the same address.

**Deployer semantics** (from `historical.ts`, authoritative):
```typescript
const creator = tx.from_address;                           // always EOA
const contractFactory = creator === from ? null : from;    // factory if present
```
Where `from = trace.action.from`. This is the canonical definition of `deployer` and `contract_factory`.

#### `bytecode_store`
Deduplicated content-addressed bytecode storage.

| Column | Semantics |
|--------|-----------|
| `hash` (blob) | sha256 of bytecode |
| `seq` (tinyint) | 0 normally; >0 only for sha256 collisions |
| `bytecode` (blob) | Raw bytecode bytes |
| `check_hash` (blob) | keccak256, secondary integrity check |
| `size` (int) | Bytecode size in bytes |
| `kind` (tinyint) | 0=deployed, 1=creation |

**PK:** `(hash, seq)`

#### `addresses_by_bytecode`
Reverse index: bytecode → all contracts using it.

| Column | Semantics |
|--------|-----------|
| `hash` | sha256 of deployed bytecode |
| `seq` | Collision seq |
| `bucket` | `address[0]` — first byte of contract address (shard spread) |
| `address` | Contract address |

**PK:** `(hash, seq, bucket, address)`

---

### ERC-20 tables

#### `erc20_tokens`
One row per ERC-20 compliant contract.

| Column | Source |
|--------|--------|
| `chain_id` | 1 (ETH mainnet) |
| `address` | Contract address |
| `name` | `name()` call at creation block |
| `symbol` | `symbol()` call at creation block |
| `decimals` | `decimals()` call at creation block |
| `has_balance_of` | selector `70a08231` found in deployed bytecode at creation |
| `has_transfer` | selector `a9059cbb` found in deployed bytecode at creation |
| `has_transfer_from` | selector `23b872dd` found in deployed bytecode at creation |
| `has_approve` | selector `095ea7b3` found in deployed bytecode at creation |
| `has_allowance` | selector `dd62ed3e` found in deployed bytecode at creation |
| `is_standard_decimals` | decimals() result ∈ [0, 255] (from multicall3 at creation block) |
| `is_fully_following_standard` | name+symbol+standardDecimals+totalSupply+all 5 selectors present |
| `is_minimally_following_standard` | all 5 selectors + standardDecimals, no metadata required |
| `is_partially_following_standard` | at least one of the above true |
| `is_not_following_standard` | none of name/symbol/decimals/totalSupply/selectors detected |
| `detection_version` | version of detection logic used (for future re-runs) |

**Note:** The Zig indexer detects ERC-20 functions by scanning deployed bytecode for 4-byte selectors at CREATE time (static analysis). The TypeScript reference script `try_discover_erc_twenty_tokens.ts` uses dynamic `multiSimulate` calls at HEAD block instead. Both methods probe the same selectors; static scanning may detect functions that are no longer accessible after a self-destruct or upgrade, while simulation would miss them.

#### `erc20_total_supplies`
Tracks total supply evolution.

| Column | Semantics |
|--------|-----------|
| `chain_id`, `address` | Token identifier |
| `initial_total_supply` | totalSupply() at creation block |
| `latest_total_supply` | totalSupply() at latest known block |
| `updated_at_block` | Block when latest was read |
| `updated_at_timestamp` | Timestamp of that block |

#### `erc20_owners`
Tracks OZ Ownable ownership.

| Column | Semantics |
|--------|-----------|
| `chain_id`, `address` | Token identifier |
| `initial_owner` | owner() at creation block |
| `latest_owner` | owner() at latest known block |
| `is_ownership_renounced` | owner is a known burn address |
| `updated_at_block`, `updated_at_timestamp` | Recency markers |

#### `erc20_self_destructed`
Records self-destructed ERC-20 contracts.

| Column | Semantics |
|--------|-----------|
| `chain_id`, `address` | Token identifier |
| `at_block` | Block where self-destruct detected |
| `at_timestamp` | Timestamp of that block |

---

## Mapping to `try_discover_erc_twenty_tokens.ts`

### Script inputs — sourced from DB

| Script field | DB source |
|-------------|-----------|
| `address` | `contracts_by_address_v2.address` |
| `chainId` | constant (1) |
| `createdAtBlockNumber` | `contracts_by_address_v2.block_number` |
| `deployedBytecode` | hex string resolved via `bytecode_store` using `bytecode_hash + bytecode_seq` |
| `lwTrace` | pass-through; caller encodes `deployer` + `contract_factory` from `contracts_by_address_v2` |

### Script outputs — where stored

| Output field | DB table / column | Stored? |
|-------------|-------------------|---------|
| `name` | `erc20_tokens.name` | ✓ |
| `symbol` | `erc20_tokens.symbol` | ✓ |
| `decimals` | `erc20_tokens.decimals` | ✓ |
| `initialTotalSupply` | `erc20_total_supplies.initial_total_supply` | ✓ |
| `initialOwnerAddress` | `erc20_owners.initial_owner` | ✓ |
| `isOwnershipRenounced` | `erc20_owners.is_ownership_renounced` | ✓ |
| `hasBalanceOf` | `erc20_tokens.has_balance_of` | ✓ |
| `hasTransfer` | `erc20_tokens.has_transfer` | ✓ |
| `hasTransferFrom` | `erc20_tokens.has_transfer_from` | ✓ |
| `hasApprove` | `erc20_tokens.has_approve` | ✓ |
| `hasAllowance` | `erc20_tokens.has_allowance` | ✓ |
| `isStandardDecimals` | `erc20_tokens.is_standard_decimals` | ✓ |
| `isFullyFollowingStandard` | `erc20_tokens.is_fully_following_standard` | ✓ |
| `isMinimallyFollowingStandard` | `erc20_tokens.is_minimally_following_standard` | ✓ |
| `isPartiallyFollowingStandard` | `erc20_tokens.is_partially_following_standard` | ✓ |
| `isNotFollowingStandard` | `erc20_tokens.is_not_following_standard` | ✓ |
| `isSelfDestructed` | `erc20_self_destructed` (row inserted if true) | ✓ |
| `historicalBlockNumber` | same as `contracts_by_address_v2.block_number` (no separate storage) | ✓ |
| `lwTrace` | pass-through — not stored; caller uses it downstream | — |
| `isProxy` | **not stored** — computed at processing time from live vs creation bytecode | ✗ |
| `processedAtBlockNumber` | **not stored** — HEAD block number at time of ERC-20 scan | ✗ |
| `latestDeployedBytecodeHash` | **not stored** — md5 of current bytecode at HEAD | ✗ |

**`isSelfDestructed` logic** (from script line 283):
```typescript
deployedBytecode === '0x' || currentBytecode === '0x' ||
deployedBytecode === null || currentBytecode === null
```
Both creation-time (`deployedBytecode`) AND current HEAD bytecode (`currentBytecode`) must be non-null non-empty to be considered alive. `currentBytecode` is fetched via RPC at HEAD, not from DB.

**`isProxy` logic** (line 284):
```typescript
deployedBytecode?.length !== currentBytecode?.length && deployedBytecode !== currentBytecode
```
Computed but not persisted. Re-computable from live RPC if needed.

---

## Backfill Status (as of 2026-07-06)

| Tool | Scope | Deployer written | contract_factory | Status |
|------|-------|-----------------|-----------------|--------|
| `backfill_deployer_from_snap` (run 1) | All 101.6M rows | EOA ✓ | - | Done |
| `backfill_deployer_v2` (deployer_main) | Blocks 0→25.4M | Factory ✗ | - | **Killed** — wrong semantics |
| `backfill_deployer_from_snap` (run 2, 05:10 Jul 5) | All 101.6M rows | EOA ✓ (corrected) | - | Done |
| `backfill_deployer_v3` | Blocks 0→25.4M, direct creates only | EOA ✓ | - | Done (dbErr=0) |
| `backfill_deployer_targeted` | Null-deployer chunks | Factory ✗ | - | **Killed** — wrong semantics |
| **`backfill_restore_from_snap`** | All rows in old snap table (~101.6M) | EOA ✓ | Factory ✓ | **Running** (~38 min ETA) |
| **`backfill_deployer_v4`** | Null rows after restore | EOA ✓ | Factory ✓ | **Pending** — run after restore |

### After backfill_restore_from_snap + backfill_deployer_v4 complete:
- `deployer` = EOA (`tx.from_address`) for all rows ✓
- `contract_factory` = factory address (when different from EOA) for all rows ✓
- Both fields populated consistent with `historical.ts` and `models.d.ts` definitions

---

## Infrastructure

| Component | Location |
|-----------|----------|
| Test server | `100.64.0.4` |
| ETH node (0→12.7M) | `http://100.64.0.7:8545` (RETH archive) |
| ETH node (12.7M→HEAD) | `http://100.64.0.60:8545` (RETH archive) |
| Scylla | `127.0.0.1:9042`, keyspace=`eth` |
| Bytecode API | `~/bytecode_api_v2` on `:8080` |
| Live indexer | `~/raw_erc20_v19`, tmux `eth60`, from block 25459261 |
| Binary naming | `raw_erc20_vN` — increment N on each deploy |
