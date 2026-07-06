# ZIG_BUGS.md — Known gaps and issues in the Zig ERC-20 indexer

Reference: `~/lotos/task1/try_discover_erc_twenty_tokens.ts`  
Canonical semantics: `~/lotos/task1/mocknode/src/entrypoints/collecting/historical.ts`

These are documented gaps between what the Zig indexer collects and what the reference TypeScript
script would produce. **Zig code is not changed** — this file is a record for future work.

---

## 1. `isProxy` — not tracked

**Script:** line 284 — detects proxy by comparing creation-time `deployedBytecode` with
`currentBytecode` (fetched from RPC at HEAD):
```typescript
const isProxy = deployedBytecode?.length !== currentBytecode?.length && deployedBytecode !== currentBytecode
```

**Zig:** no concept of `isProxy` anywhere. No column in `erc20_tokens` or any other table.

**Impact:** proxy tokens (e.g., OpenZeppelin TransparentUpgradeableProxy, UUPS) are not flagged.
The consumer cannot distinguish proxy vs non-proxy from DB alone — must call RPC to compare bytecodes.

---

## 2. ERC-20 function detection: static bytecode scan vs live simulation

**Script:** uses `ViemExtension.multiSimulate` at HEAD block — actual function calls that return
`isExisting` based on whether the call succeeded. Detects whether the function is *callable now*.

**Zig:** (`src/pipeline/transformer.zig`) scans deployed bytecode for 4-byte selectors at CREATE
time. Detects whether the selector *appears in deployed bytecode*.

**Divergence cases:**
- **Proxy contracts:** selectors may not be in deployed bytecode (they're in the implementation
  contract). Zig detects `has_*=false`; script would detect `has_*=true` via simulation.
- **Self-destructed contracts:** selectors still in bytecode after SELFDESTRUCT.
  Zig: `has_*=true`; script simulation at HEAD: functions not callable → `has_*=false`.
- **Contracts that always revert:** selector in bytecode but call always fails.
  Zig: `has_*=true`; script simulation: `has_*=false` (isExisting = false on revert).
- **Delegating dispatch without selectors:** some contracts use inline assembly dispatch
  without literal 4-byte selectors. Zig misses these; script simulation would detect them.

**Column affected:** `erc20_tokens.has_balance_of`, `has_transfer`, `has_transfer_from`,
`has_approve`, `has_allowance`.

---

## 3. `initial_total_supply` stored as raw uint256, not human-readable

**Script:** `formatUnits(rawInitialTotalSupply, decimals)` → e.g., `"1.0"` for 1e18 with 18 decimals.

**Zig:** (`src/rpc/multicall.zig` line 34) stores `totalSupply` as `"raw uint256 as decimal string"`
→ e.g., `"1000000000000000000"`.

**Column affected:** `erc20_total_supplies.initial_total_supply`, `latest_total_supply`.

**Impact:** consumers expecting formatted values (consistent with the reference script) will get
raw integers. Formatting must be done client-side using `decimals` from `erc20_tokens`.

---

## 4. `isSelfDestructed` — event-based vs state-based detection

**Script:** checks current state at HEAD:
```typescript
const isSelfDestructed = deployedBytecode === '0x' || currentBytecode === '0x' || ...
```

**Zig:** (`src/pipeline/erc20.zig` line 176) listens for `type=suicide` traces in real-time.
Only records the event if the address is **already in the bloom filter** at the time of the
SELFDESTRUCT trace.

**Gap:** If a contract self-destructs BEFORE the ERC-20 detection pipeline processes it
(e.g., very short-lived token in the same block or a nearby block), the SELFDESTRUCT trace
fires before the bloom filter is populated → missed → `erc20_self_destructed` has no row,
but bytecode IS '0x'. The script's state-based check would catch this.

**Another gap:** The bloom filter is loaded from Redis on startup. If a restart happens between
the ERC-20 detection and a later SELFDESTRUCT trace in a different block window, the entry may
not be in the bloom → missed.

---

## 5. `processedAtBlockNumber` — not stored

**Script output field:** the HEAD block number at which ERC-20 detection was run. Useful for
knowing how fresh the `has_*`, `initial_total_supply`, `initial_owner` values are.

**Zig:** not stored anywhere.

---

## 6. `latestDeployedBytecodeHash` — not stored

**Script output field:** `HasherExtension.md5(currentBytecode)` — md5 of bytecode at HEAD.

**Zig:** not stored. The DB has `bytecode_hash` (sha256 of creation-time deployed bytecode) in
`contracts_by_address_v2`, but not the current-HEAD bytecode hash.

---

## 7. `deployer` / `contract_factory` backfill status (data gap, not code bug)

As of 2026-07-06, `contracts_by_address_v2.deployer` and `contract_factory` are being restored
via `backfill_restore_from_snap` (from archived `contracts_by_addresses` snap table) + `backfill_deployer_v4`
(for re-deployed addresses not in snap).

**Previous corruption:** `backfill_deployer_v2` (deployer_main) and `backfill_deployer_targeted`
wrote `trace.action.from` as `deployer` instead of `tx.from_address`. Both processes were killed.
`backfill_restore_from_snap` (running) corrects this via last-write-wins semantics.

**Correct semantics** (from `historical.ts`):
```typescript
const creator = tx.from_address;                           // deployer = EOA always
const contractFactory = creator === from ? null : from;    // factory only when action.from ≠ EOA
```

---

## 8. `detectionVersion` increment — not wired to actual logic version

The `DETECTION_VERSION` constant in Zig marks which version of detection ran. If ERC-20 detection
logic changes (e.g., switching from selector scan to simulation), old rows won't be re-processed
unless an explicit re-scan tool is built. There is no automatic re-scan on version bump.

**No immediate action needed** — just a reminder that `detection_version` is only useful if
there's a re-scan pipeline that reads it.

---

## Summary table

| Gap | Severity | Workaround |
|-----|----------|-----------|
| `isProxy` not tracked | Medium | Call RPC to compare current vs creation bytecode |
| Static selector scan vs live simulation | Medium | Re-run detection via TypeScript script for suspicious tokens |
| `initial_total_supply` raw uint256 | Low | Format client-side using `decimals` column |
| `isSelfDestructed` event gap | Low | Rare edge case; check bytecode via RPC if needed |
| `processedAtBlockNumber` not stored | Low | Use `updated_at_block` from erc20_total_supplies as proxy |
| `latestDeployedBytecodeHash` not stored | Low | Compute from RPC if needed |
| `deployer`/`contract_factory` backfill | **In progress** | `backfill_restore_from_snap` + `backfill_deployer_v4` running |
