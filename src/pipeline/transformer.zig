// RPC response → DB row transformation.
// Uses arena allocator for all allocations — no per-string heap allocation.
// lowerInPlace: modifies arena-owned strings in-place, SIMD 16 bytes/iter.
const std = @import("std");

const types = @import("indexer/rpc").types;
const schema = @import("indexer/db").schema;
const Bloom = @import("bloom.zig").Bloom;

const RpcBlock = types.RpcBlock;
const RpcReceipt = types.RpcReceipt;
const RpcTrace = types.RpcTrace;

pub const BlockRow = schema.BlockRow;
pub const TxRow = schema.TxRow;
pub const LogRow = schema.LogRow;
pub const InternalTxRow = schema.InternalTxRow;
pub const ContractRow = schema.ContractRow;
pub const ContractByAddrRow = schema.ContractByAddrRow;
pub const Erc20Candidate = schema.Erc20Candidate;
pub const Entities = schema.Entities;

// ─── ERC-20 selector scan ───────────────────────────────────────────────────
// EIP-20 function selectors (keccak256(signature)[0..4]), lowercase hex, no "0x".
// Matched as a raw substring of the deployed bytecode — same heuristic as
// try_discover_erc_twenty_tokens.ts: a contract advertising these selectors in
// its runtime code is a detection *candidate*, confirmed later via Multicall3.
const SELECTOR_BALANCE_OF = "70a08231";
const SELECTOR_TRANSFER = "a9059cbb";
const SELECTOR_TRANSFER_FROM = "23b872dd";
const SELECTOR_APPROVE = "095ea7b3";
const SELECTOR_ALLOWANCE = "dd62ed3e";

// Case-insensitive substring search without mutating or copying `haystack`
// (which is a zero-copy slice into the raw RPC JSON buffer).
fn containsSelectorCI(haystack: []const u8, needle: []const u8) bool {
    if (haystack.len < needle.len) return false;
    var i: usize = 0;
    while (i + needle.len <= haystack.len) : (i += 1) {
        var matched = true;
        for (needle, 0..) |nc, j| {
            if (std.ascii.toLower(haystack[i + j]) != nc) {
                matched = false;
                break;
            }
        }
        if (matched) return true;
    }
    return false;
}

pub const Erc20SelectorFlags = struct {
    hasBalanceOf: bool,
    hasTransfer: bool,
    hasTransferFrom: bool,
    hasApprove: bool,
    hasAllowance: bool,

    pub fn any(self: Erc20SelectorFlags) bool {
        return self.hasBalanceOf or self.hasTransfer or self.hasTransferFrom or self.hasApprove or self.hasAllowance;
    }
};

/// Scans deployed bytecode for EIP-20 function selectors. Shared between the
/// CREATE-time candidate detection below and the --erc20-rescan maintenance
/// pass (erc20_rescan.zig), which re-fetches bytecode independently of trace data.
pub fn scanErc20Selectors(deployedBytecode: []const u8) Erc20SelectorFlags {
    return .{
        .hasBalanceOf = containsSelectorCI(deployedBytecode, SELECTOR_BALANCE_OF),
        .hasTransfer = containsSelectorCI(deployedBytecode, SELECTOR_TRANSFER),
        .hasTransferFrom = containsSelectorCI(deployedBytecode, SELECTOR_TRANSFER_FROM),
        .hasApprove = containsSelectorCI(deployedBytecode, SELECTOR_APPROVE),
        .hasAllowance = containsSelectorCI(deployedBytecode, SELECTOR_ALLOWANCE),
    };
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

// Invalid or missing numeric RPC values intentionally become 0.
pub fn hexToI64(s: []const u8) i64 {
    var p = s;
    if (p.len >= 2 and p[0] == '0' and (p[1] == 'x' or p[1] == 'X')) p = p[2..];
    if (p.len == 0) return 0;
    const u = std.fmt.parseInt(u64, p, 16) catch return 0;
    return @bitCast(u);
}

pub fn hexToI32(s: []const u8) i32 {
    return @intCast(@min(hexToI64(s), std.math.maxInt(i32)));
}

pub fn hexToI8(s: []const u8) i8 {
    return @intCast(hexToI64(s) & 0xFF);
}

fn creationMethodI8(s: ?[]const u8) i8 {
    const v = s orelse return 99;
    if (std.mem.eql(u8, v, "create")) return 0;
    if (std.mem.eql(u8, v, "create2")) return 1;
    return 99;
}

fn methodIdSlice(input: []const u8) []const u8 {
    return if (input.len >= 10) input[0..10] else input;
}

// Lowercase in-place via SIMD (16 bytes/iter). A-Z → a-z only. Zero allocation.
inline fn li(s: []const u8) []const u8 {
    const ptr = @constCast(s.ptr);
    var i: usize = 0;
    const V = @Vector(16, u8);
    const vA: V = @splat('A');
    const vZ: V = @splat('Z');
    const v32: V = @splat(@as(u8, 0x20));
    const v0: V = @splat(@as(u8, 0));
    while (i + 16 <= s.len) : (i += 16) {
        const v: V = ptr[i..][0..16].*;
        const isUpper = (v >= vA) & (v <= vZ);
        ptr[i..][0..16].* = v + @select(u8, isUpper, v32, v0);
    }
    while (i < s.len) : (i += 1) {
        ptr[i] = std.ascii.toLower(ptr[i]);
    }
    return s;
}

inline fn liOpt(s: ?[]const u8) []const u8 {
    return if (s) |v| li(v) else "";
}

// ─── Transform ────────────────────────────────────────────────────────────────

pub fn initEntities() Entities {
    return .{
        .blocks = .empty,
        .txs = .empty,
        .logs = .empty,
        .internalTxs = .empty,
        .contracts = .empty,
        .contractsByAddr = .empty,
        .erc20Candidates = .empty,
        .erc20Touches = .empty,
        .selfDestructEvents = .empty,
        .erc20Tokens = .empty,
        .erc20Supplies = .empty,
        .erc20Owners = .empty,
        .erc20SelfDestructs = .empty,
        .lastBlock = 0,
    };
}

/// Transform one block. arena must outlive Entities.
/// When remapMod > 0: chunk = block_number % remapMod (shard-aware, matches production REMAP_MOD).
/// Otherwise: chunk = block_number / chunkSize.
pub fn transformBlock(
    arena: std.mem.Allocator,
    block: RpcBlock,
    receipts: []const RpcReceipt,
    traces: []const RpcTrace,
    chunkSize: u64,
    bloom: *Bloom,
    ent: *Entities,
) !void {
    return transformBlockWithRemap(arena, block, receipts, traces, chunkSize, 0, bloom, ent);
}

pub fn transformBlockWithRemap(
    arena: std.mem.Allocator,
    block: RpcBlock,
    receipts: []const RpcReceipt,
    traces: []const RpcTrace,
    chunkSize: u64,
    remapMod: u64,
    bloom: *Bloom,
    ent: *Entities,
) !void {
    if (receipts.len != block.transactions.len) return error.IncompleteBlock;

    const number = hexToI64(block.number);
    const timestampS = hexToI64(block.timestamp);
    const timestampMs: i64 = if (block.milliTimestamp) |m| hexToI64(m) else timestampS * 1000;
    const chunk: i32 = if (remapMod > 0)
        @intCast(@mod(number, @as(i64, @intCast(remapMod))))
    else
        @intCast(@divFloor(number, @as(i64, @intCast(chunkSize))));

    try ent.blocks.append(arena, .{
        .chunk = chunk,
        .number = number,
        .timestampS = timestampS,
        .timestampMs = timestampMs,
        .miner = li(block.miner),
    });

    var txByHash = std.StringHashMap(usize).init(arena);
    const txBase = ent.txs.items.len;

    const maxK = @min(block.transactions.len, receipts.len);
    for (0..maxK) |k| {
        const tx = &block.transactions[k];
        const rcpt = &receipts[k];

        const hash = li(tx.hash);
        const fromAddr = li(tx.from);
        const toAddr = liOpt(tx.to);
        const value = li(tx.value);
        const input = li(tx.input);
        const methodId = methodIdSlice(input);

        const txRow = TxRow{
            .chunk = chunk,
            .blockNumber = number,
            .transactionIndex = hexToI32(tx.transactionIndex),
            .hash = hash,
            .blockTimestampS = timestampS,
            .blockTimestampMs = timestampMs,
            .methodId = methodId,
            .input = input,
            .fromAddress = fromAddr,
            .toAddress = toAddr,
            .value = value,
            .gasLimit = hexToI64(tx.gas),
            .gasPrice = hexToI64(tx.gasPrice),
            .gasUsed = hexToI64(rcpt.gasUsed),
            .maxPriorityFee = if (tx.maxPriorityFeePerGas) |v| hexToI64(v) else 0,
            .maxFee = if (tx.maxFeePerGas) |v| hexToI64(v) else 0,
            .cumulativeGasUsed = hexToI64(rcpt.cumulativeGasUsed),
            .effectiveGasPrice = if (rcpt.effectiveGasPrice) |v| hexToI64(v) else 0,
            .contractAddress = liOpt(rcpt.contractAddress),
            .status = hexToI8(rcpt.status),
            .txType = hexToI8(tx.type),
        };

        const txIdx = ent.txs.items.len;
        try ent.txs.append(arena, txRow);
        try txByHash.put(hash, txIdx);

        for (rcpt.logs) |log| {
            const restCount = if (log.topics.len > 4) log.topics.len - 4 else 0;
            const restTopics = try arena.alloc([]const u8, restCount);
            for (0..restCount) |ti| restTopics[ti] = li(log.topics[4 + ti]);
            try ent.logs.append(arena, .{
                .chunk = chunk,
                .blockNumber = number,
                .transactionIndex = hexToI32(log.transactionIndex),
                .logIndex = hexToI32(log.logIndex),
                .blockTimestampS = timestampS,
                .blockTimestampMs = timestampMs,
                .address = li(log.address),
                .data = li(log.data),
                .topicZeroth = if (log.topics.len > 0) li(log.topics[0]) else "",
                .topicFirst = if (log.topics.len > 1) li(log.topics[1]) else "",
                .topicSecond = if (log.topics.len > 2) li(log.topics[2]) else "",
                .topicThird = if (log.topics.len > 3) li(log.topics[3]) else "",
                .restTopics = restTopics,
                .transactionHash = li(log.transactionHash),
                .removed = log.removed,
            });
        }
    }

    for (traces, 0..) |trace, traceIdx| {
        const txIdxOpt: ?usize = if (trace.transactionPosition) |pos|
            if (pos >= 0 and pos < @as(i32, @intCast(maxK))) txBase + @as(usize, @intCast(pos)) else null
        else blk: {
            const rawHash = trace.transactionHash orelse break :blk null;
            if (rawHash.len == 0) break :blk null;
            break :blk txByHash.get(li(rawHash));
        };
        const txIdx = txIdxOpt orelse continue;
        const txRow = &ent.txs.items[txIdx];

        const fromAddr = li(trace.action.from);
        const toAddr = liOpt(trace.action.to);

        if (toAddr.len > 0 and bloom.mightContain(toAddr)) {
            try ent.erc20Touches.append(arena, .{
                .address = toAddr,
                .chunk = chunk,
                .blockNumber = number,
                .blockTimestampS = timestampS,
            });
        }

        if (fromAddr.len > 0 and trace.action.value != null) {
            try ent.internalTxs.append(arena, .{
                .chunk = chunk,
                .blockNumber = number,
                .blockTimestampS = timestampS,
                .blockTimestampMs = timestampMs,
                .transactionIndex = txRow.transactionIndex,
                .transactionHash = txRow.hash,
                .traceIndex = @intCast(traceIdx),
                .fromAddress = fromAddr,
                .toAddress = toAddr,
                .value = li(trace.action.value.?),
            });
        }

        if (std.mem.eql(u8, trace.type, "suicide")) {
            if (trace.action.address) |destructedAddr| {
                try ent.selfDestructEvents.append(arena, .{
                    .address = li(destructedAddr),
                    .chunk = chunk,
                    .blockNumber = number,
                    .blockTimestampS = timestampS,
                });
            }
        }

        if (trace.result) |res| {
            if (res.address) |contractAddr| {
                const addrLower = li(contractAddr);
                const creator = txRow.fromAddress;
                const factory = if (std.mem.eql(u8, creator, fromAddr)) "" else fromAddr;
                const rawBc = if (trace.action.init) |i| i else if (trace.action.input) |i| i else "0x";
                const creationBc = if (rawBc.len == 0) "0x" else rawBc;
                const deployedBc = if (res.code) |c| c else "0x";

                try ent.contracts.append(arena, .{
                    .chunk = chunk,
                    .blockNumber = number,
                    .transactionIndex = txRow.transactionIndex,
                    .transactionHash = txRow.hash,
                    .traceIndex = @intCast(traceIdx),
                    .blockTimestampS = timestampS,
                    .blockTimestampMs = timestampMs,
                    .address = addrLower,
                    .creationMethod = creationMethodI8(trace.action.creationMethod),
                    .creatorAddress = creator,
                    .contractFactory = factory,
                    .creationBytecode = creationBc,
                    .deployedBytecode = deployedBc,
                });
                try ent.contractsByAddr.append(arena, .{
                    .address = addrLower,
                    .creator = creator,
                    .txHash = txRow.hash,
                    .blockNumber = number,
                    .timestamp = timestampS,
                    .contractFactory = factory,
                    .creationBytecode = creationBc,
                    .deployedBytecode = deployedBc,
                });

                const sel = scanErc20Selectors(deployedBc);

                if (sel.any()) {
                    bloom.insert(addrLower);
                    try ent.erc20Candidates.append(arena, .{
                        .address = addrLower,
                        .chunk = chunk,
                        .blockNumber = number,
                        .blockTimestampS = timestampS,
                        .blockTimestampMs = timestampMs,
                        .hasBalanceOf = sel.hasBalanceOf,
                        .hasTransfer = sel.hasTransfer,
                        .hasTransferFrom = sel.hasTransferFrom,
                        .hasApprove = sel.hasApprove,
                        .hasAllowance = sel.hasAllowance,
                    });
                }
            }
        }
    }

    if (@as(u64, @intCast(number)) > ent.lastBlock) {
        ent.lastBlock = @intCast(number);
    }
}
