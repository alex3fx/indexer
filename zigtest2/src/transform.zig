// Entity building: converts RPC types → DB row types.
// Uses arena allocator for all allocations — no page_allocator mmap per string.
// lowerInPlace: modifies arena-owned strings in-place, zero copy, zero alloc.
const std = @import("std");
const rpc = @import("rpc");

// ─── DB row types ─────────────────────────────────────────────────────────────

pub const BlockRow = struct {
    chunk: i32,
    number: i64,
    timestamp_s: i64,
    timestamp_ms: i64,
    miner: []const u8,
};

pub const TxRow = struct {
    chunk: i32,
    block_number: i64,
    transaction_index: i32,
    hash: []const u8,
    block_timestamp_s: i64,
    block_timestamp_ms: i64,
    method_id: []const u8,
    input: []const u8,
    from_address: []const u8,
    to_address: []const u8,
    value: []const u8,
    gas_limit: i64,
    gas_price: i64,
    gas_used: i64,
    max_priority_fee: i64,
    max_fee: i64,
    cumulative_gas_used: i64,
    effective_gas_price: i64,
    contract_address: []const u8,
    status: i8,
    tx_type: i8,
};

pub const LogRow = struct {
    chunk: i32,
    block_number: i64,
    transaction_index: i32,
    log_index: i32,
    block_timestamp_s: i64,
    block_timestamp_ms: i64,
    address: []const u8,
    data: []const u8,
    topic_zeroth: []const u8,
    topic_first: []const u8,
    topic_second: []const u8,
    topic_third: []const u8,
    rest_topics: [][]const u8,
    transaction_hash: []const u8,
    removed: bool,
};

pub const InternalTxRow = struct {
    chunk: i32,
    block_number: i64,
    block_timestamp_s: i64,
    block_timestamp_ms: i64,
    transaction_index: i32,
    transaction_hash: []const u8,
    trace_index: i32,
    from_address: []const u8,
    to_address: []const u8,
    value: []const u8,
};

pub const ContractRow = struct {
    chunk: i32,
    block_number: i64,
    transaction_index: i32,
    transaction_hash: []const u8,
    trace_index: i32,
    block_timestamp_s: i64,
    block_timestamp_ms: i64,
    address: []const u8,
    creation_method: i8,
    creator_address: []const u8,
    contract_factory: []const u8,
    creation_bytecode: []const u8,
    deployed_bytecode: []const u8,
};

pub const ContractByAddrRow = struct {
    address: []const u8,
    creator: []const u8,
    tx_hash: []const u8,
    block_number: i64,
    timestamp: i64,
    contract_factory: []const u8,
    creation_bytecode: []const u8,
    deployed_bytecode: []const u8,
};

pub const Entities = struct {
    blocks: std.ArrayList(BlockRow),
    txs: std.ArrayList(TxRow),
    logs: std.ArrayList(LogRow),
    internal_txs: std.ArrayList(InternalTxRow),
    contracts: std.ArrayList(ContractRow),
    contracts_by_addr: std.ArrayList(ContractByAddrRow),
    last_block: u64,
};

// ─── Helpers ──────────────────────────────────────────────────────────────────

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

// Lowercase string in-place using SIMD (16 bytes/iter).
// Only A-Z→a-z: adds 0x20 to uppercase bytes. Safe for all Ethereum hex strings.
// Returns the same slice — zero allocation, zero copy.
inline fn li(s: []const u8) []const u8 {
    const ptr = @constCast(s.ptr);
    var i: usize = 0;
    const V = @Vector(16, u8);
    const vA: V = @splat('A');
    const vZ: V = @splat('Z');
    const v32: V = @splat(@as(u8, 0x20)); // 'a' - 'A'
    const v0: V = @splat(@as(u8, 0));
    while (i + 16 <= s.len) : (i += 16) {
        const v: V = ptr[i..][0..16].*;
        const is_upper = (v >= vA) & (v <= vZ);
        ptr[i..][0..16].* = v + @select(u8, is_upper, v32, v0);
    }
    while (i < s.len) : (i += 1) {
        ptr[i] = std.ascii.toLower(ptr[i]);
    }
    return s;
}

// Optional in-place lower — returns "" for null (no allocation: "" is a literal).
inline fn liOpt(s: ?[]const u8) []const u8 {
    return if (s) |v| li(v) else "";
}

// ─── Transform ────────────────────────────────────────────────────────────────

/// Transform a batch of blocks into DB entities.
/// arena: used for all allocations — caller owns the arena and frees it after save.
/// All entity strings point into arena memory; no individual frees needed.
pub fn transformBatch(
    arena: std.mem.Allocator,
    blocks: []const rpc.BlockData,
    chunk_size: u64,
    remap_mod: u64,
) !Entities {
    var ent = Entities{
        .blocks = .empty,
        .txs = .empty,
        .logs = .empty,
        .internal_txs = .empty,
        .contracts = .empty,
        .contracts_by_addr = .empty,
        .last_block = 0,
    };

    for (blocks) |bd| {
        if (bd.err or bd.block == null or bd.receipts == null) continue;

        const block = bd.block.?;
        const receipts = bd.receipts.?;
        const traces = bd.traces orelse &.{};

        const number = hexToI64(block.number);
        const timestamp_s = hexToI64(block.timestamp);
        const timestamp_ms: i64 = if (block.milliTimestamp) |m| hexToI64(m) else timestamp_s * 1000;
        const chunk = if (remap_mod > 0)
            @as(i32, @intCast(@mod(number, @as(i64, @intCast(remap_mod)))))
        else
            @as(i32, @intCast(@divFloor(number, @as(i64, @intCast(chunk_size)))));

        try ent.blocks.append(arena, .{
            .chunk = chunk,
            .number = number,
            .timestamp_s = timestamp_s,
            .timestamp_ms = timestamp_ms,
            .miner = li(block.miner), // in-place lowercase, no alloc
        });

        // tx hash → index map (HashMap backed by arena — fast bump alloc)
        var tx_by_hash = std.StringHashMap(usize).init(arena);

        const max_k = @min(block.transactions.len, receipts.len);
        for (0..max_k) |k| {
            const tx = &block.transactions[k];
            const rcpt = &receipts[k];

            // lowerInPlace: modifies arena string in-place, returns same pointer
            const hash       = li(tx.hash);
            const from_addr  = li(tx.from);
            const to_addr    = liOpt(tx.to);
            const value      = li(tx.value);
            const input      = li(tx.input); // hex data already lowercase, no-op
            const method_id  = methodIdSlice(input); // slice of input, no alloc

            const tx_row = TxRow{
                .chunk = chunk,
                .block_number = number,
                .transaction_index = hexToI32(tx.transactionIndex),
                .hash = hash,
                .block_timestamp_s = timestamp_s,
                .block_timestamp_ms = timestamp_ms,
                .method_id = method_id,
                .input = input,
                .from_address = from_addr,
                .to_address = to_addr,
                .value = value,
                .gas_limit = hexToI64(tx.gas),
                .gas_price = hexToI64(tx.gasPrice),
                .gas_used = hexToI64(rcpt.gasUsed),
                .max_priority_fee = if (tx.maxPriorityFeePerGas) |v| hexToI64(v) else 0,
                .max_fee = if (tx.maxFeePerGas) |v| hexToI64(v) else 0,
                .cumulative_gas_used = hexToI64(rcpt.cumulativeGasUsed),
                .effective_gas_price = if (rcpt.effectiveGasPrice) |v| hexToI64(v) else 0,
                .contract_address = liOpt(rcpt.contractAddress),
                .status = hexToI8(rcpt.status),
                .tx_type = hexToI8(tx.@"type"),
            };

            const tx_idx = ent.txs.items.len;
            try ent.txs.append(arena, tx_row);
            try tx_by_hash.put(hash, tx_idx);

            for (rcpt.logs) |log| {
                const rest_count = if (log.topics.len > 4) log.topics.len - 4 else 0;
                // Alloc the rest_topics slice in arena (small, usually 0)
                const rest_topics = try arena.alloc([]const u8, rest_count);
                for (0..rest_count) |ti| {
                    rest_topics[ti] = li(log.topics[4 + ti]);
                }
                try ent.logs.append(arena, .{
                    .chunk = chunk,
                    .block_number = number,
                    .transaction_index = hexToI32(log.transactionIndex),
                    .log_index = hexToI32(log.logIndex),
                    .block_timestamp_s = timestamp_s,
                    .block_timestamp_ms = timestamp_ms,
                    .address = li(log.address),
                    .data = li(log.data),
                    .topic_zeroth = if (log.topics.len > 0) li(log.topics[0]) else "",
                    .topic_first  = if (log.topics.len > 1) li(log.topics[1]) else "",
                    .topic_second = if (log.topics.len > 2) li(log.topics[2]) else "",
                    .topic_third  = if (log.topics.len > 3) li(log.topics[3]) else "",
                    .rest_topics = rest_topics,
                    .transaction_hash = li(log.transactionHash),
                    .removed = log.removed,
                });
            }
        }

        for (traces, 0..) |trace, trace_idx| {
            // Fast path: transactionPosition is always present for call/create traces.
            // Fallback to hashmap only when field is absent (reward/uncle traces).
            const tx_idx_opt: ?usize = if (trace.transactionPosition) |pos|
                if (pos >= 0 and pos < @as(i32, @intCast(ent.txs.items.len))) @intCast(pos) else null
            else blk: {
                const raw_hash = trace.transactionHash orelse break :blk null;
                if (raw_hash.len == 0) break :blk null;
                break :blk tx_by_hash.get(li(raw_hash));
            };
            const tx_idx = tx_idx_opt orelse continue;
            const tx_row = &ent.txs.items[tx_idx];

            const from_addr = li(trace.action.from);
            const to_addr   = liOpt(trace.action.to);

            if (from_addr.len > 0 and to_addr.len > 0 and trace.action.value != null) {
                try ent.internal_txs.append(arena, .{
                    .chunk = chunk,
                    .block_number = number,
                    .block_timestamp_s = timestamp_s,
                    .block_timestamp_ms = timestamp_ms,
                    .transaction_index = tx_row.transaction_index,
                    .transaction_hash = tx_row.hash, // already in arena, no copy
                    .trace_index = @intCast(trace_idx),
                    .from_address = from_addr,
                    .to_address = to_addr,
                    .value = li(trace.action.value.?),
                });
            }

            if (trace.result) |res| {
                if (res.address) |contract_addr| {
                    const addr_lower  = li(contract_addr);
                    const creator     = tx_row.from_address; // already lowercase, in arena
                    const factory     = if (std.mem.eql(u8, creator, from_addr)) "" else from_addr;
                    const raw_bc      = if (trace.action.init) |i| i else if (trace.action.input) |i| i else "0x";
                    const creation_bc = if (raw_bc.len == 0) "0x" else raw_bc;
                    const deployed_bc = if (res.code) |c| c else "0x";

                    try ent.contracts.append(arena, .{
                        .chunk = chunk,
                        .block_number = number,
                        .transaction_index = tx_row.transaction_index,
                        .transaction_hash = tx_row.hash,
                        .trace_index = @intCast(trace_idx),
                        .block_timestamp_s = timestamp_s,
                        .block_timestamp_ms = timestamp_ms,
                        .address = addr_lower,
                        .creation_method = creationMethodI8(trace.action.creationMethod),
                        .creator_address = creator,
                        .contract_factory = factory,
                        .creation_bytecode = creation_bc,
                        .deployed_bytecode = deployed_bc,
                    });
                    try ent.contracts_by_addr.append(arena, .{
                        .address = addr_lower,
                        .creator = creator,
                        .tx_hash = tx_row.hash,
                        .block_number = number,
                        .timestamp = timestamp_s,
                        .contract_factory = factory,
                        .creation_bytecode = creation_bc,
                        .deployed_bytecode = deployed_bc,
                    });
                }
            }
        }

        if (@as(u64, @intCast(number)) > ent.last_block) {
            ent.last_block = @intCast(number);
        }
    }

    return ent;
}
