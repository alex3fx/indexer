// ERC-20 detection orchestration: turns the raw per-block signals produced by
// transformer.zig (erc20Candidates / erc20Touches / selfDestructEvents) into
// the four erc20_* Scylla row types, via Multicall3 eth_call round trips and
// a Redis cursor that remembers "last block we know this address's state as of".
//
// Called once per block's `Entities`, sequentially, from the same place that
// already owns the single Redis connection (saveAccumFn in pipeline.zig for
// historical, processBlock in writer.zig for realtime) — never from the
// parallel transform workers, per the "no parallel Redis writes" constraint.
const std = @import("std");

const core = @import("indexer/core");
const rpcMod = @import("indexer/rpc");
const dbMod = @import("indexer/db");
const Bloom = @import("bloom.zig").Bloom;
const BytecodeBloom = @import("bytecode_bloom.zig").BytecodeBloom;
const transformer = @import("transformer.zig");

const Allocator = std.mem.Allocator;
const FetchClient = core.fetch.Client;
const EvmChainConfig = core.structures.EvmChainConfig;
const EvmContractConfig = core.structures.EvmContractConfig;
const multicall = rpcMod.multicall;
const cursor = dbMod.cursor;
const schema = dbMod.schema;

// Bump when classification/resolution logic changes. Addresses whose stored
// Redis cursor carries an older version are treated as stale and re-resolved
// on next touch — see isStaleOrUnknown. A full eager re-resolution (instead
// of this lazy catch-up) is the job of the --erc20-rescan maintenance mode.
pub const DETECTION_VERSION: i32 = 1;

// Cap on addresses per aggregate3 eth_call (5 calls each => up to 1000 Call3
// entries per round trip). Keeps calldata size and node-side simulation gas
// bounded even if an unusually dense block produces many candidates/touches
// at once. Override via env ERC20_MULTICALL_CHUNK_SIZE.
pub const MULTICALL_CHUNK_SIZE_DEFAULT: usize = 200;

pub const Erc20Context = struct {
    gpa: Allocator,
    bloom: Bloom,
    bytecodeBloom: BytecodeBloom,
    client: FetchClient,
    multicall3Bytecode: ?[]u8,
    multicallChunkSize: usize,

    pub fn init(gpa: Allocator, io: std.Io, chain: *const EvmChainConfig, multicallChunkSize: usize) !Erc20Context {
        var bloom = try Bloom.init(gpa);
        errdefer bloom.deinit(gpa);

        var bytecodeBloom = try BytecodeBloom.init(gpa);
        errdefer bytecodeBloom.deinit(gpa);

        var client = FetchClient.init(gpa, io);
        errdefer client.deinit();

        var multicall3Bytecode: ?[]u8 = null;
        if (chain.contracts.MULTICALL3) |mc| {
            multicall3Bytecode = multicall.fetchBytecode(
                gpa,
                &client,
                chain.rpcNodes.lotosArchiveNode.https,
                mc.address,
                "latest",
            ) catch |e| blk: {
                std.debug.print(
                    "[erc20] could not fetch Multicall3 bytecode ({s}) — resolution on pre-deployment-block ({d}) contracts will yield empty metadata until this succeeds\n",
                    .{ @errorName(e), mc.deployedAtBlock },
                );
                break :blk null;
            };
        } else {
            std.debug.print("[erc20] chain has no MULTICALL3 configured — ERC-20 tracking disabled\n", .{});
        }

        return .{
            .gpa = gpa,
            .bloom = bloom,
            .bytecodeBloom = bytecodeBloom,
            .client = client,
            .multicall3Bytecode = multicall3Bytecode,
            .multicallChunkSize = if (multicallChunkSize > 0) multicallChunkSize else MULTICALL_CHUNK_SIZE_DEFAULT,
        };
    }

    pub fn deinit(self: *Erc20Context) void {
        if (self.multicall3Bytecode) |bc| self.gpa.free(bc);
        self.client.deinit();
        self.bloom.deinit(self.gpa);
        self.bytecodeBloom.deinit(self.gpa);
    }

    fn overrideFor(self: *const Erc20Context, mc: EvmContractConfig, blockNumber: u64) ?[]const u8 {
        if (blockNumber >= mc.deployedAtBlock) return null;
        return self.multicall3Bytecode;
    }
};

/// Entry point: call once per accumulation window (e.g. SAVE_EVERY blocks in
/// historical, or a single block in realtime — pass a 1-element slice), after
/// transform, before the rows are saved. Merges candidates/touches across all
/// of `ents` and resolves each kind in as few Multicall3 round trips as
/// `multicallChunkSize` allows, instead of one round trip per block — the
/// per-block-with-candidates RPC latency was the dominant cost in dense/bursty
/// ranges (e.g. one window with a single token-factory burst measurably
/// slower than denser-but-quieter neighboring windows).
///
/// Approximation: all addresses in the window are resolved as of `pinBlock`
/// (the window's last block), not each candidate's own exact creation block.
/// Within a SAVE_EVERY-sized window (~24 blocks / ~5 min on ETH) a freshly
/// deployed token's name/symbol/decimals/totalSupply/owner essentially never
/// change before its next read, so `initial*` fields stay accurate in
/// practice — but it IS a relaxation from "at creation" to "as of window end",
/// traded for collapsing N RPC round trips into ~1. `updatedAtBlock` is set to
/// `pinBlock` to honestly reflect what was actually read.
///
/// Best-effort — logs and swallows individual RPC/Redis failures so a flaky
/// node never aborts the window's main save.
pub fn resolveAndEnrichWindow(
    self: *Erc20Context,
    chain: *const EvmChainConfig,
    rdb: *cursor.Conn,
    rowAllocator: Allocator,
    ents: []const *schema.Entities,
    pinBlock: u64,
    pinTimestampS: i64,
    carrier: *schema.Entities,
) void {
    const mc = chain.contracts.MULTICALL3 orelse return;
    const chainId: i32 = @intCast(chain.id);

    var candCount: usize = 0;
    var touchCount: usize = 0;
    for (ents) |ent| {
        candCount += ent.erc20Candidates.items.len;
        touchCount += ent.erc20Touches.items.len;
        for (ent.selfDestructEvents.items) |sd| {
            if (!self.bloom.mightContain(sd.address)) continue;
            carrier.erc20SelfDestructs.append(rowAllocator, .{
                .address = sd.address,
                .chainId = chainId,
                .atBlock = sd.blockNumber,
                .atTimestamp = sd.blockTimestampS,
            }) catch {};
        }
    }

    if (candCount > 0) {
        const allCands = self.gpa.alloc(schema.Erc20Candidate, candCount) catch |e| {
            std.debug.print("[erc20] candidate merge alloc failed: {s}\n", .{@errorName(e)});
            return resolveTouchesWindowOrLog(self, chain, mc, chainId, rdb, rowAllocator, ents, touchCount, pinBlock, pinTimestampS, carrier);
        };
        defer self.gpa.free(allCands);
        var i: usize = 0;
        for (ents) |ent| {
            for (ent.erc20Candidates.items) |c| {
                allCands[i] = c;
                i += 1;
            }
        }
        resolveCandidatesWindow(self, chain, mc, chainId, rdb, rowAllocator, allCands, pinBlock, pinTimestampS, carrier) catch |e| {
            std.debug.print("[erc20] candidate resolve failed: {s}\n", .{@errorName(e)});
        };
    }

    resolveTouchesWindowOrLog(self, chain, mc, chainId, rdb, rowAllocator, ents, touchCount, pinBlock, pinTimestampS, carrier);
}

fn resolveTouchesWindowOrLog(
    self: *Erc20Context,
    chain: *const EvmChainConfig,
    mc: EvmContractConfig,
    chainId: i32,
    rdb: *cursor.Conn,
    rowAllocator: Allocator,
    ents: []const *schema.Entities,
    touchCount: usize,
    pinBlock: u64,
    pinTimestampS: i64,
    carrier: *schema.Entities,
) void {
    if (touchCount == 0) return;
    resolveTouchesWindow(self, chain, mc, chainId, rdb, rowAllocator, ents, pinBlock, pinTimestampS, carrier) catch |e| {
        std.debug.print("[erc20] touch resolve failed: {s}\n", .{@errorName(e)});
    };
}

pub const Classification = struct {
    isStandardDecimals: bool,
    isFullyFollowingStandard: bool,
    isMinimallyFollowingStandard: bool,
    isPartiallyFollowingStandard: bool,
    isNotFollowingStandard: bool,
};

/// Shared by the window-level candidate path (applyCandidateResults) and the
/// --erc20-rescan maintenance pass (erc20_rescan.zig) — same rules as
/// try_discover_erc_twenty_tokens.ts.
pub fn classify(sel: transformer.Erc20SelectorFlags, r: multicall.Erc20MulticallResult) Classification {
    const isStandardDecimals = r.decimals != null;
    return .{
        .isStandardDecimals = isStandardDecimals,
        .isFullyFollowingStandard = r.name != null and r.symbol != null and isStandardDecimals and r.totalSupply != null and
            sel.hasBalanceOf and sel.hasTransfer and sel.hasTransferFrom and sel.hasApprove and sel.hasAllowance,
        .isMinimallyFollowingStandard = isStandardDecimals and sel.hasBalanceOf and sel.hasTransfer and sel.hasTransferFrom and sel.hasApprove and sel.hasAllowance,
        .isPartiallyFollowingStandard = isStandardDecimals or sel.hasBalanceOf or sel.hasTransfer or sel.hasTransferFrom or sel.hasApprove or sel.hasAllowance,
        .isNotFollowingStandard = r.name == null and r.symbol == null and r.decimals == null and r.totalSupply == null and !sel.any(),
    };
}

fn dupe(allocator: Allocator, s: []const u8) ![]const u8 {
    return try allocator.dupe(u8, s);
}

fn freeMulticallResults(gpa: Allocator, results: []multicall.Erc20MulticallResult) void {
    for (results) |r| {
        if (r.name) |s| gpa.free(s);
        if (r.symbol) |s| gpa.free(s);
        if (r.totalSupply) |s| gpa.free(s);
        if (r.owner) |s| gpa.free(s);
    }
    gpa.free(results);
}

fn writeErc20Cursor(rdb: *cursor.Conn, gpa: Allocator, address: []const u8, blockNumber: u64) !void {
    const key = try std.fmt.allocPrint(gpa, "erc20:{s}", .{address});
    defer gpa.free(key);
    const val = try std.fmt.allocPrint(gpa, "{d}:{d}", .{ blockNumber, DETECTION_VERSION });
    defer gpa.free(val);
    try rdb.setWithRetry(key, val);
}

/// True if we should (re-)resolve this address at `blockNumber`: never seen
/// before, recorded by an older detection version, or genuinely newer activity.
fn isStaleOrUnknown(rdb: *cursor.Conn, gpa: Allocator, address: []const u8, blockNumber: u64) bool {
    const key = std.fmt.allocPrint(gpa, "erc20:{s}", .{address}) catch return true;
    defer gpa.free(key);
    const val = (rdb.get(key) catch return true) orelse return true;
    defer gpa.free(val);

    const sep = std.mem.indexOfScalar(u8, val, ':') orelse return true;
    const storedBlock = std.fmt.parseInt(u64, val[0..sep], 10) catch return true;
    const storedVersion = std.fmt.parseInt(i32, val[sep + 1 ..], 10) catch return true;

    if (storedVersion < DETECTION_VERSION) return true;
    return blockNumber > storedBlock;
}

fn resolveCandidatesWindow(
    self: *Erc20Context,
    chain: *const EvmChainConfig,
    mc: EvmContractConfig,
    chainId: i32,
    rdb: *cursor.Conn,
    rowAllocator: Allocator,
    allCands: []const schema.Erc20Candidate,
    pinBlock: u64,
    pinTimestampS: i64,
    carrier: *schema.Entities,
) !void {
    const addrs = try self.gpa.alloc([]const u8, allCands.len);
    defer self.gpa.free(addrs);
    for (allCands, 0..) |c, i| addrs[i] = c.address;

    var blockHexBuf: [20]u8 = undefined;
    const blockHex = try std.fmt.bufPrint(&blockHexBuf, "0x{x}", .{pinBlock});
    const override = self.overrideFor(mc, pinBlock);

    var off: usize = 0;
    while (off < allCands.len) {
        const end = @min(off + self.multicallChunkSize, allCands.len);
        const cands = allCands[off..end];
        const results = try multicall.resolveErc20Metadata(
            self.gpa,
            &self.client,
            chain.rpcNodes.lotosArchiveNode.https,
            mc.address,
            addrs[off..end],
            blockHex,
            override,
        );
        defer freeMulticallResults(self.gpa, results);

        try applyCandidateResults(self.gpa, chain, chainId, rdb, rowAllocator, carrier, pinBlock, pinTimestampS, cands, results);
        off = end;
    }
}

fn applyCandidateResults(
    gpa: Allocator,
    chain: *const EvmChainConfig,
    chainId: i32,
    rdb: *cursor.Conn,
    rowAllocator: Allocator,
    ent: *schema.Entities,
    pinBlock: u64,
    pinTimestampS: i64,
    cands: []const schema.Erc20Candidate,
    results: []const multicall.Erc20MulticallResult,
) !void {
    for (cands, results) |c, r| {
        const decimalsI16: i16 = if (r.decimals) |d| @intCast(d) else -1;
        const sel = transformer.Erc20SelectorFlags{
            .hasBalanceOf = c.hasBalanceOf,
            .hasTransfer = c.hasTransfer,
            .hasTransferFrom = c.hasTransferFrom,
            .hasApprove = c.hasApprove,
            .hasAllowance = c.hasAllowance,
        };
        const cls = classify(sel, r);

        try ent.erc20Tokens.append(rowAllocator, .{
            .address = c.address,
            .chainId = chainId,
            .name = try dupe(rowAllocator, r.name orelse ""),
            .symbol = try dupe(rowAllocator, r.symbol orelse ""),
            .decimals = decimalsI16,
            .hasBalanceOf = c.hasBalanceOf,
            .hasTransfer = c.hasTransfer,
            .hasTransferFrom = c.hasTransferFrom,
            .hasApprove = c.hasApprove,
            .hasAllowance = c.hasAllowance,
            .isStandardDecimals = cls.isStandardDecimals,
            .isFullyFollowingStandard = cls.isFullyFollowingStandard,
            .isMinimallyFollowingStandard = cls.isMinimallyFollowingStandard,
            .isPartiallyFollowingStandard = cls.isPartiallyFollowingStandard,
            .isNotFollowingStandard = cls.isNotFollowingStandard,
            .detectionVersion = DETECTION_VERSION,
        });

        const supplyStr = try dupe(rowAllocator, r.totalSupply orelse "");
        try ent.erc20Supplies.append(rowAllocator, .{
            .address = c.address,
            .chainId = chainId,
            .initialTotalSupply = supplyStr,
            .latestTotalSupply = supplyStr,
            .updatedAtBlock = @intCast(pinBlock),
            .updatedAtTimestamp = pinTimestampS,
            .isUpdate = false,
        });

        const ownerStr = try dupe(rowAllocator, r.owner orelse "");
        try ent.erc20Owners.append(rowAllocator, .{
            .address = c.address,
            .chainId = chainId,
            .initialOwner = ownerStr,
            .latestOwner = ownerStr,
            .isOwnershipRenounced = if (r.owner) |o| chain.wellKnownBurnAddresses.is(o) else false,
            .updatedAtBlock = @intCast(pinBlock),
            .updatedAtTimestamp = pinTimestampS,
            .isUpdate = false,
        });

        writeErc20Cursor(rdb, gpa, c.address, pinBlock) catch |e| {
            std.debug.print("[erc20] redis cursor write failed for {s}: {s}\n", .{ c.address, @errorName(e) });
        };
    }
}

fn resolveTouchesWindow(
    self: *Erc20Context,
    chain: *const EvmChainConfig,
    mc: EvmContractConfig,
    chainId: i32,
    rdb: *cursor.Conn,
    rowAllocator: Allocator,
    ents: []const *schema.Entities,
    pinBlock: u64,
    pinTimestampS: i64,
    carrier: *schema.Entities,
) !void {
    var seen = std.StringHashMap(void).init(self.gpa);
    defer seen.deinit();

    var uniqueAddrs: std.ArrayList([]const u8) = .empty;
    defer uniqueAddrs.deinit(self.gpa);

    for (ents) |ent| {
        for (ent.erc20Touches.items) |t| {
            if (seen.contains(t.address)) continue;
            if (!isStaleOrUnknown(rdb, self.gpa, t.address, pinBlock)) continue;
            try seen.put(t.address, {});
            try uniqueAddrs.append(self.gpa, t.address);
        }
    }

    if (uniqueAddrs.items.len == 0) return;

    var blockHexBuf: [20]u8 = undefined;
    const blockHex = try std.fmt.bufPrint(&blockHexBuf, "0x{x}", .{pinBlock});
    const override = self.overrideFor(mc, pinBlock);

    var off: usize = 0;
    while (off < uniqueAddrs.items.len) {
        const end = @min(off + self.multicallChunkSize, uniqueAddrs.items.len);
        const addrsChunk = uniqueAddrs.items[off..end];
        const results = try multicall.resolveErc20Metadata(
            self.gpa,
            &self.client,
            chain.rpcNodes.lotosArchiveNode.https,
            mc.address,
            addrsChunk,
            blockHex,
            override,
        );
        defer freeMulticallResults(self.gpa, results);

        for (addrsChunk, results) |addr, r| {
            if (r.totalSupply) |ts| {
                const supplyStr = try dupe(rowAllocator, ts);
                try carrier.erc20Supplies.append(rowAllocator, .{
                    .address = addr,
                    .chainId = chainId,
                    .initialTotalSupply = "",
                    .latestTotalSupply = supplyStr,
                    .updatedAtBlock = @intCast(pinBlock),
                    .updatedAtTimestamp = pinTimestampS,
                    .isUpdate = true,
                });
            }
            if (r.owner) |o| {
                const ownerStr = try dupe(rowAllocator, o);
                try carrier.erc20Owners.append(rowAllocator, .{
                    .address = addr,
                    .chainId = chainId,
                    .initialOwner = "",
                    .latestOwner = ownerStr,
                    .isOwnershipRenounced = chain.wellKnownBurnAddresses.is(o),
                    .updatedAtBlock = @intCast(pinBlock),
                    .updatedAtTimestamp = pinTimestampS,
                    .isUpdate = true,
                });
            }
            writeErc20Cursor(rdb, self.gpa, addr, pinBlock) catch |e| {
                std.debug.print("[erc20] redis cursor write failed for {s}: {s}\n", .{ addr, @errorName(e) });
            };
        }
        off = end;
    }
}
