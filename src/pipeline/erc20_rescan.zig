// --erc20-rescan maintenance pass: re-resolves every address Redis already
// knows about (key "erc20:{address}"), bumping its stored detection version.
//
// Use this after a classification/selector-detection logic change, when the
// lazy per-touch staleness catch-up (erc20.isStaleOrUnknown, triggered by the
// next CALL trace touching the contract) isn't good enough and a full eager
// refresh is wanted instead. Not part of the hot indexing path — run as a
// one-off offline pass (`raw --erc20-rescan`), independent of --from/--to.
//
// Re-fetches each contract's CURRENT bytecode (not the historical
// creation-time bytecode) and re-runs the same selector scan used at CREATE
// time (transformer.scanErc20Selectors), then resolves metadata via
// Multicall3 at the current chain head — always past its real deployment
// block, so no stateOverride is needed here.
const std = @import("std");

const core = @import("indexer/core");
const rpcMod = @import("indexer/rpc");
const dbMod = @import("indexer/db");

const transformer = @import("transformer.zig");
const erc20 = @import("erc20.zig");

const Allocator = std.mem.Allocator;
const EvmChainConfig = core.structures.EvmChainConfig;
const EvmContractConfig = core.structures.EvmContractConfig;
const FetchClient = core.fetch.Client;
const cursor = dbMod.cursor;
const schema = dbMod.schema;
const pool = dbMod.pool;
const batch = dbMod.batch;
const multicall = rpcMod.multicall;

const SCAN_COUNT: u32 = 500;
const KEY_PREFIX = "erc20:";

pub fn run(
    gpa: Allocator,
    io: std.Io,
    chain: *const EvmChainConfig,
    rdb: *cursor.Conn,
    conn: *pool.CqlConn,
) !void {
    const mc = chain.contracts.MULTICALL3 orelse {
        std.debug.print("[erc20-rescan] chain has no MULTICALL3 configured — nothing to do\n", .{});
        return;
    };

    var client = FetchClient.init(gpa, io);
    defer client.deinit();

    const headBlock = try multicall.fetchHeadBlockNumber(gpa, &client, chain.rpcNodes.lotosArchiveNode.https);
    var headHexBuf: [20]u8 = undefined;
    const headHex = try std.fmt.bufPrint(&headHexBuf, "0x{x}", .{headBlock});
    std.debug.print("[erc20-rescan] resolving against head block {d}\n", .{headBlock});

    var cursorOwned: []u8 = try gpa.dupe(u8, "0");
    defer gpa.free(cursorOwned);

    var totalSeen: usize = 0;
    var totalResolved: usize = 0;

    while (true) {
        var res = try rdb.scan(cursorOwned, KEY_PREFIX ++ "*", SCAN_COUNT);
        defer res.deinit(gpa);

        for (res.keys) |key| {
            if (key.len <= KEY_PREFIX.len) continue;
            const address = key[KEY_PREFIX.len..];
            totalSeen += 1;
            rescanOne(gpa, &client, chain, mc, rdb, conn, address, headBlock, headHex) catch |e| {
                std.debug.print("[erc20-rescan] {s} failed: {s}\n", .{ address, @errorName(e) });
                continue;
            };
            totalResolved += 1;
        }

        gpa.free(cursorOwned);
        cursorOwned = try gpa.dupe(u8, res.cursor);

        std.debug.print("[erc20-rescan] progress: seen={d} resolved={d} redis_cursor={s}\n", .{ totalSeen, totalResolved, cursorOwned });

        if (std.mem.eql(u8, cursorOwned, "0")) break;
    }

    std.debug.print("[erc20-rescan] done: seen={d} resolved={d}\n", .{ totalSeen, totalResolved });
}

fn rescanOne(
    gpa: Allocator,
    client: *FetchClient,
    chain: *const EvmChainConfig,
    mc: EvmContractConfig,
    rdb: *cursor.Conn,
    conn: *pool.CqlConn,
    address: []const u8,
    headBlock: u64,
    headHex: []const u8,
) !void {
    const rpcUrl = chain.rpcNodes.lotosArchiveNode.https;
    const chainId: i32 = @intCast(chain.id);

    const bytecode = try multicall.fetchBytecode(gpa, client, rpcUrl, address, headHex);
    defer gpa.free(bytecode);

    if (std.mem.eql(u8, bytecode, "0x")) {
        // Self-destructed since creation — record it and skip metadata resolution.
        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();
        var ent = transformer.initEntities();
        try ent.erc20SelfDestructs.append(arena.allocator(), .{
            .address = address,
            .chainId = chainId,
            .atBlock = @intCast(headBlock),
            .atTimestamp = 0,
        });
        try flushErc20(conn, chain, &ent);
        try writeCursor(rdb, gpa, address, headBlock);
        return;
    }

    const sel = transformer.scanErc20Selectors(bytecode);

    const addrs = [_][]const u8{address};
    const results = try multicall.resolveErc20Metadata(gpa, client, rpcUrl, mc.address, &addrs, headHex, null);
    defer {
        for (results) |r| {
            if (r.name) |s| gpa.free(s);
            if (r.symbol) |s| gpa.free(s);
            if (r.totalSupply) |s| gpa.free(s);
            if (r.owner) |s| gpa.free(s);
        }
        gpa.free(results);
    }
    const r = results[0];
    const cls = erc20.classify(sel, r);
    const decimalsI16: i16 = if (r.decimals) |d| @intCast(d) else -1;

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    var ent = transformer.initEntities();
    try ent.erc20Tokens.append(aa, .{
        .address = address,
        .chainId = chainId,
        .name = r.name orelse "",
        .symbol = r.symbol orelse "",
        .decimals = decimalsI16,
        .hasBalanceOf = sel.hasBalanceOf,
        .hasTransfer = sel.hasTransfer,
        .hasTransferFrom = sel.hasTransferFrom,
        .hasApprove = sel.hasApprove,
        .hasAllowance = sel.hasAllowance,
        .isStandardDecimals = cls.isStandardDecimals,
        .isFullyFollowingStandard = cls.isFullyFollowingStandard,
        .isMinimallyFollowingStandard = cls.isMinimallyFollowingStandard,
        .isPartiallyFollowingStandard = cls.isPartiallyFollowingStandard,
        .isNotFollowingStandard = cls.isNotFollowingStandard,
        .detectionVersion = erc20.DETECTION_VERSION,
    });
    try ent.erc20Supplies.append(aa, .{
        .address = address,
        .chainId = chainId,
        .initialTotalSupply = r.totalSupply orelse "",
        .latestTotalSupply = r.totalSupply orelse "",
        .updatedAtBlock = @intCast(headBlock),
        .updatedAtTimestamp = 0,
        .isUpdate = false,
    });
    try ent.erc20Owners.append(aa, .{
        .address = address,
        .chainId = chainId,
        .initialOwner = r.owner orelse "",
        .latestOwner = r.owner orelse "",
        .isOwnershipRenounced = if (r.owner) |o| chain.wellKnownBurnAddresses.is(o) else false,
        .updatedAtBlock = @intCast(headBlock),
        .updatedAtTimestamp = 0,
        .isUpdate = false,
    });

    try flushErc20(conn, chain, &ent);
    try writeCursor(rdb, gpa, address, headBlock);
}

fn flushErc20(conn: *pool.CqlConn, chain: *const EvmChainConfig, ent: *const schema.Entities) !void {
    var entPtrs = [1]*const schema.Entities{ent};
    var g = batch.TableSave{
        .conn = conn,
        .ents = entPtrs[0..],
        .bs = pool.BatchSizes.fromChain(chain.indexingOptions),
    };
    batch.saveErc20RowsForEntities(&g);
    if (g.err) |e| return e;
}

fn writeCursor(rdb: *cursor.Conn, gpa: Allocator, address: []const u8, blockNumber: u64) !void {
    const key = try std.fmt.allocPrint(gpa, "{s}{s}", .{ KEY_PREFIX, address });
    defer gpa.free(key);
    const val = try std.fmt.allocPrint(gpa, "{d}:{d}", .{ blockNumber, erc20.DETECTION_VERSION });
    defer gpa.free(val);
    try rdb.setWithRetry(key, val);
}
