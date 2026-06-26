// Realtime block writer: fetch → parse → transform → save pipeline for a single block.
// Delegates fetch+parse+transform to pipeline.fetchParseTransform (shared with historical).
const std = @import("std");
const linux = std.os.linux;

const core = @import("indexer/core");

const pipeline = @import("pipeline.zig");
const erc20 = @import("erc20.zig");
const pool = @import("indexer/db").pool;
const http_pool = @import("indexer/rpc").pool;
const batch = @import("indexer/db").batch;
const cursor = @import("indexer/db").cursor;

const EvmChainConfig = core.structures.EvmChainConfig;
const EvmRpcNodeConfig = core.structures.EvmRpcNodeConfig;
const Allocator = std.mem.Allocator;

fn nowNs() i64 {
    var ts: linux.timespec = undefined;
    _ = linux.clock_gettime(.MONOTONIC, &ts);
    return ts.sec * 1_000_000_000 + ts.nsec;
}

fn realtimeMs() i64 {
    var ts: linux.timespec = undefined;
    _ = linux.clock_gettime(.REALTIME, &ts);
    return ts.sec * 1000 + @divTrunc(ts.nsec, 1_000_000);
}

pub const BlockMetrics = struct {
    distance_ms: i64, // time from block mined (timestampMs) to fetch request sent
    fetch_ms: f64,
    parse_ms: f64,
    transform_ms: f64,
    save_ms: f64,
    total_ms: f64, // TTP: fetch + parse + transform + save + cursor
    kb_total: usize,
};

pub const ProcessBlockStatus = union(enum) {
    saved: BlockMetrics,
    retry_later, // block not yet available — caller should retry
    fatal: anyerror,
};

pub fn processBlock(
    io: std.Io,
    gpa: Allocator,
    chain: *const EvmChainConfig,
    rtConns: *batch.RealtimeConns,
    rdb: *cursor.Conn,
    bClient: *core.fetch.Client,
    rClient: *core.fetch.Client,
    tClient: *core.fetch.Client,
    blockNum: u64,
    hPool: ?*http_pool.HttpPool,
    chunkBuckets: u64,
    backupNode: ?EvmRpcNodeConfig,
    erc20Ctx: *erc20.Erc20Context,
) ProcessBlockStatus {
    const rpcNode = chain.rpcNodes.lotosArchiveNode;
    const chunkSize = @as(u64, @intCast(chain.indexingOptions.minifiedChunkSize));
    const bs = pool.BatchSizes.fromChain(chain.indexingOptions);

    var result = pipeline.BlockResult.init(gpa);
    defer result.deinit();

    const t_recv_ms = realtimeMs();

    const primaryStatus = pipeline.fetchParseTransform(gpa, io, rpcNode, blockNum, chunkSize, chunkBuckets, bClient, rClient, tClient, hPool, &erc20Ctx.bloom, &erc20Ctx.bytecodeBloom, &result);

    const fetched = switch (primaryStatus) {
        .ok => true,
        .retry_later, .skip_missing => blk: {
            if (backupNode) |backup| {
                pipeline.resetResult(&result);
                break :blk pipeline.fetchParseTransform(gpa, io, backup, blockNum, chunkSize, chunkBuckets, bClient, rClient, tClient, null, &erc20Ctx.bloom, &erc20Ctx.bytecodeBloom, &result) == .ok;
            }
            break :blk false;
        },
        .fatal => |e| blk: {
            std.debug.print("[realtime] block={d} primary error: {s}", .{ blockNum, @errorName(e) });
            if (backupNode) |backup| {
                std.debug.print(" — trying backup\n", .{});
                pipeline.resetResult(&result);
                break :blk pipeline.fetchParseTransform(gpa, io, backup, blockNum, chunkSize, chunkBuckets, bClient, rClient, tClient, null, &erc20Ctx.bloom, &erc20Ctx.bytecodeBloom, &result) == .ok;
            }
            std.debug.print("\n", .{});
            break :blk false;
        },
    };

    if (!fetched) return .retry_later;

    const fetch_ms = @as(f64, @floatFromInt(result.fetchNs)) / 1e6;
    const parse_ms = @as(f64, @floatFromInt(result.parseNs)) / 1e6;
    const transform_ms = @as(f64, @floatFromInt(result.transformNs)) / 1e6;

    const pinTimestampS: i64 = if (result.ent.blocks.items.len > 0) result.ent.blocks.items[0].timestampS else 0;
    var windowEnts = [1]*batch.Entities{&result.ent};
    erc20.resolveAndEnrichWindow(erc20Ctx, chain, rdb, result.arena.allocator(), windowEnts[0..], blockNum, pinTimestampS, &result.ent);

    const t_save = nowNs();
    batch.saveBlockRt(rtConns, &result.ent, bs) catch |e| return .{ .fatal = e };
    const save_ms = @as(f64, @floatFromInt(nowNs() - t_save)) / 1e6;

    const t_cursor = nowNs();
    var cursorBuf: [20]u8 = undefined;
    const cursorStr = std.fmt.bufPrint(&cursorBuf, "{d}", .{blockNum}) catch unreachable;
    rdb.setWithRetry("LATEST_PROCESSED_BLOCK_NUMBER", cursorStr) catch |e| return .{ .fatal = e };
    const cursor_ms = @as(f64, @floatFromInt(nowNs() - t_cursor)) / 1e6;

    const total_ms = fetch_ms + parse_ms + transform_ms + save_ms + cursor_ms;
    const kb_blk = result.rawData.?.block.body.len / 1024;
    const kb_rcpt = result.rawData.?.receipts.body.len / 1024;
    const kb_trc = result.rawData.?.traces.body.len / 1024;
    std.debug.print("[rt] blk={d} tx={d} log={d} itx={d} kb={d}+{d}+{d} | fetch={d:.0} parse={d:.0} xform={d:.0} save={d:.0} cursor={d:.0} | total={d:.0}ms\n", .{ blockNum, result.ent.txs.items.len, result.ent.logs.items.len, result.ent.internalTxs.items.len, kb_blk, kb_rcpt, kb_trc, fetch_ms, parse_ms, transform_ms, save_ms, cursor_ms, total_ms });

    const distance_ms = if (result.ent.blocks.items.len > 0)
        t_recv_ms - result.ent.blocks.items[0].timestampMs
    else
        0;

    return .{ .saved = .{
        .distance_ms = distance_ms,
        .fetch_ms = fetch_ms,
        .parse_ms = parse_ms,
        .transform_ms = transform_ms,
        .save_ms = save_ms,
        .total_ms = total_ms,
        .kb_total = kb_blk + kb_rcpt + kb_trc,
    } };
}
