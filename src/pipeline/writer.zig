// Realtime block writer: fetch → parse → transform → save pipeline for a single block.
// Delegates fetch+parse+transform to pipeline.fetchParseTransform (shared with historical).
const std = @import("std");
const linux = std.os.linux;

const core = @import("indexer/core");

const pipeline   = @import("pipeline.zig");
const pool       = @import("../db/pool.zig");
const http_pool  = @import("../rpc/pool.zig");
const batch      = @import("../db/batch.zig");
const cursor     = @import("../db/cursor.zig");

const EvmChainConfig = core.structures.EvmChainConfig;
const Allocator      = std.mem.Allocator;

fn nowNs() i64 {
    var ts: linux.timespec = undefined;
    _ = linux.clock_gettime(.MONOTONIC, &ts);
    return ts.sec * 1_000_000_000 + ts.nsec;
}

pub const ProcessBlockStatus = union(enum) {
    saved,
    retry_later,  // block not yet available — caller should retry
    fatal: anyerror,
};

pub fn processBlock(
    io:           std.Io,
    gpa:          Allocator,
    chain:        *const EvmChainConfig,
    rtConns:      *batch.RealtimeConns,
    rdb:          *cursor.Conn,
    bClient:      *core.fetch.Client,
    rClient:      *core.fetch.Client,
    tClient:      *core.fetch.Client,
    blockNum:     u64,
    hPool:        ?*http_pool.HttpPool,
    chunkBuckets: u64,
) ProcessBlockStatus {
    const rpcNode   = chain.rpcNodes.lotosArchiveNode;
    const chunkSize = @as(u64, @intCast(chain.indexingOptions.minifiedChunkSize));
    const bs = pool.BatchSizes.fromChain(chain.indexingOptions);

    var result = pipeline.BlockResult.init(gpa);
    defer result.deinit();

    switch (pipeline.fetchParseTransform(gpa, io, rpcNode, blockNum, chunkSize, chunkBuckets,
        bClient, rClient, tClient, hPool, &result))
    {
        .ok          => {},
        .retry_later => return .retry_later,
        .skip_missing => {
            std.debug.print("[realtime] block={d} stage=parse_block: null result\n", .{blockNum});
            return .retry_later;
        },
        .fatal => |e| {
            std.debug.print("[realtime] block={d} stage=fpt error: {s}\n", .{ blockNum, @errorName(e) });
            return .{ .fatal = e };
        },
    }

    const fetch_ms     = @as(f64, @floatFromInt(result.fetchNs))     / 1e6;
    const parse_ms     = @as(f64, @floatFromInt(result.parseNs))     / 1e6;
    const transform_ms = @as(f64, @floatFromInt(result.transformNs)) / 1e6;

    const t_save = nowNs();
    batch.saveBlockRt(rtConns, &result.ent, bs) catch |e| return .{ .fatal = e };
    const save_ms = @as(f64, @floatFromInt(nowNs() - t_save)) / 1e6;

    const t_cursor = nowNs();
    var cursorBuf: [20]u8 = undefined;
    const cursorStr = std.fmt.bufPrint(&cursorBuf, "{d}", .{blockNum}) catch unreachable;
    rdb.setWithRetry("LATEST_PROCESSED_BLOCK_NUMBER", cursorStr) catch |e| return .{ .fatal = e };
    const cursor_ms = @as(f64, @floatFromInt(nowNs() - t_cursor)) / 1e6;

    const total_ms = fetch_ms + parse_ms + transform_ms + save_ms + cursor_ms;
    const kb_blk  = result.rawData.?.block.body.len    / 1024;
    const kb_rcpt = result.rawData.?.receipts.body.len / 1024;
    const kb_trc  = result.rawData.?.traces.body.len   / 1024;
    std.debug.print(
        "[rt] blk={d} tx={d} log={d} itx={d} kb={d}+{d}+{d} | fetch={d:.0} parse={d:.0} xform={d:.0} save={d:.0} cursor={d:.0} | total={d:.0}ms\n",
        .{ blockNum, result.ent.txs.items.len, result.ent.logs.items.len, result.ent.internalTxs.items.len,
           kb_blk, kb_rcpt, kb_trc,
           fetch_ms, parse_ms, transform_ms, save_ms, cursor_ms, total_ms });

    return .saved;
}
