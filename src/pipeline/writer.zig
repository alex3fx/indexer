// Realtime block writer: fetch → parse → transform → save pipeline for a single block.
const std = @import("std");
const linux = std.os.linux;

const core = @import("indexer/core");

const fetcher     = @import("fetcher.zig");
const parser      = @import("parser.zig");
const transformer = @import("transformer.zig");
const pool        = @import("../db/pool.zig");
const http_pool   = @import("../rpc/pool.zig");
const batch       = @import("../db/batch.zig");
const cursor      = @import("../db/cursor.zig");

const EvmChainConfig = core.structures.EvmChainConfig;
const FetchClient    = core.fetch.Client;
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
    io:       std.Io,
    gpa:      Allocator,
    chain:    *const EvmChainConfig,
    rtConns:  *batch.RealtimeConns,
    rdb:      *cursor.Conn,
    bClient:  *FetchClient,
    rClient:  *FetchClient,
    tClient:  *FetchClient,
    blockNum: u64,
    arena:    *std.heap.ArenaAllocator,
    hPool:    ?*http_pool.HttpPool,
    gzip:     bool,
) ProcessBlockStatus {
    const rpcNode   = chain.rpcNodes.lotosArchiveNode;
    const chunkSize = @as(u64, @intCast(chain.indexingOptions.minifiedChunkSize));
    const bs = pool.BatchSizes.fromChain(chain.indexingOptions);

    const t_fetch = nowNs();
    const maybeData = fetcher.getConsistentBlockData(gpa, io, .{
        .rpcNode        = rpcNode,
        .blockNumber    = blockNum,
        .blockClient    = bClient,
        .receiptsClient = rClient,
        .tracesClient   = tClient,
        .httpPool       = hPool,
        .skipLogs       = true,
        .gzip           = gzip,
    }) catch |e| {
        std.debug.print("[realtime] block={d} stage=fetch error: {s}\n", .{ blockNum, @errorName(e) });
        return .{ .fatal = e };
    };
    const fetch_ms = @as(f64, @floatFromInt(nowNs() - t_fetch)) / 1e6;

    var data = maybeData orelse return .retry_later;
    defer data.deinit(gpa);

    _ = arena.reset(.retain_capacity);
    const arenaAlloc = arena.allocator();

    const t_parse = nowNs();
    const block = (parser.parseBlockRespZC(data.block.body, arenaAlloc) catch |e| {
        std.debug.print("[realtime] block={d} stage=parse_block error: {s}\n", .{ blockNum, @errorName(e) });
        return .{ .fatal = e };
    }) orelse {
        std.debug.print("[realtime] block={d} stage=parse_block: null result\n", .{blockNum});
        return .retry_later;
    };
    const receipts = (parser.parseReceiptsRespZC(data.receipts.body, arenaAlloc) catch null) orelse &.{};
    const traces   = (parser.parseTracesRespZC(data.traces.body, arenaAlloc)     catch null) orelse &.{};
    const parse_ms = @as(f64, @floatFromInt(nowNs() - t_parse)) / 1e6;

    const t_transform = nowNs();
    var ent = transformer.initEntities();
    transformer.transformBlock(arenaAlloc, block, receipts, traces, chunkSize, &ent) catch |e| {
        std.debug.print("[realtime] block={d} stage=transform error: {s}\n", .{ blockNum, @errorName(e) });
        return .{ .fatal = e };
    };
    const transform_ms = @as(f64, @floatFromInt(nowNs() - t_transform)) / 1e6;

    const t_save = nowNs();
    batch.saveBlockRt(rtConns, &ent, bs) catch |e| return .{ .fatal = e };
    const save_ms = @as(f64, @floatFromInt(nowNs() - t_save)) / 1e6;

    const t_cursor = nowNs();
    var cursorBuf: [20]u8 = undefined;
    const cursorStr = std.fmt.bufPrint(&cursorBuf, "{d}", .{blockNum}) catch unreachable;
    rdb.setWithRetry("LATEST_PROCESSED_BLOCK_NUMBER", cursorStr) catch |e| return .{ .fatal = e };
    const cursor_ms = @as(f64, @floatFromInt(nowNs() - t_cursor)) / 1e6;

    const total_ms = fetch_ms + parse_ms + transform_ms + save_ms + cursor_ms;
    const kb_blk  = data.block.body.len    / 1024;
    const kb_rcpt = data.receipts.body.len / 1024;
    const kb_trc  = data.traces.body.len   / 1024;
    std.debug.print(
        "[rt] blk={d} tx={d} log={d} itx={d} kb={d}+{d}+{d} | fetch={d:.0} parse={d:.0} xform={d:.0} save={d:.0} cursor={d:.0} | total={d:.0}ms\n",
        .{ blockNum, ent.txs.items.len, ent.logs.items.len, ent.internalTxs.items.len,
           kb_blk, kb_rcpt, kb_trc,
           fetch_ms, parse_ms, transform_ms, save_ms, cursor_ms, total_ms });

    return .saved;
}
