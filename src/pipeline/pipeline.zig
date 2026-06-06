// Historical pipeline: MPSC work-queue, accumulator + double-buffer save.
//
// N persistent workers atomically claim blocks, fetch+transform, push *BlockResult
// to a pipe channel.  Workers never touch Scylla; heavy writes are isolated in the save stage.
//
// Main thread accumulates SAVE_EVERY results, then saves in a background thread
// while workers keep fetching the next batch (fetch and save overlap).
// Cursor (Redis) advances only after save completes.
const std    = @import("std");
const linux  = std.os.linux;

const core = @import("indexer/core");

const fetcher     = @import("fetcher.zig");
const parser      = @import("parser.zig");
const transformer = @import("transformer.zig");
const pool        = @import("../db/pool.zig");
const http_pool   = @import("../rpc/pool.zig");
const batch       = @import("../db/batch.zig");
const cursor      = @import("../db/cursor.zig");

const EvmChainConfig   = core.structures.EvmChainConfig;
const EvmRpcNodeConfig = core.structures.EvmRpcNodeConfig;
const FetchClient      = core.fetch.Client;
const Allocator        = std.mem.Allocator;

fn nowNs() i64 {
    var ts: linux.timespec = undefined;
    _ = linux.clock_gettime(.MONOTONIC, &ts);
    return ts.sec * 1_000_000_000 + ts.nsec;
}

// ─── Constants ────────────────────────────────────────────────────────────────

// Default accumulator batch size; override via env SAVE_EVERY.
pub const SAVE_EVERY_DEFAULT: usize = 24;

// Logical chunk bucket count: chunk = blockNum % SCYLLA_CHUNK_BUCKETS.
// Distributes rows across Scylla partitions; set to match the cluster's SMP count.
// Override via env SCYLLA_CHUNK_BUCKETS (default 24).
pub const SCYLLA_CHUNK_BUCKETS_DEFAULT: u64 = 24;

// Connection split matching production SPLIT=1,3,6,20,1,1.
const TXS_LANES: usize = batch.ACCUM_TXS_LANES;
const LOG_LANES: usize = batch.ACCUM_LOG_LANES;
const ITX_LANES: usize = batch.ACCUM_ITX_LANES;

// ─── BlockResult ──────────────────────────────────────────────────────────────

pub const BlockResult = struct {
    gpa:      Allocator,
    arena:    std.heap.ArenaAllocator,
    // Owns the raw HTTP response buffers (block.body, receipts.body, traces.body).
    // ZC-parsed strings in `ent` point directly into these buffers — no copies.
    // Must outlive saveEntitiesParallel; freed in deinit() after save completes.
    rawData:  ?fetcher.Response,
    ent:      transformer.Entities,
    blockNum: u64,
    ok:       bool,
    err:      ?anyerror,
    // Per-stage timings set by fetchParseTransform; callers may read for logging.
    fetchNs:     i64,
    parseNs:     i64,
    transformNs: i64,

    pub fn init(gpa: Allocator) BlockResult {
        return .{
            .gpa         = gpa,
            .arena       = std.heap.ArenaAllocator.init(std.heap.page_allocator),
            .rawData     = null,
            .ent         = transformer.initEntities(),
            .blockNum    = 0,
            .ok          = false,
            .err         = null,
            .fetchNs     = 0,
            .parseNs     = 0,
            .transformNs = 0,
        };
    }

    pub fn deinit(self: *BlockResult) void {
        self.arena.deinit();
        if (self.rawData) |*d| d.deinit(self.gpa);
    }
};

// ─── MPSC result channel ──────────────────────────────────────────────────────

const ResultChan = struct {
    rd:           i32,
    wr:           i32,
    workersAlive: std.atomic.Value(u32),

    fn init(n: u32) !ResultChan {
        var fds: [2]i32 = undefined;
        if (linux.pipe(&fds) != 0) return error.PipeFailed;
        return .{ .rd = fds[0], .wr = fds[1], .workersAlive = .init(n) };
    }

    fn push(self: *ResultChan, r: *BlockResult) void {
        const ptr: usize = @intFromPtr(r);
        _ = linux.write(self.wr, @ptrCast(&ptr), 8);
    }

    fn pop(self: *ResultChan) ?*BlockResult {
        var ptr: usize = undefined;
        var total: usize = 0;
        const buf: [*]u8 = @ptrCast(&ptr);
        while (total < 8) {
            const n = linux.read(self.rd, buf + total, 8 - total);
            if (n == 0 or @as(isize, @bitCast(n)) < 0) return null;
            total += n;
        }
        return @ptrFromInt(ptr);
    }

    fn workerExit(self: *ResultChan) void {
        if (self.workersAlive.fetchSub(1, .acq_rel) == 1)
            _ = linux.close(self.wr);
    }

    fn deinit(self: *ResultChan) void { _ = linux.close(self.rd); }
};

// ─── Worker ───────────────────────────────────────────────────────────────────

const WorkerArgs = struct {
    io:           std.Io,
    gpa:          Allocator,
    chain:        *const EvmChainConfig,
    next:         *std.atomic.Value(u64),
    to:           u64,
    chan:          *ResultChan,
    chunkBuckets: u64,
    cancel:       *std.atomic.Value(bool),
};

pub const FetchTransformStatus = union(enum) {
    ok,
    retry_later,    // null RPC response — block not yet available
    skip_missing,   // block parsed as null/empty — no data to save
    fatal: anyerror,
};

/// Shared fetch+parse+transform core used by both historical and realtime pipelines.
/// Stores raw HTTP buffers in result.rawData (ZC ownership).
/// Sets result.fetchNs / parseNs / transformNs for caller logging.
/// Does NOT save, does NOT advance cursor — callers handle that differently.
pub fn fetchParseTransform(
    gpa:          Allocator,
    io:           std.Io,
    rpcNode:      EvmRpcNodeConfig,
    blockNum:     u64,
    chunkSize:    u64,
    chunkBuckets: u64,
    bClient:      *FetchClient,
    rClient:      *FetchClient,
    tClient:      *FetchClient,
    httpPool:     ?*http_pool.HttpPool,
    result:       *BlockResult,
) FetchTransformStatus {
    const t0 = nowNs();
    const maybeData = fetcher.getConsistentBlockData(gpa, io, .{
        .rpcNode        = rpcNode,
        .blockNumber    = blockNum,
        .blockClient    = bClient,
        .receiptsClient = rClient,
        .tracesClient   = tClient,
        .httpPool       = httpPool,
        .skipLogs       = true,
    }) catch |e| return .{ .fatal = e };
    const data = maybeData orelse return .retry_later;
    result.fetchNs = nowNs() - t0;
    // Transfer ownership: buffers live until BlockResult.deinit() after save.
    result.rawData = data;

    const t1 = nowNs();
    const aa    = result.arena.allocator();
    const block = parser.parseBlockRespZC(data.block.body, aa) catch |e|
        return .{ .fatal = e };
    if (block == null) return .skip_missing;
    const receipts = (parser.parseReceiptsRespZC(data.receipts.body, aa) catch null) orelse &.{};
    const traces   = (parser.parseTracesRespZC(data.traces.body, aa)     catch null) orelse &.{};
    result.parseNs = nowNs() - t1;

    const t2 = nowNs();
    transformer.transformBlockWithRemap(
        aa, block.?, receipts, traces, chunkSize, chunkBuckets, &result.ent,
    ) catch |e| return .{ .fatal = e };
    result.transformNs = nowNs() - t2;

    return .ok;
}

fn workerEntry(args: *WorkerArgs) void {
    defer {
        args.chan.workerExit();
        args.gpa.destroy(args);
    }
    worker(args) catch |e|
        std.debug.print("[worker] fatal: {s}\n", .{@errorName(e)});
}

fn worker(args: *WorkerArgs) !void {
    const gpa          = args.gpa;
    const chain        = args.chain;
    const rpcNode      = chain.rpcNodes.lotosArchiveNode;
    const chunkSize    = @as(u64, @intCast(chain.indexingOptions.minifiedChunkSize));
    const chunkBuckets = args.chunkBuckets;

    var bClient = FetchClient.init(gpa, args.io);
    var rClient = FetchClient.init(gpa, args.io);
    var tClient = FetchClient.init(gpa, args.io);
    defer bClient.deinit();
    defer rClient.deinit();
    defer tClient.deinit();

    while (true) {
        if (args.cancel.load(.acquire)) break;
        const blockNum = args.next.fetchAdd(1, .monotonic);
        if (blockNum > args.to) break;

        const result = try gpa.create(BlockResult);
        result.* = BlockResult.init(gpa);
        result.blockNum = blockNum;

        retry: while (true) {
            switch (fetchParseTransform(gpa, args.io, rpcNode, blockNum, chunkSize, chunkBuckets, &bClient, &rClient, &tClient, null, result)) {
                .ok           => {
                    result.ok = true;
                    std.debug.print("[{d}] T:{d} L:{d} IT:{d}\n", .{
                        blockNum,
                        result.ent.txs.items.len,
                        result.ent.logs.items.len,
                        result.ent.internalTxs.items.len,
                    });
                    break :retry;
                },
                .skip_missing => {
                    std.debug.print("[worker] block={d} skip_missing\n", .{blockNum});
                    break :retry;
                },
                .retry_later  => {
                    if (args.cancel.load(.acquire)) break :retry;
                    const ts = linux.timespec{ .sec = 0, .nsec = 200_000_000 };
                    _ = linux.nanosleep(&ts, null);
                    result.arena.deinit();
                    result.arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
                    result.ent = transformer.initEntities();
                },
                .fatal        => |e| {
                    result.err = e;
                    std.debug.print("[worker] block={d} fatal: {s}\n", .{ blockNum, @errorName(e) });
                    break :retry;
                },
            }
        }
        args.chan.push(result);
    }
}

// ─── AccumState ───────────────────────────────────────────────────────────────

const AccumState = struct {
    gpa:        Allocator,
    sources:    std.ArrayList(*BlockResult),
    entPtrs:    std.ArrayList(*const transformer.Entities),
    blockStart: u64,
    blockEnd:   u64,
    saveErr:    ?anyerror,
    saveMs:     f64,

    fn init(gpa: Allocator) AccumState {
        return .{
            .gpa        = gpa,
            .sources    = .empty,
            .entPtrs    = .empty,
            .blockStart = std.math.maxInt(u64),
            .blockEnd   = 0,
            .saveErr    = null,
            .saveMs     = 0,
        };
    }

    fn add(self: *AccumState, r: *BlockResult) !void {
        try self.sources.append(self.gpa, r);
        if (r.ok) {
            try self.entPtrs.append(self.gpa, &r.ent);
            if (r.blockNum < self.blockStart) self.blockStart = r.blockNum;
            if (r.blockNum > self.blockEnd)   self.blockEnd   = r.blockNum;
        }
    }

    fn savedCount(self: *const AccumState) usize {
        return self.entPtrs.items.len;
    }

    fn deinit(self: *AccumState) void {
        for (self.sources.items) |r| { r.deinit(); self.gpa.destroy(r); }
        self.sources.deinit(self.gpa);
        self.entPtrs.deinit(self.gpa);
    }
};

// ─── SaveArgs + save thread ───────────────────────────────────────────────────

const SaveArgs = struct {
    accum:      *AccumState,
    cBlocks:    *pool.CqlConn,
    cTxs:       *[TXS_LANES]pool.CqlConn,
    cContracts: *pool.CqlConn,
    cLogs:      *[LOG_LANES]pool.CqlConn,
    cItxs:      *[ITX_LANES]pool.CqlConn,
    cComp:      *pool.CqlConn,
    rdb:        *cursor.Conn,
    bs:         pool.BatchSizes,
};

fn saveAccumFn(args: *SaveArgs) void {
    const ac = args.accum;
    if (ac.entPtrs.items.len == 0) return;

    const t0 = nowNs();
    batch.saveEntitiesParallel(
        args.cBlocks,
        args.cTxs,
        args.cContracts,
        args.cLogs, args.cItxs, args.cComp,
        ac.entPtrs.items,
        args.bs,
    ) catch |e| { ac.saveErr = e; return; };

    ac.saveMs = @as(f64, @floatFromInt(nowNs() - t0)) / 1e6;

    // Advance Redis cursor to the last saved block.
    var maxBlock: u64 = 0;
    for (ac.entPtrs.items) |ep| {
        if (ep.lastBlock > maxBlock) maxBlock = ep.lastBlock;
    }
    if (maxBlock > 0) {
        var buf: [20]u8 = undefined;
        const s = std.fmt.bufPrint(&buf, "{d}", .{maxBlock}) catch unreachable;
        args.rdb.setWithRetry("LATEST_PROCESSED_BLOCK_NUMBER", s) catch {};
    }
}

const PrevSave = struct {
    thread: std.Thread,
    accum:  *AccumState,
    args:   *SaveArgs,

    fn start(gpa: Allocator, args: SaveArgs) !PrevSave {
        const heapArgs = try gpa.create(SaveArgs);
        heapArgs.* = args;
        const thread = std.Thread.spawn(.{}, saveAccumFn, .{heapArgs}) catch |e| {
            gpa.destroy(heapArgs);
            return e;
        };
        return .{ .thread = thread, .accum = args.accum, .args = heapArgs };
    }

    fn finish(self: *PrevSave, gpa: Allocator, blocksDone: *u64, t0: i64) !void {
        self.thread.join();
        gpa.destroy(self.args);
        defer {
            self.accum.deinit();
            gpa.destroy(self.accum);
        }
        if (self.accum.saveErr) |e| return e;
        blocksDone.* += self.accum.savedCount();
        const ms = @as(f64, @floatFromInt(nowNs() - t0)) / 1e6;
        std.debug.print(
            "\nAccum {d}→{d}: saved={d} save={d:.0}ms | {d:.1} blk/s avg\n\n",
            .{ self.accum.blockStart, self.accum.blockEnd, self.accum.savedCount(),
               self.accum.saveMs, @as(f64, @floatFromInt(blocksDone.*)) / ms * 1000.0 },
        );
    }
};

// ─── runHistorical ────────────────────────────────────────────────────────────

pub fn runHistorical(
    io:           std.Io,
    gpa:          Allocator,
    chain:        *const EvmChainConfig,
    from:         u64,
    to:           u64,
    scyllaHost:   []const u8,
    scyllaPort:   u16,
    scyllaKs:     []const u8,
    scyllaUser:   []const u8,
    scyllaPass:   []const u8,
    redisUrl:     []const u8,
    chunkBuckets: u64,
    saveEvery:    usize,
) !void {
    if (from > to) return;

    const workerCount = chain.indexingOptions.workerCount;
    const bs          = pool.BatchSizes.fromChain(chain.indexingOptions);
    const rUrl        = cursor.parseUrl(redisUrl);

    std.debug.print(
        "Historical: blocks {d}→{d}  workers={d}  save_every={d}  chunk_buckets={d}  split=1,3,6,20,1,1\n\n",
        .{ from, to, workerCount, saveEvery, chunkBuckets });

    var cBlocks = try pool.CqlConn.init(gpa, scyllaHost, scyllaPort, scyllaKs, scyllaUser, scyllaPass);
    defer cBlocks.deinit();
    var cContracts = try pool.CqlConn.init(gpa, scyllaHost, scyllaPort, scyllaKs, scyllaUser, scyllaPass);
    defer cContracts.deinit();
    var cComp = try pool.CqlConn.init(gpa, scyllaHost, scyllaPort, scyllaKs, scyllaUser, scyllaPass);
    defer cComp.deinit();

    var cTxs:  [TXS_LANES]pool.CqlConn = undefined;
    var cTxsN: usize = 0;
    defer for (0..cTxsN) |i| cTxs[i].deinit();
    for (0..TXS_LANES) |i| {
        cTxs[i] = try pool.CqlConn.init(
            gpa, scyllaHost, scyllaPort, scyllaKs, scyllaUser, scyllaPass);
        cTxsN += 1;
    }

    var cLogs:  [LOG_LANES]pool.CqlConn = undefined;
    var cLogsN: usize = 0;
    defer for (0..cLogsN) |i| cLogs[i].deinit();
    for (0..LOG_LANES) |i| {
        cLogs[i] = try pool.CqlConn.init(
            gpa, scyllaHost, scyllaPort, scyllaKs, scyllaUser, scyllaPass);
        cLogsN += 1;
    }

    var cItxs:  [ITX_LANES]pool.CqlConn = undefined;
    var cItxsN: usize = 0;
    defer for (0..cItxsN) |i| cItxs[i].deinit();
    for (0..ITX_LANES) |i| {
        cItxs[i] = try pool.CqlConn.init(
            gpa, scyllaHost, scyllaPort, scyllaKs, scyllaUser, scyllaPass);
        cItxsN += 1;
    }

    var rdb = try cursor.Conn.init(gpa, rUrl.host, rUrl.port);
    defer rdb.deinit();
    if (rUrl.password.len > 0) rdb.auth(rUrl.password) catch {};
    if (rUrl.db > 0) rdb.selectDb(rUrl.db) catch {};

    var next   = std.atomic.Value(u64).init(from);
    var cancel = std.atomic.Value(bool).init(false);
    var chan    = try ResultChan.init(@intCast(workerCount));
    defer chan.deinit();

    const threads = try gpa.alloc(std.Thread, workerCount);
    defer gpa.free(threads);
    for (0..workerCount) |w| {
        const wargs = try gpa.create(WorkerArgs);
        wargs.* = .{
            .io           = io,
            .gpa          = gpa,
            .chain        = chain,
            .next         = &next,
            .to           = to,
            .chan         = &chan,
            .chunkBuckets = chunkBuckets,
            .cancel       = &cancel,
        };
        threads[w] = try std.Thread.spawn(
            .{ .stack_size = 4 * 1024 * 1024 }, workerEntry, .{wargs});
    }

    var prevSave: ?PrevSave = null;
    var accum = try gpa.create(AccumState);
    accum.* = AccumState.init(gpa);

    const t0         = nowNs();
    var blocksDone: u64 = 0;

    while (chan.pop()) |result| {
        if (result.err) |e| {
            std.debug.print("[historical] fatal error at block {d}: {s} — aborting\n",
                .{ result.blockNum, @errorName(e) });
            result.deinit();
            gpa.destroy(result);
            cancel.store(true, .release);
            while (chan.pop()) |r| { r.deinit(); gpa.destroy(r); }
            for (threads) |t| t.join();
            accum.deinit();
            gpa.destroy(accum);
            if (prevSave) |*ps| {
                ps.thread.join();
                gpa.destroy(ps.args);
                ps.accum.deinit();
                gpa.destroy(ps.accum);
            }
            return e;
        }
        try accum.add(result);

        if (accum.entPtrs.items.len >= saveEvery) {
            if (prevSave) |*ps| {
                try ps.finish(gpa, &blocksDone, t0);
                prevSave = null;
            }
            prevSave = try PrevSave.start(gpa, .{
                .accum      = accum,
                .cBlocks    = &cBlocks,
                .cTxs       = &cTxs,
                .cContracts = &cContracts,
                .cLogs      = &cLogs,
                .cItxs      = &cItxs,
                .cComp      = &cComp,
                .rdb        = &rdb,
                .bs         = bs,
            });
            accum  = try gpa.create(AccumState);
            accum.* = AccumState.init(gpa);
        }
    }

    for (threads) |t| t.join();

    if (prevSave) |*ps| {
        try ps.finish(gpa, &blocksDone, t0);
        prevSave = null;
    }

    if (accum.savedCount() > 0) {
        var finalSave = try PrevSave.start(gpa, .{
            .accum      = accum,
            .cBlocks    = &cBlocks,
            .cTxs       = &cTxs,
            .cContracts = &cContracts,
            .cLogs      = &cLogs,
            .cItxs      = &cItxs,
            .cComp      = &cComp,
            .rdb        = &rdb,
            .bs         = bs,
        });
        try finalSave.finish(gpa, &blocksDone, t0);
    } else {
        accum.deinit();
        gpa.destroy(accum);
    }

    const elapsedMs = @as(f64, @floatFromInt(nowNs() - t0)) / 1e6;
    std.debug.print("\nHistorical sync done: {d:.0}ms  saved_blocks={d}\n",
        .{ elapsedMs, blocksDone });
}
