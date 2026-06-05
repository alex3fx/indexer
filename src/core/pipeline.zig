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

const core      = @import("indexer/core");
const utils     = @import("indexer/utils");
const scylla    = @import("common/scylla.zig");
const redis     = @import("common/redis.zig");
const rpcSpec   = @import("common/chains/evm/on_chain/rpc_spec.zig");
const transform = @import("common/chains/evm/transform.zig");
const rpcApi    = @import("common/chains/evm/on_chain/rpc.zig");

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

// Accumulate this many blocks before each Scylla flush.
// Matches production SAVE_EVERY=24: keeps batches small → low Scylla latency.
const SAVE_EVERY: usize = 24;

// Logical chunk bucket count: chunk = blockNum % SCYLLA_CHUNK_BUCKETS.
// Distributes rows across Scylla partitions; set to match the cluster's SMP count.
// Override via env SCYLLA_CHUNK_BUCKETS (default 24).
pub const SCYLLA_CHUNK_BUCKETS_DEFAULT: u64 = 24;

// Connection split matching production SPLIT=1,3,6,20,1,1.
const TXS_LANES: usize = scylla.ACCUM_TXS_LANES;
const LOG_LANES: usize = scylla.ACCUM_LOG_LANES;
const ITX_LANES: usize = scylla.ACCUM_ITX_LANES;

// ─── BlockResult ──────────────────────────────────────────────────────────────
// Heap-allocated per block.  Worker fills it and pushes the pointer to the channel.
// Arena outlives the save: freed by AccumState.deinit after save completes.

// ok=true:              save candidate.
// err!=null:            fatal fetch/parse/transform — historical aborts.
// ok=false && err=null: missing/empty block marker — skipped by AccumState.
const BlockResult = struct {
    arena:    std.heap.ArenaAllocator,
    ent:      transform.Entities,
    blockNum: u64,
    ok:       bool,
    err:      ?anyerror,

    fn init() BlockResult {
        return .{
            .arena    = std.heap.ArenaAllocator.init(std.heap.page_allocator),
            .ent      = transform.initEntities(),
            .blockNum = 0,
            .ok       = false,
            .err      = null,
        };
    }

    fn deinit(self: *BlockResult) void { self.arena.deinit(); }
};

// ─── MPSC result channel ──────────────────────────────────────────────────────
// Workers push *BlockResult (8 bytes) through a Linux pipe.
// pop() blocks until a pointer arrives; returns null on EOF (last worker exits).

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
// Atomically claims blocks from `next`, fetches and transforms each one,
// then pushes a *BlockResult to the channel.  Never touches Scylla.

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

const FetchTransformStatus = union(enum) {
    ok,
    retry_later,    // null RPC response — block not yet available
    skip_missing,   // block parsed as null/empty — no data to save
    fatal: anyerror,
};

// Fetch, parse and transform one block into result.ent.
// Returns the outcome as an explicit tagged union; caller decides how to handle each case.
// retry_later: null RPC response — caller is responsible for sleep/backoff/cancel checks.
fn fetchAndTransform(
    gpa:          Allocator,
    io:           std.Io,
    rpcNode:      EvmRpcNodeConfig,
    blockNum:     u64,
    chunkSize:    u64,
    chunkBuckets: u64,
    bClient:      *FetchClient,
    rClient:      *FetchClient,
    tClient:      *FetchClient,
    result:       *BlockResult,
) FetchTransformStatus {
    const maybeData = core.getConsistentBlockData(gpa, io, .{
        .rpcNode        = rpcNode,
        .blockNumber    = blockNum,
        .blockClient    = bClient,
        .receiptsClient = rClient,
        .tracesClient   = tClient,
        .skipLogs       = true,
    }) catch |e| return .{ .fatal = e };
    var data = maybeData orelse return .retry_later;
    defer data.deinit(gpa);

    const aa    = result.arena.allocator();
    const block = rpcSpec.parseBlockResp(data.block.body, aa) catch |e|
        return .{ .fatal = e };
    if (block == null) return .skip_missing;
    const receipts = (rpcSpec.parseReceiptsResp(data.receipts.body, aa) catch null) orelse &.{};
    const traces   = (rpcSpec.parseTracesResp(data.traces.body, aa)     catch null) orelse &.{};

    transform.transformBlockWithRemap(
        aa, block.?, receipts, traces, chunkSize, chunkBuckets, &result.ent,
    ) catch |e| return .{ .fatal = e };

    std.debug.print("[{d}] T:{d} L:{d} IT:{d}\n", .{
        blockNum,
        result.ent.txs.items.len,
        result.ent.logs.items.len,
        result.ent.internalTxs.items.len,
    });
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
        result.* = BlockResult.init();
        result.blockNum = blockNum;

        retry: while (true) {
            switch (fetchAndTransform(gpa, args.io, rpcNode, blockNum, chunkSize, chunkBuckets, &bClient, &rClient, &tClient, result)) {
                .ok           => { result.ok = true; break :retry; },
                .skip_missing => {
                    // ok=false, err=null: AccumState skips, channel tracks progress
                    std.debug.print("[worker] block={d} skip_missing\n", .{blockNum});
                    break :retry;
                },
                .retry_later  => {
                    if (args.cancel.load(.acquire)) break :retry;
                    const ts = linux.timespec{ .sec = 0, .nsec = 200_000_000 };
                    _ = linux.nanosleep(&ts, null);
                    // Reset arena and entities for next attempt
                    result.arena.deinit();
                    result.arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
                    result.ent = transform.initEntities();
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
// Collects up to SAVE_EVERY BlockResult pointers.
// entPtrs holds the subset where ok=true (valid parsed blocks).
// Source arenas must stay alive until after save completes (strings point into them).

const AccumState = struct {
    gpa:        Allocator,
    sources:    std.ArrayList(*BlockResult),
    entPtrs:    std.ArrayList(*const transform.Entities),
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

    // Number of blocks actually saved (ok=true results).
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
    cBlocks:    *scylla.CqlConn,
    cTxs:       *[TXS_LANES]scylla.CqlConn,
    cContracts: *scylla.CqlConn,
    cLogs:      *[LOG_LANES]scylla.CqlConn,
    cItxs:      *[ITX_LANES]scylla.CqlConn,
    cComp:      *scylla.CqlConn,
    rdb:        *redis.Conn,
    bs:         scylla.BatchSizes,
};

fn saveAccumFn(args: *SaveArgs) void {
    const ac = args.accum;
    if (ac.entPtrs.items.len == 0) return;

    const t0 = nowNs();
    scylla.saveEntitiesParallel(
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

// ─── PrevSave ─────────────────────────────────────────────────────────────────
// Bundles the background save thread with its heap-allocated args and accumulator.
// Heap-allocating args keeps the pointer stable for the thread regardless of
// what happens to PrevSave itself on the caller's stack.

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

    // Join the thread, log the result, free resources.
    // accum is always freed; save error (if any) is returned to caller.
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
) !void {
    if (from > to) return;

    const workerCount = chain.indexingOptions.workerCount;
    const bs          = scylla.BatchSizes.fromChain(chain.indexingOptions);
    const rUrl        = redis.parseUrl(redisUrl);

    std.debug.print(
        "Historical: blocks {d}→{d}  workers={d}  save_every={d}  chunk_buckets={d}  split=1,3,6,20,1,1\n\n",
        .{ from, to, workerCount, SAVE_EVERY, chunkBuckets });

    // Open 32 persistent CQL connections: split 1,3,6,20,1,1 (blocks,txs,contracts,logs,itxs,comp).
    // Arrays are init one-at-a-time; cXxxN tracks how many succeeded so the
    // defer closes only the connections that were actually opened.
    var cBlocks = try scylla.CqlConn.init(gpa, scyllaHost, scyllaPort, scyllaKs, scyllaUser, scyllaPass);
    defer cBlocks.deinit();
    var cContracts = try scylla.CqlConn.init(gpa, scyllaHost, scyllaPort, scyllaKs, scyllaUser, scyllaPass);
    defer cContracts.deinit();
    var cComp = try scylla.CqlConn.init(gpa, scyllaHost, scyllaPort, scyllaKs, scyllaUser, scyllaPass);
    defer cComp.deinit();

    var cTxs:  [TXS_LANES]scylla.CqlConn = undefined;
    var cTxsN: usize = 0;
    defer for (0..cTxsN) |i| cTxs[i].deinit();
    for (0..TXS_LANES) |i| {
        cTxs[i] = try scylla.CqlConn.init(
            gpa, scyllaHost, scyllaPort, scyllaKs, scyllaUser, scyllaPass);
        cTxsN += 1;
    }

    var cLogs:  [LOG_LANES]scylla.CqlConn = undefined;
    var cLogsN: usize = 0;
    defer for (0..cLogsN) |i| cLogs[i].deinit();
    for (0..LOG_LANES) |i| {
        cLogs[i] = try scylla.CqlConn.init(
            gpa, scyllaHost, scyllaPort, scyllaKs, scyllaUser, scyllaPass);
        cLogsN += 1;
    }

    var cItxs:  [ITX_LANES]scylla.CqlConn = undefined;
    var cItxsN: usize = 0;
    defer for (0..cItxsN) |i| cItxs[i].deinit();
    for (0..ITX_LANES) |i| {
        cItxs[i] = try scylla.CqlConn.init(
            gpa, scyllaHost, scyllaPort, scyllaKs, scyllaUser, scyllaPass);
        cItxsN += 1;
    }

    var rdb = try redis.Conn.init(gpa, rUrl.host, rUrl.port);
    defer rdb.deinit();
    if (rUrl.password.len > 0) rdb.auth(rUrl.password) catch {};
    if (rUrl.db > 0) rdb.selectDb(rUrl.db) catch {};

    // Spawn N persistent workers.
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

    // Accumulate results and double-buffer save.
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
            // Signal workers to stop, drain remaining results, then join all threads.
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

        if (accum.entPtrs.items.len >= SAVE_EVERY) {
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

    // Final partial flush (< SAVE_EVERY blocks remaining).
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

// ─── processBlock (realtime) ──────────────────────────────────────────────────

pub const ProcessBlockStatus = union(enum) {
    saved,
    retry_later,  // block not yet available — caller should retry
    fatal: anyerror,
};

pub fn processBlock(
    io:       std.Io,
    gpa:      Allocator,
    chain:    *const EvmChainConfig,
    rtConns:  *scylla.RealtimeConns,
    rdb:      *redis.Conn,
    bClient:  *FetchClient,
    rClient:  *FetchClient,
    tClient:  *FetchClient,
    blockNum: u64,
    arena:    *std.heap.ArenaAllocator,
) ProcessBlockStatus {
    const rpcNode   = chain.rpcNodes.lotosArchiveNode;
    const chunkSize = @as(u64, @intCast(chain.indexingOptions.minifiedChunkSize));
    const bs = scylla.BatchSizes.fromChain(chain.indexingOptions);

    const t_fetch = nowNs();
    const maybeData = core.getConsistentBlockData(gpa, io, .{
        .rpcNode        = rpcNode,
        .blockNumber    = blockNum,
        .blockClient    = bClient,
        .receiptsClient = rClient,
        .tracesClient   = tClient,
        .skipLogs       = true,
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
    const block = (rpcSpec.parseBlockRespZC(data.block.body, arenaAlloc) catch |e| {
        std.debug.print("[realtime] block={d} stage=parse_block error: {s}\n", .{ blockNum, @errorName(e) });
        return .{ .fatal = e };
    }) orelse {
        std.debug.print("[realtime] block={d} stage=parse_block: null result\n", .{blockNum});
        return .retry_later;
    };
    const receipts = (rpcSpec.parseReceiptsRespZC(data.receipts.body, arenaAlloc) catch null) orelse &.{};
    const traces   = (rpcSpec.parseTracesRespZC(data.traces.body, arenaAlloc)     catch null) orelse &.{};
    const parse_ms = @as(f64, @floatFromInt(nowNs() - t_parse)) / 1e6;

    const t_transform = nowNs();
    var ent = transform.initEntities();
    transform.transformBlock(arenaAlloc, block, receipts, traces, chunkSize, &ent) catch |e| {
        std.debug.print("[realtime] block={d} stage=transform error: {s}\n", .{ blockNum, @errorName(e) });
        return .{ .fatal = e };
    };
    const transform_ms = @as(f64, @floatFromInt(nowNs() - t_transform)) / 1e6;

    const t_save = nowNs();
    scylla.saveBlockRt(rtConns, &ent, bs) catch |e| return .{ .fatal = e };
    const save_ms = @as(f64, @floatFromInt(nowNs() - t_save)) / 1e6;

    const t_cursor = nowNs();
    var cursorBuf: [20]u8 = undefined;
    const cursorStr = std.fmt.bufPrint(&cursorBuf, "{d}", .{blockNum}) catch unreachable;
    rdb.setWithRetry("LATEST_PROCESSED_BLOCK_NUMBER", cursorStr) catch |e| return .{ .fatal = e };
    const cursor_ms = @as(f64, @floatFromInt(nowNs() - t_cursor)) / 1e6;

    const total_ms = fetch_ms + parse_ms + transform_ms + save_ms + cursor_ms;
    std.debug.print(
        "[rt] blk={d} tx={d} log={d} itx={d} | fetch={d:.0} parse={d:.0} xform={d:.0} save={d:.0} cursor={d:.0} | total={d:.0}ms\n",
        .{ blockNum, ent.txs.items.len, ent.logs.items.len, ent.internalTxs.items.len,
           fetch_ms, parse_ms, transform_ms, save_ms, cursor_ms, total_ms });

    return .saved;
}
