// Historical pipeline: MPSC work-queue, accumulator + double-buffer save.
//
// N persistent workers atomically claim blocks, fetch+transform, push *BlockResult
// to a pipe channel.  Workers never touch Scylla; heavy writes are isolated in the save stage.
//
// Main thread accumulates SAVE_EVERY results, then saves in a background thread
// while workers keep fetching the next batch (fetch and save overlap).
// Cursor (Redis) advances only after save completes.
const std = @import("std");
const linux = std.os.linux;

const core = @import("indexer/core");
const Logger = core.logger.Logger;

const fetcher = @import("fetcher.zig");
const parser = @import("parser.zig");
const transformer = @import("transformer.zig");
const pool = @import("indexer/db").pool;
const http_pool = @import("indexer/rpc").pool;
const batch = @import("indexer/db").batch;
const cursor = @import("indexer/db").cursor;
const node_probe = @import("indexer/rpc").node_probe;

const EvmChainConfig = core.structures.EvmChainConfig;
const EvmRpcNodeConfig = core.structures.EvmRpcNodeConfig;
const FetchClient = core.fetch.Client;
const Allocator = std.mem.Allocator;

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

// Default connection split matching production SPLIT=1,3,6,20,1,1.
// Override via env ACCUM_TXS_LANES, ACCUM_LOG_LANES, ACCUM_ITX_LANES.
pub const TXS_LANES_DEFAULT: usize = batch.ACCUM_TXS_LANES;
pub const LOG_LANES_DEFAULT: usize = batch.ACCUM_LOG_LANES;
pub const ITX_LANES_DEFAULT: usize = batch.ACCUM_ITX_LANES;

// ─── BlockResult ──────────────────────────────────────────────────────────────

pub const BlockResult = struct {
    gpa: Allocator,
    arena: std.heap.ArenaAllocator,
    // Owns the raw HTTP response buffers (block.body, receipts.body, traces.body).
    // ZC-parsed strings in `ent` point directly into these buffers — no copies.
    // Must outlive saveEntitiesParallel; freed in deinit() after save completes.
    rawData: ?fetcher.Response,
    ent: transformer.Entities,
    blockNum: u64,
    ok: bool,
    err: ?anyerror,
    // Per-stage timings set by fetchParseTransform; callers may read for logging.
    fetchNs: i64,
    parseNs: i64,
    transformNs: i64,

    pub fn init(gpa: Allocator) BlockResult {
        return .{
            .gpa = gpa,
            .arena = std.heap.ArenaAllocator.init(std.heap.page_allocator),
            .rawData = null,
            .ent = transformer.initEntities(),
            .blockNum = 0,
            .ok = false,
            .err = null,
            .fetchNs = 0,
            .parseNs = 0,
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
    rd: i32,
    wr: i32,
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

    fn deinit(self: *ResultChan) void {
        _ = linux.close(self.rd);
    }
};

// ─── Worker ───────────────────────────────────────────────────────────────────

const WorkerArgs = struct {
    io: std.Io,
    gpa: Allocator,
    chain: *const EvmChainConfig,
    next: *std.atomic.Value(u64),
    to: u64,
    chan: *ResultChan,
    chunkBuckets: u64,
    chunkEra: u64,
    cancel: *std.atomic.Value(bool),
    backupNode: ?EvmRpcNodeConfig,
    neighborNode: ?EvmRpcNodeConfig,
    connErrors: *std.atomic.Value(u64),
};

// Errors that indicate the RPC node itself is unreachable/refusing connections
// (vs. an isolated per-block issue). Used to alert immediately instead of only
// discovering a fully-failed run after it has already finished.
fn isConnIssue(e: anyerror) bool {
    return switch (e) {
        error.ConnectionRefused, error.ConnectionResetByPeer, error.ConnectionTimedOut, error.NetworkUnreachable, error.TemporaryNameServerFailure, error.UnknownHostName, error.AddressUnavailable, error.ProcessFdQuotaExceeded, error.HttpConnectionClosing => true,
        else => false,
    };
}

// Backoff between full primary+backup retry cycles for a block that exhausted both —
// 1s, 2s, 4s, ... capped at 30s. Never gives up: a node outage longer than this just
// means the worker spins slowly on this one block while other workers keep progressing.
fn sleepBackoff(cycleCount: u32) void {
    const capSec: u32 = 30;
    const shift = @min(cycleCount, 5); // 2^5 = 32s, already past the cap
    const sec: u32 = @min(capSec, @as(u32, 1) << @intCast(shift));
    const ts = linux.timespec{ .sec = sec, .nsec = 0 };
    _ = linux.nanosleep(&ts, null);
}

// Fires once per threshold crossing (10, 20, 30, ...) so it can't spam but is
// still visible immediately, well before a batch finishes.
fn maybeAlertConnIssues(count: u64) void {
    if (count > 0 and count % 10 == 0) {
        std.debug.print(
            "\n[ALERT] {d} connection errors so far this run — RPC node may be unreachable/refusing connections (check node health, reduce FETCH_WORKERS)\n\n",
            .{count},
        );
    }
}

pub const FetchTransformStatus = union(enum) {
    ok,
    retry_later, // null RPC response — block not yet available
    skip_missing, // block parsed as null/empty — no data to save
    fatal: anyerror,
};

/// Shared fetch+parse+transform core used by both historical and realtime pipelines.
/// Stores raw HTTP buffers in result.rawData (ZC ownership).
/// Sets result.fetchNs / parseNs / transformNs for caller logging.
/// Does NOT save, does NOT advance cursor — callers handle that differently.
pub fn fetchParseTransform(
    gpa: Allocator,
    io: std.Io,
    rpcNode: EvmRpcNodeConfig,
    blockNum: u64,
    chunkSize: u64,
    chunkBuckets: u64,
    chunkEra: u64,
    bClient: *FetchClient,
    rClient: *FetchClient,
    tClient: *FetchClient,
    httpPool: ?*http_pool.HttpPool,
    result: *BlockResult,
) FetchTransformStatus {
    const t0 = nowNs();
    const maybeData = fetcher.getConsistentBlockData(gpa, io, .{
        .rpcNode = rpcNode,
        .blockNumber = blockNum,
        .blockClient = bClient,
        .receiptsClient = rClient,
        .tracesClient = tClient,
        .httpPool = httpPool,
        .skipLogs = true,
    }) catch |e| return .{ .fatal = e };
    const data = maybeData orelse return .retry_later;
    result.fetchNs = nowNs() - t0;
    // Transfer ownership: buffers live until BlockResult.deinit() after save.
    result.rawData = data;

    const t1 = nowNs();
    const aa = result.arena.allocator();
    const block = parser.parseBlockRespZC(data.block.body, aa) catch |e|
        return .{ .fatal = e };
    if (block == null) return .skip_missing;
    const receipts = (parser.parseReceiptsRespZC(data.receipts.body, aa) catch null) orelse &.{};
    // Choose trace parser based on detected node type (cached from requestSync probe)
    const traces: []const parser.RpcTrace = switch (node_probe.getCached(rpcNode.https)) {
        .trace_block => (parser.parseTracesRespZC(data.traces.body, aa) catch null) orelse &.{},
        .debug_trace_block => (parser.parseGethTracesRespZC(data.traces.body, aa) catch null) orelse &.{},
    };
    result.parseNs = nowNs() - t1;

    const t2 = nowNs();
    transformer.transformBlockWithRemap(
        aa,
        block.?,
        receipts,
        traces,
        chunkSize,
        chunkBuckets,
        chunkEra,
        &result.ent,
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

pub fn resetResult(result: *BlockResult) void {
    if (result.rawData) |*d| {
        d.deinit(result.gpa);
        result.rawData = null;
    }
    result.arena.deinit();
    result.arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    result.ent = transformer.initEntities();
}

// Same chunk formula as transformer.zig's transformBlockWithRemap — needed here
// (independent of a successful transform) so a block we're about to give up on
// can still be recorded against its correct partition in pol.skipped_blocks.
fn computeChunk(blockNum: u64, chunkSize: u64, chunkBuckets: u64, chunkEra: u64) i32 {
    const number: i64 = @intCast(blockNum);
    if (chunkBuckets > 0 and chunkEra > 0) {
        return @intCast(@mod(number, @as(i64, @intCast(chunkBuckets))) +
            @as(i64, @intCast(chunkBuckets)) * @divFloor(number, @as(i64, @intCast(chunkEra))));
    } else if (chunkBuckets > 0) {
        return @intCast(@mod(number, @as(i64, @intCast(chunkBuckets))));
    } else {
        return @intCast(@divFloor(number, @as(i64, @intCast(chunkSize))));
    }
}

// After exhausting primary + neighbor + backup this many full cycles (with
// sleepBackoff between: 1s,2s,4s,8s,16s ≈ 31s total), give up for good instead
// of retrying forever — see worker()'s give-up path below. Bounded so one truly
// unfetchable block (RPC node genuinely doesn't have it, not a transient blip)
// can't park a worker indefinitely; the gap is explicitly recorded in
// pol.skipped_blocks (docs/INTEGRITY_CHECKS.md) instead of silently lost or
// silently retried forever.
const MAX_GIVE_UP_CYCLES: u32 = 5;

/// Tries neighbor then backup (whichever are configured) once each. Returns true
/// and sets result.ok=true if either succeeds.
fn tryFallbackNodes(
    gpa: Allocator,
    io: std.Io,
    blockNum: u64,
    chunkSize: u64,
    chunkBuckets: u64,
    chunkEra: u64,
    bClient: *FetchClient,
    rClient: *FetchClient,
    tClient: *FetchClient,
    neighborNode: ?EvmRpcNodeConfig,
    backupNode: ?EvmRpcNodeConfig,
    result: *BlockResult,
) bool {
    if (neighborNode) |neighbor| {
        resetResult(result);
        if (fetchParseTransform(gpa, io, neighbor, blockNum, chunkSize, chunkBuckets, chunkEra, bClient, rClient, tClient, null, result) == .ok) {
            result.ok = true;
            std.debug.print("[{d}] T:{d} L:{d} IT:{d} (neighbor)\n", .{
                blockNum,                  result.ent.txs.items.len,
                result.ent.logs.items.len, result.ent.internalTxs.items.len,
            });
            return true;
        }
    }
    if (backupNode) |backup| {
        resetResult(result);
        if (fetchParseTransform(gpa, io, backup, blockNum, chunkSize, chunkBuckets, chunkEra, bClient, rClient, tClient, null, result) == .ok) {
            result.ok = true;
            std.debug.print("[{d}] T:{d} L:{d} IT:{d} (backup)\n", .{
                blockNum,                  result.ent.txs.items.len,
                result.ent.logs.items.len, result.ent.internalTxs.items.len,
            });
            return true;
        }
    }
    return false;
}

/// Records the block as explicitly skipped (pol.skipped_blocks) and marks it
/// ok=true with empty entities — it flows through the normal accum/save/watermark
// pipeline as a (real, durable) zero-row block, so the watermark can advance
/// instead of blocking the whole run on one unfetchable block forever. Caller
/// must resetResult() before this so result.ent is empty.
fn giveUpAndRecord(gpa: Allocator, blockNum: u64, chunkSize: u64, chunkBuckets: u64, chunkEra: u64, reason: []const u8, result: *BlockResult) void {
    const chunk = computeChunk(blockNum, chunkSize, chunkBuckets, chunkEra);
    std.debug.print(
        "[worker] block={d} GIVING UP after {d} cycles (primary+neighbor+backup exhausted each time) — " ++
            "recording in pol.skipped_blocks and moving on ({s})\n",
        .{ blockNum, MAX_GIVE_UP_CYCLES, reason },
    );
    pool.recordSkippedBlock(gpa, @intCast(blockNum), chunk, reason);
    result.ok = true;
}

fn worker(args: *WorkerArgs) !void {
    const gpa = args.gpa;
    const chain = args.chain;
    const rpcNode = chain.rpcNodes.lotosArchiveNode;
    const chunkSize = @as(u64, @intCast(chain.indexingOptions.minifiedChunkSize));
    const chunkBuckets = args.chunkBuckets;
    const chunkEra = args.chunkEra;

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

        const MAX_RETRY_LATER: u32 = 25; // ~5s of 200ms sleeps before escalating to neighbor/backup
        var retryCount: u32 = 0;
        var cycleCount: u32 = 0; // full primary+neighbor+backup-exhausted cycles, bounded by MAX_GIVE_UP_CYCLES

        // Retry chain per block: primary (short transient-retry loop below) -> neighbor
        // node -> backup/public node -> a few backed-off full cycles of the above ->
        // explicit recorded give-up (pol.skipped_blocks). Never silently drop a block:
        // either it's saved for real, or its absence is a durable, queryable fact.
        retry: while (true) {
            if (args.cancel.load(.acquire)) break :retry;
            switch (fetchParseTransform(gpa, args.io, rpcNode, blockNum, chunkSize, chunkBuckets, chunkEra, &bClient, &rClient, &tClient, null, result)) {
                .ok => {
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
                    if (tryFallbackNodes(gpa, args.io, blockNum, chunkSize, chunkBuckets, chunkEra, &bClient, &rClient, &tClient, args.neighborNode, args.backupNode, result)) break :retry;
                    cycleCount += 1;
                    if (cycleCount >= MAX_GIVE_UP_CYCLES) {
                        resetResult(result);
                        giveUpAndRecord(gpa, blockNum, chunkSize, chunkBuckets, chunkEra, "skip_missing on primary+neighbor+backup", result);
                        break :retry;
                    }
                    std.debug.print("[worker] block={d} skip_missing on primary+neighbor+backup — retry cycle #{d}/{d}\n", .{ blockNum, cycleCount, MAX_GIVE_UP_CYCLES });
                    sleepBackoff(cycleCount);
                    retryCount = 0;
                    resetResult(result);
                },
                .retry_later => {
                    if (args.cancel.load(.acquire)) break :retry;
                    retryCount += 1;
                    if (retryCount >= MAX_RETRY_LATER) {
                        std.debug.print("[worker] block={d} stuck retry_later x{d} — trying neighbor/backup\n", .{ blockNum, retryCount });
                        if (tryFallbackNodes(gpa, args.io, blockNum, chunkSize, chunkBuckets, chunkEra, &bClient, &rClient, &tClient, args.neighborNode, args.backupNode, result)) break :retry;
                        cycleCount += 1;
                        if (cycleCount >= MAX_GIVE_UP_CYCLES) {
                            resetResult(result);
                            giveUpAndRecord(gpa, blockNum, chunkSize, chunkBuckets, chunkEra, "stuck retry_later on primary+neighbor+backup", result);
                            break :retry;
                        }
                        std.debug.print("[worker] block={d} unavailable on all nodes after {d} retries — retry cycle #{d}/{d}\n", .{ blockNum, retryCount, cycleCount, MAX_GIVE_UP_CYCLES });
                        sleepBackoff(cycleCount);
                        retryCount = 0;
                        resetResult(result);
                        continue :retry;
                    }
                    const ts = linux.timespec{ .sec = 0, .nsec = 200_000_000 };
                    _ = linux.nanosleep(&ts, null);
                    resetResult(result);
                },
                .fatal => |e| {
                    if (isConnIssue(e)) {
                        const n = args.connErrors.fetchAdd(1, .monotonic) + 1;
                        maybeAlertConnIssues(n);
                        if (!args.cancel.load(.acquire)) {
                            retryCount += 1;
                            if (retryCount < MAX_RETRY_LATER) {
                                const ts = linux.timespec{ .sec = 0, .nsec = 200_000_000 };
                                _ = linux.nanosleep(&ts, null);
                                resetResult(result);
                                continue :retry;
                            }
                        }
                        std.debug.print("[worker] block={d} conn issue persisted x{d}: {s}\n", .{ blockNum, retryCount, @errorName(e) });
                    }
                    std.debug.print("[worker] block={d} primary error: {s} — trying neighbor/backup\n", .{ blockNum, @errorName(e) });
                    if (tryFallbackNodes(gpa, args.io, blockNum, chunkSize, chunkBuckets, chunkEra, &bClient, &rClient, &tClient, args.neighborNode, args.backupNode, result)) break :retry;
                    cycleCount += 1;
                    if (cycleCount >= MAX_GIVE_UP_CYCLES) {
                        resetResult(result);
                        var reasonBuf: [128]u8 = undefined;
                        const reason = std.fmt.bufPrint(&reasonBuf, "error {s} on primary+neighbor+backup", .{@errorName(e)}) catch "error on primary+neighbor+backup";
                        giveUpAndRecord(gpa, blockNum, chunkSize, chunkBuckets, chunkEra, reason, result);
                        break :retry;
                    }
                    std.debug.print("[worker] block={d} error: {s} on all nodes — retry cycle #{d}/{d}\n", .{ blockNum, @errorName(e), cycleCount, MAX_GIVE_UP_CYCLES });
                    sleepBackoff(cycleCount);
                    retryCount = 0;
                    resetResult(result);
                },
            }
        }
        args.chan.push(result);
    }
}

// ─── AccumState ───────────────────────────────────────────────────────────────

const AccumState = struct {
    gpa: Allocator,
    sources: std.ArrayList(*BlockResult),
    entPtrs: std.ArrayList(*const transformer.Entities),
    blockStart: u64,
    blockEnd: u64,
    saveErr: ?anyerror,
    saveMs: f64,

    fn init(gpa: Allocator) AccumState {
        return .{
            .gpa = gpa,
            .sources = .empty,
            .entPtrs = .empty,
            .blockStart = std.math.maxInt(u64),
            .blockEnd = 0,
            .saveErr = null,
            .saveMs = 0,
        };
    }

    fn add(self: *AccumState, r: *BlockResult) !void {
        try self.sources.append(self.gpa, r);
        if (r.ok) {
            try self.entPtrs.append(self.gpa, &r.ent);
            if (r.blockNum < self.blockStart) self.blockStart = r.blockNum;
            if (r.blockNum > self.blockEnd) self.blockEnd = r.blockNum;
        }
    }

    fn savedCount(self: *const AccumState) usize {
        return self.entPtrs.items.len;
    }

    fn deinit(self: *AccumState) void {
        for (self.sources.items) |r| {
            r.deinit();
            self.gpa.destroy(r);
        }
        self.sources.deinit(self.gpa);
        self.entPtrs.deinit(self.gpa);
    }
};

// ─── HistoricalConns — all CQL connections for a historical run ───────────────

const HistoricalConns = struct {
    gpa: Allocator,
    blocks: pool.CqlConn,
    txs: []pool.CqlConn,
    contracts: pool.CqlConn,
    logs: []pool.CqlConn,
    itxs: []pool.CqlConn,
    comp: pool.CqlConn,

    fn open(
        gpa: Allocator,
        host: []const u8,
        port: u16,
        ks: []const u8,
        user: []const u8,
        pass: []const u8,
        txsN: usize,
        logsN: usize,
        itxsN: usize,
    ) !HistoricalConns {
        var self: HistoricalConns = undefined;
        self.gpa = gpa;

        self.blocks = try pool.CqlConn.init(gpa, host, port, ks, user, pass);
        errdefer self.blocks.deinit();
        self.contracts = try pool.CqlConn.init(gpa, host, port, ks, user, pass);
        errdefer self.contracts.deinit();
        self.comp = try pool.CqlConn.init(gpa, host, port, ks, user, pass);
        errdefer self.comp.deinit();

        self.txs = try gpa.alloc(pool.CqlConn, txsN);
        var txsOpened: usize = 0;
        errdefer {
            for (0..txsOpened) |i| self.txs[i].deinit();
            gpa.free(self.txs);
        }
        for (0..txsN) |i| {
            self.txs[i] = try pool.CqlConn.init(gpa, host, port, ks, user, pass);
            txsOpened += 1;
        }

        self.logs = try gpa.alloc(pool.CqlConn, logsN);
        var logsOpened: usize = 0;
        errdefer {
            for (0..logsOpened) |i| self.logs[i].deinit();
            gpa.free(self.logs);
        }
        for (0..logsN) |i| {
            self.logs[i] = try pool.CqlConn.init(gpa, host, port, ks, user, pass);
            logsOpened += 1;
        }

        self.itxs = try gpa.alloc(pool.CqlConn, itxsN);
        var itxsOpened: usize = 0;
        errdefer {
            for (0..itxsOpened) |i| self.itxs[i].deinit();
            gpa.free(self.itxs);
        }
        for (0..itxsN) |i| {
            self.itxs[i] = try pool.CqlConn.init(gpa, host, port, ks, user, pass);
            itxsOpened += 1;
        }

        return self;
    }

    fn deinit(self: *HistoricalConns) void {
        self.blocks.deinit();
        self.contracts.deinit();
        self.comp.deinit();
        for (self.txs) |*c| c.deinit();
        self.gpa.free(self.txs);
        for (self.logs) |*c| c.deinit();
        self.gpa.free(self.logs);
        for (self.itxs) |*c| c.deinit();
        self.gpa.free(self.itxs);
    }

    fn saveArgs(self: *HistoricalConns, accum: *AccumState, rdb: *cursor.Conn, bs: pool.BatchSizes) SaveArgs {
        return .{
            .accum = accum,
            .cBlocks = &self.blocks,
            .cTxs = self.txs,
            .cContracts = &self.contracts,
            .cLogs = self.logs,
            .cItxs = self.itxs,
            .cComp = &self.comp,
            .rdb = rdb,
            .bs = bs,
        };
    }
};

// ─── SaveArgs + save thread ───────────────────────────────────────────────────

const SaveArgs = struct {
    accum: *AccumState,
    cBlocks: *pool.CqlConn,
    cTxs: []pool.CqlConn,
    cContracts: *pool.CqlConn,
    cLogs: []pool.CqlConn,
    cItxs: []pool.CqlConn,
    cComp: *pool.CqlConn,
    rdb: *cursor.Conn,
    bs: pool.BatchSizes,
};

fn saveAccumFn(args: *SaveArgs) void {
    const ac = args.accum;
    if (ac.entPtrs.items.len == 0) return;

    const t0 = nowNs();
    batch.saveEntitiesParallel(
        args.cBlocks,
        args.cTxs,
        args.cContracts,
        args.cLogs,
        args.cItxs,
        args.cComp,
        ac.entPtrs.items,
        args.bs,
    ) catch |e| {
        ac.saveErr = e;
        return;
    };

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

// ─── Watermark ────────────────────────────────────────────────────────────────
// Tracks the true contiguous-from-`from` durably-saved frontier, independent of
// save-batch boundaries. Workers complete out of order under concurrency (a slow
// worker on block N can still be retrying while workers on N+1..N+50 finish first,
// land in an earlier-flushed batch, and get printed in an "Accum X→Y" line with
// Y > N) — resuming from that batch's Y would skip N forever if the process is
// killed/crashes before N's worker finally succeeds. This tracker only ever
// reports a block number as safe-to-resume-past once every block from `from` up
// to it has actually been saved, regardless of arrival/save order. Confirmed via
// a real data scan 2026-06-23: kill -9 restarts during this exact race window
// silently dropped ~2000+ blocks even after the worker-level "never skip" fix.
const Watermark = struct {
    gpa: Allocator,
    nextExpected: u64,
    pending: std.AutoHashMap(u64, void),

    fn init(gpa: Allocator, from: u64) Watermark {
        return .{ .gpa = gpa, .nextExpected = from, .pending = std.AutoHashMap(u64, void).init(gpa) };
    }

    fn deinit(self: *Watermark) void {
        self.pending.deinit();
    }

    /// Returns true if the contiguous frontier advanced (caller should log/print it).
    fn markSaved(self: *Watermark, blockNum: u64) !bool {
        if (blockNum < self.nextExpected) return false; // already covered, defensive
        if (blockNum == self.nextExpected) {
            self.nextExpected += 1;
            while (self.pending.remove(self.nextExpected)) {
                self.nextExpected += 1;
            }
            return true;
        }
        try self.pending.put(blockNum, {});
        return false;
    }

    /// The safe resume point: every block strictly below this is durably saved.
    fn resumePoint(self: *const Watermark) u64 {
        return self.nextExpected;
    }
};

const PrevSave = struct {
    thread: std.Thread,
    accum: *AccumState,
    args: *SaveArgs,

    fn start(gpa: Allocator, args: SaveArgs) !PrevSave {
        const heapArgs = try gpa.create(SaveArgs);
        heapArgs.* = args;
        const thread = std.Thread.spawn(.{}, saveAccumFn, .{heapArgs}) catch |e| {
            gpa.destroy(heapArgs);
            return e;
        };
        return .{ .thread = thread, .accum = args.accum, .args = heapArgs };
    }

    fn finish(self: *PrevSave, gpa: Allocator, blocksDone: *u64, t0: i64, watermark: *Watermark) !void {
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
            .{ self.accum.blockStart, self.accum.blockEnd, self.accum.savedCount(), self.accum.saveMs, @as(f64, @floatFromInt(blocksDone.*)) / ms * 1000.0 },
        );

        // Feed every durably-saved block number (this batch may be a disjoint subset
        // of [from,to], arrived out of order) into the contiguous tracker, then
        // report the resume-safe frontier — NOT this batch's own max — so a crash
        // right after this print can never skip a still-in-flight earlier block.
        var advanced = false;
        for (self.accum.sources.items) |r| {
            if (r.ok) {
                if (try watermark.markSaved(r.blockNum)) advanced = true;
            }
        }
        if (advanced) {
            std.debug.print("[watermark] {d}\n", .{watermark.resumePoint()});
        }
    }
};

// ─── Worker spawn ─────────────────────────────────────────────────────────────

fn spawnHistoricalWorkers(
    gpa: Allocator,
    io: std.Io,
    chain: *const EvmChainConfig,
    from: u64,
    to: u64,
    chan: *ResultChan,
    chunkBuckets: u64,
    chunkEra: u64,
    cancel: *std.atomic.Value(bool),
    next: *std.atomic.Value(u64),
    threads: []std.Thread,
    backupNode: ?EvmRpcNodeConfig,
    neighborNode: ?EvmRpcNodeConfig,
    connErrors: *std.atomic.Value(u64),
) !void {
    for (0..threads.len) |w| {
        const wargs = try gpa.create(WorkerArgs);
        wargs.* = .{
            .io = io,
            .gpa = gpa,
            .chain = chain,
            .next = next,
            .to = to,
            .chan = chan,
            .chunkBuckets = chunkBuckets,
            .chunkEra = chunkEra,
            .cancel = cancel,
            .backupNode = backupNode,
            .neighborNode = neighborNode,
            .connErrors = connErrors,
        };
        _ = from; // from is encoded in next (already set to from by caller)
        threads[w] = try std.Thread.spawn(.{ .stack_size = 4 * 1024 * 1024 }, workerEntry, .{wargs});
    }
}

// ─── Accumulator error path cleanup ───────────────────────────────────────────

fn abortAndDrain(
    gpa: Allocator,
    chan: *ResultChan,
    threads: []std.Thread,
    accum: *AccumState,
    prevSave: *?PrevSave,
    cancel: *std.atomic.Value(bool),
) void {
    cancel.store(true, .release);
    while (chan.pop()) |r| {
        r.deinit();
        gpa.destroy(r);
    }
    for (threads) |t| t.join();
    accum.deinit();
    gpa.destroy(accum);
    if (prevSave.*) |*ps| {
        ps.thread.join();
        gpa.destroy(ps.args);
        ps.accum.deinit();
        gpa.destroy(ps.accum);
        prevSave.* = null;
    }
}

// ─── retryMissingBlocks ───────────────────────────────────────────────────────

// After historical sync: retry blocks that all workers couldn't fetch.
// Tries primary, then backup. Saves recovered blocks directly (no AccumState).
// Returns error.MissingHistoricalBlocks if any remain unresolved — caller must
// not proceed to realtime mode in that case.
fn retryMissingBlocks(
    gpa: Allocator,
    io: std.Io,
    chain: *const EvmChainConfig,
    backupNode: ?EvmRpcNodeConfig,
    skipped: []const u64,
    conns: *HistoricalConns,
    bs: pool.BatchSizes,
    chunkBuckets: u64,
    chunkEra: u64,
) !void {
    if (skipped.len == 0) return;
    std.debug.print("\n[historical] {d} skipped block(s) — retrying...\n", .{skipped.len});

    const rpcNode = chain.rpcNodes.lotosArchiveNode;
    const chunkSize = @as(u64, @intCast(chain.indexingOptions.minifiedChunkSize));

    var bClient = FetchClient.init(gpa, io);
    var rClient = FetchClient.init(gpa, io);
    var tClient = FetchClient.init(gpa, io);
    defer bClient.deinit();
    defer rClient.deinit();
    defer tClient.deinit();

    var stillMissing: usize = 0;

    for (skipped) |blockNum| {
        var result = BlockResult.init(gpa);
        defer result.deinit();
        result.blockNum = blockNum;

        // Try primary.
        var found = switch (fetchParseTransform(gpa, io, rpcNode, blockNum, chunkSize, chunkBuckets, chunkEra, &bClient, &rClient, &tClient, null, &result)) {
            .ok => true,
            else => false,
        };

        // Try backup if primary didn't deliver.
        if (!found) {
            if (backupNode) |backup| {
                resetResult(&result);
                found = switch (fetchParseTransform(gpa, io, backup, blockNum, chunkSize, chunkBuckets, chunkEra, &bClient, &rClient, &tClient, null, &result)) {
                    .ok => true,
                    else => false,
                };
                if (!found) {
                    std.debug.print("[historical-retry] block={d} unavailable on primary and backup\n", .{blockNum});
                }
            } else {
                std.debug.print("[historical-retry] block={d} unavailable (no backup configured)\n", .{blockNum});
            }
        }

        if (found) {
            std.debug.print("[historical-retry] block={d} recovered — saving\n", .{blockNum});
            var ents = [1]*const transformer.Entities{&result.ent};
            batch.saveEntitiesParallel(
                &conns.blocks,
                conns.txs,
                &conns.contracts,
                conns.logs,
                conns.itxs,
                &conns.comp,
                ents[0..],
                bs,
            ) catch |e| {
                std.debug.print("[historical-retry] save error block={d}: {s}\n", .{ blockNum, @errorName(e) });
                stillMissing += 1;
            };
        } else {
            stillMissing += 1;
        }
    }

    if (stillMissing > 0) {
        std.debug.print("[historical] {d} block(s) permanently missing — cannot proceed to realtime\n", .{stillMissing});
        return error.MissingHistoricalBlocks;
    }
    std.debug.print("[historical] all skipped blocks recovered\n", .{});
}

// ─── runHistorical ────────────────────────────────────────────────────────────

pub fn runHistorical(
    io: std.Io,
    gpa: Allocator,
    chain: *const EvmChainConfig,
    from: u64,
    to: u64,
    scyllaHost: []const u8,
    scyllaPort: u16,
    scyllaKs: []const u8,
    scyllaUser: []const u8,
    scyllaPass: []const u8,
    redisUrl: []const u8,
    chunkBuckets: u64,
    chunkEra: u64,
    saveEvery: usize,
    backupNode: ?EvmRpcNodeConfig,
    neighborNode: ?EvmRpcNodeConfig,
    log: *Logger,
    txsLanes: usize,
    logsLanes: usize,
    itxsLanes: usize,
    fetchWorkers: usize,
) !void {
    if (from > to) return;

    const workerCount = fetchWorkers;
    const bs = pool.BatchSizes.fromChain(chain.indexingOptions);
    const rUrl = cursor.parseUrl(redisUrl);

    std.debug.print("Historical: blocks {d}→{d}  workers={d}  save_every={d}  chunk_buckets={d}  split=1,{d},{d},{d},1,1\n\n", .{ from, to, workerCount, saveEvery, chunkBuckets, txsLanes, logsLanes, itxsLanes });

    var conns = try HistoricalConns.open(gpa, scyllaHost, scyllaPort, scyllaKs, scyllaUser, scyllaPass, txsLanes, logsLanes, itxsLanes);
    defer conns.deinit();

    var rdb = try cursor.Conn.init(gpa, rUrl.host, rUrl.port);
    defer rdb.deinit();
    if (rUrl.password.len > 0) rdb.auth(rUrl.password) catch {};
    if (rUrl.db > 0) rdb.selectDb(rUrl.db) catch {};

    var next = std.atomic.Value(u64).init(from);
    var cancel = std.atomic.Value(bool).init(false);
    var chan = try ResultChan.init(@intCast(workerCount));
    defer chan.deinit();

    const threads = try gpa.alloc(std.Thread, workerCount);
    defer gpa.free(threads);
    var connErrors = std.atomic.Value(u64).init(0);
    try spawnHistoricalWorkers(gpa, io, chain, from, to, &chan, chunkBuckets, chunkEra, &cancel, &next, threads, backupNode, neighborNode, &connErrors);

    var prevSave: ?PrevSave = null;
    var accum = try gpa.create(AccumState);
    accum.* = AccumState.init(gpa);

    var watermark = Watermark.init(gpa, from);
    defer watermark.deinit();

    // Collect block numbers that workers could not fetch from any node.
    var skippedBlocks: std.ArrayList(u64) = .empty;
    defer skippedBlocks.deinit(gpa);

    const t0 = nowNs();
    var blocksDone: u64 = 0;

    while (chan.pop()) |result| {
        if (!result.ok) try skippedBlocks.append(gpa, result.blockNum);

        try accum.add(result);

        if (accum.entPtrs.items.len >= saveEvery) {
            if (prevSave) |*ps| {
                try ps.finish(gpa, &blocksDone, t0, &watermark);
                prevSave = null;
            }
            prevSave = try PrevSave.start(gpa, conns.saveArgs(accum, &rdb, bs));
            accum = try gpa.create(AccumState);
            accum.* = AccumState.init(gpa);
        }
    }

    for (threads) |t| t.join();

    if (prevSave) |*ps| {
        try ps.finish(gpa, &blocksDone, t0, &watermark);
        prevSave = null;
    }

    if (accum.savedCount() > 0) {
        var finalSave = try PrevSave.start(gpa, conns.saveArgs(accum, &rdb, bs));
        try finalSave.finish(gpa, &blocksDone, t0, &watermark);
    } else {
        accum.deinit();
        gpa.destroy(accum);
    }

    const elapsedMs = @as(f64, @floatFromInt(nowNs() - t0)) / 1e6;
    std.debug.print("\nHistorical sync done: {d:.0}ms  saved_blocks={d}\n", .{ elapsedMs, blocksDone });

    // Final integrity check: a clean run (every worker thread joined, every result
    // drained from the channel) must have a contiguous saved frontier reaching all
    // the way to `to+1` — any gap left in `watermark.pending` here is not a
    // resume-point race (those are now impossible by construction), it would be a
    // genuine logic bug. Loud and visible, never silent, per data-integrity policy —
    // do not proceed to realtime with an unexplained hole.
    const safeResumePoint = watermark.resumePoint();
    if (safeResumePoint != to + 1) {
        std.debug.print(
            "[INTEGRITY ERROR] watermark={d} but expected {d} after a clean run — " ++
                "{d} block(s) completed out of order but the gap below them was never filled. " ++
                "This should be impossible after a full clean pass — investigate before resuming.\n",
            .{ safeResumePoint, to + 1, watermark.pending.count() },
        );
        log.err("historical sync integrity check FAILED — watermark gap, see stdout log");
        return error.WatermarkIntegrityGap;
    }
    std.debug.print("[watermark] integrity check passed: contiguous through {d}\n", .{to});

    const requested = to - from + 1;
    const totalConnErrors = connErrors.load(.monotonic);
    if (skippedBlocks.items.len * 100 > requested * 20) {
        std.debug.print(
            "[WARNING] {d}/{d} blocks ({d}%) were skipped on the main pass (conn_errors={d}) — " ++
                "this run's blk/s is NOT a valid throughput measurement, results below are misleading\n",
            .{ skippedBlocks.items.len, requested, skippedBlocks.items.len * 100 / requested, totalConnErrors },
        );
    }

    const syncMsg = std.fmt.allocPrint(gpa, "historical sync done: {d} blocks in {d:.0}ms (skipped={d}, conn_errors={d})", .{ blocksDone, elapsedMs, skippedBlocks.items.len, totalConnErrors }) catch "";
    defer if (syncMsg.len > 0) gpa.free(syncMsg);
    log.info(if (syncMsg.len > 0) syncMsg else "historical sync done");

    // Retry any blocks that were skipped during the main pass, then verify
    // all are present before the caller transitions to realtime mode.
    try retryMissingBlocks(gpa, io, chain, backupNode, skippedBlocks.items, &conns, bs, chunkBuckets, chunkEra);
}
