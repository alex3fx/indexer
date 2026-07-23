// BC completeness verifier: runs after historical sync, before realtime.
//
// Scans block_completions era by era, identifies blocks absent from BC
// (which means they were never fully written — e.g. indexer crashed mid-save),
// re-fetches and re-indexes those blocks, then marks the era as verified
// in the verified_eras table so the next restart can skip it.
//
// On re-run: loads verified_eras at startup → skips already-clean eras.
// If any block permanently fails re-indexing: returns error so the caller
// does NOT enter realtime mode (data integrity first).
const std = @import("std");
const linux = std.os.linux;

const core = @import("indexer/core");
const Logger = core.logger.Logger;

const pipeline = @import("pipeline.zig");
const erc20 = @import("erc20.zig");
const pool = @import("indexer/db").pool;
const batch = @import("indexer/db").batch;

const EvmChainConfig = core.structures.EvmChainConfig;
const EvmRpcNodeConfig = core.structures.EvmRpcNodeConfig;
const FetchClient = core.fetch.Client;
const Allocator = std.mem.Allocator;

fn delayMs(ms: u64) void {
    if (ms == 0) return;
    const ts = linux.timespec{ .sec = @intCast(ms / 1000), .nsec = @intCast((ms % 1000) * 1_000_000) };
    _ = linux.nanosleep(&ts, null);
}

fn nowMs() i64 {
    var ts: linux.timespec = undefined;
    _ = linux.clock_gettime(.REALTIME, &ts);
    return ts.sec * 1000 + @divTrunc(ts.nsec, 1_000_000);
}

fn parseBigint(bytes: ?[]const u8) i64 {
    const b = bytes orelse return 0;
    if (b.len < 8) return 0;
    return std.mem.readInt(i64, b[0..8], .big);
}

// Load all verified era indices into a set (single full-table scan).
fn loadVerifiedEras(conn: *pool.CqlConn, gpa: Allocator) !std.AutoHashMap(i64, void) {
    var set = std.AutoHashMap(i64, void).init(gpa);
    errdefer set.deinit();

    var params: std.ArrayList(u8) = .empty;
    defer params.deinit(pool.tempAllocator);

    var res = try conn.executeSelect(&conn.prepIds.verifiedErasAll, 0, params.items, gpa);
    defer res.deinit();

    for (res.rows) |row| {
        if (row.len < 1) continue;
        const era = parseBigint(row[0]);
        try set.put(era, {});
    }
    return set;
}

// Scan block_completions for one era, populate `present` with all block_numbers found.
fn scanEra(
    conn: *pool.CqlConn,
    eraIdx: i64,
    eraStart: u64,
    eraEnd: u64,
    lanes: u64,
    gpa: Allocator,
    present: *std.AutoHashMap(u64, void),
) !void {
    present.clearRetainingCapacity();

    var lane: u64 = 0;
    while (lane < lanes) : (lane += 1) {
        const chunk: i32 = @intCast(lane + lanes * @as(u64, @intCast(eraIdx)));

        var params: std.ArrayList(u8) = .empty;
        defer params.deinit(pool.tempAllocator);
        try pool.valInt32(&params, chunk);
        try pool.valBigint(&params, @intCast(eraStart));
        try pool.valBigint(&params, @intCast(eraEnd));

        var attempt: u32 = 0;
        var delay: u64 = 500;
        while (attempt < 5) : ({
            attempt += 1;
            delay = @min(delay * 2, 16_000);
        }) {
            if (attempt > 0) delayMs(delay);
            var res = conn.executeSelect(&conn.prepIds.bcScan, 3, params.items, gpa) catch |e| {
                std.debug.print("[verifier] BC chunk={d} attempt {d}: {s}\n", .{ chunk, attempt + 1, @errorName(e) });
                continue;
            };
            defer res.deinit();
            for (res.rows) |row| {
                if (row.len < 1) continue;
                const bn: u64 = @intCast(@max(0, parseBigint(row[0])));
                try present.put(bn, {});
            }
            break;
        } else {
            std.debug.print("[verifier] BC chunk={d} all retries failed — aborting era scan\n", .{chunk});
            return error.VerifierBcScanFailed;
        }
    }
}

// Fetch + transform + save one block; tries primary then backup nodes.
// Uses saveBlock (single-connection, sequential) — suitable for low-volume re-indexing.
fn reindexBlock(
    io: std.Io,
    gpa: Allocator,
    chain: *const EvmChainConfig,
    blockNum: u64,
    chunkBuckets: u64,
    chunkEra: u64,
    backupNode: ?EvmRpcNodeConfig,
    backupNode2: ?EvmRpcNodeConfig,
    writeConn: *pool.CqlConn,
    bs: pool.BatchSizes,
    erc20Ctx: *erc20.Erc20Context,
) !void {
    const rpcNode = chain.rpcNodes.lotosArchiveNode;
    const chunkSize = @as(u64, @intCast(chain.indexingOptions.minifiedChunkSize));

    var bClient = FetchClient.init(gpa, io);
    var rClient = FetchClient.init(gpa, io);
    var tClient = FetchClient.init(gpa, io);
    defer bClient.deinit();
    defer rClient.deinit();
    defer tClient.deinit();

    var result = pipeline.BlockResult.init(gpa);
    defer result.deinit();
    result.blockNum = blockNum;

    var found = pipeline.fetchParseTransform(
        gpa, io, rpcNode, blockNum, chunkSize, chunkBuckets, chunkEra,
        &bClient, &rClient, &tClient, null,
        &erc20Ctx.bloom, &erc20Ctx.bytecodeBloom, &result,
    ) == .ok;

    if (!found) if (backupNode) |bn| {
        pipeline.resetResult(&result);
        found = pipeline.fetchParseTransform(
            gpa, io, bn, blockNum, chunkSize, chunkBuckets, chunkEra,
            &bClient, &rClient, &tClient, null,
            &erc20Ctx.bloom, &erc20Ctx.bytecodeBloom, &result,
        ) == .ok;
    };

    if (!found) if (backupNode2) |bn2| {
        pipeline.resetResult(&result);
        found = pipeline.fetchParseTransform(
            gpa, io, bn2, blockNum, chunkSize, chunkBuckets, chunkEra,
            &bClient, &rClient, &tClient, null,
            &erc20Ctx.bloom, &erc20Ctx.bytecodeBloom, &result,
        ) == .ok;
    };

    if (!found) return error.BlockUnfetchable;

    try batch.saveBlock(writeConn, &result.ent, bs);
    std.debug.print("[verifier] reindexed block={d} tx={d} log={d} itx={d}\n", .{
        blockNum,
        result.ent.txs.items.len,
        result.ent.logs.items.len,
        result.ent.internalTxs.items.len,
    });
}

// Record era as verified in Scylla.
fn markEraVerified(conn: *pool.CqlConn, era: i64, atMs: i64, found: i32, reindexed: i32) !void {
    var params: std.ArrayList(u8) = .empty;
    defer params.deinit(pool.tempAllocator);
    try pool.valBigint(&params, era);
    try pool.valBigint(&params, atMs);
    try pool.valInt32(&params, found);
    try pool.valInt32(&params, reindexed);
    try conn.batchSendRows(&conn.prepIds.verifiedErasInsert, 4, &[_][]const u8{params.items});
}

// ─── Public entry point ───────────────────────────────────────────────────────

// Run after historical sync completes, before entering realtime mode.
// Verifies block_completions completeness for blocks [0, toBlock].
// Already-verified eras are skipped (read from verified_eras table).
// Returns error if any blocks permanently fail re-indexing.
pub fn runBcVerification(
    io: std.Io,
    gpa: Allocator,
    chain: *const EvmChainConfig,
    toBlock: u64,
    scyllaHost: []const u8,
    scyllaPort: u16,
    scyllaKs: []const u8,
    scyllaUser: []const u8,
    scyllaPass: []const u8,
    chunkBuckets: u64,
    chunkEra: u64,
    backupNode: ?EvmRpcNodeConfig,
    backupNode2: ?EvmRpcNodeConfig,
    log: *Logger,
    erc20Ctx: *erc20.Erc20Context,
) !void {
    if (chunkEra == 0) {
        log.warn("BC verification skipped: chunkEra=0 (flat chunk mode, no era boundaries)");
        return;
    }

    const bs = pool.BatchSizes.fromChain(chain.indexingOptions);
    const maxEra = toBlock / chunkEra;

    std.debug.print("\n[verifier] BC verification starting: range=[0..{d}] eras={d}\n", .{ toBlock, maxEra + 1 });
    log.info("BC verification starting");

    var scanConn = try pool.CqlConn.init(gpa, scyllaHost, scyllaPort, scyllaKs, scyllaUser, scyllaPass);
    defer scanConn.deinit();
    var writeConn = try pool.CqlConn.init(gpa, scyllaHost, scyllaPort, scyllaKs, scyllaUser, scyllaPass);
    defer writeConn.deinit();

    // Load already-verified eras to skip them.
    var verified = try loadVerifiedEras(&scanConn, gpa);
    defer verified.deinit();

    const alreadyVerified = verified.count();
    const toVerify = maxEra + 1 - alreadyVerified;
    std.debug.print("[verifier] already verified: {d}  to scan: {d}\n", .{ alreadyVerified, toVerify });

    var present = std.AutoHashMap(u64, void).init(gpa);
    defer present.deinit();

    var totalMissingFound: u64 = 0;
    var totalReindexed: u64 = 0;
    var totalFailed: u64 = 0;
    var erasScanned: u64 = 0;
    var erasWithMissing: u64 = 0;

    const t0 = nowMs();
    var era: i64 = 0;
    while (@as(u64, @intCast(era)) <= maxEra) : (era += 1) {
        if (verified.contains(era)) continue;

        const eraStart = @as(u64, @intCast(era)) * chunkEra;
        const eraEnd = @min(eraStart + chunkEra - 1, toBlock);

        // Scan BC chunks for this era.
        scanEra(&scanConn, era, eraStart, eraEnd, chunkBuckets, gpa, &present) catch |e| {
            std.debug.print("[verifier] era={d} scan failed: {s} — skipping\n", .{ era, @errorName(e) });
            totalFailed += 1;
            continue;
        };

        // Collect missing blocks.
        var missing: std.ArrayList(u64) = .empty;
        defer missing.deinit(gpa);

        var b = eraStart;
        while (b <= eraEnd) : (b += 1) {
            if (!present.contains(b)) try missing.append(gpa, b);
        }

        erasScanned += 1;
        if (missing.items.len > 0) {
            erasWithMissing += 1;
            totalMissingFound += missing.items.len;
            std.debug.print("[verifier] era={d} [{d}..{d}]: {d} missing blocks\n", .{
                era, eraStart, eraEnd, missing.items.len,
            });
        }

        // Re-index each missing block.
        var eraFailed: u32 = 0;
        var eraReindexed: u32 = 0;
        for (missing.items) |blockNum| {
            reindexBlock(io, gpa, chain, blockNum, chunkBuckets, chunkEra, backupNode, backupNode2, &writeConn, bs, erc20Ctx) catch |e| {
                std.debug.print("[verifier] block={d} reindex failed: {s}\n", .{ blockNum, @errorName(e) });
                eraFailed += 1;
                continue;
            };
            eraReindexed += 1;
        }
        totalReindexed += eraReindexed;
        totalFailed += eraFailed;

        if (eraFailed > 0) {
            // Do NOT mark era verified — will be retried on next run.
            std.debug.print("[verifier] era={d} NOT marked verified ({d} blocks failed)\n", .{ era, eraFailed });
            continue;
        }

        markEraVerified(&writeConn, era, nowMs(), @intCast(missing.items.len), @intCast(eraReindexed)) catch |e| {
            std.debug.print("[verifier] era={d} mark-verified failed: {s}\n", .{ era, @errorName(e) });
        };

        if (erasScanned % 100 == 0) {
            const elapsed = @divFloor(nowMs() - t0, 1000);
            std.debug.print("[verifier] progress: era={d}/{d} scanned={d} missing={d} elapsed={d}s\n", .{
                era, maxEra, erasScanned, totalMissingFound, elapsed,
            });
        }
    }

    const elapsedS = @divFloor(nowMs() - t0, 1000);
    std.debug.print(
        "\n[verifier] Done: elapsed={d}s  eras_scanned={d}  eras_with_missing={d}  blocks_found={d}  reindexed={d}  failed={d}\n\n",
        .{ elapsedS, erasScanned, erasWithMissing, totalMissingFound, totalReindexed, totalFailed },
    );

    const msg = std.fmt.allocPrint(gpa,
        "BC verification done: {d} eras scanned, {d} missing blocks found, {d} reindexed, {d} failed",
        .{ erasScanned, totalMissingFound, totalReindexed, totalFailed },
    ) catch "";
    defer if (msg.len > 0) gpa.free(msg);

    if (totalFailed > 0) {
        log.err(if (msg.len > 0) msg else "BC verification done with failures");
        return error.VerificationIncomplete;
    }
    log.info(if (msg.len > 0) msg else "BC verification done");
}
