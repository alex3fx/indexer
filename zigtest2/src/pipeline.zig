// Historical batch pipeline: PIPELINE=N fetch workers running in parallel,
// each with its own CQL pool. Saves overlap with the next round of fetches.
const std    = @import("std");
const rpc    = @import("rpc");
const transform = @import("transform");
const db     = @import("db");
const config = @import("config");
const met    = @import("metrics");

const Config      = config.Config;
const BatchMetric = met.BatchMetric;
const Metrics     = met.Metrics;

fn nowNs() i64 {
    const linux = std.os.linux;
    var ts: linux.timespec = undefined;
    _ = linux.clock_gettime(.MONOTONIC, &ts);
    return ts.sec * 1_000_000_000 + ts.nsec;
}

// ─── BatchState ───────────────────────────────────────────────────────────────

pub const BatchState = struct {
    method_arenas: [3]std.heap.ArenaAllocator,
    batch_arena:   std.heap.ArenaAllocator,
    block_results: []rpc.BlockData,
    block_nums:    []u64,
    ent:           transform.Entities,
    batch_start:   u64,
    batch_end:     u64,
    fbdr_ms:       f64,
    transform_ms:  f64,
    tpt_ms:        f64,
    save_ms:       f64 = 0,
    save_error:    ?anyerror = null,
    cql_pool:      *db.CqlPool,
    gpa:           std.mem.Allocator,
    dump_file:     ?std.Io.File = null,
    dump_io:       std.Io = undefined,
    batch_id:      u32 = 0,

    pub fn deinit(self: *BatchState) void {
        for (&self.method_arenas) |*a| a.deinit();
        self.batch_arena.deinit();
        self.gpa.free(self.block_results);
        self.gpa.free(self.block_nums);
    }
};

pub fn doSave(state: *BatchState) void {
    if (state.dump_file) |f| {
        db.dumpBatch(.{
            .file      = f,
            .io        = state.dump_io,
            .batch_id  = state.batch_id,
            .ent       = &state.ent,
            .result_ms = &state.save_ms,
        });
    } else {
        db.saveBatch(.{
            .pool      = state.cql_pool,
            .ent       = &state.ent,
            .result_ms = &state.save_ms,
        }) catch |err| {
            state.save_error = err;
        };
    }
}

// ─── Fetch+transform worker ───────────────────────────────────────────────────

pub const FetchArgs = struct {
    io:    std.Io,
    gpa:   std.mem.Allocator,
    cfg:   *const Config,
    state: *BatchState,
};

pub fn fetchWorker(args: *FetchArgs) void {
    const s   = args.state;
    const cfg = args.cfg;
    const t0  = nowNs();
    s.fbdr_ms = if (cfg.fetch_mode == 0)
        rpc.fetchBatchFlat(args.io, args.gpa, &s.method_arenas, cfg.rpc_url, s.block_nums, s.block_results)
    else
        cfg.fetch_fn(args.io, args.gpa, s.batch_arena.allocator(), cfg.rpc_url, s.block_nums, s.block_results);

    // Retry failed blocks via reserve RPC if available.
    if (cfg.reserve_rpc_url.len > 0) {
        var retry_buf: [64]u64 = undefined;
        var retry_idx: [64]usize = undefined;
        var retry_count: usize = 0;
        for (s.block_results, 0..) |bd, i| {
            if (bd.err and retry_count < retry_buf.len) {
                retry_buf[retry_count] = s.block_nums[i];
                retry_idx[retry_count] = i;
                retry_count += 1;
            }
        }
        if (retry_count > 0) {
            const retry_results = args.gpa.alloc(rpc.BlockData, retry_count) catch unreachable;
            defer args.gpa.free(retry_results);
            _ = rpc.fetchBlockBatch(args.io, args.gpa, s.batch_arena.allocator(),
                cfg.reserve_rpc_url, retry_buf[0..retry_count], retry_results);
            for (retry_results, retry_idx[0..retry_count]) |rb, idx| {
                s.block_results[idx] = rb;
            }
        }
    }

    // Log blocks that are still failed after all retries.
    for (s.block_results, s.block_nums) |bd, bn| {
        if (bd.err) std.debug.print("[WARN] block {d} fetch failed — will be skipped\n", .{bn});
    }

    const t1 = nowNs();
    s.ent = transform.transformBatch(s.batch_arena.allocator(), s.block_results, cfg.chunk_size, cfg.remap_mod) catch return;
    const t2 = nowNs();
    s.transform_ms = @as(f64, @floatFromInt(t2 - t1)) / 1e6;
    s.tpt_ms       = @as(f64, @floatFromInt(t2 - t0)) / 1e6;
}

// ─── PrevBatch: tracks one in-flight save ────────────────────────────────────

pub const PrevBatch = struct {
    thread:     std.Thread,
    state:      *BatchState,
    metric_idx: usize,
};

pub fn finishPrev(
    p:          *PrevBatch,
    gpa:        std.mem.Allocator,
    redis:      *db.RedisConn,
    batches:    *std.ArrayList(BatchMetric),
    to_block:   u64,
    blocks_done: *u64,
) !void {
    p.thread.join();
    const ps = p.state;
    if (ps.save_error) |err| {
        std.debug.print("[FATAL] save failed blocks {d}-{d}: {s}\n",
            .{ ps.batch_start, ps.batch_end, @errorName(err) });
        ps.deinit();
        gpa.destroy(ps);
        return err;
    }
    batches.items[p.metric_idx].save_ms  = ps.save_ms;
    batches.items[p.metric_idx].total_ms = ps.tpt_ms + ps.save_ms;
    if (ps.ent.last_block > 0) {
        var buf: [20]u8 = undefined;
        const s = std.fmt.bufPrint(&buf, "{d}", .{ps.ent.last_block}) catch unreachable;
        try redis.setStr("LATEST_PROCESSED_BLOCK_NUMBER", s);
    }
    const batch_blocks = ps.batch_end - ps.batch_start + 1;
    blocks_done.* += batch_blocks;
    const left = if (to_block >= ps.batch_end) to_block - ps.batch_end else 0;
    const e = &ps.ent;
    std.debug.print(
        "\u{2705} [{d}\u{2025}{d}] left={d} FBDR={d:.0}ms TPT={d:.0}ms save={d:.0}ms | B:{d} T:{d} L:{d} IT:{d} C:{d}\n",
        .{ ps.batch_start, ps.batch_end, left,
           ps.fbdr_ms, ps.tpt_ms, ps.save_ms,
           e.blocks.items.len, e.txs.items.len, e.logs.items.len,
           e.internal_txs.items.len, e.contracts.items.len },
    );
    ps.deinit();
    gpa.destroy(ps);
}

// ─── runHistorical: main batch pipeline loop ─────────────────────────────────

pub fn runHistorical(
    io:           std.Io,
    gpa:          std.mem.Allocator,
    cfg:          *const Config,
    pools:        []db.CqlPool,
    dump_file_opt: ?std.Io.File,
    redis:        *db.RedisConn,
    from:         u64,
    to:           u64,
    metrics:      *Metrics,
) !void {
    const P = cfg.pipeline;

    std.debug.print("Historical: blocks {d} → {d}  pipeline={d}\n\n", .{ from, to, P });

    const t_run_start = nowNs();
    var i: u64 = from;
    var batch_counter: u32 = 0;
    var blocks_done: u64 = 0;

    const prev_saves  = try gpa.alloc(?PrevBatch, P); defer gpa.free(prev_saves);
    const round_states  = try gpa.alloc(?*BatchState, P); defer gpa.free(round_states);
    const fetch_threads = try gpa.alloc(?std.Thread, P); defer gpa.free(fetch_threads);
    const fetch_args    = try gpa.alloc(?*FetchArgs, P); defer gpa.free(fetch_args);
    @memset(prev_saves, null);

    while (i <= to) {
        @memset(round_states, null);
        @memset(fetch_threads, null);
        @memset(fetch_args, null);

        // Launch P fetch+transform workers in parallel
        for (0..P) |p| {
            if (i > to) break;
            const batch_start = i;
            const batch_end   = @min(i + cfg.batch_size - 1, to);
            const batch_count = batch_end - batch_start + 1;

            const state = try gpa.create(BatchState);
            const block_nums = try gpa.alloc(u64, batch_count);
            for (0..batch_count) |k| block_nums[k] = batch_start + k;

            state.* = .{
                .method_arenas = .{
                    std.heap.ArenaAllocator.init(std.heap.page_allocator),
                    std.heap.ArenaAllocator.init(std.heap.page_allocator),
                    std.heap.ArenaAllocator.init(std.heap.page_allocator),
                },
                .batch_arena   = std.heap.ArenaAllocator.init(std.heap.page_allocator),
                .block_results = try gpa.alloc(rpc.BlockData, batch_count),
                .block_nums    = block_nums,
                .ent           = undefined,
                .batch_start   = batch_start,
                .batch_end     = batch_end,
                .fbdr_ms = 0, .transform_ms = 0, .tpt_ms = 0, .save_ms = 0,
                .cql_pool  = &pools[p],
                .gpa       = gpa,
                .dump_file = dump_file_opt,
                .dump_io   = io,
                .batch_id  = batch_counter,
            };
            batch_counter += 1;
            round_states[p] = state;

            const fargs = try gpa.create(FetchArgs);
            fargs.* = .{ .io = io, .gpa = gpa, .cfg = cfg, .state = state };
            fetch_args[p]    = fargs;
            fetch_threads[p] = try std.Thread.spawn(.{}, fetchWorker, .{fargs});
            i = batch_end + 1;
        }

        // Join previous round's saves (overlap: ran while we were fetching)
        for (prev_saves) |*slot| {
            if (slot.*) |*ps| {
                try finishPrev(ps, gpa, redis, &metrics.batches, to, &blocks_done);
                slot.* = null;
            }
        }

        // Wait for this round's fetches; record metrics; spawn saves
        for (0..P) |p| {
            if (fetch_threads[p]) |t| t.join();
            if (fetch_args[p])    |fa| { gpa.destroy(fa); fetch_args[p] = null; }
            const state = round_states[p] orelse continue;

            for (state.block_results) |bd| {
                if (!bd.err and bd.block != null) {
                    try metrics.blocks.append(gpa, .{
                        .block_num     = bd.block_num,
                        .fbdr_ms       = state.fbdr_ms,
                        .http_block_ms = bd.http_block_ms,
                        .http_rcpt_ms  = bd.http_rcpt_ms,
                        .http_trc_ms   = bd.http_trc_ms,
                    });
                }
            }
            const metric_idx = metrics.batches.items.len;
            try metrics.batches.append(gpa, .{
                .from_block   = state.batch_start,
                .to_block     = state.batch_end,
                .fbdr_ms      = state.fbdr_ms,
                .transform_ms = state.transform_ms,
                .tpt_ms       = state.tpt_ms,
                .save_ms      = 0, .total_ms = 0,
                .blocks       = state.ent.blocks.items.len,
                .txs          = state.ent.txs.items.len,
                .logs         = state.ent.logs.items.len,
                .internal_txs = state.ent.internal_txs.items.len,
                .contracts    = state.ent.contracts.items.len,
            });
            const save_thread = try std.Thread.spawn(.{}, doSave, .{state});
            prev_saves[p] = .{ .thread = save_thread, .state = state, .metric_idx = metric_idx };
        }
    }

    // Final join
    for (prev_saves) |*slot| {
        if (slot.*) |*ps| {
            try finishPrev(ps, gpa, redis, &metrics.batches, to, &blocks_done);
            slot.* = null;
        }
    }

    const elapsed_ms = @as(f64, @floatFromInt(nowNs() - t_run_start)) / 1e6;
    std.debug.print("\nTotal run time: {d:.0} ms\n", .{elapsed_ms});
}
