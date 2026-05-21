const std = @import("std");
const rpc = @import("rpc");
const transform = @import("transform");
const db = @import("db");

fn nowNs() i64 {
    const linux = std.os.linux;
    var ts: linux.timespec = undefined;
    _ = linux.clock_gettime(.MONOTONIC, &ts);
    return ts.sec * 1_000_000_000 + ts.nsec;
}

// ─── Fetch mode ───────────────────────────────────────────────────────────────
// RPC_FETCH_MODE=0  flat-parallel  N×3 threads, 1 request each  (default, best on localhost)
// RPC_FETCH_MODE=1  batch-method   3 threads, N-block batch per method (best with high RTT)
// RPC_FETCH_MODE=2  block-batch    N threads, 3-method batch per block (best for single block sync)

pub const FetchFn = *const fn (std.Io, std.mem.Allocator, std.mem.Allocator, []const u8, []const u64, []rpc.BlockData) f64;

const fetch_mode_names = [3][]const u8{
    "flat-parallel (N×3 threads)",
    "batch-method  (3 threads, N blocks/method)",
    "block-batch   (N threads, 3 methods/block)",
};

// ─── Config ───────────────────────────────────────────────────────────────────

const Config = struct {
    rpc_url: []const u8,
    chain_id: u64,
    chunk_size: u64,
    remap_mod: u64,
    to_block: u64,
    pipeline: usize,
    batch_size: usize,
    redis_host: []const u8,
    redis_port: u16,
    redis_pass: []const u8,
    redis_db: u8,
    scylla_host: []const u8,
    scylla_port: u16,
    scylla_keyspace: []const u8,
    scylla_user: []const u8,
    scylla_pass: []const u8,
    results_dir: []const u8,
    dump_file: []const u8,
    fetch_fn: FetchFn,
    fetch_mode: u8,
};

fn getEnv(env: *std.process.Environ.Map, key: []const u8, default: []const u8) []const u8 {
    return env.get(key) orelse default;
}

fn getEnvInt(comptime T: type, env: *std.process.Environ.Map, key: []const u8, default: T) T {
    const s = env.get(key) orelse return default;
    return std.fmt.parseInt(T, s, 10) catch default;
}

fn parseConfig(gpa: std.mem.Allocator, io: std.Io, env: *std.process.Environ.Map) !Config {
    const redis_url = getEnv(env, "CM_CONNECTION_URL", "redis://:mockpass@127.0.0.1:6379/0");

    var redis_host: []const u8 = "127.0.0.1";
    var redis_port: u16 = 6379;
    var redis_pass: []const u8 = "";
    var redis_db: u8 = 0;

    if (std.mem.startsWith(u8, redis_url, "redis://")) {
        const after_scheme = redis_url[8..];
        var rest = after_scheme;
        if (std.mem.startsWith(u8, rest, ":")) {
            if (std.mem.indexOfScalar(u8, rest, '@')) |at_idx| {
                redis_pass = rest[1..at_idx];
                rest = rest[at_idx + 1 ..];
            }
        }
        if (std.mem.indexOfScalar(u8, rest, '/')) |slash_idx| {
            const db_str = rest[slash_idx + 1 ..];
            rest = rest[0..slash_idx];
            redis_db = std.fmt.parseInt(u8, db_str, 10) catch 0;
        }
        if (std.mem.lastIndexOfScalar(u8, rest, ':')) |colon_idx| {
            redis_host = rest[0..colon_idx];
            redis_port = std.fmt.parseInt(u16, rest[colon_idx + 1 ..], 10) catch 6379;
        } else {
            redis_host = rest;
        }
    }

    var scylla_host: []const u8 = "127.0.0.1";
    var scylla_port: u16 = 9042;
    const scylla_cp = getEnv(env, "SCYLLA_DB_CONTACT_POINTS", "[\"127.0.0.1:9042\"]");
    if (std.mem.indexOf(u8, scylla_cp, "\"")) |q1| {
        if (std.mem.indexOf(u8, scylla_cp[q1 + 1 ..], "\"")) |q2| {
            const host_port = scylla_cp[q1 + 1 ..][0..q2];
            if (std.mem.lastIndexOfScalar(u8, host_port, ':')) |c| {
                scylla_host = host_port[0..c];
                scylla_port = std.fmt.parseInt(u16, host_port[c + 1 ..], 10) catch 9042;
            } else {
                scylla_host = host_port;
            }
        }
    }

    var scylla_user: []const u8 = "cassandra";
    var scylla_pass_: []const u8 = "cassandra";
    const creds_str = getEnv(env, "SCYLLA_DB_CREDENTIALS", "{\"username\":\"cassandra\",\"password\":\"cassandra\"}");
    if (extractJsonStr(gpa, creds_str, "username")) |u| scylla_user = u;
    if (extractJsonStr(gpa, creds_str, "password")) |p| scylla_pass_ = p;

    var exe_buf: [std.fs.max_path_bytes]u8 = undefined;
    const exe_len = std.process.executablePath(io, &exe_buf) catch 0;
    const exe_dir = std.fs.path.dirname(exe_buf[0..exe_len]) orelse ".";
    const results_dir = std.fs.path.join(gpa, &.{ exe_dir, "results" }) catch "./results";

    return Config{
        .rpc_url = getEnv(env, "RPC_URL", "http://127.0.0.1:8545"),
        .chain_id = getEnvInt(u64, env, "CHAIN_ID", 1),
        .chunk_size = getEnvInt(u64, env, "RAW_CHUNK_SIZE", 1000),
        .remap_mod  = getEnvInt(u64, env, "REMAP_MOD", 0),
        .pipeline   = @max(1, getEnvInt(usize, env, "PIPELINE", 1)),
        .to_block = getEnvInt(u64, env, "TO_BLOCK", 25_079_196),
        .batch_size = getEnvInt(usize, env, "BATCH_SIZE", 10),
        .redis_host = redis_host,
        .redis_port = redis_port,
        .redis_pass = redis_pass,
        .redis_db = redis_db,
        .scylla_host = scylla_host,
        .scylla_port = scylla_port,
        .scylla_keyspace = getEnv(env, "SCYLLA_DB_KEYSPACE", "eth"),
        .scylla_user = scylla_user,
        .scylla_pass = scylla_pass_,
        .results_dir = results_dir,
        .dump_file = getEnv(env, "DUMP_FILE", ""),
        .fetch_mode = blk: {
            const mode_str = getEnv(env, "RPC_FETCH_MODE", getEnv(env, "RPC_BATCH", "0"));
            break :blk std.fmt.parseInt(u8, mode_str, 10) catch 0;
        },
        .fetch_fn = blk: {
            const mode_str = getEnv(env, "RPC_FETCH_MODE", getEnv(env, "RPC_BATCH", "0"));
            const mode = std.fmt.parseInt(u8, mode_str, 10) catch 0;
            // Mode 0 is called directly (not through FetchFn) — uses method_arenas.
            break :blk switch (mode) {
                1 => &rpc.fetchBatch3,
                2 => &rpc.fetchBlockBatch,
                else => &rpc.fetchBatch3, // placeholder for mode 0, never called via fetch_fn
            };
        },
    };
}

fn extractJsonStr(gpa: std.mem.Allocator, json_str: []const u8, key: []const u8) ?[]const u8 {
    const search = std.fmt.allocPrint(gpa, "\"{s}\":\"", .{key}) catch return null;
    defer gpa.free(search);
    const start = std.mem.indexOf(u8, json_str, search) orelse return null;
    const after = json_str[start + search.len ..];
    const end = std.mem.indexOfScalar(u8, after, '"') orelse return null;
    return gpa.dupe(u8, after[0..end]) catch null;
}

// ─── Metrics ──────────────────────────────────────────────────────────────────

const BlockMetric = struct {
    block_num: u64,
    fbdr_ms: f64,
    http_block_ms: f64,
    http_rcpt_ms: f64,
    http_trc_ms: f64,
};

const BatchMetric = struct {
    from_block: u64,
    to_block: u64,
    fbdr_ms: f64,
    transform_ms: f64,
    tpt_ms: f64,
    save_ms: f64,
    total_ms: f64,
    blocks: usize,
    txs: usize,
    logs: usize,
    internal_txs: usize,
    contracts: usize,
};

const Metrics = struct {
    blocks: std.ArrayList(BlockMetric) = .empty,
    batches: std.ArrayList(BatchMetric) = .empty,

    fn deinit(self: *Metrics, gpa: std.mem.Allocator) void {
        self.blocks.deinit(gpa);
        self.batches.deinit(gpa);
    }

    fn save(self: *Metrics, gpa: std.mem.Allocator, io: std.Io, results_dir: []const u8) void {
        var fbdr_sum: f64 = 0;
        var fbdr_max: f64 = 0;
        var http_sum: f64 = 0;
        var tpt_total: f64 = 0;
        var save_total: f64 = 0;
        var total_rows: usize = 0;

        for (self.blocks.items) |b| {
            fbdr_sum += b.fbdr_ms;
            if (b.fbdr_ms > fbdr_max) fbdr_max = b.fbdr_ms;
            http_sum += (b.http_block_ms + b.http_rcpt_ms + b.http_trc_ms) / 3.0;
        }
        for (self.batches.items) |b| {
            tpt_total += b.tpt_ms;
            save_total += b.save_ms;
            total_rows += b.txs + b.logs + b.internal_txs + b.contracts + b.blocks;
        }
        const n = @as(f64, @floatFromInt(self.blocks.items.len));
        const fbdr_avg = if (n > 0) fbdr_sum / n else 0;
        const http_avg = if (n > 0) http_sum / n else 0;
        const tpt_avg = if (n > 0) tpt_total / n else 0;

        std.debug.print("\n📊 Zig2 Parser Results ({d} blocks, {d} batches)\n", .{ self.blocks.items.len, self.batches.items.len });
        std.debug.print("  FBDR avg/block  : {d:.1} ms\n", .{fbdr_avg});
        std.debug.print("  FBDR max        : {d:.1} ms\n", .{fbdr_max});
        std.debug.print("  HTTP avg/req    : {d:.1} ms\n", .{http_avg});
        std.debug.print("  TPT total       : {d:.0} ms\n", .{tpt_total});
        std.debug.print("  TPT avg/block   : {d:.1} ms\n", .{tpt_avg});
        std.debug.print("  Save total      : {d:.0} ms\n", .{save_total});
        std.debug.print("  Total rows      : {d}\n", .{total_rows});

        self.writeJsonFile(gpa, io, results_dir, fbdr_avg, fbdr_max, http_avg, tpt_total, tpt_avg, save_total, total_rows) catch |err| {
            std.debug.print("Warning: could not save results JSON: {}\n", .{err});
        };
    }

    fn writeJsonFile(
        self: *Metrics,
        gpa: std.mem.Allocator,
        io: std.Io,
        results_dir: []const u8,
        fbdr_avg: f64, fbdr_max: f64, http_avg: f64,
        tpt_total: f64, tpt_avg: f64, save_total: f64, total_rows: usize,
    ) !void {
        const ts_ms = @divTrunc(nowNs(), 1_000_000);
        var aw = std.Io.Writer.Allocating.init(gpa);
        defer aw.deinit();

        const w = &aw.writer;
        try w.print(
            \\{{"run_id":"zig2_{d}","timestamp_ms":{d},"summary":{{
            \\"total_blocks":{d},"total_batches":{d},
            \\"fbdr_avg_ms":{d:.1},"fbdr_max_ms":{d:.1},
            \\"http_avg_ms":{d:.1},
            \\"tpt_total_ms":{d:.0},"tpt_avg_block_ms":{d:.1},
            \\"save_total_ms":{d:.0},"total_rows":{d}
            \\}}}}
        , .{
            ts_ms, ts_ms,
            self.blocks.items.len, self.batches.items.len,
            fbdr_avg, fbdr_max,
            http_avg,
            tpt_total, tpt_avg,
            save_total, total_rows,
        });

        const json_data = aw.written();
        const filename = try std.fmt.allocPrint(gpa, "zigtest2_{d}.json", .{ts_ms});
        defer gpa.free(filename);
        const path = try std.fs.path.join(gpa, &.{ results_dir, filename });
        defer gpa.free(path);

        std.Io.Dir.createDirAbsolute(io, results_dir, .default_dir) catch {};
        const out_file = try std.Io.Dir.createFileAbsolute(io, path, .{});
        defer out_file.close(io);
        try out_file.writeStreamingAll(io, json_data);
        std.debug.print("Results: {s}\n", .{path});
    }
};

// ─── Batch pipeline state ─────────────────────────────────────────────────────
// Holds all resources for one batch through its full lifetime (fetch→parse→transform→save).
// save runs in a background thread; arenas freed after join.

const BatchState = struct {
    method_arenas: [3]std.heap.ArenaAllocator,
    batch_arena:   std.heap.ArenaAllocator,
    block_results: []rpc.BlockData,
    block_nums:    []u64,           // owned; freed in deinit
    ent:           transform.Entities,
    batch_start:   u64,
    batch_end:     u64,
    fbdr_ms:       f64,
    transform_ms:  f64,
    tpt_ms:        f64,
    save_ms:       f64 = 0,
    cql_pool:      *db.CqlPool,
    prep_ids:      *db.PreparedIds,
    gpa:           std.mem.Allocator,
    dump_file:     ?std.Io.File = null,
    dump_io:       std.Io = undefined,
    batch_id:      u32 = 0,

    fn deinit(self: *BatchState) void {
        for (&self.method_arenas) |*a| a.deinit();
        self.batch_arena.deinit();
        self.gpa.free(self.block_results);
        self.gpa.free(self.block_nums);
    }
};

fn doSave(state: *BatchState) void {
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
            .gpa       = state.gpa,
            .prep_ids  = state.prep_ids,
            .ent       = &state.ent,
            .result_ms = &state.save_ms,
        });
    }
}

// ─── Fetch+transform worker (for parallel pipeline) ──────────────────────────
// Runs fetch + transform in a background thread so multiple batches can be
// fetched simultaneously. Each worker owns its BatchState exclusively.

const FetchArgs = struct { io: std.Io, gpa: std.mem.Allocator, cfg: *const Config, state: *BatchState };

fn fetchWorker(args: *FetchArgs) void {
    const s = args.state;
    const cfg = args.cfg;
    const t0 = nowNs();
    s.fbdr_ms = if (cfg.fetch_mode == 0)
        rpc.fetchBatchFlat(args.io, args.gpa, &s.method_arenas, cfg.rpc_url, s.block_nums, s.block_results)
    else
        cfg.fetch_fn(args.io, args.gpa, s.batch_arena.allocator(), cfg.rpc_url, s.block_nums, s.block_results);
    const t1 = nowNs();
    s.ent = transform.transformBatch(s.batch_arena.allocator(), s.block_results, cfg.chunk_size, cfg.remap_mod) catch return;
    const t2 = nowNs();
    s.transform_ms = @as(f64, @floatFromInt(t2 - t1)) / 1e6;
    s.tpt_ms       = @as(f64, @floatFromInt(t2 - t0)) / 1e6;
}

// ─── Batch pipelining ─────────────────────────────────────────────────────────
// Tracks the in-flight save thread so fetch+transform of the next batch
// overlaps with save of the current batch.

const PrevBatch = struct {
    thread:     std.Thread,
    state:      *BatchState,
    metric_idx: usize,
};

fn finishPrev(
    p:       *PrevBatch,
    gpa:     std.mem.Allocator,
    redis:   *db.RedisConn,
    batches: *std.ArrayList(BatchMetric),
) !void {
    p.thread.join();
    const ps = p.state;
    batches.items[p.metric_idx].save_ms  = ps.save_ms;
    batches.items[p.metric_idx].total_ms = ps.tpt_ms + ps.save_ms;
    if (ps.ent.last_block > 0) {
        const s = try std.fmt.allocPrint(gpa, "{d}", .{ps.ent.last_block});
        defer gpa.free(s);
        try redis.setStr("LATEST_PROCESSED_BLOCK_NUMBER", s);
    }
    const e = &ps.ent;
    std.debug.print(
        "\u{2705} [{d}\u{2025}{d}] FBDR={d:.0}ms transform={d:.0}ms TPT={d:.0}ms save={d:.0}ms | B:{d} T:{d} L:{d} IT:{d} C:{d}\n",
        .{ ps.batch_start, ps.batch_end,
           ps.fbdr_ms, ps.transform_ms, ps.tpt_ms, ps.save_ms,
           e.blocks.items.len, e.txs.items.len, e.logs.items.len,
           e.internal_txs.items.len, e.contracts.items.len },
    );
    ps.deinit();
    gpa.destroy(ps);
}

// ─── Main ─────────────────────────────────────────────────────────────────────

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const gpa = std.heap.page_allocator;

    const cfg = try parseConfig(gpa, io, init.environ_map);

    if (cfg.remap_mod > 0) {
        std.debug.print("Zig2 EVM Parser [{d}] {s}\nRPC: {s}  to={d}  batch={d}  chunk=block%{d} (remap)\n",
            .{ cfg.fetch_mode, fetch_mode_names[cfg.fetch_mode],
               cfg.rpc_url, cfg.to_block, cfg.batch_size, cfg.remap_mod });
    } else {
        std.debug.print("Zig2 EVM Parser [{d}] {s}\nRPC: {s}  to={d}  batch={d}  chunk=block/{d}\n",
            .{ cfg.fetch_mode, fetch_mode_names[cfg.fetch_mode],
               cfg.rpc_url, cfg.to_block, cfg.batch_size, cfg.chunk_size });
    }

    // ── Redis ────────────────────────────────────────────────────────────────
    var redis = try db.RedisConn.init(io, gpa, cfg.redis_host, cfg.redis_port);
    defer redis.deinit();
    if (cfg.redis_pass.len > 0) try redis.auth(cfg.redis_pass);
    try redis.selectDb(cfg.redis_db);

    var from: u64 = 0;
    if (try redis.get("LATEST_PROCESSED_BLOCK_NUMBER")) |val| {
        defer gpa.free(val);
        from = (std.fmt.parseInt(u64, val, 10) catch 0) + 1;
    }
    if (from == 0) {
        std.debug.print("ERROR: LATEST_PROCESSED_BLOCK_NUMBER not set in Redis\n", .{});
        return error.NoStartBlock;
    }

    // ── Dump mode or ScyllaDB ─────────────────────────────────────────────────
    // PIPELINE=N: N CQL pools, N fetch workers running in parallel per round.
    // Each pool has POOL_SIZE connections; saves use their own pool → no contention.
    const P = cfg.pipeline;
    const dump_mode = cfg.dump_file.len > 0;
    var dump_file_opt: ?std.Io.File = null;
    const pools    = try gpa.alloc(db.CqlPool, P);
    const prep_ids = try gpa.alloc(db.PreparedIds, P);
    defer gpa.free(pools);
    defer gpa.free(prep_ids);

    if (dump_mode) {
        std.debug.print("DUMP MODE: writing to {s}\n", .{cfg.dump_file});
        dump_file_opt = try db.dumpOpen(io, cfg.dump_file);
        for (0..P) |p| {
            pools[p]    = .{};
            prep_ids[p] = .{ .blocks=&.{}, .transactions=&.{}, .logs=&.{}, .internal_txs=&.{}, .contracts=&.{}, .contracts_by_addr=&.{} };
        }
    } else {
        std.debug.print("ScyllaDB: {s}:{d}  pool={d}  workers={d}  split={}+{}+{}+{}+{}+{}\n",
            .{ cfg.scylla_host, cfg.scylla_port, db.POOL_SIZE, P,
               db.SPLIT[0], db.SPLIT[1], db.SPLIT[2],
               db.SPLIT[3], db.SPLIT[4], db.SPLIT[5] });
        std.debug.print("Connecting to ScyllaDB ({d} pool(s))...\n", .{P});
        for (0..P) |p| {
            pools[p]    = try db.CqlPool.init(io, gpa, cfg.scylla_host, cfg.scylla_port,
                              cfg.scylla_keyspace, cfg.scylla_user, cfg.scylla_pass);
            prep_ids[p] = try db.prepareAll(pools[p].conns[0]);
        }
    }
    defer if (!dump_mode) { for (0..P) |p| pools[p].deinit(); };

    var metrics: Metrics = .{};
    defer metrics.deinit(gpa);

    std.debug.print("Starting: blocks {d} → {d}\n\n", .{ from, cfg.to_block });

    const t_run_start = nowNs();
    var i: u64 = from;
    var batch_counter: u32 = 0;

    // Ring of P save slots: save for round N overlaps with fetch for round N+1.
    const prev_saves  = try gpa.alloc(?PrevBatch, P);
    defer gpa.free(prev_saves);
    @memset(prev_saves, null);

    // Scratch slices reused each round (alloc once outside loop).
    const round_states  = try gpa.alloc(?*BatchState, P);
    const fetch_threads = try gpa.alloc(?std.Thread, P);
    const fetch_args    = try gpa.alloc(?*FetchArgs, P);
    defer gpa.free(round_states);
    defer gpa.free(fetch_threads);
    defer gpa.free(fetch_args);

    while (i <= cfg.to_block) {
        @memset(round_states, null);
        @memset(fetch_threads, null);
        @memset(fetch_args, null);

        // ── Launch P fetch+transform workers in parallel ───────────────────
        for (0..P) |p| {
            if (i > cfg.to_block) break;
            const batch_start = i;
            const batch_end   = @min(i + cfg.batch_size - 1, cfg.to_block);
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
                .fbdr_ms       = 0,
                .transform_ms  = 0,
                .tpt_ms        = 0,
                .save_ms       = 0,
                .cql_pool      = &pools[p],
                .prep_ids      = &prep_ids[p],
                .gpa           = gpa,
                .dump_file     = dump_file_opt,
                .dump_io       = io,
                .batch_id      = batch_counter,
            };
            batch_counter += 1;
            round_states[p] = state;

            const fargs = try gpa.create(FetchArgs);
            fargs.* = .{ .io = io, .gpa = gpa, .cfg = &cfg, .state = state };
            fetch_args[p]    = fargs;
            fetch_threads[p] = try std.Thread.spawn(.{}, fetchWorker, .{fargs});

            i = batch_end + 1;
        }

        // ── Join previous round's saves (overlap: ran while we were fetching) ─
        for (prev_saves) |*slot| {
            if (slot.*) |*ps| {
                try finishPrev(ps, gpa, &redis, &metrics.batches);
                slot.* = null;
            }
        }

        // ── Wait for this round's fetches; record metrics; spawn saves ─────
        for (0..P) |p| {
            if (fetch_threads[p]) |t| t.join();
            if (fetch_args[p]) |fa| { gpa.destroy(fa); fetch_args[p] = null; }
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
                .save_ms      = 0,
                .total_ms     = 0,
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

    // ── Final join ────────────────────────────────────────────────────────
    for (prev_saves) |*slot| {
        if (slot.*) |*ps| {
            try finishPrev(ps, gpa, &redis, &metrics.batches);
            slot.* = null;
        }
    }

    const elapsed_ms = @as(f64, @floatFromInt(nowNs() - t_run_start)) / 1e6;
    std.debug.print("\nTotal run time: {d:.0} ms\n", .{elapsed_ms});

    metrics.save(gpa, io, cfg.results_dir);
}

fn freeBlockData(gpa: std.mem.Allocator, bd: *rpc.BlockData) void {
    if (bd.block) |*blk| {
        gpa.free(blk.number);
        gpa.free(blk.timestamp);
        if (blk.milliTimestamp) |m| gpa.free(m);
        gpa.free(blk.miner);
        for (blk.transactions) |tx| {
            gpa.free(tx.hash);
            gpa.free(tx.transactionIndex);
            gpa.free(tx.from);
            if (tx.to) |t| gpa.free(t);
            gpa.free(tx.value);
            gpa.free(tx.gas);
            gpa.free(tx.gasPrice);
            gpa.free(tx.input);
            gpa.free(tx.@"type");
            if (tx.maxPriorityFeePerGas) |v| gpa.free(v);
            if (tx.maxFeePerGas) |v| gpa.free(v);
        }
        gpa.free(blk.transactions);
    }
    if (bd.receipts) |receipts| {
        for (receipts) |rcpt| {
            for (rcpt.logs) |log| {
                for (log.topics) |t| gpa.free(t);
                gpa.free(log.topics);
                gpa.free(log.address);
                gpa.free(log.data);
                gpa.free(log.transactionHash);
                gpa.free(log.transactionIndex);
                gpa.free(log.logIndex);
            }
            gpa.free(rcpt.logs);
            gpa.free(rcpt.transactionHash);
            gpa.free(rcpt.transactionIndex);
            gpa.free(rcpt.gasUsed);
            gpa.free(rcpt.cumulativeGasUsed);
            if (rcpt.effectiveGasPrice) |v| gpa.free(v);
            if (rcpt.contractAddress) |v| gpa.free(v);
            gpa.free(rcpt.status);
        }
        gpa.free(receipts);
    }
    if (bd.traces) |traces| {
        for (traces) |trace| {
            if (trace.transactionHash) |v| gpa.free(v);
            gpa.free(trace.action.from);
            if (trace.action.to) |v| gpa.free(v);
            if (trace.action.value) |v| gpa.free(v);
            if (trace.action.init) |v| gpa.free(v);
            if (trace.action.input) |v| gpa.free(v);
            if (trace.action.creationMethod) |v| gpa.free(v);
            if (trace.result) |r| {
                if (r.address) |v| gpa.free(v);
                if (r.code) |v| gpa.free(v);
            }
        }
        gpa.free(traces);
    }
}
