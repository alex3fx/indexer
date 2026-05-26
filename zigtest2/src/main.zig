// indexer — EVM blockchain parser. Entry point.
// Reads blocks from a JSON-RPC node, transforms them, writes to ScyllaDB + Redis.
//
// Modes:
//   Historical (default): PIPELINE=N batch workers, PrevBatch overlap pattern.
//   Realtime (REALTIME=1): single-block loop, minimum latency per block.
//   Dump (DUMP_FILE=path): write encoded rows to disk instead of ScyllaDB.
//
// Key env vars:
//   REMAP_MOD=N    chunk = block_number % N  (distributes across N Scylla shards)
//   PIPELINE=N     N parallel fetch+save workers (each owns a CQL pool)
//   BATCH_SIZE=N   blocks per fetch batch (default 10)
//   REALTIME=1     switch to single-block realtime mode
const std       = @import("std");
const rpc       = @import("rpc");
const db        = @import("db");
const cfg_mod   = @import("config");
const met       = @import("metrics");
const pipe      = @import("pipeline");
const rt        = @import("realtime");
const verify    = @import("verify");

const parseConfig    = cfg_mod.parseConfig;
const fetch_mode_names = cfg_mod.fetch_mode_names;
const Metrics        = met.Metrics;

pub fn main(init: std.process.Init) !void {
    const io  = init.io;
    const gpa = std.heap.page_allocator;

    const cfg = try parseConfig(gpa, io, init.environ_map);

    if (cfg.remap_mod > 0) {
        std.debug.print("EVM Indexer [{d}] {s}\nRPC: {s}  to={d}  batch={d}  chunk=block%{d} (remap)\n",
            .{ cfg.fetch_mode, fetch_mode_names[cfg.fetch_mode],
               cfg.rpc_url, cfg.to_block, cfg.batch_size, cfg.remap_mod });
    } else {
        std.debug.print("EVM Indexer [{d}] {s}\nRPC: {s}  to={d}  batch={d}  chunk=block/{d}\n",
            .{ cfg.fetch_mode, fetch_mode_names[cfg.fetch_mode],
               cfg.rpc_url, cfg.to_block, cfg.batch_size, cfg.chunk_size });
    }

    // ── Redis ────────────────────────────────────────────────────────────────
    var redis = try db.RedisConn.init(io, gpa, cfg.redis_host, cfg.redis_port);
    defer redis.deinit();
    if (cfg.redis_pass.len > 0) try redis.auth(cfg.redis_pass);
    try redis.selectDb(cfg.redis_db);

    var from: u64 = cfg.from_block;

    // ── Verify mode (VERIFY=1) — uses explicit FROM_BLOCK, not cursor ─────────
    if (cfg.verify) {
        return verify.runVerify(io, gpa, &cfg, from, cfg.to_block);
    }

    if (try redis.get("LATEST_PROCESSED_BLOCK_NUMBER")) |val| {
        defer gpa.free(val);
        from = (std.fmt.parseInt(u64, val, 10) catch 0) + 1;
    }

    // ── CQL pools (PIPELINE=N → N pools, each POOL_SIZE connections) ─────────
    const P = cfg.pipeline;
    const dump_mode = cfg.dump_file.len > 0;
    var dump_file_opt: ?std.Io.File = null;
    const pools = try gpa.alloc(db.CqlPool, P);
    defer gpa.free(pools);

    if (dump_mode) {
        std.debug.print("DUMP MODE: writing to {s}\n", .{cfg.dump_file});
        dump_file_opt = try db.dumpOpen(io, cfg.dump_file);
        for (0..P) |p| pools[p] = .{};
    } else {
        std.debug.print("ScyllaDB: {s}:{d}  pool={d}  workers={d}  split={}+{}+{}+{}+{}+{}\n",
            .{ cfg.scylla_host, cfg.scylla_port, db.POOL_SIZE, P,
               db.SPLIT[0], db.SPLIT[1], db.SPLIT[2], db.SPLIT[3], db.SPLIT[4], db.SPLIT[5] });
        std.debug.print("batch: blk={d} txs={d} logs={d} itxs={d} cont={d} cba={d}\n",
            .{ db.BS_BLK, db.BS_TXS, db.BS_LOGS, db.BS_ITXS, db.BS_CONT, db.BS_CBA });
        std.debug.print("Connecting ({d} pool(s))...\n", .{P});
        for (0..P) |p| {
            pools[p] = try db.CqlPool.init(io, gpa, cfg.scylla_host, cfg.scylla_port,
                           cfg.scylla_keyspace, cfg.scylla_user, cfg.scylla_pass);
        }
    }
    defer if (!dump_mode) { for (0..P) |p| pools[p].deinit(); };

    // ── Catchup + Realtime mode (WS_URL set) ─────────────────────────────────
    if (cfg.ws_url.len > 0 and !dump_mode) {
        var metrics: Metrics = .{};
        defer metrics.deinit(gpa);
        try rt.runCatchupAndRealtime(io, gpa, &cfg, pools, &redis, from, &metrics);
        metrics.saveJson(gpa, io, cfg.results_dir);
        return;
    }

    // ── Pure realtime mode (REALTIME=1, no WS) ───────────────────────────────
    if (cfg.realtime and !dump_mode) {
        return rt.runRealtime(io, gpa, &cfg, &pools[0], &redis);
    }

    // ── Historical batch mode ─────────────────────────────────────────────────
    var metrics: Metrics = .{};
    defer metrics.deinit(gpa);

    try pipe.runHistorical(io, gpa, &cfg, pools, dump_file_opt, &redis, from, cfg.to_block, &metrics);

    metrics.print();
    metrics.saveJson(gpa, io, cfg.results_dir);
}
