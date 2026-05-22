// Realtime mode: processes blocks one at a time as they arrive.
// Two sub-modes:
//   Polling (default): fetchBlock every POLL_MS until block available.
//   WebSocket (WS_URL set): subscribe to eth_subscribe newHeads, fetch on notification.
// Minimum latency path: [ws notification →] fetchBlock → transformBlock → saveBatch → Redis.
const std       = @import("std");
const rpc       = @import("rpc");
const transform = @import("transform");
const db        = @import("db");
const config    = @import("config");
const ws        = @import("ws");

const Config = config.Config;

fn nowNs() i64 {
    const linux = std.os.linux;
    var ts: linux.timespec = undefined;
    _ = linux.clock_gettime(.MONOTONIC, &ts);
    return ts.sec * 1_000_000_000 + ts.nsec;
}

fn sleepMs(ms: u64) void {
    const linux = std.os.linux;
    const ts = linux.timespec{
        .sec  = @intCast(ms / 1000),
        .nsec = @intCast((ms % 1000) * 1_000_000),
    };
    _ = linux.nanosleep(&ts, null);
}

pub const BlockResult = struct {
    fetch_ms:     f64,
    transform_ms: f64,
    save_ms:      f64,
    total_ms:     f64,
};

/// Process a single block end-to-end: fetch → transform → save.
/// Returns null if the block is not yet available.
pub fn processBlock(
    io:        std.Io,
    gpa:       std.mem.Allocator,
    cfg:       *const Config,
    pool:      *db.CqlPool,
    prep_ids:  *db.PreparedIds,
    block_num: u64,
    method_arenas: *[3]std.heap.ArenaAllocator,
    block_arena:   *std.heap.ArenaAllocator,
) !?BlockResult {
    _ = block_arena.reset(.retain_capacity);
    for (method_arenas) |*a| _ = a.reset(.retain_capacity);

    const arena = block_arena.allocator();
    var bd: rpc.BlockData = .{
        .block_num     = block_num,
        .block         = null,  .receipts      = null, .traces        = null,
        .http_block_ms = 0,     .json_block_ms = 0,
        .http_rcpt_ms  = 0,     .json_rcpt_ms  = 0,
        .http_trc_ms   = 0,     .json_trc_ms   = 0,
        .fbdr_ms       = 0,     .err            = false,
    };

    const t0 = nowNs();
    const fetch_ms = rpc.fetchBlock(io, gpa, method_arenas, cfg.rpc_url, block_num, &bd);

    if (bd.block == null) return null; // not yet available

    const t1 = nowNs();
    var ent = transform.initEntities();
    try transform.transformBlock(arena, bd, cfg.chunk_size, cfg.remap_mod, &ent);

    const t2 = nowNs();
    const transform_ms = @as(f64, @floatFromInt(t2 - t1)) / 1e6;

    var save_ms: f64 = 0;
    db.saveBatch(.{
        .pool      = pool,
        .gpa       = gpa,
        .prep_ids  = prep_ids,
        .ent       = &ent,
        .result_ms = &save_ms,
    });

    const total_ms = @as(f64, @floatFromInt(nowNs() - t0)) / 1e6;

    std.debug.print(
        "\u{26a1} [{d}] fetch={d:.0}ms transform={d:.0}ms save={d:.0}ms total={d:.0}ms | T:{d} L:{d} IT:{d}\n",
        .{ block_num, fetch_ms, transform_ms, save_ms, total_ms,
           ent.txs.items.len, ent.logs.items.len, ent.internal_txs.items.len },
    );

    return .{ .fetch_ms = fetch_ms, .transform_ms = transform_ms,
               .save_ms = save_ms, .total_ms = total_ms };
}

/// Realtime loop: poll for new blocks, process each immediately.
pub fn runRealtime(
    io:       std.Io,
    gpa:      std.mem.Allocator,
    cfg:      *const Config,
    pool:     *db.CqlPool,
    prep_ids: *db.PreparedIds,
    redis:    *db.RedisConn,
) !void {
    std.debug.print("Realtime mode: poll_ms={d}  chunk=block%{d}  to={d}\n\n",
        .{ cfg.poll_ms, if (cfg.remap_mod > 0) cfg.remap_mod else cfg.chunk_size, cfg.to_block });

    var method_arenas = [3]std.heap.ArenaAllocator{
        std.heap.ArenaAllocator.init(std.heap.page_allocator),
        std.heap.ArenaAllocator.init(std.heap.page_allocator),
        std.heap.ArenaAllocator.init(std.heap.page_allocator),
    };
    defer for (&method_arenas) |*a| a.deinit();
    var block_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer block_arena.deinit();

    var block_num: u64 = 0;
    if (try redis.get("LATEST_PROCESSED_BLOCK_NUMBER")) |val| {
        defer gpa.free(val);
        block_num = (std.fmt.parseInt(u64, val, 10) catch 0) + 1;
    }
    if (block_num == 0) {
        std.debug.print("ERROR: LATEST_PROCESSED_BLOCK_NUMBER not set\n", .{});
        return error.NoStartBlock;
    }

    // Per-block stats
    var n: u64 = 0;
    var sum_total: f64 = 0;  var min_total: f64 = std.math.floatMax(f64);  var max_total: f64 = 0;
    var sum_fetch: f64 = 0;  var min_fetch: f64 = std.math.floatMax(f64);  var max_fetch: f64 = 0;
    var sum_save:  f64 = 0;  var min_save:  f64 = std.math.floatMax(f64);  var max_save:  f64 = 0;

    while (block_num <= cfg.to_block) {
        const br = try processBlock(io, gpa, cfg, pool, prep_ids, block_num,
                                    &method_arenas, &block_arena);

        if (br == null) {
            sleepMs(cfg.poll_ms);
            continue;
        }

        const r = br.?;
        sum_total += r.total_ms;
        sum_fetch += r.fetch_ms;
        sum_save  += r.save_ms;
        if (r.total_ms < min_total) min_total = r.total_ms;
        if (r.total_ms > max_total) max_total = r.total_ms;
        if (r.fetch_ms < min_fetch) min_fetch = r.fetch_ms;
        if (r.fetch_ms > max_fetch) max_fetch = r.fetch_ms;
        if (r.save_ms  < min_save)  min_save  = r.save_ms;
        if (r.save_ms  > max_save)  max_save  = r.save_ms;
        n += 1;

        const s = try std.fmt.allocPrint(gpa, "{d}", .{block_num});
        defer gpa.free(s);
        try redis.setStr("LATEST_PROCESSED_BLOCK_NUMBER", s);
        block_num += 1;
    }

    if (n > 0) {
        const fn_ = @as(f64, @floatFromInt(n));
        std.debug.print(
            "\n⚡ Realtime summary ({d} blocks, poll_ms={d})\n" ++
            "  fetch  avg={d:.1}ms  min={d:.1}ms  max={d:.1}ms\n" ++
            "  save   avg={d:.1}ms  min={d:.1}ms  max={d:.1}ms\n" ++
            "  TOTAL  avg={d:.1}ms  min={d:.1}ms  max={d:.1}ms\n",
            .{ n, cfg.poll_ms,
               sum_fetch/fn_, min_fetch, max_fetch,
               sum_save/fn_,  min_save,  max_save,
               sum_total/fn_, min_total, max_total },
        );
    }
}

// ─── WebSocket realtime mode ──────────────────────────────────────────────────
// Connects to WS_URL, subscribes to newHeads, processes each block immediately.
// Measures latency from WS event receipt to DB write (no poll overhead).

fn parseWsUrl(url: []const u8) struct { host: []const u8, port: u16, path: []const u8 } {
    var s = url;
    if (std.mem.startsWith(u8, s, "ws://"))  s = s[5..];
    if (std.mem.startsWith(u8, s, "wss://")) s = s[6..];
    const slash = std.mem.indexOfScalar(u8, s, '/') orelse s.len;
    const path = if (slash < s.len) s[slash..] else "/";
    const host_port = s[0..slash];
    if (std.mem.lastIndexOfScalar(u8, host_port, ':')) |c| {
        return .{
            .host = host_port[0..c],
            .port = std.fmt.parseInt(u16, host_port[c + 1 ..], 10) catch 8545,
            .path = path,
        };
    }
    return .{ .host = host_port, .port = 8545, .path = path };
}

pub fn runRealtimeWs(
    io:       std.Io,
    gpa:      std.mem.Allocator,
    cfg:      *const Config,
    pool:     *db.CqlPool,
    prep_ids: *db.PreparedIds,
    redis:    *db.RedisConn,
) !void {
    const parsed = parseWsUrl(cfg.ws_url);
    std.debug.print(
        "Realtime WS mode: ws://{s}:{d}{s}  chunk=block%{d}  to={d}\n\n",
        .{ parsed.host, parsed.port, parsed.path,
           if (cfg.remap_mod > 0) cfg.remap_mod else cfg.chunk_size,
           cfg.to_block },
    );

    var conn = try ws.WsConn.init(gpa, parsed.host, parsed.port, parsed.path);
    defer conn.deinit();

    const sub_id = try conn.subscribeNewHeads();
    std.debug.print("Subscribed: {s}\n\n", .{sub_id});

    var method_arenas = [3]std.heap.ArenaAllocator{
        std.heap.ArenaAllocator.init(std.heap.page_allocator),
        std.heap.ArenaAllocator.init(std.heap.page_allocator),
        std.heap.ArenaAllocator.init(std.heap.page_allocator),
    };
    defer for (&method_arenas) |*a| a.deinit();
    var block_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer block_arena.deinit();

    var n: u64 = 0;
    var sum_total: f64 = 0;  var min_total: f64 = std.math.floatMax(f64);  var max_total: f64 = 0;
    var sum_fetch: f64 = 0;  var min_fetch: f64 = std.math.floatMax(f64);  var max_fetch: f64 = 0;
    var sum_save:  f64 = 0;  var min_save:  f64 = std.math.floatMax(f64);  var max_save:  f64 = 0;

    // Get cursor from Redis (skip already-processed blocks)
    var cursor: u64 = 0;
    if (try redis.get("LATEST_PROCESSED_BLOCK_NUMBER")) |val| {
        defer gpa.free(val);
        cursor = std.fmt.parseInt(u64, val, 10) catch 0;
    }

    while (true) {
        const block_num = conn.nextBlockNum() catch |err| {
            std.debug.print("WS error: {}\n", .{err});
            break;
        };

        // Skip already-processed blocks (WS may replay on reconnect)
        if (block_num <= cursor) continue;
        if (block_num > cfg.to_block) break;

        const t_ws = nowNs(); // t=0: WS notification received

        const br = try processBlock(io, gpa, cfg, pool, prep_ids, block_num,
                                    &method_arenas, &block_arena);

        if (br == null) {
            // Block not available yet via HTTP (race: WS arrived before HTTP cache)
            // Retry once after a brief wait
            sleepMs(5);
            const br2 = try processBlock(io, gpa, cfg, pool, prep_ids, block_num,
                                         &method_arenas, &block_arena);
            if (br2 == null) {
                std.debug.print("⚠ [{d}] block not available after WS notification\n", .{block_num});
                continue;
            }
            const r = br2.?;
            const ws_to_db_ms = @as(f64, @floatFromInt(nowNs() - t_ws)) / 1e6;
            std.debug.print("  ws→db={d:.0}ms\n", .{ws_to_db_ms});
            sum_total += r.total_ms; sum_fetch += r.fetch_ms; sum_save += r.save_ms; n += 1;
            if (r.total_ms < min_total) min_total = r.total_ms;
            if (r.total_ms > max_total) max_total = r.total_ms;
            if (r.fetch_ms < min_fetch) min_fetch = r.fetch_ms;
            if (r.fetch_ms > max_fetch) max_fetch = r.fetch_ms;
            if (r.save_ms  < min_save)  min_save  = r.save_ms;
            if (r.save_ms  > max_save)  max_save  = r.save_ms;
        } else {
            const r = br.?;
            const ws_to_db_ms = @as(f64, @floatFromInt(nowNs() - t_ws)) / 1e6;
            _ = ws_to_db_ms;
            sum_total += r.total_ms; sum_fetch += r.fetch_ms; sum_save += r.save_ms; n += 1;
            if (r.total_ms < min_total) min_total = r.total_ms;
            if (r.total_ms > max_total) max_total = r.total_ms;
            if (r.fetch_ms < min_fetch) min_fetch = r.fetch_ms;
            if (r.fetch_ms > max_fetch) max_fetch = r.fetch_ms;
            if (r.save_ms  < min_save)  min_save  = r.save_ms;
            if (r.save_ms  > max_save)  max_save  = r.save_ms;
        }

        cursor = block_num;
        const s = try std.fmt.allocPrint(gpa, "{d}", .{block_num});
        defer gpa.free(s);
        try redis.setStr("LATEST_PROCESSED_BLOCK_NUMBER", s);

        if (block_num >= cfg.to_block) break; // done
    }

    if (n > 0) {
        const fn_ = @as(f64, @floatFromInt(n));
        std.debug.print(
            "\n⚡ Realtime WS summary ({d} blocks)\n" ++
            "  fetch  avg={d:.1}ms  min={d:.1}ms  max={d:.1}ms\n" ++
            "  save   avg={d:.1}ms  min={d:.1}ms  max={d:.1}ms\n" ++
            "  TOTAL  avg={d:.1}ms  min={d:.1}ms  max={d:.1}ms\n",
            .{ n,
               sum_fetch/fn_, min_fetch, max_fetch,
               sum_save/fn_,  min_save,  max_save,
               sum_total/fn_, min_total, max_total },
        );
    }
}

