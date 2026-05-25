// Realtime mode: processes blocks one at a time as they arrive.
// Three sub-modes:
//   Polling (REALTIME=1): fetchBlock every POLL_MS until block available.
//   WebSocket (REALTIME=1, WS_URL set): subscribe to newHeads, fetch on notification.
//   Catchup+Realtime (WS_URL set, default): sync history then transition to WS realtime.
const std       = @import("std");
const rpc       = @import("rpc");
const transform = @import("transform");
const db        = @import("db");
const config    = @import("config");
const met       = @import("metrics");
const pipe      = @import("pipeline");
const ws        = @import("ws");
const Config  = config.Config;
const Metrics = met.Metrics;

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

const RtStats = struct {
    n:         u64 = 0,
    sum_total: f64 = 0,  min_total: f64 = std.math.floatMax(f64),  max_total: f64 = 0,
    sum_fetch: f64 = 0,  min_fetch: f64 = std.math.floatMax(f64),  max_fetch: f64 = 0,
    sum_save:  f64 = 0,  min_save:  f64 = std.math.floatMax(f64),  max_save:  f64 = 0,

    fn update(self: *RtStats, r: BlockResult) void {
        self.n += 1;
        self.sum_total += r.total_ms;  self.sum_fetch += r.fetch_ms;  self.sum_save += r.save_ms;
        if (r.total_ms < self.min_total) self.min_total = r.total_ms;
        if (r.total_ms > self.max_total) self.max_total = r.total_ms;
        if (r.fetch_ms < self.min_fetch) self.min_fetch = r.fetch_ms;
        if (r.fetch_ms > self.max_fetch) self.max_fetch = r.fetch_ms;
        if (r.save_ms  < self.min_save)  self.min_save  = r.save_ms;
        if (r.save_ms  > self.max_save)  self.max_save  = r.save_ms;
    }

    fn print(self: *const RtStats, label: []const u8) void {
        if (self.n == 0) return;
        const fn_ = @as(f64, @floatFromInt(self.n));
        std.debug.print(
            "\n\u{26a1} {s} ({d} blocks)\n" ++
            "  fetch  avg={d:.1}ms  min={d:.1}ms  max={d:.1}ms\n" ++
            "  save   avg={d:.1}ms  min={d:.1}ms  max={d:.1}ms\n" ++
            "  TOTAL  avg={d:.1}ms  min={d:.1}ms  max={d:.1}ms\n",
            .{ label, self.n,
               self.sum_fetch/fn_, self.min_fetch, self.max_fetch,
               self.sum_save/fn_,  self.min_save,  self.max_save,
               self.sum_total/fn_, self.min_total, self.max_total },
        );
    }
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

    if (bd.block == null) return null;

    const t1 = nowNs();
    var ent = transform.initEntities();
    try transform.transformBlock(arena, bd, cfg.chunk_size, cfg.remap_mod, &ent);

    const t2 = nowNs();
    const transform_ms = @as(f64, @floatFromInt(t2 - t1)) / 1e6;

    var save_ms: f64 = 0;
    try db.saveBatch(.{
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

/// Process a WS-notified block with one retry; update stats and Redis cursor on success.
fn processWsBlock(
    io:           std.Io,
    gpa:          std.mem.Allocator,
    cfg:          *const Config,
    pool:         *db.CqlPool,
    prep_ids:     *db.PreparedIds,
    block_num:    u64,
    method_arenas: *[3]std.heap.ArenaAllocator,
    block_arena:  *std.heap.ArenaAllocator,
    stats:        *RtStats,
    cursor:       *u64,
    redis:        *db.RedisConn,
) !void {
    const br = try processBlock(io, gpa, cfg, pool, prep_ids, block_num, method_arenas, block_arena);
    const r = br orelse blk: {
        sleepMs(5);
        const br2 = try processBlock(io, gpa, cfg, pool, prep_ids, block_num, method_arenas, block_arena);
        if (br2 == null) {
            std.debug.print("\u{26a0} [{d}] block not available after retry — skipping\n", .{block_num});
            return;
        }
        break :blk br2.?;
    };
    stats.update(r);
    cursor.* = block_num;
    const s = try std.fmt.allocPrint(gpa, "{d}", .{block_num});
    defer gpa.free(s);
    try redis.setStr("LATEST_PROCESSED_BLOCK_NUMBER", s);
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

    var stats = RtStats{};

    while (block_num <= cfg.to_block) {
        const br = try processBlock(io, gpa, cfg, pool, prep_ids, block_num,
                                    &method_arenas, &block_arena);
        if (br == null) {
            sleepMs(cfg.poll_ms);
            continue;
        }
        stats.update(br.?);

        const s = try std.fmt.allocPrint(gpa, "{d}", .{block_num});
        defer gpa.free(s);
        try redis.setStr("LATEST_PROCESSED_BLOCK_NUMBER", s);
        block_num += 1;
    }

    stats.print("Realtime summary");
}

// ─── WebSocket support ────────────────────────────────────────────────────────

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

// OS pipe for WS listener → processor communication.
// Listener writes 8-byte block numbers; processor reads (blocks until data).
// Closing the write end returns 0 bytes on read → signals done.
const BlockChannel = struct {
    rd: i32,
    wr: i32,

    fn init() !BlockChannel {
        const lnx = std.os.linux;
        var fds: [2]i32 = undefined;
        const rc = lnx.pipe2(&fds, lnx.O{});
        if (rc != 0) return error.PipeFailed;
        return .{ .rd = fds[0], .wr = fds[1] };
    }

    fn send(self: *const BlockChannel, n: u64) void {
        var val = n;
        _ = std.os.linux.write(self.wr, @ptrCast(&val), 8);
    }

    fn recv(self: *const BlockChannel) ?u64 {
        var val: u64 = 0;
        const n = std.os.linux.read(self.rd, @ptrCast(&val), 8);
        if (n != 8) return null;
        return val;
    }

    fn closeWrite(self: *const BlockChannel) void { _ = std.os.linux.close(self.wr); }
    fn closeRead(self: *const BlockChannel)  void { _ = std.os.linux.close(self.rd); }
};

const WsListenerArg = struct {
    conn:     *ws.WsConn,
    ch:       *const BlockChannel,
    to_block: u64,
};

fn wsListenerThread(arg: WsListenerArg) void {
    while (true) {
        const block_num = arg.conn.nextBlockNum() catch break;
        arg.ch.send(block_num);
        if (block_num >= arg.to_block) break;
    }
    arg.ch.closeWrite();
}

/// Pure WS realtime mode (REALTIME=1 + WS_URL): no historical sync.
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
    std.debug.print("Subscribed: {s}  (WS listener in separate thread)\n\n", .{sub_id});

    const ch = try BlockChannel.init();
    defer ch.closeRead();
    const listener_arg = WsListenerArg{ .conn = &conn, .ch = &ch, .to_block = cfg.to_block };
    const ws_thread = try std.Thread.spawn(.{}, wsListenerThread, .{listener_arg});
    defer ws_thread.join();

    var method_arenas = [3]std.heap.ArenaAllocator{
        std.heap.ArenaAllocator.init(std.heap.page_allocator),
        std.heap.ArenaAllocator.init(std.heap.page_allocator),
        std.heap.ArenaAllocator.init(std.heap.page_allocator),
    };
    defer for (&method_arenas) |*a| a.deinit();
    var block_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer block_arena.deinit();

    var cursor: u64 = 0;
    if (try redis.get("LATEST_PROCESSED_BLOCK_NUMBER")) |val| {
        defer gpa.free(val);
        cursor = std.fmt.parseInt(u64, val, 10) catch 0;
    }

    var stats = RtStats{};

    while (ch.recv()) |block_num| {
        if (block_num <= cursor) continue;
        if (block_num > cfg.to_block) break;
        try processWsBlock(io, gpa, cfg, pool, prep_ids, block_num,
                           &method_arenas, &block_arena, &stats, &cursor, redis);
        if (cursor >= cfg.to_block) break;
    }

    stats.print("Realtime WS summary");
}

/// Catchup+Realtime mode (WS_URL set, default when WS_URL is configured).
/// Subscribes to WS first to buffer notifications, runs historical batch
/// up to ws_first-1, then processes buffered + future WS blocks.
pub fn runCatchupAndRealtime(
    io:       std.Io,
    gpa:      std.mem.Allocator,
    cfg:      *const Config,
    pools:    []db.CqlPool,
    prep_ids: []db.PreparedIds,
    redis:    *db.RedisConn,
    from:     u64,
    metrics:  *Metrics,
) !void {
    const parsed = parseWsUrl(cfg.ws_url);
    std.debug.print(
        "Catchup+Realtime: from={d}  ws://{s}:{d}{s}  to={d}\n\n",
        .{ from, parsed.host, parsed.port, parsed.path, cfg.to_block },
    );

    var conn = try ws.WsConn.init(gpa, parsed.host, parsed.port, parsed.path);
    defer conn.deinit();
    const sub_id = try conn.subscribeNewHeads();
    std.debug.print("Subscribed: {s}\n\n", .{sub_id});

    // WS listener buffers block notifications while history syncs.
    const ch = try BlockChannel.init();
    defer ch.closeRead();
    const listener_arg = WsListenerArg{ .conn = &conn, .ch = &ch, .to_block = cfg.to_block };
    const ws_thread = try std.Thread.spawn(.{}, wsListenerThread, .{listener_arg});
    defer ws_thread.join();

    // First WS block tells us where history ends.
    const ws_first = ch.recv() orelse return;
    std.debug.print("WS first block: {d}\n", .{ws_first});

    // Sync history from `from` to ws_first-1. WS listener buffers new blocks during this.
    if (from < ws_first) {
        std.debug.print("Syncing history {d}\u{2192}{d}...\n\n", .{ from, ws_first - 1 });
        try pipe.runHistorical(io, gpa, cfg, pools, prep_ids, null, redis, from, ws_first - 1, metrics);
        metrics.print();
    }

    // Realtime phase: process ws_first (already consumed from channel) + buffered + future blocks.
    std.debug.print("\nRealtime phase from block {d}\n\n", .{ws_first});

    var method_arenas = [3]std.heap.ArenaAllocator{
        std.heap.ArenaAllocator.init(std.heap.page_allocator),
        std.heap.ArenaAllocator.init(std.heap.page_allocator),
        std.heap.ArenaAllocator.init(std.heap.page_allocator),
    };
    defer for (&method_arenas) |*a| a.deinit();
    var block_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer block_arena.deinit();

    // cursor = last processed block before realtime phase
    var cursor: u64 = if (from < ws_first) ws_first - 1 else if (from > 0) from - 1 else 0;
    var stats = RtStats{};

    if (ws_first > cursor and ws_first <= cfg.to_block) {
        try processWsBlock(io, gpa, cfg, &pools[0], &prep_ids[0], ws_first,
                           &method_arenas, &block_arena, &stats, &cursor, redis);
    }

    while (ch.recv()) |block_num| {
        if (block_num <= cursor) continue;
        if (block_num > cfg.to_block) break;
        try processWsBlock(io, gpa, cfg, &pools[0], &prep_ids[0], block_num,
                           &method_arenas, &block_arena, &stats, &cursor, redis);
        if (cursor >= cfg.to_block) break;
    }

    stats.print("Realtime WS summary");
}
