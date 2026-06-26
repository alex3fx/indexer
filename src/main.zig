// EVM indexer entry point.
// Usage: zig build start -- --from=<block> --to=<block>
//   --from  start block (default: cursor+1 from Redis, or 0)
//   --to    end block   (default: current chain head from WSS)
//
// Flow: read cursor → connect WSS → historical sync [from..head] → realtime loop.
const std = @import("std");
const Init = std.process.Init;

const core = @import("indexer/core");
const utils = core.utils;

const EnvVariable = core.enums.EnvVariable;
const FetchClient = core.fetch.Client;
const EvmRpcNodeConfig = core.structures.EvmRpcNodeConfig;
const Logger = core.logger.Logger;

const pipeline = @import("pipeline/pipeline.zig");
const writer = @import("pipeline/writer.zig");
const erc20 = @import("pipeline/erc20.zig");
const erc20_rescan = @import("pipeline/erc20_rescan.zig");
const ws = @import("indexer/rpc").ws;
const cursor = @import("indexer/db").cursor;
const pool = @import("indexer/db").pool;
const batch = @import("indexer/db").batch;
const http_pool = @import("indexer/rpc").pool;
const node_probe = @import("indexer/rpc").node_probe;

// ─── CLI args ─────────────────────────────────────────────────────────────────

const CliArgs = struct { from: ?u64, to: ?u64, erc20Rescan: bool };

fn parseCli(init: Init) !CliArgs {
    var from: ?u64 = null;
    var to: ?u64 = null;
    var erc20Rescan = false;
    var it = std.process.Args.iterate(init.minimal.args);
    _ = it.next(); // skip executable name
    while (it.next()) |arg| {
        if (std.mem.startsWith(u8, arg, "--from=")) {
            from = std.fmt.parseInt(u64, arg[7..], 10) catch {
                std.debug.print("Invalid --from value: {s}\n", .{arg[7..]});
                return error.InvalidCliArg;
            };
        } else if (std.mem.startsWith(u8, arg, "--to=")) {
            to = std.fmt.parseInt(u64, arg[5..], 10) catch {
                std.debug.print("Invalid --to value: {s}\n", .{arg[5..]});
                return error.InvalidCliArg;
            };
        } else if (std.mem.eql(u8, arg, "--erc20-rescan")) {
            erc20Rescan = true;
        }
    }
    return .{ .from = from, .to = to, .erc20Rescan = erc20Rescan };
}

// ─── Redis helpers ────────────────────────────────────────────────────────────

fn openRedis(gpa: std.mem.Allocator, url: []const u8) !cursor.Conn {
    const rUrl = cursor.parseUrl(url);
    var conn = try cursor.Conn.init(gpa, rUrl.host, rUrl.port);
    if (rUrl.password.len > 0) try conn.auth(rUrl.password);
    if (rUrl.db > 0) try conn.selectDb(rUrl.db);
    return conn;
}

fn readCursorBlock(rdb: *cursor.Conn, gpa: std.mem.Allocator) ?u64 {
    const maybeVal = rdb.get("LATEST_PROCESSED_BLOCK_NUMBER") catch return null;
    const val = maybeVal orelse return null;
    defer gpa.free(val);
    return (std.fmt.parseInt(u64, val, 10) catch 0) + 1;
}

// ─── Realtime loop ────────────────────────────────────────────────────────────

const RealtimeContext = struct {
    rtConns: batch.RealtimeConns,
    hPool: http_pool.HttpPool,
    redis: cursor.Conn,
    redisHost: []const u8,
    redisPort: u16,
    redisPassword: []const u8,
    redisDb: u8,
    bClient: FetchClient,
    rClient: FetchClient,
    tClient: FetchClient,
    wsDelayMs: u64,
    retryDelayMs: u64,
    chunkBuckets: u64,
    backupNode: ?EvmRpcNodeConfig,

    fn init(
        gpa: std.mem.Allocator,
        io: std.Io,
        env: anytype,
        chain: anytype,
        redisUrl: []const u8,
        chunkBuckets: u64,
        environ: anytype,
        backupNode: ?EvmRpcNodeConfig,
        txsLanes: usize,
        logsLanes: usize,
        itxsLanes: usize,
    ) !RealtimeContext {
        const totalConns = 2 + txsLanes + logsLanes + itxsLanes;
        std.debug.print("Connecting realtime CQL ({d} conns)...\n", .{totalConns});
        var rtConns = try batch.RealtimeConns.init(
            gpa,
            env.SCYLLA_DB_HOST,
            env.SCYLLA_DB_PORT,
            env.SCYLLA_DB_KEYSPACE,
            env.SCYLLA_DB_USERNAME,
            env.SCYLLA_DB_PASSWORD,
            txsLanes,
            logsLanes,
            itxsLanes,
        );
        errdefer rtConns.deinit();

        std.debug.print("Starting HTTP thread pool (3 persistent threads)...\n", .{});
        // startThreads() is deliberately NOT called here: the threads it spawns
        // capture *Slot pointers into this HttpPool's `slots` array, and `hPool`
        // is about to be copied into the returned RealtimeContext value — those
        // pointers would dangle into this function's dead stack frame. Call
        // startThreads() once `hPool` is at its final, stable address instead
        // (see the caller, after `var ctx = try RealtimeContext.init(...)`).
        var hPool = try http_pool.HttpPool.init(gpa, io);
        errdefer hPool.deinit();

        var redis = try openRedis(gpa, redisUrl);
        errdefer redis.deinit();
        const rUrl = cursor.parseUrl(redisUrl);

        const wsDelayMs: u64 = if (environ.get("WS_DELAY_MS")) |v|
            std.fmt.parseInt(u64, v, 10) catch 100
        else
            100;

        const retryDelayMs: u64 = if (environ.get("RT_RETRY_DELAY_MS")) |v|
            std.fmt.parseInt(u64, v, 10) catch 2000
        else
            2000;

        _ = chain;

        return .{
            .rtConns = rtConns,
            .hPool = hPool,
            .redis = redis,
            .redisHost = rUrl.host,
            .redisPort = rUrl.port,
            .redisPassword = rUrl.password,
            .redisDb = rUrl.db,
            .bClient = FetchClient.init(gpa, io),
            .rClient = FetchClient.init(gpa, io),
            .tClient = FetchClient.init(gpa, io),
            .wsDelayMs = wsDelayMs,
            .retryDelayMs = retryDelayMs,
            .chunkBuckets = chunkBuckets,
            .backupNode = backupNode,
        };
    }

    fn deinit(self: *RealtimeContext) void {
        self.rtConns.deinit();
        self.hPool.deinit();
        self.redis.deinit();
        self.bClient.deinit();
        self.rClient.deinit();
        self.tClient.deinit();
    }

    /// True only if Scylla and Redis both answer a liveness probe.
    fn isHealthy(self: *RealtimeContext) bool {
        if (!self.rtConns.pingAll()) return false;
        self.redis.ping() catch return false;
        return true;
    }

    /// Reconnects Scylla (all lanes) and Redis. Best-effort: attempts both
    /// even if one fails, returns the first error so the caller can log it.
    fn reconnectAll(self: *RealtimeContext) !void {
        var firstErr: ?anyerror = null;
        self.rtConns.reconnectAll() catch |e| {
            firstErr = firstErr orelse e;
        };
        self.redis.reopen(self.redisHost, self.redisPort, self.redisPassword, self.redisDb) catch |e| {
            firstErr = firstErr orelse e;
        };
        if (firstErr) |e| return e;
    }
};

fn realtimeMs() i64 {
    var ts: std.os.linux.timespec = undefined;
    _ = std.os.linux.clock_gettime(.REALTIME, &ts);
    return ts.sec * 1000 + @divTrunc(ts.nsec, 1_000_000);
}

// How often the realtime loop proactively probes Scylla/Redis liveness,
// independent of whether a save has actually failed yet.
const DB_HEALTH_CHECK_INTERVAL_MS: i64 = 60_000;

fn delayMs(ms: u64) void {
    if (ms == 0) return;
    const ts = std.os.linux.timespec{
        .sec = @intCast(ms / 1000),
        .nsec = @intCast((ms % 1000) * 1_000_000),
    };
    _ = std.os.linux.nanosleep(&ts, null);
}

fn runRealtimeLoop(
    io: std.Io,
    gpa: std.mem.Allocator,
    chain: anytype,
    wsConn: *ws.Conn,
    wsParsed: ws.ParsedUrl,
    ctx: *RealtimeContext,
    startBlock: u64,
    log: *Logger,
    erc20Ctx: *erc20.Erc20Context,
) !void {
    var cursorPos = startBlock;
    var lastHealthCheckMs = realtimeMs();

    while (true) {
        const nowMs = realtimeMs();
        if (nowMs - lastHealthCheckMs >= DB_HEALTH_CHECK_INTERVAL_MS) {
            lastHealthCheckMs = nowMs;
            if (!ctx.isHealthy()) {
                log.err("DB health check failed (Scylla and/or Redis not responding) — reconnecting");
                std.debug.print("[realtime] DB health check failed — reconnecting\n", .{});
                if (ctx.reconnectAll()) |_| {
                    log.warn("DB reconnected after health-check failure");
                } else |re| {
                    const msg = std.fmt.allocPrint(gpa, "DB reconnect after health-check failure FAILED: {s}", .{@errorName(re)}) catch "";
                    defer if (msg.len > 0) gpa.free(msg);
                    log.err(if (msg.len > 0) msg else "DB reconnect failed");
                }
            }
        }

        const blockNum = wsConn.nextBlockNum() catch |err| {
            const msg = std.fmt.allocPrint(gpa, "WSS disconnected: {s} — reconnecting", .{@errorName(err)}) catch "";
            defer if (msg.len > 0) gpa.free(msg);
            log.warn(if (msg.len > 0) msg else "WSS disconnected — reconnecting");
            wsConn.deinit();
            wsConn.* = ws.Conn.init(gpa, wsParsed.host, wsParsed.port, wsParsed.path) catch break;
            _ = wsConn.subscribeNewHeads() catch break;
            continue;
        };

        if (blockNum <= cursorPos) continue;

        delayMs(ctx.wsDelayMs);

        // Process this block and any skipped blocks (gap fill).
        var blk = cursorPos + 1;
        while (blk <= blockNum) : (blk += 1) {
            // Infinite retry: keep trying until the block is saved.
            // processBlock tries primary, then backup on any failure.
            // Only returns .retry_later when both are unavailable.
            while (true) {
                switch (writer.processBlock(io, gpa, chain, &ctx.rtConns, &ctx.redis, &ctx.bClient, &ctx.rClient, &ctx.tClient, blk, &ctx.hPool, ctx.chunkBuckets, ctx.backupNode, erc20Ctx)) {
                    .saved => |m| {
                        const kb = @as(f64, @floatFromInt(m.kb_total));
                        const fetch_us_kb = if (kb > 0) m.fetch_ms * 1000.0 / kb else 0;
                        const parse_us_kb = if (kb > 0) m.parse_ms * 1000.0 / kb else 0;
                        const xform_us_kb = if (kb > 0) m.transform_ms * 1000.0 / kb else 0;
                        const save_us_kb = if (kb > 0) m.save_ms * 1000.0 / kb else 0;
                        const msg = std.fmt.allocPrint(gpa,
                            "blk={d} D={d}ms | fetch={d:.0}ms|{d:.2}µs/KB parse={d:.0}ms|{d:.2}µs/KB xform={d:.0}ms|{d:.2}µs/KB save={d:.0}ms|{d:.2}µs/KB | TTP={d:.0}ms | kb={d}",
                            .{ blk, m.distance_ms, m.fetch_ms, fetch_us_kb, m.parse_ms, parse_us_kb, m.transform_ms, xform_us_kb, m.save_ms, save_us_kb, m.total_ms, m.kb_total },
                        ) catch "";
                        defer if (msg.len > 0) gpa.free(msg);
                        log.info(if (msg.len > 0) msg else "block saved");
                        break;
                    },
                    .retry_later => {
                        const msg = std.fmt.allocPrint(gpa, "block {d} unavailable — retry in {d}ms", .{ blk, ctx.retryDelayMs }) catch "";
                        defer if (msg.len > 0) gpa.free(msg);
                        log.warn(if (msg.len > 0) msg else "block unavailable — retrying");
                        std.debug.print("[realtime] block={d} unavailable on all nodes — retry in {d}ms\n", .{ blk, ctx.retryDelayMs });
                        delayMs(ctx.retryDelayMs);
                    },
                    .fatal => |e| {
                        const msg = std.fmt.allocPrint(gpa, "block {d} fatal: {s} — retry in {d}ms", .{ blk, @errorName(e), ctx.retryDelayMs }) catch "";
                        defer if (msg.len > 0) gpa.free(msg);
                        log.err(if (msg.len > 0) msg else "block fatal error — retrying");
                        std.debug.print("[realtime] block={d} fatal: {s} — retry in {d}ms\n", .{ blk, @errorName(e), ctx.retryDelayMs });

                        // A fatal error here is most often a dropped Scylla/Redis
                        // connection (e.g. a DB restart) — those don't recover on
                        // their own, so reconnect proactively before retrying.
                        // Harmless if the connection was actually fine.
                        if (ctx.reconnectAll()) |_| {
                            log.warn("DB reconnected after fatal block error");
                        } else |re| {
                            const rmsg = std.fmt.allocPrint(gpa, "DB reconnect after fatal block error FAILED: {s}", .{@errorName(re)}) catch "";
                            defer if (rmsg.len > 0) gpa.free(rmsg);
                            log.err(if (rmsg.len > 0) rmsg else "DB reconnect failed");
                        }

                        delayMs(ctx.retryDelayMs);
                    },
                }
            }
        }
        cursorPos = blockNum;
    }
}

// ─── main ─────────────────────────────────────────────────────────────────────

pub fn main(init: Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    const runtime = try core.getRuntimeContext(init);
    const env = runtime.env;

    var log = Logger.init(gpa, runtime.details,
        env.LOGS_GRAYLOG_HOST, env.LOGS_GRAYLOG_PORT, env.LOGS_GRAYLOG_APP, env.TIME_ZONE);
    defer log.deinit();
    log.info("EVM indexer starting");

    const chain = core.getEvmChainConfig(runtime.details.evmChainId) orelse {
        log.err("Unsupported chain");
        return error.UnsupportedChain;
    };

    const cli = try parseCli(init);

    // ── Redis: read cursor ────────────────────────────────────────────────────
    const redisUrl = env.CM_CONNECTION_URL;
    var rdb = try openRedis(gpa, redisUrl);
    defer rdb.deinit();

    // ── Maintenance mode: --erc20-rescan (re-resolve all known ERC-20s, exits) ─
    if (cli.erc20Rescan) {
        var conn = try pool.CqlConn.init(gpa, env.SCYLLA_DB_HOST, env.SCYLLA_DB_PORT, env.SCYLLA_DB_KEYSPACE, env.SCYLLA_DB_USERNAME, env.SCYLLA_DB_PASSWORD);
        defer conn.deinit();
        try erc20_rescan.run(gpa, io, &chain, &rdb, &conn);
        return;
    }

    const fromBlock: u64 = cli.from orelse
        (readCursorBlock(&rdb, gpa) orelse 0);

    // ── WSS: get current head ─────────────────────────────────────────────────
    // Only opened when `--to` is absent (realtime/head-discovery path needs it).
    // In pure historical mode (--to given, e.g. benchmarks) the connection was
    // never read after subscribeNewHeads() — its receive buffer fills up with
    // unread newHeads pushes for the whole run and can eventually stall the
    // process (observed live: throughput collapsed to ~0 after the socket's
    // backlog grew unbounded over a long historical run).
    const wssUrl = chain.rpcNodes.lotosArchiveNode.wss;
    const wsParsed = ws.parseUrl(wssUrl);

    var wsConn: ws.Conn = undefined;
    var wsConnInit = false;
    defer if (wsConnInit) wsConn.deinit();

    const toBlock: u64 = cli.to orelse blk: {
        wsConn = ws.Conn.init(gpa, wsParsed.host, wsParsed.port, wsParsed.path) catch |err| {
            std.debug.print("WSS connect failed ({s}): {s}\n", .{ wssUrl, @errorName(err) });
            return err;
        };
        wsConnInit = true;
        _ = try wsConn.subscribeNewHeads();
        const head = wsConn.nextBlockNum() catch 0;
        break :blk if (head > 0) head else {
            std.debug.print("Could not get current head from WSS\n", .{});
            return error.NoHeadBlock;
        };
    };

    std.debug.print(
        "EVM Indexer | chain={d} mode={s}\n" ++
            "RPC: {s}\n" ++
            "WSS: {s}\n" ++
            "Scylla: {s}:{d}\n" ++
            "Sync: {d} → {d}  workers={d}\n\n",
        .{
            chain.id,                              @tagName(env.MODE),
            chain.rpcNodes.lotosArchiveNode.https, wssUrl,
            env.SCYLLA_DB_HOST,                    env.SCYLLA_DB_PORT,
            fromBlock,                             toBlock,
            chain.indexingOptions.workerCount,
        },
    );

    // ── Backup RPC node (optional) ────────────────────────────────────────────
    const backupNode: ?EvmRpcNodeConfig = if (init.environ_map.get("BACKUP_RPC_HTTPS")) |url|
        if (url.len > 0) EvmRpcNodeConfig{
            .https = url,
            .wss = "",
        } else null
    else
        null;

    if (backupNode) |bn| {
        std.debug.print("Backup RPC: {s}\n", .{bn.https});
    }

    // ── Pre-detect node type from main thread ─────────────────────────────────
    // Must run before any worker threads or pool workers start, so all subsequent
    // detect() calls are cache hits (no HTTP from thread-pool context).
    {
        var detectClient = FetchClient.init(gpa, io);
        defer detectClient.deinit();
        _ = node_probe.detect(gpa, &detectClient, chain.rpcNodes.lotosArchiveNode.https);
        if (backupNode) |bn| {
            _ = node_probe.detect(gpa, &detectClient, bn.https);
        }
    }

    // ── Env overrides ─────────────────────────────────────────────────────────
    const chunkBuckets: u64 = if (init.environ_map.get("SCYLLA_CHUNK_BUCKETS")) |v|
        std.fmt.parseInt(u64, v, 10) catch {
            std.debug.print("Invalid SCYLLA_CHUNK_BUCKETS: {s}\n", .{v});
            return error.InvalidEnv;
        }
    else
        pipeline.SCYLLA_CHUNK_BUCKETS_DEFAULT;

    const saveEvery: usize = if (init.environ_map.get("SAVE_EVERY")) |v|
        std.fmt.parseInt(usize, v, 10) catch {
            std.debug.print("Invalid SAVE_EVERY: {s}\n", .{v});
            return error.InvalidEnv;
        }
    else
        pipeline.SAVE_EVERY_DEFAULT;

    const txsLanes: usize = if (init.environ_map.get("ACCUM_TXS_LANES")) |v|
        std.fmt.parseInt(usize, v, 10) catch pipeline.TXS_LANES_DEFAULT
    else
        pipeline.TXS_LANES_DEFAULT;

    const logsLanes: usize = if (init.environ_map.get("ACCUM_LOG_LANES")) |v|
        std.fmt.parseInt(usize, v, 10) catch pipeline.LOG_LANES_DEFAULT
    else
        pipeline.LOG_LANES_DEFAULT;

    const itxsLanes: usize = if (init.environ_map.get("ACCUM_ITX_LANES")) |v|
        std.fmt.parseInt(usize, v, 10) catch pipeline.ITX_LANES_DEFAULT
    else
        pipeline.ITX_LANES_DEFAULT;

    // ── ERC-20 tracking context (bloom filter + Multicall3 client) ───────────
    // Shared across historical and realtime phases so the bloom filter built
    // up during historical sync keeps catching re-checks once realtime starts.
    const erc20ChunkSize: usize = if (init.environ_map.get("ERC20_MULTICALL_CHUNK_SIZE")) |v|
        std.fmt.parseInt(usize, v, 10) catch erc20.MULTICALL_CHUNK_SIZE_DEFAULT
    else
        erc20.MULTICALL_CHUNK_SIZE_DEFAULT;
    var erc20Ctx = try erc20.Erc20Context.init(gpa, io, &chain, erc20ChunkSize);
    defer erc20Ctx.deinit();

    // ── Historical sync ───────────────────────────────────────────────────────
    if (fromBlock <= toBlock) {
        try pipeline.runHistorical(
            io,
            gpa,
            &chain,
            fromBlock,
            toBlock,
            env.SCYLLA_DB_HOST,
            env.SCYLLA_DB_PORT,
            env.SCYLLA_DB_KEYSPACE,
            env.SCYLLA_DB_USERNAME,
            env.SCYLLA_DB_PASSWORD,
            redisUrl,
            chunkBuckets,
            saveEvery,
            backupNode,
            &log,
            txsLanes,
            logsLanes,
            itxsLanes,
            &erc20Ctx,
        );
    }

    // If --to was explicitly given, exit after historical sync (benchmark mode).
    if (cli.to != null) return;

    // ── Realtime loop ─────────────────────────────────────────────────────────
    log.info("Realtime mode — listening for new blocks via WSS...");

    var ctx = try RealtimeContext.init(gpa, io, env, &chain, redisUrl, chunkBuckets, init.environ_map, backupNode, txsLanes, logsLanes, itxsLanes);
    defer ctx.deinit();
    // Must run after `ctx` is at its final address — see the comment in
    // RealtimeContext.init() next to where HttpPool.init() is called.
    try ctx.hPool.startThreads();

    try runRealtimeLoop(io, gpa, &chain, &wsConn, wsParsed, &ctx, toBlock, &log, &erc20Ctx);
}
