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
const ws = @import("indexer/rpc").ws;
const cursor = @import("indexer/db").cursor;
const pool = @import("indexer/db").pool;
const batch = @import("indexer/db").batch;
const http_pool = @import("indexer/rpc").pool;
const node_probe = @import("indexer/rpc").node_probe;

// ─── CLI args ─────────────────────────────────────────────────────────────────

const CliArgs = struct { from: ?u64, to: ?u64 };

fn parseCli(init: Init) !CliArgs {
    var from: ?u64 = null;
    var to: ?u64 = null;
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
        }
    }
    return .{ .from = from, .to = to };
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
    bClient: FetchClient,
    rClient: FetchClient,
    tClient: FetchClient,
    wsDelayMs: u64,
    retryDelayMs: u64,
    chunkBuckets: u64,
    chunkEra: u64,
    backupNode: ?EvmRpcNodeConfig,

    fn init(
        gpa: std.mem.Allocator,
        io: std.Io,
        env: anytype,
        redisUrl: []const u8,
        chunkBuckets: u64,
        chunkEra: u64,
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
        var hPool = try http_pool.HttpPool.init(gpa, io);
        try hPool.startThreads();
        errdefer hPool.deinit();

        var redis = try openRedis(gpa, redisUrl);
        errdefer redis.deinit();

        const wsDelayMs: u64 = if (environ.get("WS_DELAY_MS")) |v|
            std.fmt.parseInt(u64, v, 10) catch 100
        else
            100;

        const retryDelayMs: u64 = if (environ.get("RT_RETRY_DELAY_MS")) |v|
            std.fmt.parseInt(u64, v, 10) catch 2000
        else
            2000;

        return .{
            .rtConns = rtConns,
            .hPool = hPool,
            .redis = redis,
            .bClient = FetchClient.init(gpa, io),
            .rClient = FetchClient.init(gpa, io),
            .tClient = FetchClient.init(gpa, io),
            .wsDelayMs = wsDelayMs,
            .retryDelayMs = retryDelayMs,
            .chunkBuckets = chunkBuckets,
            .chunkEra = chunkEra,
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
};

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
) !void {
    var cursorPos = startBlock;

    while (true) {
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
                switch (writer.processBlock(io, gpa, chain, &ctx.rtConns, &ctx.redis, &ctx.bClient, &ctx.rClient, &ctx.tClient, blk, &ctx.hPool, ctx.chunkBuckets, ctx.chunkEra, ctx.backupNode)) {
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

    // CqlConn save-worker threads have no Logger of their own — let pool.zig fire
    // its own one-shot GELF alert on CqlError ("Batch too large" etc.) using the
    // same GrayLog endpoint, set once here before any worker threads spawn.
    pool.configureAlerts(env.LOGS_GRAYLOG_HOST, env.LOGS_GRAYLOG_PORT, env.LOGS_GRAYLOG_APP, runtime.details.isDev or runtime.details.isProd);
    // Lets pool.recordSkippedBlock() (called from worker threads, which have no
    // Scylla connection of their own) open its own one-shot connection to record
    // explicitly-given-up blocks into pol.skipped_blocks.
    pool.configureScylla(env.SCYLLA_DB_HOST, env.SCYLLA_DB_PORT, env.SCYLLA_DB_KEYSPACE, env.SCYLLA_DB_USERNAME, env.SCYLLA_DB_PASSWORD);

    var chain = core.getEvmChainConfig(runtime.details.evmChainId) orelse {
        log.err("Unsupported chain");
        return error.UnsupportedChain;
    };

    // RPC_URL/RPC_WSS override the hardcoded primary node (chain config default) —
    // needed to run independent indexer processes against different physical RPC
    // nodes (e.g. dual-node split), since the static chain config only has one.
    if (init.environ_map.get("RPC_URL")) |url| {
        if (url.len > 0) chain.rpcNodes.lotosArchiveNode.https = url;
    }
    if (init.environ_map.get("RPC_WSS")) |wss| {
        if (wss.len > 0) chain.rpcNodes.lotosArchiveNode.wss = wss;
    }

    const cli = try parseCli(init);

    // ── Redis: read cursor ────────────────────────────────────────────────────
    const redisUrl = env.CM_CONNECTION_URL;
    var rdb = try openRedis(gpa, redisUrl);
    defer rdb.deinit();

    const fromBlock: u64 = cli.from orelse
        (readCursorBlock(&rdb, gpa) orelse 0);

    // ── WSS: get current head (only needed when --to is not given) ────────────
    const wssUrl = chain.rpcNodes.lotosArchiveNode.wss;
    const wsParsed = ws.parseUrl(wssUrl);

    var wsConnOpt: ?ws.Conn = if (cli.to == null) blk: {
        var c = ws.Conn.init(gpa, wsParsed.host, wsParsed.port, wsParsed.path) catch |err| {
            std.debug.print("WSS connect failed ({s}): {s}\n", .{ wssUrl, @errorName(err) });
            return err;
        };
        _ = try c.subscribeNewHeads();
        break :blk c;
    } else null;
    defer if (wsConnOpt) |*c| c.deinit();

    const toBlock: u64 = cli.to orelse blk: {
        const head = wsConnOpt.?.nextBlockNum() catch 0;
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
    // https:// fetches go through fetch.zig's curl-subprocess path (works around a
    // ReleaseFast SIGILL in std.http.Client's TLS stack — see TODO.md), so it's
    // safe to point this at an https:// backup. RESERVE_RPC_URL is the canonical
    // env var; BACKUP_RPC_HTTPS is kept as an alias for the same slot.
    const backupNode: ?EvmRpcNodeConfig = if (init.environ_map.get("RESERVE_RPC_URL") orelse init.environ_map.get("BACKUP_RPC_HTTPS")) |url|
        if (url.len > 0) EvmRpcNodeConfig{
            .https = url,
            .wss = "",
        } else null
    else
        null;

    if (backupNode) |bn| {
        std.debug.print("Backup RPC: {s}\n", .{bn.https});
    }

    // ── Neighbor RPC node (optional) ───────────────────────────────────────────
    // Second retry tier for historical sync, between primary and "give up and
    // record as skipped" (see pipeline.zig worker()) — typically the other half
    // of a dual-node split, plain HTTP so it never hits the HTTPS SIGILL issue
    // that backupNode works around.
    const neighborNode: ?EvmRpcNodeConfig = if (init.environ_map.get("NEIGHBOR_RPC_URL")) |url|
        if (url.len > 0) EvmRpcNodeConfig{
            .https = url,
            .wss = "",
        } else null
    else
        null;

    if (neighborNode) |nn| {
        std.debug.print("Neighbor RPC: {s}\n", .{nn.https});
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
        if (neighborNode) |nn| {
            _ = node_probe.detect(gpa, &detectClient, nn.https);
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

    // SCYLLA_CHUNK_ERA: 0 = flat scheme (chunk=block%chunkBuckets, old behavior).
    // >0 = chunk=(block%chunkBuckets)+chunkBuckets*(block/era) — bounds partition size by era.
    const chunkEra: u64 = if (init.environ_map.get("SCYLLA_CHUNK_ERA")) |v|
        std.fmt.parseInt(u64, v, 10) catch {
            std.debug.print("Invalid SCYLLA_CHUNK_ERA: {s}\n", .{v});
            return error.InvalidEnv;
        }
    else
        0;

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

    const fetchWorkers: usize = if (init.environ_map.get("FETCH_WORKERS")) |v|
        std.fmt.parseInt(usize, v, 10) catch chain.indexingOptions.workerCount
    else
        chain.indexingOptions.workerCount;

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
            chunkEra,
            saveEvery,
            backupNode,
            neighborNode,
            &log,
            txsLanes,
            logsLanes,
            itxsLanes,
            fetchWorkers,
        );
    }

    // If --to was explicitly given, exit after historical sync (benchmark mode).
    if (cli.to != null) return;

    // ── Realtime loop ─────────────────────────────────────────────────────────
    log.info("Realtime mode — listening for new blocks via WSS...");

    var ctx = try RealtimeContext.init(gpa, io, env, redisUrl, chunkBuckets, chunkEra, init.environ_map, backupNode, txsLanes, logsLanes, itxsLanes);
    defer ctx.deinit();

    try runRealtimeLoop(io, gpa, &chain, &wsConnOpt.?, wsParsed, &ctx, toBlock, &log);
}
