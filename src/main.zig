// EVM indexer entry point.
// Usage: zig build start -- --from=<block> --to=<block>
//   --from  start block (default: cursor+1 from Redis, or 0)
//   --to    end block   (default: current chain head from WSS)
//
// Flow: read cursor → connect WSS → historical sync [from..head] → realtime loop.
const std = @import("std");
const Init = std.process.Init;

const core  = @import("indexer/core");
const utils = @import("indexer/utils");

const EnvVariable      = core.enums.EnvVariable;
const FetchClient      = core.fetch.Client;
const EvmRpcNodeConfig = core.structures.EvmRpcNodeConfig;

const pipeline  = @import("pipeline/pipeline.zig");
const writer    = @import("pipeline/writer.zig");
const ws        = @import("rpc/ws.zig");
const cursor    = @import("db/cursor.zig");
const pool      = @import("db/pool.zig");
const batch     = @import("db/batch.zig");
const http_pool = @import("rpc/pool.zig");

// ─── CLI args ─────────────────────────────────────────────────────────────────

const CliArgs = struct { from: ?u64, to: ?u64 };

fn parseCli(init: Init) !CliArgs {
    var from: ?u64 = null;
    var to:   ?u64 = null;
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
    var conn   = try cursor.Conn.init(gpa, rUrl.host, rUrl.port);
    if (rUrl.password.len > 0) try conn.auth(rUrl.password);
    if (rUrl.db > 0)           try conn.selectDb(rUrl.db);
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
    rtConns:        batch.RealtimeConns,
    hPool:          http_pool.HttpPool,
    redis:          cursor.Conn,
    bClient:        FetchClient,
    rClient:        FetchClient,
    tClient:        FetchClient,
    wsDelayMs:      u64,
    retryDelayMs:   u64,
    chunkBuckets:   u64,
    backupNode:     ?EvmRpcNodeConfig,

    fn init(
        gpa:          std.mem.Allocator,
        io:           std.Io,
        env:          anytype,
        chain:        anytype,
        redisUrl:     []const u8,
        chunkBuckets: u64,
        environ:      anytype,
        backupNode:   ?EvmRpcNodeConfig,
    ) !RealtimeContext {
        std.debug.print("Connecting realtime CQL (32 conns)...\n", .{});
        var rtConns = try batch.RealtimeConns.init(gpa,
            env.SCYLLA_DB_HOST, env.SCYLLA_DB_PORT,
            env.SCYLLA_DB_KEYSPACE,
            env.SCYLLA_DB_USERNAME, env.SCYLLA_DB_PASSWORD,
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

        _ = chain;

        return .{
            .rtConns        = rtConns,
            .hPool          = hPool,
            .redis          = redis,
            .bClient        = FetchClient.init(gpa, io),
            .rClient        = FetchClient.init(gpa, io),
            .tClient        = FetchClient.init(gpa, io),
            .wsDelayMs      = wsDelayMs,
            .retryDelayMs   = retryDelayMs,
            .chunkBuckets   = chunkBuckets,
            .backupNode     = backupNode,
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
        .sec  = @intCast(ms / 1000),
        .nsec = @intCast((ms % 1000) * 1_000_000),
    };
    _ = std.os.linux.nanosleep(&ts, null);
}

fn runRealtimeLoop(
    io:      std.Io,
    gpa:     std.mem.Allocator,
    chain:   anytype,
    wsConn:  *ws.Conn,
    wsParsed: ws.ParsedUrl,
    ctx:     *RealtimeContext,
    startBlock: u64,
) !void {
    var cursorPos = startBlock;

    while (true) {
        const blockNum = wsConn.nextBlockNum() catch |err| {
            std.debug.print("WSS error: {s} — reconnecting...\n", .{@errorName(err)});
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
                switch (writer.processBlock(io, gpa, chain, &ctx.rtConns, &ctx.redis,
                    &ctx.bClient, &ctx.rClient, &ctx.tClient, blk, &ctx.hPool, ctx.chunkBuckets,
                    ctx.backupNode))
                {
                    .saved => break,
                    .retry_later => {
                        std.debug.print(
                            "[realtime] block={d} unavailable on all nodes — retry in {d}ms\n",
                            .{ blk, ctx.retryDelayMs });
                        delayMs(ctx.retryDelayMs);
                    },
                    .fatal => |e| {
                        std.debug.print(
                            "[realtime] block={d} fatal: {s} — retry in {d}ms\n",
                            .{ blk, @errorName(e), ctx.retryDelayMs });
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
    const io  = init.io;

    const runtime = try core.getRuntimeContext(init);
    const env     = runtime.env;
    const chain   = core.getEvmChainConfig(runtime.details.evmChainId) orelse {
        std.debug.print("Unsupported chain id: {d}\n", .{runtime.details.evmChainId});
        return error.UnsupportedChain;
    };

    const cli = try parseCli(init);

    // ── Redis: read cursor ────────────────────────────────────────────────────
    const redisUrl = env.CM_CONNECTION_URL;
    var rdb        = try openRedis(gpa, redisUrl);
    defer rdb.deinit();

    const fromBlock: u64 = cli.from orelse
        (readCursorBlock(&rdb, gpa) orelse 0);

    // ── WSS: get current head ─────────────────────────────────────────────────
    const wssUrl   = chain.rpcNodes.lotosArchiveNode.wss;
    const wsParsed = ws.parseUrl(wssUrl);

    var wsConn = ws.Conn.init(gpa, wsParsed.host, wsParsed.port, wsParsed.path) catch |err| {
        std.debug.print("WSS connect failed ({s}): {s}\n", .{ wssUrl, @errorName(err) });
        return err;
    };
    defer wsConn.deinit();
    _ = try wsConn.subscribeNewHeads();

    const toBlock: u64 = cli.to orelse blk: {
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
            chain.id, @tagName(env.MODE),
            chain.rpcNodes.lotosArchiveNode.https,
            wssUrl,
            env.SCYLLA_DB_HOST, env.SCYLLA_DB_PORT,
            fromBlock, toBlock, chain.indexingOptions.workerCount,
        },
    );

    // ── Backup RPC node (optional) ────────────────────────────────────────────
    const backupNode: ?EvmRpcNodeConfig = if (init.environ_map.get("BACKUP_RPC_HTTPS")) |url|
        if (url.len > 0) EvmRpcNodeConfig{
            .https = url,
            .wss   = "",
        } else null
    else
        null;

    if (backupNode) |bn| {
        std.debug.print("Backup RPC: {s}\n", .{bn.https});
    }

    // ── Env overrides ─────────────────────────────────────────────────────────
    const chunkBuckets: u64 = if (init.environ_map.get("SCYLLA_CHUNK_BUCKETS")) |v|
        std.fmt.parseInt(u64, v, 10) catch {
            std.debug.print("Invalid SCYLLA_CHUNK_BUCKETS: {s}\n", .{v});
            return error.InvalidEnv;
        }
    else pipeline.SCYLLA_CHUNK_BUCKETS_DEFAULT;

    const saveEvery: usize = if (init.environ_map.get("SAVE_EVERY")) |v|
        std.fmt.parseInt(usize, v, 10) catch {
            std.debug.print("Invalid SAVE_EVERY: {s}\n", .{v});
            return error.InvalidEnv;
        }
    else pipeline.SAVE_EVERY_DEFAULT;

    // ── Historical sync ───────────────────────────────────────────────────────
    if (fromBlock <= toBlock) {
        try pipeline.runHistorical(
            io, gpa, &chain,
            fromBlock, toBlock,
            env.SCYLLA_DB_HOST, env.SCYLLA_DB_PORT,
            env.SCYLLA_DB_KEYSPACE,
            env.SCYLLA_DB_USERNAME, env.SCYLLA_DB_PASSWORD,
            redisUrl,
            chunkBuckets,
            saveEvery,
            backupNode,
        );
    }

    // If --to was explicitly given, exit after historical sync (benchmark mode).
    if (cli.to != null) return;

    // ── Realtime loop ─────────────────────────────────────────────────────────
    std.debug.print("\nRealtime mode — listening for new blocks via WSS...\n\n", .{});

    var ctx = try RealtimeContext.init(gpa, io, env, &chain, redisUrl, chunkBuckets, init.environ_map, backupNode);
    defer ctx.deinit();

    try runRealtimeLoop(io, gpa, &chain, &wsConn, wsParsed, &ctx, toBlock);
}
