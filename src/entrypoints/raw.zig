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

const EnvVariable = core.enums.EnvVariable;
const pipeline    = core.pipeline;
const redis       = core.redis;
const ws          = core.ws;
const scylla      = core.scylla;

pub fn main(init: Init) !void {
    const gpa = init.gpa;
    const io  = init.io;

    // ── Runtime context (env vars) ────────────────────────────────────────────
    const runtime = try core.getRuntimeContext(init);
    const env     = runtime.env;
    const chain   = core.getEvmChainConfig(runtime.details.evmChainId) orelse {
        std.debug.print("Unsupported chain id: {d}\n", .{runtime.details.evmChainId});
        return error.UnsupportedChain;
    };

    // ── CLI args: --from=N --to=N ──────────────────────────────────────────────
    var cliFrom: ?u64 = null;
    var cliTo:   ?u64 = null;
    {
        var it = std.process.Args.iterate(init.minimal.args);
        _ = it.next(); // skip executable name
        while (it.next()) |arg| {
            if (std.mem.startsWith(u8, arg, "--from=")) {
                cliFrom = std.fmt.parseInt(u64, arg[7..], 10) catch {
                    std.debug.print("Invalid --from value: {s}\n", .{arg[7..]});
                    return error.InvalidCliArg;
                };
            } else if (std.mem.startsWith(u8, arg, "--to=")) {
                cliTo = std.fmt.parseInt(u64, arg[5..], 10) catch {
                    std.debug.print("Invalid --to value: {s}\n", .{arg[5..]});
                    return error.InvalidCliArg;
                };
            }
        }
    }

    // ── Redis: read cursor ────────────────────────────────────────────────────
    const redisUrl = env.CM_CONNECTION_URL;
    const rUrl = redis.parseUrl(redisUrl);
    var rdb = try redis.Conn.init(gpa, rUrl.host, rUrl.port);
    defer rdb.deinit();
    if (rUrl.password.len > 0) try rdb.auth(rUrl.password);
    if (rUrl.db > 0) try rdb.selectDb(rUrl.db);

    var fromBlock: u64 = cliFrom orelse 0;
    if (cliFrom == null) {
        if (try rdb.get("LATEST_PROCESSED_BLOCK_NUMBER")) |val| {
            defer gpa.free(val);
            fromBlock = (std.fmt.parseInt(u64, val, 10) catch 0) + 1;
        }
    }

    // ── WSS: get current head ─────────────────────────────────────────────────
    const wssUrl = chain.rpcNodes.lotosArchiveNode.wss;
    const wsParsed = ws.parseUrl(wssUrl);

    var wsConn = ws.Conn.init(gpa, wsParsed.host, wsParsed.port, wsParsed.path) catch |err| {
        std.debug.print("WSS connect failed ({s}): {s}\n", .{ wssUrl, @errorName(err) });
        return err;
    };
    defer wsConn.deinit();

    _ = try wsConn.subscribeNewHeads();

    // toBlock = CLI arg or current head from WSS.
    const toBlock: u64 = cliTo orelse blk: {
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

    // ── Historical sync ───────────────────────────────────────────────────────
    if (fromBlock <= toBlock) {
        try pipeline.runHistorical(
            io, gpa, &chain,
            fromBlock, toBlock,
            env.SCYLLA_DB_HOST, env.SCYLLA_DB_PORT,
            env.SCYLLA_DB_KEYSPACE,
            env.SCYLLA_DB_USERNAME, env.SCYLLA_DB_PASSWORD,
            redisUrl,
        );
    }

    // If --to was explicitly given, exit after historical sync (benchmark mode).
    if (cliTo != null) return;

    // ── Realtime loop ─────────────────────────────────────────────────────────
    std.debug.print("\nRealtime mode — listening for new blocks via WSS...\n\n", .{});

    // Per-realtime connections.
    var cql = try scylla.CqlConn.init(gpa,
        env.SCYLLA_DB_HOST, env.SCYLLA_DB_PORT,
        env.SCYLLA_DB_KEYSPACE,
        env.SCYLLA_DB_USERNAME, env.SCYLLA_DB_PASSWORD,
    );
    defer cql.deinit();

    var realtimeRedis = try redis.Conn.init(gpa, rUrl.host, rUrl.port);
    defer realtimeRedis.deinit();
    if (rUrl.password.len > 0) try realtimeRedis.auth(rUrl.password);
    if (rUrl.db > 0) try realtimeRedis.selectDb(rUrl.db);

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    var cursor = toBlock;

    while (true) {
        const blockNum = wsConn.nextBlockNum() catch |err| {
            std.debug.print("WSS error: {s} — reconnecting...\n", .{@errorName(err)});
            // Reconnect.
            wsConn.deinit();
            wsConn = ws.Conn.init(gpa, wsParsed.host, wsParsed.port, wsParsed.path) catch break;
            _ = wsConn.subscribeNewHeads() catch break;
            continue;
        };

        if (blockNum <= cursor) continue;

        // Process any skipped blocks (gap fill).
        var blk = cursor + 1;
        while (blk <= blockNum) : (blk += 1) {
            var attempts: usize = 0;
            while (attempts < 5) : (attempts += 1) {
                const ok = try pipeline.processBlock(io, gpa, &chain, &cql, &realtimeRedis,
                                                      blk, &arena);
                if (ok) break;
                const ts = std.os.linux.timespec{ .sec = 0, .nsec = 200_000_000 };
                _ = std.os.linux.nanosleep(&ts, null);
            }
            if (attempts == 5) {
                std.debug.print("[realtime] block {d}: skipped after 5 attempts\n", .{blk});
            }
        }
        cursor = blockNum;
    }
}
