// Parser configuration — parsed from environment variables.
const std = @import("std");
const rpc = @import("rpc");

// ─── Fetch mode ───────────────────────────────────────────────────────────────
// RPC_FETCH_MODE=0  flat-parallel  N×3 threads, 1 request each  (default)
// RPC_FETCH_MODE=1  batch-method   3 threads, N-block batch per method
// RPC_FETCH_MODE=2  block-batch    N threads, 3-method batch per block

pub const FetchFn = *const fn (std.Io, std.mem.Allocator, std.mem.Allocator, []const u8, []const u64, []rpc.BlockData) f64;

pub const fetch_mode_names = [3][]const u8{
    "flat-parallel (N×3 threads)",
    "batch-method  (3 threads, N blocks/method)",
    "block-batch   (N threads, 3 methods/block)",
};

pub const Config = struct {
    rpc_url:        []const u8,
    reserve_rpc_url: []const u8, // RESERVE_RPC_URL="" — fallback if primary RPC fails
    ws_url:         []const u8,  // WS_URL=ws://host:port/path — use newHeads instead of polling
    chain_id:       u64,
    chunk_size:     u64,
    remap_mod:      u64,   // 0 = use chunk_size; N = chunk = block_number % N
    from_block:     u64,   // FROM_BLOCK=0 — start block when Redis key is absent
    to_block:       u64,
    batch_size:     usize,
    pipeline:       usize, // parallel fetch+save workers (1 = classic PrevBatch)
    realtime:       bool,  // REALTIME=1: single-block loop after history sync
    poll_ms:        u64,   // polling interval when no new block available (ignored if ws_url set)
    redis_host:     []const u8,
    redis_port:     u16,
    redis_pass:     []const u8,
    redis_db:       u8,
    scylla_host:    []const u8,
    scylla_port:    u16,
    scylla_keyspace: []const u8,
    scylla_user:    []const u8,
    scylla_pass:    []const u8,
    results_dir:    []const u8,
    dump_file:      []const u8,
    fetch_fn:       FetchFn,
    fetch_mode:     u8,
};

pub fn getEnv(env: *std.process.Environ.Map, key: []const u8, default: []const u8) []const u8 {
    return env.get(key) orelse default;
}

pub fn getEnvInt(comptime T: type, env: *std.process.Environ.Map, key: []const u8, default: T) T {
    const s = env.get(key) orelse return default;
    return std.fmt.parseInt(T, s, 10) catch default;
}

fn extractJsonStr(gpa: std.mem.Allocator, json_str: []const u8, key: []const u8) ?[]const u8 {
    const search = std.fmt.allocPrint(gpa, "\"{s}\":\"", .{key}) catch return null;
    defer gpa.free(search);
    const start = std.mem.indexOf(u8, json_str, search) orelse return null;
    const after = json_str[start + search.len ..];
    const end = std.mem.indexOfScalar(u8, after, '"') orelse return null;
    return gpa.dupe(u8, after[0..end]) catch null;
}

pub fn parseConfig(gpa: std.mem.Allocator, io: std.Io, env: *std.process.Environ.Map) !Config {
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
            redis_db = std.fmt.parseInt(u8, rest[slash_idx + 1 ..], 10) catch 0;
            rest = rest[0..slash_idx];
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

    const mode_str = getEnv(env, "RPC_FETCH_MODE", getEnv(env, "RPC_BATCH", "0"));
    const fetch_mode = std.fmt.parseInt(u8, mode_str, 10) catch 0;

    return Config{
        .rpc_url         = getEnv(env, "RPC_URL", "http://127.0.0.1:8545"),
        .reserve_rpc_url = getEnv(env, "RESERVE_RPC_URL", ""),
        .ws_url          = getEnv(env, "WS_URL", ""),
        .chain_id        = getEnvInt(u64, env, "CHAIN_ID", 1),
        .chunk_size      = getEnvInt(u64, env, "RAW_CHUNK_SIZE", 1000),
        .remap_mod       = getEnvInt(u64, env, "REMAP_MOD", 0),
        .from_block      = getEnvInt(u64, env, "FROM_BLOCK", 0),
        .to_block        = getEnvInt(u64, env, "TO_BLOCK", 25_079_196),
        .batch_size     = getEnvInt(usize, env, "BATCH_SIZE", 10),
        .pipeline       = @max(1, getEnvInt(usize, env, "PIPELINE", 1)),
        .realtime       = std.mem.eql(u8, getEnv(env, "REALTIME", "0"), "1"),
        .poll_ms        = getEnvInt(u64, env, "POLL_MS", 500),
        .redis_host     = redis_host,
        .redis_port     = redis_port,
        .redis_pass     = redis_pass,
        .redis_db       = redis_db,
        .scylla_host    = scylla_host,
        .scylla_port    = scylla_port,
        .scylla_keyspace = getEnv(env, "SCYLLA_DB_KEYSPACE", "eth"),
        .scylla_user    = scylla_user,
        .scylla_pass    = scylla_pass_,
        .results_dir    = results_dir,
        .dump_file      = getEnv(env, "DUMP_FILE", ""),
        .fetch_mode     = fetch_mode,
        .fetch_fn       = switch (fetch_mode) {
            1   => &rpc.fetchBatch3,
            2   => &rpc.fetchBlockBatch,
            else => &rpc.fetchBatch3,
        },
    };
}
