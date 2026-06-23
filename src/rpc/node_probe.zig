// Node-type detection: probe web3_clientVersion on first call, cache result forever.
// GETH → debug_traceBlockByNumber (callTracer); RETH/Erigon → trace_block (parity API).
const std = @import("std");
const core = @import("indexer/core");
const FetchClient = core.fetch.Client;
const Allocator = std.mem.Allocator;

pub const TraceMethod = enum { trace_block, debug_trace_block };

const Entry = struct { url_hash: u64, method: TraceMethod };

var mu: std.atomic.Mutex = .unlocked;
var entries: [8]Entry = undefined;
var n_entries: usize = 0;

fn muLock() void {
    while (!mu.tryLock()) std.atomic.spinLoopHint();
}
fn muUnlock() void {
    mu.unlock();
}

fn urlHash(url: []const u8) u64 {
    return std.hash.Wyhash.hash(0, url);
}

fn cacheLookup(h: u64) ?TraceMethod {
    for (entries[0..n_entries]) |e| {
        if (e.url_hash == h) return e.method;
    }
    return null;
}

fn cacheStore(h: u64, method: TraceMethod) void {
    if (n_entries < entries.len) {
        entries[n_entries] = .{ .url_hash = h, .method = method };
        n_entries += 1;
    }
}

/// Detect trace method for this URL. Cached after first call — subsequent calls return instantly.
/// On probe failure, defaults to .trace_block.
pub fn detect(gpa: Allocator, client: *FetchClient, url: []const u8) TraceMethod {
    const h = urlHash(url);

    muLock();
    const cached = cacheLookup(h);
    muUnlock();
    if (cached) |m| return m;

    // https:// now goes through fetch.zig's curl-subprocess path (see task #4 — the
    // ReleaseFast SIGILL was in std.http.Client's TLS stack, not here), so this can
    // probe normally instead of defaulting blind.
    const method = probeNode(gpa, client, url) catch blk: {
        std.debug.print("[node_probe] {s}: probe failed, defaulting to trace_block\n", .{url});
        break :blk TraceMethod.trace_block;
    };

    muLock();
    const cached2 = cacheLookup(h); // another thread may have stored while we probed
    if (cached2 == null) cacheStore(h, method);
    muUnlock();
    return if (cached2) |m| m else method;
}

/// Returns cached trace method, or .trace_block as safe default.
pub fn getCached(url: []const u8) TraceMethod {
    const h = urlHash(url);
    muLock();
    const m = cacheLookup(h) orelse .trace_block;
    muUnlock();
    return m;
}

fn probeNode(gpa: Allocator, client: *FetchClient, url: []const u8) !TraceMethod {
    const payload = "{\"id\":1,\"jsonrpc\":\"2.0\",\"method\":\"web3_clientVersion\",\"params\":[]}";

    var resp = try client.fetch(.{
        .url = url,
        .method = .POST,
        .body = payload,
        .contentType = "application/json",
        .responseInitialCapacity = 4 * 1024,
    });
    defer resp.deinit(gpa);

    if (!resp.ok()) return error.NodeProbeHttpError;

    // Detect GETH by version string: {"result":"Geth/v1.13.0/linux-amd64/go1.21.0"}
    const body = resp.body;
    const is_geth = std.mem.indexOf(u8, body, "\"Geth/") != null or
        std.mem.indexOf(u8, body, "\"geth/") != null;
    const method: TraceMethod = if (is_geth) .debug_trace_block else .trace_block;
    std.debug.print("[node_probe] {s}: {s}\n", .{ url, @tagName(method) });
    return method;
}
