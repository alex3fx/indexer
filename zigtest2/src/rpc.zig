// JSON-RPC HTTP client (raw Linux TCP) + flat-parallel batch fetch.
// Key difference from zigtest: fetchBatchFlat spawns N*3 threads from one call
// so all RPC requests (block + receipts + traces) run in parallel across all blocks.
const std = @import("std");
const Io = std.Io;
const rpc_spec = @import("rpc_spec");

fn nowNs() i64 {
    var ts: std.os.linux.timespec = undefined;
    _ = std.os.linux.clock_gettime(.MONOTONIC, &ts);
    return ts.sec * 1_000_000_000 + ts.nsec;
}

// ─── JSON-RPC wrapper ─────────────────────────────────────────────────────────

pub const RpcBlockResp = struct {
    result: ?RpcBlock = null,
};
pub const RpcReceiptsResp = struct {
    result: ?[]RpcReceipt = null,
};
pub const RpcTracesResp = struct {
    result: ?[]RpcTrace = null,
};

// ─── Ethereum types ───────────────────────────────────────────────────────────

pub const RpcBlock = struct {
    number: []const u8 = "",
    timestamp: []const u8 = "",
    milliTimestamp: ?[]const u8 = null,
    miner: []const u8 = "",
    transactions: []RpcTransaction = &.{},
};

pub const RpcTransaction = struct {
    hash: []const u8 = "",
    transactionIndex: []const u8 = "",
    from: []const u8 = "",
    to: ?[]const u8 = null,
    value: []const u8 = "",
    gas: []const u8 = "",
    gasPrice: []const u8 = "",
    input: []const u8 = "",
    @"type": []const u8 = "",
    maxPriorityFeePerGas: ?[]const u8 = null,
    maxFeePerGas: ?[]const u8 = null,
};

pub const RpcReceipt = struct {
    transactionHash: []const u8 = "",
    transactionIndex: []const u8 = "",
    gasUsed: []const u8 = "",
    cumulativeGasUsed: []const u8 = "",
    effectiveGasPrice: ?[]const u8 = null,
    contractAddress: ?[]const u8 = null,
    status: []const u8 = "",
    logs: []RpcLog = &.{},
};

pub const RpcLog = struct {
    address: []const u8 = "",
    topics: [][]const u8 = &.{},
    data: []const u8 = "",
    transactionHash: []const u8 = "",
    transactionIndex: []const u8 = "",
    logIndex: []const u8 = "",
    removed: bool = false,
};

pub const RpcTrace = struct {
    transactionHash:     ?[]const u8 = null,
    transactionPosition: ?i32        = null, // tx index within block, avoids hashmap lookup
    action: RpcAction = .{},
    result: ?RpcResult = null,
};

pub const RpcAction = struct {
    from: []const u8 = "",
    to: ?[]const u8 = null,
    value: ?[]const u8 = null,
    init: ?[]const u8 = null,
    input: ?[]const u8 = null,
    creationMethod: ?[]const u8 = null,
};

pub const RpcResult = struct {
    address: ?[]const u8 = null,
    code: ?[]const u8 = null,
};

// ─── Fetch result ─────────────────────────────────────────────────────────────

pub const BlockData = struct {
    block_num: u64,
    block: ?RpcBlock,
    receipts: ?[]RpcReceipt,
    traces: ?[]RpcTrace,
    http_block_ms: f64,
    json_block_ms: f64,
    http_rcpt_ms: f64,
    json_rcpt_ms: f64,
    http_trc_ms: f64,
    json_trc_ms: f64,
    fbdr_ms: f64,
    err: bool,
};

// ─── Raw TCP (Linux syscalls, thread-safe) ────────────────────────────────────

const linux = std.os.linux;

fn parseHostPort(url: []const u8) struct { host: []const u8, port: u16 } {
    var s = url;
    if (std.mem.startsWith(u8, s, "http://")) s = s[7..];
    if (std.mem.startsWith(u8, s, "https://")) s = s[8..];
    if (std.mem.indexOfScalar(u8, s, '/')) |slash| s = s[0..slash];
    if (std.mem.lastIndexOfScalar(u8, s, ':')) |colon| {
        return .{
            .host = s[0..colon],
            .port = std.fmt.parseInt(u16, s[colon + 1 ..], 10) catch 8545,
        };
    }
    return .{ .host = s, .port = 8545 };
}

fn ipv4Parts(host: []const u8) [4]u8 {
    var parts: [4]u8 = .{ 127, 0, 0, 1 };
    var iter = std.mem.splitScalar(u8, host, '.');
    var i: usize = 0;
    while (iter.next()) |p| : (i += 1) {
        if (i >= 4) break;
        parts[i] = std.fmt.parseInt(u8, p, 10) catch 0;
    }
    return parts;
}

fn tcpConnect(host: []const u8, port: u16) !i32 {
    const sock_fd = linux.socket(linux.AF.INET, linux.SOCK.STREAM, 0);
    if (sock_fd > @as(usize, std.math.maxInt(i32)))
        return error.SocketFailed;
    const fd: i32 = @intCast(sock_fd);

    const ip = ipv4Parts(host);
    const ip_host = (@as(u32, ip[0]) << 24) | (@as(u32, ip[1]) << 16) |
        (@as(u32, ip[2]) << 8) | @as(u32, ip[3]);
    const addr = linux.sockaddr.in{
        .family = linux.AF.INET,
        .port = std.mem.nativeToBig(u16, port),
        .addr = std.mem.nativeToBig(u32, ip_host),
        .zero = std.mem.zeroes([8]u8),
    };
    const rc = linux.connect(fd, @ptrCast(&addr), @sizeOf(linux.sockaddr.in));
    if (rc != 0) {
        _ = linux.close(fd);
        return error.ConnectFailed;
    }
    const nodelay: c_int = 1;
    _ = linux.setsockopt(
        fd,
        @as(c_int, @intCast(linux.IPPROTO.TCP)),
        linux.TCP.NODELAY,
        @ptrCast(&nodelay),
        @sizeOf(c_int),
    );
    return fd;
}

fn fdWrite(fd: i32, data: []const u8) !void {
    var written: usize = 0;
    while (written < data.len) {
        const n = linux.write(fd, data[written..].ptr, data.len - written);
        if (n == 0) return error.WriteEof;
        if (n > data.len) return error.WriteFailed;
        written += n;
    }
}

fn fdRead(fd: i32, buf: []u8) !usize {
    const n = linux.read(fd, buf.ptr, buf.len);
    if (n == 0) return 0;
    if (n > buf.len) return error.ReadFailed;
    return n;
}

// httpPostZC: like httpPost but allocates the result buffer from `result_arena` when non-null.
// Used for zero-copy parse: result stays alive in the arena until deinit.
fn httpPostZC(
    io: Io,
    gpa: std.mem.Allocator,
    result_arena: ?*LockedArena,
    url: []const u8,
    body: []const u8,
    t_http_ms: *f64,
    t_json_ms: *f64,
) ![]u8 {
    _ = io;
    const hp = parseHostPort(url);
    const t0 = nowNs();
    const fd = try tcpConnect(hp.host, hp.port);
    defer _ = linux.close(fd);

    var req_buf: [512]u8 = undefined;
    const req_header = try std.fmt.bufPrint(&req_buf,
        "POST / HTTP/1.1\r\nHost: {s}:{d}\r\nContent-Type: application/json\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n",
        .{ hp.host, hp.port, body.len });
    try fdWrite(fd, req_header);
    try fdWrite(fd, body);
    const t1 = nowNs();

    var hdr_buf: [4096]u8 = undefined;
    var hdr_len: usize = 0;
    const sep = "\r\n\r\n";
    var sep_pos: ?usize = null;
    while (hdr_len < hdr_buf.len) {
        const n = try fdRead(fd, hdr_buf[hdr_len..]);
        if (n == 0) break;
        hdr_len += n;
        if (std.mem.indexOf(u8, hdr_buf[0..hdr_len], sep)) |p| { sep_pos = p; break; }
    }
    const header_end = sep_pos orelse return error.HttpMalformed;
    const body_start_in_hdr = header_end + sep.len;

    var content_length: usize = 0;
    var line_iter = std.mem.splitSequence(u8, hdr_buf[0..header_end], "\r\n");
    _ = line_iter.next();
    while (line_iter.next()) |line| {
        var lower_buf: [128]u8 = undefined;
        const ll = @min(line.len, lower_buf.len);
        _ = std.ascii.lowerString(lower_buf[0..ll], line[0..ll]);
        const lower = lower_buf[0..ll];
        if (std.mem.startsWith(u8, lower, "content-length:"))
            content_length = std.fmt.parseInt(usize, std.mem.trim(u8, lower[15..], " "), 10) catch 0;
    }
    if (content_length == 0) {
        var buf: std.ArrayList(u8) = .empty;
        errdefer buf.deinit(gpa);
        try buf.appendSlice(gpa, hdr_buf[body_start_in_hdr..hdr_len]);
        var rbuf2: [65536]u8 = undefined;
        while (true) { const nr = try fdRead(fd, &rbuf2); if (nr == 0) break; try buf.appendSlice(gpa, rbuf2[0..nr]); }
        const t2 = nowNs();
        t_http_ms.* = @as(f64, @floatFromInt(t1 - t0)) / 1e6;
        t_json_ms.* = @as(f64, @floatFromInt(t2 - t1)) / 1e6;
        return try buf.toOwnedSlice(gpa);
    }

    // Allocate result buffer: from arena (zero-copy) or gpa (regular).
    // No fallback: if arena is provided but full, fail fast — avoids from_arena mismatch.
    const result = if (result_arena) |la|
        (la.alloc(content_length) orelse return error.OutOfMemory)
    else
        try gpa.alloc(u8, content_length);

    const tail_len = hdr_len - body_start_in_hdr;
    const copy_len = @min(tail_len, content_length);
    @memcpy(result[0..copy_len], hdr_buf[body_start_in_hdr..][0..copy_len]);
    var filled: usize = copy_len;
    while (filled < content_length) {
        const n = try fdRead(fd, result[filled..]);
        if (n == 0) break;
        filled += n;
    }

    const t2 = nowNs();
    t_http_ms.* = @as(f64, @floatFromInt(t1 - t0)) / 1e6;
    t_json_ms.* = @as(f64, @floatFromInt(t2 - t1)) / 1e6;
    return result;
}

// One-shot HTTP POST. Thread-safe (no shared state).
pub fn httpPost(
    io: Io,
    gpa: std.mem.Allocator,
    url: []const u8,
    body: []const u8,
    t_http_ms: *f64,
    t_json_ms: *f64,
) ![]u8 {
    _ = io;
    const hp = parseHostPort(url);

    const t0 = nowNs();
    const fd = try tcpConnect(hp.host, hp.port);
    defer _ = linux.close(fd);

    var req_buf: [512]u8 = undefined;
    const req_header = try std.fmt.bufPrint(&req_buf,
        "POST / HTTP/1.1\r\nHost: {s}:{d}\r\nContent-Type: application/json\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n",
        .{ hp.host, hp.port, body.len });
    try fdWrite(fd, req_header);
    try fdWrite(fd, body);

    const t1 = nowNs();

    // Single-copy path: read response header into stack buffer, then allocate
    // exactly content_length bytes for the body — no intermediate all_data buffer.
    var hdr_buf: [4096]u8 = undefined;
    var hdr_len: usize = 0;

    // Read until \r\n\r\n appears in hdr_buf
    const sep = "\r\n\r\n";
    var sep_pos: ?usize = null;
    while (hdr_len < hdr_buf.len) {
        const n = try fdRead(fd, hdr_buf[hdr_len..]);
        if (n == 0) break;
        hdr_len += n;
        if (std.mem.indexOf(u8, hdr_buf[0..hdr_len], sep)) |p| {
            sep_pos = p;
            break;
        }
    }
    const header_end = sep_pos orelse return error.HttpMalformed;
    const body_start_in_hdr = header_end + sep.len;

    // Parse Content-Length from header
    var content_length: usize = 0;
    var chunked = false;
    var line_iter = std.mem.splitSequence(u8, hdr_buf[0..header_end], "\r\n");
    _ = line_iter.next(); // status line
    while (line_iter.next()) |line| {
        var lower_buf: [128]u8 = undefined;
        const lower_len = @min(line.len, lower_buf.len);
        _ = std.ascii.lowerString(lower_buf[0..lower_len], line[0..lower_len]);
        const lower = lower_buf[0..lower_len];
        if (std.mem.startsWith(u8, lower, "content-length:")) {
            content_length = std.fmt.parseInt(usize, std.mem.trim(u8, lower[15..], " "), 10) catch 0;
        } else if (std.mem.startsWith(u8, lower, "transfer-encoding:") and
            std.mem.indexOf(u8, lower, "chunked") != null)
        {
            chunked = true;
        }
    }

    if (chunked) {
        // Fallback: chunked responses (rare for JSON-RPC). Use growing buffer.
        var buf: std.ArrayList(u8) = .empty;
        errdefer buf.deinit(gpa);
        // Append header tail first, then read more
        const tail = hdr_buf[body_start_in_hdr..hdr_len];
        var rbuf: [65536]u8 = undefined;
        var all: std.ArrayList(u8) = .empty;
        defer all.deinit(gpa);
        try all.appendSlice(gpa, tail);
        while (true) {
            const n = try fdRead(fd, &rbuf);
            if (n == 0) break;
            try all.appendSlice(gpa, rbuf[0..n]);
        }
        var pos: usize = 0;
        const d = all.items;
        while (pos < d.len) {
            const line_end = std.mem.indexOfPos(u8, d, pos, "\r\n") orelse break;
            const chunk_size = std.fmt.parseInt(usize, d[pos..line_end], 16) catch break;
            if (chunk_size == 0) break;
            pos = line_end + 2;
            try buf.appendSlice(gpa, d[pos..@min(pos + chunk_size, d.len)]);
            pos += chunk_size + 2;
        }
        const t2 = nowNs();
        t_http_ms.* = @as(f64, @floatFromInt(t1 - t0)) / 1e6;
        t_json_ms.* = @as(f64, @floatFromInt(t2 - t1)) / 1e6;
        return try buf.toOwnedSlice(gpa);
    }

    if (content_length == 0) {
        // No Content-Length: read until close (rare for JSON-RPC, use growing buffer).
        var buf: std.ArrayList(u8) = .empty;
        errdefer buf.deinit(gpa);
        try buf.appendSlice(gpa, hdr_buf[body_start_in_hdr..hdr_len]);
        var rbuf2: [65536]u8 = undefined;
        while (true) {
            const nr = try fdRead(fd, &rbuf2);
            if (nr == 0) break;
            try buf.appendSlice(gpa, rbuf2[0..nr]);
        }
        const t2 = nowNs();
        t_http_ms.* = @as(f64, @floatFromInt(t1 - t0)) / 1e6;
        t_json_ms.* = @as(f64, @floatFromInt(t2 - t1)) / 1e6;
        return try buf.toOwnedSlice(gpa);
    }

    // Content-Length path: allocate exactly content_length bytes — one copy, no resize.
    // Caller frees the full slice; size matches the allocation.
    const result = try gpa.alloc(u8, content_length);
    errdefer gpa.free(result);

    const tail_len = hdr_len - body_start_in_hdr;
    const copy_len = @min(tail_len, content_length);
    @memcpy(result[0..copy_len], hdr_buf[body_start_in_hdr..][0..copy_len]);
    var filled: usize = copy_len;

    while (filled < content_length) {
        const n = try fdRead(fd, result[filled..]);
        if (n == 0) break;
        filled += n;
    }

    const t2 = nowNs();
    t_http_ms.* = @as(f64, @floatFromInt(t1 - t0)) / 1e6;
    t_json_ms.* = @as(f64, @floatFromInt(t2 - t1)) / 1e6;
    return result; // full content_length slice — caller frees correctly
}

// ─── Flat parallel fetch ───────────────────────────────────────────────────────
// Spawns N*3 threads — one per (block, method) pair — so all requests run concurrently.

const RawOut = struct {
    data:       []u8  = &.{},
    http_ms:    f64   = 0,
    failed:     bool  = false,
    bytes:      usize = 0,
    from_arena: bool  = false, // true → data owned by method_arena, do NOT free via gpa
};

// Spinlock-protected arena for allocating raw HTTP response buffers from multiple threads.
// N=10 HTTP threads per method arena → low contention (10 large allocs, not 10K small ones).
const LockedArena = struct {
    arena: *std.heap.ArenaAllocator,
    state: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

    fn lock(self: *LockedArena) void {
        while (self.state.cmpxchgWeak(0, 1, .acquire, .monotonic) != null)
            std.atomic.spinLoopHint();
    }
    fn unlock(self: *LockedArena) void { self.state.store(0, .release); }

    fn alloc(self: *LockedArena, n: usize) ?[]u8 {
        self.lock();
        defer self.unlock();
        return self.arena.allocator().alloc(u8, n) catch null;
    }
};

const FlatFetchArg = struct {
    io:           Io,
    gpa:          std.mem.Allocator,
    url:          []const u8,
    body:         []const u8,
    out:          *RawOut,
    locked_arena: ?*LockedArena = null, // non-null → allocate result from arena (zero-copy)
};

fn flatFetch(arg: FlatFetchArg) void {
    var dummy: f64 = 0;
    const data = httpPostZC(arg.io, arg.gpa, arg.locked_arena, arg.url, arg.body, &arg.out.http_ms, &dummy) catch {
        arg.out.failed = true;
        return;
    };
    arg.out.data = data;
    arg.out.bytes = data.len;
    arg.out.from_arena = arg.locked_arena != null;
}

// ─── Mutex-wrapped allocator for concurrent parse into a shared arena ─────────

// ─── 3-method parallel parse ──────────────────────────────────────────────────
// Each method thread owns its arena — no locking, no sharing.
// Thread 0: parseBlockResp × N into arenas[0]
// Thread 1: parseReceiptsResp × N into arenas[1]
// Thread 2: parseTracesResp × N into arenas[2]

const ParseMethodArg = struct {
    outs:    []const RawOut,
    results: []BlockData,
    arena:   *std.heap.ArenaAllocator,
    method:  u2,   // 0=block  1=receipts  2=traces
    n:       usize,
};

fn parseMethodThread(arg: ParseMethodArg) void {
    const alloc = arg.arena.allocator();
    for (0..arg.n) |b| {
        const o = &arg.outs[b * 3 + arg.method];
        if (o.failed or o.data.len == 0) continue;
        // Use ZC variants: raw buffer is owned by method_arena (from_arena=true),
        // so strings are zero-copy slices that live until method_arena.deinit().
        switch (arg.method) {
            0 => arg.results[b].block    = rpc_spec.parseBlockRespZC(o.data, alloc) catch null,
            1 => arg.results[b].receipts = rpc_spec.parseReceiptsRespZC(o.data, alloc) catch null,
            2 => arg.results[b].traces   = rpc_spec.parseTracesRespZC(o.data, alloc) catch null,
            else => unreachable,
        }
    }
}

fn buildBlockBatchBody(gpa: std.mem.Allocator, block_num: u64) ![]u8 {
    var hex_buf: [32]u8 = undefined;
    const hex = std.fmt.bufPrint(&hex_buf, "0x{x}", .{block_num}) catch unreachable;
    return std.fmt.allocPrint(gpa,
        \\[{{"jsonrpc":"2.0","id":0,"method":"eth_getBlockByNumber","params":["{s}",true]}},{{"jsonrpc":"2.0","id":1,"method":"eth_getBlockReceipts","params":["{s}"]}},{{"jsonrpc":"2.0","id":2,"method":"trace_block","params":["{s}"]}}]
    , .{ hex, hex, hex });
}

/// Fetch N blocks using N parallel threads, each sending a 3-method batch request per block.
/// Best for single-block (sync) mode and low-connection scenarios with non-zero RTT.
pub fn fetchBlockBatch(
    io: Io,
    gpa: std.mem.Allocator,
    dc_alloc: std.mem.Allocator,
    url: []const u8,
    block_nums: []const u64,
    results: []BlockData,
) f64 {
    const n = block_nums.len;
    if (n == 0) return 0;

    const bodies = gpa.alloc([]u8, n) catch return 0;
    defer { for (bodies) |b| if (b.len > 0) gpa.free(b); gpa.free(bodies); }
    for (bodies) |*b| b.* = &.{};

    const outs = gpa.alloc(RawOut, n) catch return 0;
    defer gpa.free(outs);
    for (outs) |*o| o.* = .{};

    const args = gpa.alloc(FlatFetchArg, n) catch return 0;
    defer gpa.free(args);

    const threads = gpa.alloc(std.Thread, n) catch return 0;
    defer gpa.free(threads);

    // Build per-block batch bodies
    for (block_nums, 0..) |bn, b| {
        bodies[b] = buildBlockBatchBody(gpa, bn) catch continue;
    }

    const t_start = nowNs();

    // Spawn N threads — one per block, each sends a 3-method batch
    var spawned: usize = 0;
    for (0..n) |b| {
        if (bodies[b].len == 0) { outs[b].failed = true; continue; }
        args[b] = .{ .io = io, .gpa = gpa, .url = url, .body = bodies[b], .out = &outs[b] };
        if (std.Thread.spawn(.{}, flatFetch, .{args[b]})) |t| {
            threads[spawned] = t;
            spawned += 1;
        } else |_| {
            flatFetch(args[b]);
        }
    }
    for (threads[0..spawned]) |t| t.join();

    const fbdr_ms = @as(f64, @floatFromInt(nowNs() - t_start)) / 1e6;

    // Parse sequentially into dc_alloc (arena-safe, no concurrent writes)
    for (block_nums, 0..) |block_num, b| {
        const o = &outs[b];
        results[b] = BlockData{
            .block_num = block_num,
            .block = null, .receipts = null, .traces = null,
            .http_block_ms = o.http_ms, .json_block_ms = 0,
            .http_rcpt_ms = o.http_ms, .json_rcpt_ms = 0,
            .http_trc_ms = o.http_ms, .json_trc_ms = 0,
            .fbdr_ms = fbdr_ms, .err = o.failed,
        };
        if (o.failed or o.data.len == 0) continue;
        defer gpa.free(o.data);

        var blk: ?RpcBlock = null;
        var rcpts: ?[]RpcReceipt = null;
        var trcs: ?[]RpcTrace = null;
        rpc_spec.parseBlockBatchResp(o.data, &blk, &rcpts, &trcs, dc_alloc) catch {};
        results[b].block = blk;
        results[b].receipts = rcpts;
        results[b].traces = trcs;
        results[b].err = blk == null or rcpts == null or trcs == null;
    }

    return fbdr_ms;
}

fn buildBatchBody(
    gpa: std.mem.Allocator,
    method: []const u8,
    extra: []const u8,
    block_nums: []const u64,
) ![]u8 {
    var buf: std.ArrayList(u8) = .empty;
    errdefer buf.deinit(gpa);
    try buf.append(gpa, '[');
    var hex_buf: [32]u8 = undefined;
    for (block_nums, 0..) |bn, idx| {
        if (idx > 0) try buf.append(gpa, ',');
        const hex = std.fmt.bufPrint(&hex_buf, "0x{x}", .{bn}) catch unreachable;
        const part = try std.fmt.allocPrint(gpa,
            "{{\"jsonrpc\":\"2.0\",\"id\":{d},\"method\":\"{s}\",\"params\":[\"{s}\"{s}]}}",
            .{ idx, method, hex, extra });
        defer gpa.free(part);
        try buf.appendSlice(gpa, part);
    }
    try buf.append(gpa, ']');
    return try buf.toOwnedSlice(gpa);
}

/// Fetch a batch of N blocks using 3 parallel HTTP requests (one per method).
/// Returns wall-clock time of the HTTP phase in ms.
pub fn fetchBatch3(
    io: Io,
    gpa: std.mem.Allocator,
    dc_alloc: std.mem.Allocator,
    url: []const u8,
    block_nums: []const u64,
    results: []BlockData,
) f64 {
    const n = block_nums.len;
    if (n == 0) return 0;

    const t_start = nowNs();

    const blk_body = buildBatchBody(gpa, "eth_getBlockByNumber", ",true", block_nums) catch return 0;
    defer gpa.free(blk_body);
    const rcpt_body = buildBatchBody(gpa, "eth_getBlockReceipts", "", block_nums) catch return 0;
    defer gpa.free(rcpt_body);
    const trc_body = buildBatchBody(gpa, "trace_block", "", block_nums) catch return 0;
    defer gpa.free(trc_body);

    var outs: [3]RawOut = .{ .{}, .{}, .{} };
    const args = [3]FlatFetchArg{
        .{ .io = io, .gpa = gpa, .url = url, .body = blk_body,  .out = &outs[0] },
        .{ .io = io, .gpa = gpa, .url = url, .body = rcpt_body, .out = &outs[1] },
        .{ .io = io, .gpa = gpa, .url = url, .body = trc_body,  .out = &outs[2] },
    };

    var threads: [3]?std.Thread = .{ null, null, null };
    for (0..3) |k| {
        if (std.Thread.spawn(.{}, flatFetch, .{args[k]})) |t| {
            threads[k] = t;
        } else |_| {
            flatFetch(args[k]);
        }
    }
    for (threads) |mt| if (mt) |t| t.join();

    const fbdr_ms = @as(f64, @floatFromInt(nowNs() - t_start)) / 1e6;
    defer for (&outs) |*o| if (o.data.len > 0) gpa.free(o.data);

    for (block_nums, 0..) |block_num, b| {
        results[b] = BlockData{
            .block_num = block_num,
            .block = null, .receipts = null, .traces = null,
            .http_block_ms = outs[0].http_ms,
            .json_block_ms = 0,
            .http_rcpt_ms = outs[1].http_ms,
            .json_rcpt_ms = 0,
            .http_trc_ms = outs[2].http_ms,
            .json_trc_ms = 0,
            .fbdr_ms = fbdr_ms,
            .err = outs[0].failed or outs[1].failed or outs[2].failed,
        };
    }

    if (outs[0].failed or outs[1].failed or outs[2].failed) return fbdr_ms;

    const blk_out = gpa.alloc(?RpcBlock, n) catch return fbdr_ms;
    defer gpa.free(blk_out);
    for (blk_out) |*x| x.* = null;

    const rcpt_out = gpa.alloc(?[]RpcReceipt, n) catch return fbdr_ms;
    defer gpa.free(rcpt_out);
    for (rcpt_out) |*x| x.* = null;

    const trc_out = gpa.alloc(?[]RpcTrace, n) catch return fbdr_ms;
    defer gpa.free(trc_out);
    for (trc_out) |*x| x.* = null;

    rpc_spec.parseBatchBlockResp(outs[0].data, blk_out, dc_alloc) catch {};
    rpc_spec.parseBatchReceiptsResp(outs[1].data, rcpt_out, dc_alloc) catch {};
    rpc_spec.parseBatchTracesResp(outs[2].data, trc_out, dc_alloc) catch {};

    std.debug.print(
        "[batch3] HTTP={d:.0}ms(blk={d:.0} rcpt={d:.0} trc={d:.0}) | blk={d}KB rcpt={d}KB trc={d}KB\n",
        .{
            fbdr_ms, outs[0].http_ms, outs[1].http_ms, outs[2].http_ms,
            outs[0].bytes / 1024, outs[1].bytes / 1024, outs[2].bytes / 1024,
        },
    );

    for (0..n) |b| {
        results[b].block = blk_out[b];
        results[b].receipts = rcpt_out[b];
        results[b].traces = trc_out[b];
        if (blk_out[b] == null or rcpt_out[b] == null or trc_out[b] == null)
            results[b].err = true;
    }

    return fbdr_ms;
}

/// Fetch a batch of blocks using N*3 parallel threads (batch_size * 3 requests in flight).
/// Fetch N blocks using N×3 parallel HTTP threads, then parse with 3 method threads.
/// method_arenas[0/1/2] receive block/receipts/traces strings — caller deinits after save.
pub fn fetchBatchFlat(
    io: Io,
    gpa: std.mem.Allocator,
    method_arenas: *[3]std.heap.ArenaAllocator,
    url: []const u8,
    block_nums: []const u64,
    results: []BlockData,
) f64 {
    const n = block_nums.len;
    if (n == 0) return 0;

    const total = n * 3;

    // One pool for all request body strings — avoids N*3 separate allocPrint calls.
    // Max body sizes: eth_getBlockByNumber=110, eth_getBlockReceipts=95, trace_block=80.
    const BODY_SLOT = 128;
    const body_pool = gpa.alloc(u8, total * BODY_SLOT) catch return 0;
    defer gpa.free(body_pool);

    const outs = gpa.alloc(RawOut, total) catch return 0;
    defer gpa.free(outs);
    for (outs) |*o| o.* = .{};

    const args = gpa.alloc(FlatFetchArg, total) catch return 0;
    defer gpa.free(args);

    const threads = gpa.alloc(std.Thread, total) catch return 0;
    defer gpa.free(threads);

    const t_start = nowNs();

    // Write all request bodies into pool slots — zero heap allocs.
    const bodies = gpa.alloc([]u8, total) catch return 0;
    defer gpa.free(bodies);
    for (bodies) |*b| b.* = &.{};
    var body_ok = gpa.alloc(bool, total) catch return 0;
    defer gpa.free(body_ok);
    for (body_ok) |*ok| ok.* = false;

    for (block_nums, 0..) |block_num, b| {
        var hex_buf: [32]u8 = undefined;
        const hex_num = std.fmt.bufPrint(&hex_buf, "0x{x}", .{block_num}) catch continue;

        const s0 = body_pool[(b * 3 + 0) * BODY_SLOT ..][0..BODY_SLOT];
        const s1 = body_pool[(b * 3 + 1) * BODY_SLOT ..][0..BODY_SLOT];
        const s2 = body_pool[(b * 3 + 2) * BODY_SLOT ..][0..BODY_SLOT];

        bodies[b * 3 + 0] = std.fmt.bufPrint(s0,
            \\{{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["{s}",true]}}
        , .{hex_num}) catch continue;
        bodies[b * 3 + 1] = std.fmt.bufPrint(s1,
            \\{{"jsonrpc":"2.0","id":1,"method":"eth_getBlockReceipts","params":["{s}"]}}
        , .{hex_num}) catch continue;
        bodies[b * 3 + 2] = std.fmt.bufPrint(s2,
            \\{{"jsonrpc":"2.0","id":1,"method":"trace_block","params":["{s}"]}}
        , .{hex_num}) catch continue;

        body_ok[b * 3 + 0] = true;
        body_ok[b * 3 + 1] = true;
        body_ok[b * 3 + 2] = true;
    }

    // LockedArenas: one per method (block/receipts/traces).
    // HTTP threads allocate raw response buffers directly into method_arenas —
    // buffers stay alive for zero-copy parse and are freed at method_arena.deinit().
    var locked: [3]LockedArena = .{
        .{ .arena = &method_arenas[0] },
        .{ .arena = &method_arenas[1] },
        .{ .arena = &method_arenas[2] },
    };

    // Spawn all threads
    var spawned: usize = 0;
    for (0..total) |i| {
        if (!body_ok[i]) {
            outs[i].failed = true;
            continue;
        }
        args[i] = .{
            .io           = io,
            .gpa          = gpa,
            .url          = url,
            .body         = bodies[i],
            .out          = &outs[i],
            .locked_arena = &locked[i % 3],
        };
        if (std.Thread.spawn(.{}, flatFetch, .{args[i]})) |thread| {
            threads[spawned] = thread;
            spawned += 1;
        } else |_| {
            flatFetch(args[i]);
        }
    }

    // Join all spawned threads
    for (threads[0..spawned]) |t| t.join();

    const fbdr_ms = @as(f64, @floatFromInt(nowNs() - t_start)) / 1e6;

    // ── Initialize results with HTTP timings ──────────────────────────────────
    var bytes_blk: usize = 0;
    var bytes_rcpt: usize = 0;
    var bytes_trc: usize = 0;
    for (block_nums, 0..) |block_num, b| {
        const bo = &outs[b * 3 + 0];
        const ro = &outs[b * 3 + 1];
        const to = &outs[b * 3 + 2];
        bytes_blk += bo.bytes;
        bytes_rcpt += ro.bytes;
        bytes_trc += to.bytes;
        results[b] = BlockData{
            .block_num = block_num,
            .block = null, .receipts = null, .traces = null,
            .http_block_ms = bo.http_ms, .json_block_ms = 0,
            .http_rcpt_ms  = ro.http_ms, .json_rcpt_ms = 0,
            .http_trc_ms   = to.http_ms, .json_trc_ms = 0,
            .fbdr_ms = fbdr_ms,
            .err = bo.failed or ro.failed or to.failed,
        };
    }

    // ── Parallel parse: 3 method threads, each owns its arena (no locking) ───
    const parse_args = [3]ParseMethodArg{
        .{ .outs = outs, .results = results, .arena = &method_arenas[0], .method = 0, .n = n },
        .{ .outs = outs, .results = results, .arena = &method_arenas[1], .method = 1, .n = n },
        .{ .outs = outs, .results = results, .arena = &method_arenas[2], .method = 2, .n = n },
    };
    var parse_threads: [3]?std.Thread = .{ null, null, null };
    for (0..3) |k| {
        if (std.Thread.spawn(.{}, parseMethodThread, .{parse_args[k]})) |t| {
            parse_threads[k] = t;
        } else |_| {
            parseMethodThread(parse_args[k]);
        }
    }
    for (parse_threads) |mt| if (mt) |t| t.join();

    // Free raw HTTP buffers: arena-owned buffers freed at method_arena.deinit() (zero-copy).
    // gpa-owned buffers (fallback) freed here.
    for (0..n) |b| {
        for (0..3) |k| {
            const o = &outs[b * 3 + k];
            if (o.data.len > 0 and !o.from_arena) gpa.free(o.data);
        }
    }

    // Mark blocks where any method failed to parse.
    for (0..n) |b| {
        if (!results[b].err)
            results[b].err = results[b].block == null or
                             results[b].receipts == null or
                             results[b].traces == null;
    }

    std.debug.print(
        "[JSON breakdown] HTTP(parallel)={d:.0}ms | data: blk={d}KB rcpt={d}KB trc={d}KB\n",
        .{ fbdr_ms, bytes_blk / 1024, bytes_rcpt / 1024, bytes_trc / 1024 },
    );

    return fbdr_ms;
}

/// Fetch a single block with 3 parallel HTTP requests — zero heap allocation.
/// Uses stack buffers for all scaffolding (avoids 6 mmap syscalls vs fetchBatchFlat).
/// t_start is set before any work so fetch_ms is the true wall-clock time.
pub fn fetchBlock(
    io: Io,
    gpa: std.mem.Allocator,
    method_arenas: *[3]std.heap.ArenaAllocator,
    url: []const u8,
    block_num: u64,
    result: *BlockData,
) f64 {
    const BODY_SLOT = 128;

    // Stack-allocated scaffolding — no mmap, no defer free
    var body_pool:  [3 * BODY_SLOT]u8           = undefined;
    var outs:       [3]RawOut                   = .{ .{}, .{}, .{} };
    var args:       [3]FlatFetchArg             = undefined;
    var threads:    [3]std.Thread               = undefined;
    var bodies: [3][]u8 = .{ &.{}, &.{}, &.{} };

    const t_start = nowNs(); // timer before any work (no allocations precede this)

    var hex_buf: [32]u8 = undefined;
    const hex_num = std.fmt.bufPrint(&hex_buf, "0x{x}", .{block_num}) catch return 0;

    bodies[0] = std.fmt.bufPrint(body_pool[0 * BODY_SLOT ..][0..BODY_SLOT],
        \\{{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["{s}",true]}}
    , .{hex_num}) catch return 0;
    bodies[1] = std.fmt.bufPrint(body_pool[1 * BODY_SLOT ..][0..BODY_SLOT],
        \\{{"jsonrpc":"2.0","id":1,"method":"eth_getBlockReceipts","params":["{s}"]}}
    , .{hex_num}) catch return 0;
    bodies[2] = std.fmt.bufPrint(body_pool[2 * BODY_SLOT ..][0..BODY_SLOT],
        \\{{"jsonrpc":"2.0","id":1,"method":"trace_block","params":["{s}"]}}
    , .{hex_num}) catch return 0;

    var locked: [3]LockedArena = .{
        .{ .arena = &method_arenas[0] },
        .{ .arena = &method_arenas[1] },
        .{ .arena = &method_arenas[2] },
    };

    var spawned: usize = 0;
    for (0..3) |i| {
        args[i] = .{
            .io           = io,
            .gpa          = gpa,
            .url          = url,
            .body         = bodies[i],
            .out          = &outs[i],
            .locked_arena = &locked[i],
        };
        if (std.Thread.spawn(.{}, flatFetch, .{args[i]})) |t| {
            threads[spawned] = t;
            spawned += 1;
        } else |_| {
            flatFetch(args[i]);
        }
    }
    for (threads[0..spawned]) |t| t.join();

    const fbdr_ms = @as(f64, @floatFromInt(nowNs() - t_start)) / 1e6;

    result.* = BlockData{
        .block_num     = block_num,
        .block         = null, .receipts = null, .traces = null,
        .http_block_ms = outs[0].http_ms, .json_block_ms = 0,
        .http_rcpt_ms  = outs[1].http_ms, .json_rcpt_ms  = 0,
        .http_trc_ms   = outs[2].http_ms, .json_trc_ms   = 0,
        .fbdr_ms       = fbdr_ms,
        .err           = outs[0].failed or outs[1].failed or outs[2].failed,
    };

    if (result.err) return fbdr_ms;

    // Parse all 3 responses in parallel (same as fetchBatchFlat for n=1).
    // Traces are the heaviest (~60% of parse time) — parallel saves ~0.7ms.
    var results1 = [1]BlockData{result.*};
    const parse_args = [3]ParseMethodArg{
        .{ .outs = &outs, .results = &results1, .arena = &method_arenas[0], .method = 0, .n = 1 },
        .{ .outs = &outs, .results = &results1, .arena = &method_arenas[1], .method = 1, .n = 1 },
        .{ .outs = &outs, .results = &results1, .arena = &method_arenas[2], .method = 2, .n = 1 },
    };
    var parse_threads: [3]?std.Thread = .{ null, null, null };
    for (0..3) |k| {
        if (std.Thread.spawn(.{}, parseMethodThread, .{parse_args[k]})) |t| {
            parse_threads[k] = t;
        } else |_| {
            parseMethodThread(parse_args[k]);
        }
    }
    for (parse_threads) |mt| if (mt) |t| t.join();

    result.block    = results1[0].block;
    result.receipts = results1[0].receipts;
    result.traces   = results1[0].traces;

    if (result.block == null or result.receipts == null or result.traces == null)
        result.err = true;

    std.debug.print(
        "[JSON breakdown] HTTP(parallel)={d:.0}ms | data: blk={d}KB rcpt={d}KB trc={d}KB\n",
        .{ fbdr_ms, outs[0].bytes / 1024, outs[1].bytes / 1024, outs[2].bytes / 1024 },
    );

    return fbdr_ms;
}

