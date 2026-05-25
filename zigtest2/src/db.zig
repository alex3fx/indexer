// ScyllaDB (CQL v4 binary protocol) + Redis (RESP) clients.
// Uses raw Linux syscalls for all network IO — bypasses Io.Threaded entirely.
const std = @import("std");
const transform = @import("transform");
const linux = std.os.linux;
const Io = std.Io; // still referenced in CqlPool.init signature

fn nowNs() i64 {
    var ts: linux.timespec = undefined;
    _ = linux.clock_gettime(.MONOTONIC, &ts);
    return ts.sec * 1_000_000_000 + ts.nsec;
}

// ─── Raw TCP helpers (same pattern as rpc.zig) ────────────────────────────────

fn tcpConnectRaw(host: []const u8, port: u16) !i32 {
    const sock_fd = linux.socket(linux.AF.INET, linux.SOCK.STREAM, 0);
    if (sock_fd > @as(usize, std.math.maxInt(i32)))
        return error.SocketFailed;
    const fd: i32 = @intCast(sock_fd);

    // Parse IPv4 host
    var ip: [4]u8 = .{127, 0, 0, 1};
    var iter = std.mem.splitScalar(u8, host, '.');
    var idx: usize = 0;
    while (iter.next()) |p| : (idx += 1) {
        if (idx >= 4) break;
        ip[idx] = std.fmt.parseInt(u8, p, 10) catch 0;
    }

    const ip_host = (@as(u32, ip[0]) << 24) | (@as(u32, ip[1]) << 16) |
                    (@as(u32, ip[2]) << 8)  |  @as(u32, ip[3]);
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
    // Disable Nagle's algorithm: critical for CQL latency (reduces ~40ms buffering delay)
    const nodelay: c_int = 1;
    _ = linux.setsockopt(fd, @as(c_int, @intCast(linux.IPPROTO.TCP)),
        linux.TCP.NODELAY,
        @ptrCast(&nodelay), @sizeOf(c_int));
    return fd;
}

// Connect and bind to a specific local port (for shard-aware routing via Scylla port 19042).
// Scylla routes connections on port 19042 to shard = source_port % num_shards.
fn tcpConnectBound(host: []const u8, dst_port: u16, src_port: u16) !i32 {
    const sock_fd = linux.socket(linux.AF.INET, linux.SOCK.STREAM, 0);
    if (sock_fd > @as(usize, std.math.maxInt(i32))) return error.SocketFailed;
    const fd: i32 = @intCast(sock_fd);
    errdefer _ = linux.close(fd);

    const reuse: c_int = 1;
    _ = linux.setsockopt(fd, linux.SOL.SOCKET, linux.SO.REUSEADDR,
        @ptrCast(&reuse), @sizeOf(c_int));

    const local = linux.sockaddr.in{
        .family = linux.AF.INET,
        .port   = std.mem.nativeToBig(u16, src_port),
        .addr   = std.mem.nativeToBig(u32, 0x7f000001),
        .zero   = std.mem.zeroes([8]u8),
    };
    if (linux.bind(fd, @ptrCast(&local), @sizeOf(linux.sockaddr.in)) != 0)
        return error.BindFailed;

    var ip: [4]u8 = .{127, 0, 0, 1};
    var iter = std.mem.splitScalar(u8, host, '.');
    var idx: usize = 0;
    while (iter.next()) |p| : (idx += 1) {
        if (idx >= 4) break;
        ip[idx] = std.fmt.parseInt(u8, p, 10) catch 0;
    }
    const ip_host = (@as(u32, ip[0]) << 24) | (@as(u32, ip[1]) << 16) |
                    (@as(u32, ip[2]) << 8)  |  @as(u32, ip[3]);
    const addr = linux.sockaddr.in{
        .family = linux.AF.INET,
        .port   = std.mem.nativeToBig(u16, dst_port),
        .addr   = std.mem.nativeToBig(u32, ip_host),
        .zero   = std.mem.zeroes([8]u8),
    };
    if (linux.connect(fd, @ptrCast(&addr), @sizeOf(linux.sockaddr.in)) != 0)
        return error.ConnectFailed;

    const nodelay: c_int = 1;
    _ = linux.setsockopt(fd, @as(c_int, @intCast(linux.IPPROTO.TCP)),
        linux.TCP.NODELAY, @ptrCast(&nodelay), @sizeOf(c_int));
    return fd;
}

// Query Scylla SUPPORTED frame to find SCYLLA_NR_SHARDS.
// Returns 1 on any error (safe fallback).
fn queryNumShards(host: []const u8, port: u16, gpa: std.mem.Allocator) u32 {
    const fd = tcpConnectRaw(host, port) catch return 1;
    defer _ = linux.close(fd);

    const req: [9]u8 = .{0x04, 0x00, 0x00, 0x01, CQL_OPCODE_OPTIONS, 0, 0, 0, 0};
    tcpWrite(fd, &req) catch return 1;

    var hdr: [9]u8 = undefined;
    tcpReadExact(fd, &hdr) catch return 1;
    const body_len = std.mem.readInt(u32, hdr[5..9], .big);
    if (body_len > 65536) return 1;

    const body = gpa.alloc(u8, body_len) catch return 1;
    defer gpa.free(body);
    tcpReadExact(fd, body) catch return 1;

    var pos: usize = 0;
    if (pos + 2 > body.len) return 1;
    const n_pairs = std.mem.readInt(u16, body[pos..][0..2], .big);
    pos += 2;

    for (0..n_pairs) |_| {
        if (pos + 2 > body.len) break;
        const kl = std.mem.readInt(u16, body[pos..][0..2], .big);
        pos += 2;
        if (pos + kl > body.len) break;
        const key = body[pos..][0..kl];
        pos += kl;

        if (pos + 2 > body.len) break;
        const nv = std.mem.readInt(u16, body[pos..][0..2], .big);
        pos += 2;

        var first: ?[]const u8 = null;
        for (0..nv) |vi| {
            if (pos + 2 > body.len) break;
            const vl = std.mem.readInt(u16, body[pos..][0..2], .big);
            pos += 2;
            if (pos + vl > body.len) break;
            const val = body[pos..][0..vl];
            pos += vl;
            if (vi == 0) first = val;
        }
        if (std.mem.eql(u8, key, "SCYLLA_NR_SHARDS")) {
            if (first) |v| return std.fmt.parseInt(u32, v, 10) catch 1;
        }
    }
    return 1;
}

fn tcpWrite(fd: i32, data: []const u8) !void {
    var written: usize = 0;
    while (written < data.len) {
        const n = linux.write(fd, data[written..].ptr, data.len - written);
        if (n == 0 or n > data.len) return error.WriteFailed;
        written += n;
    }
}

// Send two buffers as a single writev syscall — eliminates one syscall per EXECUTE frame.
fn tcpWritev(fd: i32, a: []const u8, b: []const u8) !void {
    var iov = [2]std.posix.iovec_const{
        .{ .base = a.ptr, .len = a.len },
        .{ .base = b.ptr, .len = b.len },
    };
    const total_len = a.len + b.len;
    var total_written: usize = 0;

    while (total_written < total_len) {
        const n = linux.writev(fd, @ptrCast(&iov), 2);
        if (n == 0 or n > total_len) return error.WriteFailed;
        total_written += n;

        var consumed: usize = n;
        for (&iov) |*v| {
            if (consumed == 0) break;
            if (consumed >= v.len) {
                consumed -= v.len;
                v.base += v.len;
                v.len = 0;
            } else {
                v.base += consumed;
                v.len -= consumed;
                consumed = 0;
            }
        }
    }
}

fn tcpReadExact(fd: i32, buf: []u8) !void {
    var pos: usize = 0;
    while (pos < buf.len) {
        const n = linux.read(fd, buf[pos..].ptr, buf.len - pos);
        if (n == 0) return error.ConnectionClosed;
        if (n > buf.len) return error.ReadFailed;
        pos += n;
    }
}

fn tcpReadSome(fd: i32, buf: []u8) !usize {
    const n = linux.read(fd, buf.ptr, buf.len);
    if (n == 0) return 0;
    if (n > buf.len) return error.ReadFailed;
    return n;
}

// ─── CQL frame protocol ───────────────────────────────────────────────────────

const CQL_REQUEST_VERSION: u8 = 0x04;
const CQL_OPCODE_STARTUP:      u8 = 0x01;
const CQL_OPCODE_OPTIONS:      u8 = 0x05;
const CQL_OPCODE_SUPPORTED:    u8 = 0x06;
const CQL_OPCODE_AUTH_RESPONSE: u8 = 0x0F;
const CQL_OPCODE_QUERY: u8 = 0x07;
const CQL_OPCODE_PREPARE: u8 = 0x09;
const CQL_OPCODE_EXECUTE: u8 = 0x0A;
const CQL_OPCODE_READY: u8 = 0x02;
const CQL_OPCODE_AUTHENTICATE: u8 = 0x03;
const CQL_OPCODE_AUTH_SUCCESS: u8 = 0x10;
const CQL_OPCODE_RESULT: u8 = 0x08;
const CQL_OPCODE_ERROR: u8 = 0x00;
const CQL_CONSISTENCY_ONE: u16  = 0x0001;
const CQL_OPCODE_BATCH:    u8   = 0x0D;
const BATCH_SIZE: usize = 100; // rows per UNLOGGED BATCH frame

pub const CqlConn = struct {
    fd: i32,
    gpa: std.mem.Allocator,

    pub fn init(
        io: std.Io,
        gpa: std.mem.Allocator,
        host: []const u8,
        port: u16,
        keyspace: []const u8,
        user: []const u8,
        pass: []const u8,
    ) !CqlConn {
        _ = io;
        const fd = try tcpConnectRaw(host, port);
        return initWithFd(fd, gpa, keyspace, user, pass);
    }

    // Used by shard-aware pool init: fd already bound/connected externally.
    pub fn initWithFd(
        fd: i32,
        gpa: std.mem.Allocator,
        keyspace: []const u8,
        user: []const u8,
        pass: []const u8,
    ) !CqlConn {
        var self = CqlConn{ .fd = fd, .gpa = gpa };
        errdefer _ = linux.close(fd);

        try self.sendStartup();
        {
            const r = try self.recvFrame();
            self.gpa.free(r.body);
            if (r.opcode == CQL_OPCODE_AUTHENTICATE) {
                try self.sendAuthResponse(user, pass);
                const ar = try self.recvFrame();
                self.gpa.free(ar.body);
                if (ar.opcode != CQL_OPCODE_AUTH_SUCCESS) return error.CqlAuthFailed;
            } else if (r.opcode != CQL_OPCODE_READY) {
                return error.CqlUnexpectedOpcode;
            }
        }

        const use_query = try std.fmt.allocPrint(gpa, "USE {s}", .{keyspace});
        defer gpa.free(use_query);
        try self.sendQuery(use_query);
        {
            const r = try self.recvFrame();
            self.gpa.free(r.body);
            if (r.opcode == CQL_OPCODE_ERROR) return error.CqlUseKeyspaceFailed;
        }

        return self;
    }

    pub fn deinit(self: *CqlConn) void {
        _ = linux.close(self.fd);
    }

    // Send a CQL frame with a body (stream=1, for handshake/prepare/query)
    fn sendFrame(self: *CqlConn, opcode: u8, body: []const u8) !void {
        var header: [9]u8 = undefined;
        header[0] = CQL_REQUEST_VERSION;
        header[1] = 0x00;
        header[2] = 0x00;
        header[3] = 0x01;
        header[4] = opcode;
        std.mem.writeInt(u32, header[5..9], @intCast(body.len), .big);
        try tcpWrite(self.fd, &header);
        if (body.len > 0) try tcpWrite(self.fd, body);
    }

    // Send a CQL frame with an explicit stream ID (for pipelining)
    fn sendFrameStream(self: *CqlConn, opcode: u8, body: []const u8, stream: u16) !void {
        var header: [9]u8 = undefined;
        header[0] = CQL_REQUEST_VERSION;
        header[1] = 0x00;
        std.mem.writeInt(u16, header[2..4], stream, .big);
        header[4] = opcode;
        std.mem.writeInt(u32, header[5..9], @intCast(body.len), .big);
        try tcpWrite(self.fd, &header);
        if (body.len > 0) try tcpWrite(self.fd, body);
    }

    // Send EXECUTE frame only — zero heap allocation.
    // Builds [CQL header 9B] + [prep_id prefix] + [values_body] using stack buffers.
    // Two tcpWrite calls instead of ArrayList+mmap per frame.
    fn executeSend(self: *CqlConn, prep_id: []const u8, values_body: []const u8, n_values: u16, stream: u16) !void {
        // prefix = [short prep_id_len][prep_id][short consistency][byte flags][short n_values]
        const prefix_body_len: u32 = @intCast(2 + prep_id.len + 2 + 1 + 2 + values_body.len);
        const hdr_prefix_len = 9 + 2 + prep_id.len + 2 + 1 + 2;

        // Stack buffer: 9 (CQL header) + 2 (len) + 32 (prep_id, typically 16B) + 5 (cons+flags+nval)
        var buf: [9 + 2 + 64 + 2 + 1 + 2]u8 = undefined;

        // CQL v4 frame header
        buf[0] = CQL_REQUEST_VERSION;
        buf[1] = 0x00;
        std.mem.writeInt(u16, buf[2..4], stream, .big);
        buf[4] = CQL_OPCODE_EXECUTE;
        std.mem.writeInt(u32, buf[5..9], prefix_body_len, .big);

        // EXECUTE body: [short bytes] prep_id
        std.mem.writeInt(u16, buf[9..11], @intCast(prep_id.len), .big);
        @memcpy(buf[11..][0..prep_id.len], prep_id);

        // consistency + flags + n_values
        const off = 11 + prep_id.len;
        std.mem.writeInt(u16, buf[off..][0..2], CQL_CONSISTENCY_ONE, .big);
        buf[off + 2] = 0x01; // flags: VALUES
        std.mem.writeInt(u16, buf[off + 3 ..][0..2], n_values, .big);

        // One writev syscall instead of two tcpWrite calls.
        try tcpWritev(self.fd, buf[0..hdr_prefix_len], values_body);
    }

    // Send one UNLOGGED BATCH frame containing multiple prepared-statement rows.
    // row_bufs[i] = pre-encoded values for row i (same format as EXECUTE values_body).
    // Sends frame, reads one response. No pipelining needed — one round-trip for N rows.
    fn batchSendRows(
        self:    *CqlConn,
        prep_id: []const u8,
        n_vals:  u16,
        row_bufs: []const []const u8,
    ) !void {
        if (row_bufs.len == 0) return;

        // Estimate: 3 (type+n) + n×(1+2+prep_id.len+2+avg_row) + 3 (consistency+flags)
        const est: usize = 10 + row_bufs.len * (5 + prep_id.len + 300);
        var frame: std.ArrayList(u8) = .empty;
        defer frame.deinit(A);
        frame.ensureTotalCapacity(A, est) catch {};

        var tmp: [2]u8 = undefined;

        // BATCH body: type=UNLOGGED
        try frame.append(A, 0x01);
        // n_statements
        std.mem.writeInt(u16, &tmp, @intCast(row_bufs.len), .big);
        try frame.appendSlice(A, &tmp);

        for (row_bufs) |rb| {
            // kind=PREPARED
            try frame.append(A, 0x01);
            // prep_id: [short bytes]
            std.mem.writeInt(u16, &tmp, @intCast(prep_id.len), .big);
            try frame.appendSlice(A, &tmp);
            try frame.appendSlice(A, prep_id);
            // n_values
            std.mem.writeInt(u16, &tmp, n_vals, .big);
            try frame.appendSlice(A, &tmp);
            // encoded values
            try frame.appendSlice(A, rb);
        }

        // consistency=ONE, flags=0
        std.mem.writeInt(u16, &tmp, CQL_CONSISTENCY_ONE, .big);
        try frame.appendSlice(A, &tmp);
        try frame.append(A, 0x00);

        try self.sendFrame(CQL_OPCODE_BATCH, frame.items);
        try recvFrameCheck(self.fd);
    }

    // Receive full CQL frame: 9-byte header + body
    fn recvFrame(self: *CqlConn) !struct { opcode: u8, body: []u8 } {
        var header: [9]u8 = undefined;
        try tcpReadExact(self.fd, &header);
        const opcode = header[4];
        const body_len = std.mem.readInt(u32, header[5..9], .big);

        if (body_len == 0) return .{ .opcode = opcode, .body = &.{} };

        const body = try self.gpa.alloc(u8, body_len);
        errdefer self.gpa.free(body);
        try tcpReadExact(self.fd, body);
        return .{ .opcode = opcode, .body = body };
    }

    fn sendStartup(self: *CqlConn) !void {
        var body: std.ArrayList(u8) = .empty;
        defer body.deinit(self.gpa);
        // String map: {"CQL_VERSION": "3.0.0"}
        try appendShort(&body, self.gpa, 1); // 1 pair
        try appendCqlString(&body, self.gpa, "CQL_VERSION");
        try appendCqlString(&body, self.gpa, "3.0.0");
        try self.sendFrame(CQL_OPCODE_STARTUP, body.items);
    }

    fn sendAuthResponse(self: *CqlConn, user: []const u8, pass: []const u8) !void {
        // SASL plain: \x00 + user + \x00 + pass
        var sasl: std.ArrayList(u8) = .empty;
        defer sasl.deinit(self.gpa);
        try sasl.append(self.gpa, 0);
        try sasl.appendSlice(self.gpa, user);
        try sasl.append(self.gpa, 0);
        try sasl.appendSlice(self.gpa, pass);

        var body: std.ArrayList(u8) = .empty;
        defer body.deinit(self.gpa);
        try appendBytes(&body, self.gpa, sasl.items);
        try self.sendFrame(CQL_OPCODE_AUTH_RESPONSE, body.items);
    }

    fn sendQuery(self: *CqlConn, query: []const u8) !void {
        var body: std.ArrayList(u8) = .empty;
        defer body.deinit(self.gpa);
        try appendLongString(&body, self.gpa, query);
        try appendShort(&body, self.gpa, CQL_CONSISTENCY_ONE);
        try appendByte(&body, self.gpa, 0x00); // no flags
        try self.sendFrame(CQL_OPCODE_QUERY, body.items);
    }

    pub fn prepare(self: *CqlConn, query: []const u8) ![]u8 {
        var body: std.ArrayList(u8) = .empty;
        defer body.deinit(self.gpa);
        try appendLongString(&body, self.gpa, query);
        var flags: [4]u8 = .{0, 0, 0, 0};
        try body.appendSlice(self.gpa, &flags);
        try self.sendFrame(CQL_OPCODE_PREPARE, body.items);

        const resp = try self.recvFrame();
        defer self.gpa.free(resp.body);

        if (resp.opcode == CQL_OPCODE_ERROR) return error.CqlPrepareError;
        if (resp.opcode != CQL_OPCODE_RESULT) return error.CqlUnexpectedOpcode;

        // Parse RESULT kind=4 (PREPARED): [int32] kind + [short bytes] id
        if (resp.body.len < 6) return error.CqlMalformedResult;
        const kind = std.mem.readInt(i32, resp.body[0..4], .big);
        if (kind != 4) return error.CqlNotPrepared;
        const id_len = std.mem.readInt(u16, resp.body[4..6], .big);
        if (resp.body.len < 6 + id_len) return error.CqlMalformedResult;
        return try self.gpa.dupe(u8, resp.body[6..][0..id_len]);
    }

    // Execute a prepared statement with pre-built values buffer.
    // CQL v4: <id>[short bytes] <consistency>[short] <flags>[byte] <n_values>[short] <values>
    pub fn execute(self: *CqlConn, prep_id: []const u8, values_body: []const u8, n_values: u16) !void {
        var body: std.ArrayList(u8) = .empty;
        defer body.deinit(self.gpa);
        // [short bytes] prep_id
        try appendShort(&body, self.gpa, @intCast(prep_id.len));
        try body.appendSlice(self.gpa, prep_id);
        // [short] consistency = ONE
        try appendShort(&body, self.gpa, CQL_CONSISTENCY_ONE);
        // [byte] flags: 0x01 = VALUES (CQL v4 uses byte, not int)
        try appendByte(&body, self.gpa, 0x01);
        // [short] n_values
        try appendShort(&body, self.gpa, n_values);
        // values
        try body.appendSlice(self.gpa, values_body);

        try self.sendFrame(CQL_OPCODE_EXECUTE, body.items);

        const resp = try self.recvFrame();
        defer self.gpa.free(resp.body);
        if (resp.opcode == CQL_OPCODE_ERROR) {
            return error.CqlExecuteError;
        }
    }
};

// ─── CQL value encoding helpers ───────────────────────────────────────────────

fn appendShort(list: *std.ArrayList(u8), gpa: std.mem.Allocator, v: u16) !void {
    var b: [2]u8 = undefined;
    std.mem.writeInt(u16, &b, v, .big);
    try list.appendSlice(gpa, &b);
}

fn appendByte(list: *std.ArrayList(u8), gpa: std.mem.Allocator, v: u8) !void {
    try list.append(gpa, v);
}

fn appendCqlString(list: *std.ArrayList(u8), gpa: std.mem.Allocator, s: []const u8) !void {
    try appendShort(list, gpa, @intCast(s.len));
    try list.appendSlice(gpa, s);
}

fn appendLongString(list: *std.ArrayList(u8), gpa: std.mem.Allocator, s: []const u8) !void {
    var b: [4]u8 = undefined;
    std.mem.writeInt(u32, &b, @intCast(s.len), .big);
    try list.appendSlice(gpa, &b);
    try list.appendSlice(gpa, s);
}

fn appendBytes(list: *std.ArrayList(u8), gpa: std.mem.Allocator, data: []const u8) !void {
    var b: [4]u8 = undefined;
    std.mem.writeInt(i32, &b, @intCast(data.len), .big);
    try list.appendSlice(gpa, &b);
    try list.appendSlice(gpa, data);
}

// Value encoding: each value is [int32 size] + bytes, or [int32 -1] for null
pub fn valNull(list: *std.ArrayList(u8), gpa: std.mem.Allocator) !void {
    var b: [4]u8 = undefined;
    std.mem.writeInt(i32, &b, -1, .big);
    try list.appendSlice(gpa, &b);
}

pub fn valBigint(list: *std.ArrayList(u8), gpa: std.mem.Allocator, v: i64) !void {
    var b: [12]u8 = undefined;
    std.mem.writeInt(i32, b[0..4], 8, .big);
    std.mem.writeInt(i64, b[4..12], v, .big);
    try list.appendSlice(gpa, &b);
}

// CQL varint: big-endian two's complement, variable length.
// Writes directly into list's backing buffer — zero heap allocation, zero intermediate copy.
//
// Layout written into list: [i32 payload_len][optional 0x00][trimmed big-endian bytes]
//
// Algorithm:
//   1. ensureTotalCapacity for worst case (4 + 1 + nbytes_max)
//   2. decode hex nibbles directly into list.items[out_start..]
//   3. trim leading zero bytes in-place with copyForwards
//   4. if MSB set, shift right with copyBackwards and prepend 0x00
//   5. write length prefix, set list.items.len to exact final size
pub fn valVarint(list: *std.ArrayList(u8), gpa: std.mem.Allocator, hex_s: []const u8) !void {
    var hex = hex_s;
    if (std.mem.startsWith(u8, hex, "0x") or std.mem.startsWith(u8, hex, "0X"))
        hex = hex[2..];

    if (hex.len == 0) {
        return list.appendSlice(gpa, &.{ 0, 0, 0, 1, 0 });
    }

    const nbytes_max = (hex.len + 1) / 2;
    const old_len   = list.items.len;
    const out_start = old_len + 4; // 4 bytes reserved for the length prefix

    // Reserve space: [4B len] + [1B optional prefix] + [payload]
    try list.ensureTotalCapacity(gpa, old_len + 4 + 1 + nbytes_max);
    list.items.len = old_len + 4 + 1 + nbytes_max;

    // Decode hex directly into list buffer
    var wi = out_start; // write index
    var i: usize = 0;
    if (hex.len % 2 != 0) {
        list.items[wi] = try varintHexNibble(hex[0]); // single nibble → low 4 bits
        wi += 1;
        i = 1;
    }
    while (i < hex.len) : ({ i += 2; wi += 1; }) {
        list.items[wi] = (try varintHexNibble(hex[i]) << 4) | (try varintHexNibble(hex[i + 1]));
    }

    // Trim leading zero bytes (in-place, keep at least one byte)
    const raw_len = wi - out_start;
    var trim: usize = 0;
    while (trim + 1 < raw_len and list.items[out_start + trim] == 0) : (trim += 1) {}

    const trimmed_len = raw_len - trim;
    if (trim > 0) {
        std.mem.copyForwards(u8, list.items[out_start..][0..trimmed_len],
                                 list.items[out_start + trim..][0..trimmed_len]);
    }

    // Prepend 0x00 sign byte if MSB is set (marks value as positive in two's complement)
    const need_prefix = list.items[out_start] >= 0x80;
    const payload_len = trimmed_len + if (need_prefix) @as(usize, 1) else 0;

    if (need_prefix) {
        std.mem.copyBackwards(u8, list.items[out_start + 1..][0..trimmed_len],
                                  list.items[out_start..][0..trimmed_len]);
        list.items[out_start] = 0x00;
    }

    // Write 4-byte length prefix and fix up items.len
    std.mem.writeInt(i32, list.items[old_len..][0..4], @intCast(payload_len), .big);
    list.items.len = old_len + 4 + payload_len;
}

inline fn varintHexNibble(c: u8) !u8 {
    return switch (c) {
        '0'...'9' => c - '0',
        'a'...'f' => c - 'a' + 10,
        'A'...'F' => c - 'A' + 10,
        else => error.InvalidHex,
    };
}

pub fn valInt32(list: *std.ArrayList(u8), gpa: std.mem.Allocator, v: i32) !void {
    var b: [8]u8 = undefined;
    std.mem.writeInt(i32, b[0..4], 4, .big);
    std.mem.writeInt(i32, b[4..8], v, .big);
    try list.appendSlice(gpa, &b);
}

pub fn valTinyint(list: *std.ArrayList(u8), gpa: std.mem.Allocator, v: i8) !void {
    var b: [5]u8 = undefined;
    std.mem.writeInt(i32, b[0..4], 1, .big);
    b[4] = @bitCast(v);
    try list.appendSlice(gpa, &b);
}

pub fn valText(list: *std.ArrayList(u8), gpa: std.mem.Allocator, s: []const u8) !void {
    if (s.len == 0) {
        try valNull(list, gpa);
        return;
    }
    var b: [4]u8 = undefined;
    std.mem.writeInt(i32, &b, @intCast(s.len), .big);
    try list.appendSlice(gpa, &b);
    try list.appendSlice(gpa, s);
}

pub fn valTextRequired(list: *std.ArrayList(u8), gpa: std.mem.Allocator, s: []const u8) !void {
    var b: [4]u8 = undefined;
    std.mem.writeInt(i32, &b, @intCast(s.len), .big);
    try list.appendSlice(gpa, &b);
    try list.appendSlice(gpa, s);
}

pub fn valBool(list: *std.ArrayList(u8), gpa: std.mem.Allocator, v: bool) !void {
    var b: [5]u8 = undefined;
    std.mem.writeInt(i32, b[0..4], 1, .big);
    b[4] = if (v) 1 else 0;
    try list.appendSlice(gpa, &b);
}

pub fn valListText(list: *std.ArrayList(u8), gpa: std.mem.Allocator, items: []const []const u8) !void {
    // Compute total size of list encoding: 4 (n) + sum(4+len(item))
    var total: usize = 4;
    for (items) |item| total += 4 + item.len;

    var size_b: [4]u8 = undefined;
    std.mem.writeInt(i32, &size_b, @intCast(total), .big);
    try list.appendSlice(gpa, &size_b);

    std.mem.writeInt(i32, &size_b, @intCast(items.len), .big);
    try list.appendSlice(gpa, &size_b);

    for (items) |item| {
        std.mem.writeInt(i32, &size_b, @intCast(item.len), .big);
        try list.appendSlice(gpa, &size_b);
        try list.appendSlice(gpa, item);
    }
}

// ─── Tuning constants from build options ─────────────────────────────────────

const cfg = @import("cfg");

pub const POOL_SIZE: u32 = cfg.pool_size;
pub const PIPELINE:  u32 = cfg.pipeline;

// Parse "1,3,6,20,1,1" into [6]u32 at comptime.
pub const SPLIT: [6]u32 = blk: {
    var result = [6]u32{ 0, 0, 0, 0, 0, 0 };
    var i: usize = 0;
    var cur: u32 = 0;
    for (cfg.split) |ch| {
        if (ch == ',') { result[i] = cur; i += 1; cur = 0; }
        else           { cur = cur * 10 + (ch - '0'); }
    }
    result[i] = cur;
    break :blk result;
};

// Cumulative offsets [0, s0, s0+s1, ...] — 7 entries for 6 tables.
const SPLIT_OFF: [7]u32 = blk: {
    var off = [7]u32{ 0, 0, 0, 0, 0, 0, 0 };
    var sum: u32 = 0;
    for (0..6) |k| { sum += SPLIT[k]; off[k + 1] = sum; }
    // Validate at compile time: sum must equal POOL_SIZE.
    if (sum != POOL_SIZE) @compileError("split sum != pool_size");
    break :blk off;
};

// ─── CQL connection pool ──────────────────────────────────────────────────────

pub const CqlPool = struct {
    conns: []*CqlConn = &.{},
    count: usize = 0,
    next_idx: std.atomic.Value(usize) = .{ .raw = 0 },
    gpa: std.mem.Allocator = undefined,

    pub fn init(
        io: Io,
        gpa: std.mem.Allocator,
        host: []const u8,
        port: u16,
        keyspace: []const u8,
        user: []const u8,
        pass: []const u8,
    ) !CqlPool {
        const conns = try gpa.alloc(*CqlConn, POOL_SIZE);
        var pool = CqlPool{ .conns = conns, .gpa = gpa };

        if (cfg.shard_aware) {
            // Connect all pool connections to the target shard via port 19042.
            // Scylla routes connections on 19042 to shard = source_port % num_shards.
            const num_shards = queryNumShards(host, port, gpa);
            const target: u32 = @intCast(cfg.shard_aware_target_shard);
            std.log.info("[db] shard-aware: target_shard={} / num_shards={}", .{ target, num_shards });
            for (0..POOL_SIZE) |i| {
                const conn = try gpa.create(CqlConn);
                const src: u16 = @intCast(40000 + i * num_shards + target);
                const fd = try tcpConnectBound(host, 19042, src);
                conn.* = try CqlConn.initWithFd(fd, gpa, keyspace, user, pass);
                pool.conns[i] = conn;
                pool.count = i + 1;
            }
        } else {
            for (0..POOL_SIZE) |i| {
                const conn = try gpa.create(CqlConn);
                conn.* = try CqlConn.init(io, gpa, host, port, keyspace, user, pass);
                pool.conns[i] = conn;
                pool.count = i + 1;
            }
        }
        return pool;
    }

    pub fn deinit(self: *CqlPool) void {
        for (self.conns[0..self.count]) |c| {
            c.deinit();
            self.gpa.destroy(c);
        }
        self.gpa.free(self.conns);
    }

    // Round-robin connection selection (lock-free, thread-safe)
    pub fn acquire(self: *CqlPool) *CqlConn {
        const idx = self.next_idx.fetchAdd(1, .monotonic) % self.count;
        return self.conns[idx];
    }
};

// ─── Prepared statement IDs ───────────────────────────────────────────────────

pub const PreparedIds = struct {
    blocks: []u8,
    transactions: []u8,
    logs: []u8,
    internal_txs: []u8,
    contracts: []u8,
    contracts_by_addr: []u8,
};

const INSERT_BLOCKS = "INSERT INTO blocks (chunk,number,timestamp_s,timestamp_ms,miner) VALUES (?,?,?,?,?)";
const INSERT_TXS = "INSERT INTO transactions (chunk,block_number,transaction_index,hash,block_timestamp_s,block_timestamp_ms,method_id,input,from_address,to_address,value,gas_limit,gas_price,gas_used,max_priority_fee_per_gas,max_fee_per_gas,cumulative_gas_used,effective_gas_price,contract_address,status,type) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
const INSERT_LOGS = "INSERT INTO logs (chunk,block_number,transaction_index,log_index,block_timestamp_s,block_timestamp_ms,address,data,topic_zeroth,topic_first,topic_second,topic_third,rest_topics,transaction_hash,removed) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
const INSERT_INT_TXS = "INSERT INTO internal_transactions (chunk,block_number,block_timestamp_s,block_timestamp_ms,transaction_index,transaction_hash,trace_index,from_address,to_address,value) VALUES (?,?,?,?,?,?,?,?,?,?)";
const INSERT_CONTRACTS = "INSERT INTO contracts (chunk,block_number,transaction_index,transaction_hash,trace_index,block_timestamp_s,block_timestamp_ms,address,creation_method,creator_address,contract_factory,creation_bytecode,deployed_bytecode) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)";
const INSERT_CONTRACTS_BY_ADDR = "INSERT INTO contracts_by_addresses (address,creator,tx_hash,block_number,timestamp,contract_factory,creation_bytecode,deployed_bytecode) VALUES (?,?,?,?,?,?,?,?)";

pub fn prepareAll(conn: *CqlConn) !PreparedIds {
    return .{
        .blocks           = try conn.prepare(INSERT_BLOCKS),
        .transactions     = try conn.prepare(INSERT_TXS),
        .logs             = try conn.prepare(INSERT_LOGS),
        .internal_txs     = try conn.prepare(INSERT_INT_TXS),
        .contracts        = try conn.prepare(INSERT_CONTRACTS),
        .contracts_by_addr = try conn.prepare(INSERT_CONTRACTS_BY_ADDR),
    };
}

// ─── Encode-on-send: typed workers encode each row directly into a reusable buffer ─
//
// Pattern per worker:
//   var v = workerBuf(EST_*_ROW)    ← pre-allocate typical capacity, no growth needed
//   for each row:
//     v.items.len = 0               ← reset O(1), no syscall, reuses capacity
//     encode row fields into v
//     batchSendRows(...)            ← one BATCH frame for up to BATCH_SIZE rows
//   v.deinit(A)                     ← one free at worker end
//
// Result: N_rows individual round-trips → ceil(N_rows/BATCH_SIZE) round-trips.

// PIPELINE is kept for compatibility but workers use BATCH_SIZE instead.
const A = std.heap.page_allocator; // per-worker encode buffer allocator

// Estimated row sizes for pre-allocation. Covers the common case without growth.
// Larger rows (e.g. contract bytecode) will cause one-time growth and then stabilise.
const EST_BLOCK_ROW    = 96;
const EST_TX_ROW       = 512;  // input can be large; 512 covers typical non-deploy txs
const EST_LOG_ROW      = 512;  // data + 4 topics
const EST_ITX_ROW      = 192;
const EST_CONTRACT_ROW = 1024; // creation/deployed bytecode can be large
const EST_CBA_ROW      = 512;

// Pre-allocate worker encode buffer. Falls back to growing on demand if alloc fails.
fn workerBuf(comptime est: usize) std.ArrayList(u8) {
    var v: std.ArrayList(u8) = .empty;
    v.ensureTotalCapacity(A, est) catch {}; // best-effort; grows automatically if needed
    return v;
}

// Read and discard one CQL response frame — zero heap allocation.
fn recvFrameCheck(fd: i32) !void {
    var header: [9]u8 = undefined;
    tcpReadExact(fd, &header) catch return;
    const opcode   = header[4];
    const body_len = std.mem.readInt(u32, header[5..9], .big);
    if (body_len == 0) {
        if (opcode == 0x00) return error.CqlError;
        return;
    }

    var buf: [512]u8 = undefined;
    const read_len = @min(body_len, buf.len);
    tcpReadExact(fd, buf[0..read_len]) catch {};

    if (opcode == 0x00) {
        const code    = if (read_len >= 4) std.mem.readInt(i32, buf[0..4], .big) else -1;
        const msg_len = if (read_len >= 6) std.mem.readInt(u16, buf[4..6], .big) else 0;
        const msg     = buf[6..@min(6 + @as(usize, msg_len), read_len)];
        std.debug.print("[CQL ERROR] code=0x{x:0>4} msg={s}\n", .{ code, msg });
    }

    var discard: [128]u8 = undefined;
    var remaining: usize = body_len -| read_len;
    while (remaining > 0) {
        const n = @min(remaining, discard.len);
        tcpReadExact(fd, discard[0..n]) catch return;
        remaining -= n;
    }

    if (opcode == 0x00) return error.CqlError;
}

// ─── Typed worker args ────────────────────────────────────────────────────────

const BlocksWA  = struct { conn: *CqlConn, prep_id: []const u8, rows: []const transform.BlockRow,           had_error: *bool };
const TxsWA     = struct { conn: *CqlConn, prep_id: []const u8, rows: []const transform.TxRow,             had_error: *bool };
const LogsWA    = struct { conn: *CqlConn, prep_id: []const u8, rows: []const transform.LogRow,            had_error: *bool };
const ITxsWA    = struct { conn: *CqlConn, prep_id: []const u8, rows: []const transform.InternalTxRow,     had_error: *bool };
const ContWA    = struct { conn: *CqlConn, prep_id: []const u8, rows: []const transform.ContractRow,       had_error: *bool };
const CBAWA     = struct { conn: *CqlConn, prep_id: []const u8, rows: []const transform.ContractByAddrRow, had_error: *bool };

// ─── Typed worker functions — UNLOGGED BATCH mode ─────────────────────────────
// Each worker collects up to BATCH_SIZE rows into bd[], sends as one BATCH frame.
// bd = contiguous buffer, starts[] marks per-row slice boundaries.

fn blocksWorker(wa: BlocksWA) void {
    var v  = workerBuf(EST_BLOCK_ROW);              defer v.deinit(A);
    var bd = workerBuf(BATCH_SIZE * EST_BLOCK_ROW); defer bd.deinit(A);
    var starts: [BATCH_SIZE + 1]usize = undefined;
    var ptrs:   [BATCH_SIZE][]const u8 = undefined;
    var i: usize = 0;
    while (i < wa.rows.len) {
        const end = @min(i + BATCH_SIZE, wa.rows.len);
        bd.items.len = 0; var enc: usize = 0;
        for (i..end) |j| {
            v.items.len = 0; starts[enc] = bd.items.len;
            const r = wa.rows[j];
            valInt32(&v, A, r.chunk) catch continue;
            valBigint(&v, A, r.number) catch continue;
            valBigint(&v, A, r.timestamp_s) catch continue;
            valBigint(&v, A, r.timestamp_ms) catch continue;
            valTextRequired(&v, A, r.miner) catch continue;
            bd.appendSlice(A, v.items) catch continue;
            enc += 1;
        }
        starts[enc] = bd.items.len;
        for (0..enc) |k| ptrs[k] = bd.items[starts[k]..starts[k + 1]];
        wa.conn.batchSendRows(wa.prep_id, 5, ptrs[0..enc]) catch { wa.had_error.* = true; return; };
        i = end;
    }
}

fn txsWorker(wa: TxsWA) void {
    var v  = workerBuf(EST_TX_ROW);              defer v.deinit(A);
    var bd = workerBuf(BATCH_SIZE * EST_TX_ROW); defer bd.deinit(A);
    var starts: [BATCH_SIZE + 1]usize = undefined;
    var ptrs:   [BATCH_SIZE][]const u8 = undefined;
    var i: usize = 0;
    while (i < wa.rows.len) {
        const end = @min(i + BATCH_SIZE, wa.rows.len);
        bd.items.len = 0; var enc: usize = 0;
        for (i..end) |j| {
            v.items.len = 0; starts[enc] = bd.items.len;
            const r = wa.rows[j];
            valInt32(&v, A, r.chunk) catch continue;
            valBigint(&v, A, r.block_number) catch continue;
            valInt32(&v, A, r.transaction_index) catch continue;
            valTextRequired(&v, A, r.hash) catch continue;
            valBigint(&v, A, r.block_timestamp_s) catch continue;
            valBigint(&v, A, r.block_timestamp_ms) catch continue;
            valText(&v, A, r.method_id) catch continue;
            valText(&v, A, r.input) catch continue;
            valTextRequired(&v, A, r.from_address) catch continue;
            valText(&v, A, r.to_address) catch continue;
            valVarint(&v, A, r.value) catch continue;
            valBigint(&v, A, r.gas_limit) catch continue;
            valBigint(&v, A, r.gas_price) catch continue;
            valBigint(&v, A, r.gas_used) catch continue;
            valBigint(&v, A, r.max_priority_fee) catch continue;
            valBigint(&v, A, r.max_fee) catch continue;
            valBigint(&v, A, r.cumulative_gas_used) catch continue;
            valBigint(&v, A, r.effective_gas_price) catch continue;
            valText(&v, A, r.contract_address) catch continue;
            valTinyint(&v, A, r.status) catch continue;
            valTinyint(&v, A, r.tx_type) catch continue;
            bd.appendSlice(A, v.items) catch continue;
            enc += 1;
        }
        starts[enc] = bd.items.len;
        for (0..enc) |k| ptrs[k] = bd.items[starts[k]..starts[k + 1]];
        wa.conn.batchSendRows(wa.prep_id, 21, ptrs[0..enc]) catch { wa.had_error.* = true; return; };
        i = end;
    }
}

fn logsWorker(wa: LogsWA) void {
    var v  = workerBuf(EST_LOG_ROW);              defer v.deinit(A);
    var bd = workerBuf(BATCH_SIZE * EST_LOG_ROW); defer bd.deinit(A);
    var starts: [BATCH_SIZE + 1]usize = undefined;
    var ptrs:   [BATCH_SIZE][]const u8 = undefined;
    var i: usize = 0;
    while (i < wa.rows.len) {
        const end = @min(i + BATCH_SIZE, wa.rows.len);
        bd.items.len = 0; var enc: usize = 0;
        for (i..end) |j| {
            v.items.len = 0; starts[enc] = bd.items.len;
            const r = wa.rows[j];
            valInt32(&v, A, r.chunk) catch continue;
            valBigint(&v, A, r.block_number) catch continue;
            valInt32(&v, A, r.transaction_index) catch continue;
            valInt32(&v, A, r.log_index) catch continue;
            valBigint(&v, A, r.block_timestamp_s) catch continue;
            valBigint(&v, A, r.block_timestamp_ms) catch continue;
            valTextRequired(&v, A, r.address) catch continue;
            valTextRequired(&v, A, r.data) catch continue;
            valText(&v, A, r.topic_zeroth) catch continue;
            valText(&v, A, r.topic_first) catch continue;
            valText(&v, A, r.topic_second) catch continue;
            valText(&v, A, r.topic_third) catch continue;
            valListText(&v, A, r.rest_topics) catch continue;
            valTextRequired(&v, A, r.transaction_hash) catch continue;
            valBool(&v, A, r.removed) catch continue;
            bd.appendSlice(A, v.items) catch continue;
            enc += 1;
        }
        starts[enc] = bd.items.len;
        for (0..enc) |k| ptrs[k] = bd.items[starts[k]..starts[k + 1]];
        wa.conn.batchSendRows(wa.prep_id, 15, ptrs[0..enc]) catch { wa.had_error.* = true; return; };
        i = end;
    }
}

fn itxsWorker(wa: ITxsWA) void {
    var v  = workerBuf(EST_ITX_ROW);              defer v.deinit(A);
    var bd = workerBuf(BATCH_SIZE * EST_ITX_ROW); defer bd.deinit(A);
    var starts: [BATCH_SIZE + 1]usize = undefined;
    var ptrs:   [BATCH_SIZE][]const u8 = undefined;
    var i: usize = 0;
    while (i < wa.rows.len) {
        const end = @min(i + BATCH_SIZE, wa.rows.len);
        bd.items.len = 0; var enc: usize = 0;
        for (i..end) |j| {
            v.items.len = 0; starts[enc] = bd.items.len;
            const r = wa.rows[j];
            valInt32(&v, A, r.chunk) catch continue;
            valBigint(&v, A, r.block_number) catch continue;
            valBigint(&v, A, r.block_timestamp_s) catch continue;
            valBigint(&v, A, r.block_timestamp_ms) catch continue;
            valInt32(&v, A, r.transaction_index) catch continue;
            valTextRequired(&v, A, r.transaction_hash) catch continue;
            valInt32(&v, A, r.trace_index) catch continue;
            valTextRequired(&v, A, r.from_address) catch continue;
            valTextRequired(&v, A, r.to_address) catch continue;
            valVarint(&v, A, r.value) catch continue;
            bd.appendSlice(A, v.items) catch continue;
            enc += 1;
        }
        starts[enc] = bd.items.len;
        for (0..enc) |k| ptrs[k] = bd.items[starts[k]..starts[k + 1]];
        wa.conn.batchSendRows(wa.prep_id, 10, ptrs[0..enc]) catch { wa.had_error.* = true; return; };
        i = end;
    }
}

fn contWorker(wa: ContWA) void {
    var v  = workerBuf(EST_CONTRACT_ROW);              defer v.deinit(A);
    var bd = workerBuf(BATCH_SIZE * EST_CONTRACT_ROW); defer bd.deinit(A);
    var starts: [BATCH_SIZE + 1]usize = undefined;
    var ptrs:   [BATCH_SIZE][]const u8 = undefined;
    var i: usize = 0;
    while (i < wa.rows.len) {
        const end = @min(i + BATCH_SIZE, wa.rows.len);
        bd.items.len = 0; var enc: usize = 0;
        for (i..end) |j| {
            v.items.len = 0; starts[enc] = bd.items.len;
            const r = wa.rows[j];
            valInt32(&v, A, r.chunk) catch continue;
            valBigint(&v, A, r.block_number) catch continue;
            valInt32(&v, A, r.transaction_index) catch continue;
            valTextRequired(&v, A, r.transaction_hash) catch continue;
            valInt32(&v, A, r.trace_index) catch continue;
            valBigint(&v, A, r.block_timestamp_s) catch continue;
            valBigint(&v, A, r.block_timestamp_ms) catch continue;
            valTextRequired(&v, A, r.address) catch continue;
            valTinyint(&v, A, r.creation_method) catch continue;
            valTextRequired(&v, A, r.creator_address) catch continue;
            valText(&v, A, r.contract_factory) catch continue;
            valTextRequired(&v, A, r.creation_bytecode) catch continue;
            valTextRequired(&v, A, r.deployed_bytecode) catch continue;
            bd.appendSlice(A, v.items) catch continue;
            enc += 1;
        }
        starts[enc] = bd.items.len;
        for (0..enc) |k| ptrs[k] = bd.items[starts[k]..starts[k + 1]];
        wa.conn.batchSendRows(wa.prep_id, 13, ptrs[0..enc]) catch { wa.had_error.* = true; return; };
        i = end;
    }
}

fn cbaWorker(wa: CBAWA) void {
    var v  = workerBuf(EST_CBA_ROW);              defer v.deinit(A);
    var bd = workerBuf(BATCH_SIZE * EST_CBA_ROW); defer bd.deinit(A);
    var starts: [BATCH_SIZE + 1]usize = undefined;
    var ptrs:   [BATCH_SIZE][]const u8 = undefined;
    var i: usize = 0;
    while (i < wa.rows.len) {
        const end = @min(i + BATCH_SIZE, wa.rows.len);
        bd.items.len = 0; var enc: usize = 0;
        for (i..end) |j| {
            v.items.len = 0; starts[enc] = bd.items.len;
            const r = wa.rows[j];
            valTextRequired(&v, A, r.address) catch continue;
            valTextRequired(&v, A, r.creator) catch continue;
            valTextRequired(&v, A, r.tx_hash) catch continue;
            valBigint(&v, A, r.block_number) catch continue;
            valBigint(&v, A, r.timestamp) catch continue;
            valText(&v, A, r.contract_factory) catch continue;
            valTextRequired(&v, A, r.creation_bytecode) catch continue;
            valTextRequired(&v, A, r.deployed_bytecode) catch continue;
            bd.appendSlice(A, v.items) catch continue;
            enc += 1;
        }
        starts[enc] = bd.items.len;
        for (0..enc) |k| ptrs[k] = bd.items[starts[k]..starts[k + 1]];
        wa.conn.batchSendRows(wa.prep_id, 8, ptrs[0..enc]) catch { wa.had_error.* = true; return; };
        i = end;
    }
}

// ─── Spawn helper ─────────────────────────────────────────────────────────────

// Split rows across connections, spawn one worker thread per connection.
fn spawnTable(
    comptime WA: type,
    comptime workerFn: fn(WA) void,
    conns: []*CqlConn,
    prep_id: []const u8,
    rows: anytype,
    threads: []std.Thread,
    spawned: *usize,
    had_error: *bool,
) void {
    if (rows.len == 0 or conns.len == 0) return;
    const n = @min(conns.len, rows.len);
    const per = rows.len / n;
    for (0..n) |w| {
        const s = w * per;
        const e = if (w == n - 1) rows.len else s + per;
        const wa = WA{ .conn = conns[w], .prep_id = prep_id, .rows = rows[s..e], .had_error = had_error };
        if (std.Thread.spawn(.{}, workerFn, .{wa})) |t| {
            threads[spawned.*] = t;
            spawned.* += 1;
        } else |_| {
            workerFn(wa);
        }
    }
}

// ─── saveBatch ───────────────────────────────────────────────────────────────

pub const SaveArgs = struct {
    pool: *CqlPool,
    gpa: std.mem.Allocator,
    prep_ids: *PreparedIds,
    ent: *transform.Entities,
    result_ms: *f64,
};

pub fn saveBatch(args: SaveArgs) !void {
    const t0 = nowNs();
    const c  = args.pool.conns;
    const p  = args.prep_ids;
    const e  = args.ent;

    var had_error: bool = false;
    var threads: [6 * POOL_SIZE]std.Thread = undefined;
    var spawned: usize = 0;

    spawnTable(BlocksWA, blocksWorker, c[SPLIT_OFF[0]..SPLIT_OFF[1]], p.blocks,            e.blocks.items,            &threads, &spawned, &had_error);
    spawnTable(TxsWA,    txsWorker,    c[SPLIT_OFF[1]..SPLIT_OFF[2]], p.transactions,      e.txs.items,               &threads, &spawned, &had_error);
    spawnTable(LogsWA,   logsWorker,   c[SPLIT_OFF[2]..SPLIT_OFF[3]], p.logs,              e.logs.items,              &threads, &spawned, &had_error);
    spawnTable(ITxsWA,   itxsWorker,   c[SPLIT_OFF[3]..SPLIT_OFF[4]], p.internal_txs,      e.internal_txs.items,      &threads, &spawned, &had_error);
    spawnTable(ContWA,   contWorker,   c[SPLIT_OFF[4]..SPLIT_OFF[5]], p.contracts,         e.contracts.items,         &threads, &spawned, &had_error);
    spawnTable(CBAWA,    cbaWorker,    c[SPLIT_OFF[5]..SPLIT_OFF[6]], p.contracts_by_addr, e.contracts_by_addr.items, &threads, &spawned, &had_error);

    for (threads[0..spawned]) |t| t.join();

    args.result_ms.* = @as(f64, @floatFromInt(nowNs() - t0)) / 1e6;

    if (had_error) return error.SaveFailed;
}

// ─── Dump mode: write encoded CQL rows to a binary file ──────────────────────
// Format: [magic:5][ver:1] then rows: [table_id:1][nvals:2][len:4][values:...]
// Batch boundary: [0xFF][0x00][batch_id:4]
// Table IDs: 0=blocks 1=txs 2=logs 3=itxs 4=contracts 5=cba

const DUMP_MAGIC = "ZIG2D\x01";

pub fn dumpOpen(io: std.Io, path: []const u8) !std.Io.File {
    const f = try std.Io.Dir.createFileAbsolute(io, path, .{});
    try f.writeStreamingAll(io, DUMP_MAGIC);
    return f;
}

fn dumpRows(
    comptime encFn: fn (*std.ArrayList(u8), transform.Entities, usize) void,
    file: std.Io.File, io: std.Io,
    table_id: u8, n_values: u16,
    ent: *transform.Entities, count: usize,
) !void {
    var v: std.ArrayList(u8) = .empty;
    defer v.deinit(A);
    for (0..count) |i| {
        v.items.len = 0;
        encFn(&v, ent.*, i);
        var hdr: [7]u8 = undefined;
        hdr[0] = table_id;
        std.mem.writeInt(u16, hdr[1..3], n_values, .big);
        std.mem.writeInt(u32, hdr[3..7], @intCast(v.items.len), .big);
        try file.writeStreamingAll(io, &hdr);
        try file.writeStreamingAll(io, v.items);
    }
}

// Encode helpers that fill v from entities[i]
fn encBlock(v: *std.ArrayList(u8), e: transform.Entities, i: usize) void {
    const r = e.blocks.items[i];
    valInt32(v, A, r.chunk) catch return;
    valBigint(v, A, r.number) catch return;
    valBigint(v, A, r.timestamp_s) catch return;
    valBigint(v, A, r.timestamp_ms) catch return;
    valTextRequired(v, A, r.miner) catch return;
}
fn encTx(v: *std.ArrayList(u8), e: transform.Entities, i: usize) void {
    const r = e.txs.items[i];
    valInt32(v, A, r.chunk) catch return;
    valBigint(v, A, r.block_number) catch return;
    valInt32(v, A, r.transaction_index) catch return;
    valTextRequired(v, A, r.hash) catch return;
    valBigint(v, A, r.block_timestamp_s) catch return;
    valBigint(v, A, r.block_timestamp_ms) catch return;
    valText(v, A, r.method_id) catch return;
    valText(v, A, r.input) catch return;
    valTextRequired(v, A, r.from_address) catch return;
    valText(v, A, r.to_address) catch return;
    valVarint(v, A, r.value) catch return;
    valBigint(v, A, r.gas_limit) catch return;
    valBigint(v, A, r.gas_price) catch return;
    valBigint(v, A, r.gas_used) catch return;
    valBigint(v, A, r.max_priority_fee) catch return;
    valBigint(v, A, r.max_fee) catch return;
    valBigint(v, A, r.cumulative_gas_used) catch return;
    valBigint(v, A, r.effective_gas_price) catch return;
    valText(v, A, r.contract_address) catch return;
    valTinyint(v, A, r.status) catch return;
    valTinyint(v, A, r.tx_type) catch return;
}
fn encLog(v: *std.ArrayList(u8), e: transform.Entities, i: usize) void {
    const r = e.logs.items[i];
    valInt32(v, A, r.chunk) catch return;
    valBigint(v, A, r.block_number) catch return;
    valInt32(v, A, r.transaction_index) catch return;
    valInt32(v, A, r.log_index) catch return;
    valBigint(v, A, r.block_timestamp_s) catch return;
    valBigint(v, A, r.block_timestamp_ms) catch return;
    valTextRequired(v, A, r.address) catch return;
    valTextRequired(v, A, r.data) catch return;
    valText(v, A, r.topic_zeroth) catch return;
    valText(v, A, r.topic_first) catch return;
    valText(v, A, r.topic_second) catch return;
    valText(v, A, r.topic_third) catch return;
    valListText(v, A, r.rest_topics) catch return;
    valTextRequired(v, A, r.transaction_hash) catch return;
    valBool(v, A, r.removed) catch return;
}
fn encItx(v: *std.ArrayList(u8), e: transform.Entities, i: usize) void {
    const r = e.internal_txs.items[i];
    valInt32(v, A, r.chunk) catch return;
    valBigint(v, A, r.block_number) catch return;
    valBigint(v, A, r.block_timestamp_s) catch return;
    valBigint(v, A, r.block_timestamp_ms) catch return;
    valInt32(v, A, r.transaction_index) catch return;
    valTextRequired(v, A, r.transaction_hash) catch return;
    valInt32(v, A, r.trace_index) catch return;
    valTextRequired(v, A, r.from_address) catch return;
    valTextRequired(v, A, r.to_address) catch return;
    valVarint(v, A, r.value) catch return;
}
fn encContract(v: *std.ArrayList(u8), e: transform.Entities, i: usize) void {
    const r = e.contracts.items[i];
    valInt32(v, A, r.chunk) catch return;
    valBigint(v, A, r.block_number) catch return;
    valInt32(v, A, r.transaction_index) catch return;
    valTextRequired(v, A, r.transaction_hash) catch return;
    valInt32(v, A, r.trace_index) catch return;
    valBigint(v, A, r.block_timestamp_s) catch return;
    valBigint(v, A, r.block_timestamp_ms) catch return;
    valTextRequired(v, A, r.address) catch return;
    valTinyint(v, A, r.creation_method) catch return;
    valTextRequired(v, A, r.creator_address) catch return;
    valText(v, A, r.contract_factory) catch return;
    valTextRequired(v, A, r.creation_bytecode) catch return;
    valTextRequired(v, A, r.deployed_bytecode) catch return;
}
fn encCba(v: *std.ArrayList(u8), e: transform.Entities, i: usize) void {
    const r = e.contracts_by_addr.items[i];
    valTextRequired(v, A, r.address) catch return;
    valTextRequired(v, A, r.creator) catch return;
    valTextRequired(v, A, r.tx_hash) catch return;
    valBigint(v, A, r.block_number) catch return;
    valBigint(v, A, r.timestamp) catch return;
    valText(v, A, r.contract_factory) catch return;
    valTextRequired(v, A, r.creation_bytecode) catch return;
    valTextRequired(v, A, r.deployed_bytecode) catch return;
}

pub const DumpArgs = struct {
    file: std.Io.File,
    io: std.Io,
    batch_id: u32,
    ent: *transform.Entities,
    result_ms: *f64,
};

pub fn dumpBatch(args: DumpArgs) void {
    const t0 = nowNs();
    const e = args.ent;
    const file = args.file;
    const io = args.io;

    // Batch header: [0xFF][0x00][batch_id:4BE][total_rows:4BE] = 10 bytes
    var batch_hdr: [10]u8 = undefined;
    batch_hdr[0] = 0xFF;
    batch_hdr[1] = 0x00;
    std.mem.writeInt(u32, batch_hdr[2..6], args.batch_id, .big);
    const total: u32 = @intCast(e.blocks.items.len + e.txs.items.len + e.logs.items.len +
        e.internal_txs.items.len + e.contracts.items.len + e.contracts_by_addr.items.len);
    std.mem.writeInt(u32, batch_hdr[6..10], total, .big);
    file.writeStreamingAll(io, &batch_hdr) catch {};

    dumpRows(encBlock,    file, io, 0,  5, e, e.blocks.items.len) catch {};
    dumpRows(encTx,       file, io, 1, 21, e, e.txs.items.len) catch {};
    dumpRows(encLog,      file, io, 2, 15, e, e.logs.items.len) catch {};
    dumpRows(encItx,      file, io, 3, 10, e, e.internal_txs.items.len) catch {};
    dumpRows(encContract, file, io, 4, 13, e, e.contracts.items.len) catch {};
    dumpRows(encCba,      file, io, 5,  8, e, e.contracts_by_addr.items.len) catch {};

    args.result_ms.* = @as(f64, @floatFromInt(nowNs() - t0)) / 1e6;
}

// ─── Redis RESP client (raw Linux TCP) ───────────────────────────────────────

pub const RedisConn = struct {
    fd: i32,
    gpa: std.mem.Allocator,

    pub fn init(io: std.Io, gpa: std.mem.Allocator, host: []const u8, port: u16) !RedisConn {
        _ = io;
        const fd = try tcpConnectRaw(host, port);
        return RedisConn{ .fd = fd, .gpa = gpa };
    }

    pub fn deinit(self: *RedisConn) void {
        _ = linux.close(self.fd);
    }

    fn sendCmd(self: *RedisConn, parts: []const []const u8) !void {
        var cmd: std.ArrayList(u8) = .empty;
        defer cmd.deinit(self.gpa);

        try cmd.appendSlice(self.gpa, try std.fmt.allocPrint(self.gpa, "*{d}\r\n", .{parts.len}));
        for (parts) |p| {
            try cmd.appendSlice(self.gpa, try std.fmt.allocPrint(self.gpa, "${d}\r\n", .{p.len}));
            try cmd.appendSlice(self.gpa, p);
            try cmd.appendSlice(self.gpa, "\r\n");
        }
        try tcpWrite(self.fd, cmd.items);
    }

    // Read RESP line (\r\n terminated)
    fn readLine(self: *RedisConn) ![]u8 {
        var line: std.ArrayList(u8) = .empty;
        errdefer line.deinit(self.gpa);
        var b: [1]u8 = undefined;
        while (true) {
            try tcpReadExact(self.fd, &b);
            if (b[0] == '\r') {
                var n: [1]u8 = undefined;
                try tcpReadExact(self.fd, &n);
                if (n[0] == '\n') break;
                try line.append(self.gpa, '\r');
                try line.append(self.gpa, n[0]);
            } else {
                try line.append(self.gpa, b[0]);
            }
        }
        return try line.toOwnedSlice(self.gpa);
    }

    pub fn get(self: *RedisConn, key: []const u8) !?[]u8 {
        try self.sendCmd(&.{"GET", key});
        const first_line = try self.readLine();
        defer self.gpa.free(first_line);

        if (first_line.len == 0) return null;
        if (first_line[0] == '$') {
            const n = std.fmt.parseInt(i32, first_line[1..], 10) catch return null;
            if (n < 0) return null;
            const data = try self.gpa.alloc(u8, @intCast(n));
            errdefer self.gpa.free(data);
            try tcpReadExact(self.fd, data);
            var crlf: [2]u8 = undefined;
            try tcpReadExact(self.fd, &crlf);
            return data;
        }
        if (first_line[0] == '+') {
            return try self.gpa.dupe(u8, first_line[1..]);
        }
        return null;
    }

    pub fn setStr(self: *RedisConn, key: []const u8, value: []const u8) !void {
        try self.sendCmd(&.{"SET", key, value});
        const line = try self.readLine();
        self.gpa.free(line);
    }

    pub fn auth(self: *RedisConn, password: []const u8) !void {
        try self.sendCmd(&.{"AUTH", password});
        const line = try self.readLine();
        self.gpa.free(line);
    }

    pub fn selectDb(self: *RedisConn, db: u8) !void {
        var db_str: [4]u8 = undefined;
        const s = try std.fmt.bufPrint(&db_str, "{d}", .{db});
        try self.sendCmd(&.{"SELECT", s});
        const line = try self.readLine();
        self.gpa.free(line);
    }
};
