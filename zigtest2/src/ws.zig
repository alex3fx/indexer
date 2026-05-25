// Minimal WebSocket client (RFC 6455) — raw Linux TCP, no dependencies.
// Only what's needed: connect, upgrade, send text frame, receive text frame.
// Used for eth_subscribe newHeads: gonode → push block number → processBlock.
const std = @import("std");
const linux = std.os.linux;

// ─── TCP helpers (same as rpc.zig, duplicated to avoid cross-module deps) ────

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

fn tcpConn(host: []const u8, port: u16) !i32 {
    const sock_fd = linux.socket(linux.AF.INET, linux.SOCK.STREAM, 0);
    if (sock_fd > @as(usize, std.math.maxInt(i32))) return error.SocketFailed;
    const fd: i32 = @intCast(sock_fd);
    const ip = ipv4Parts(host);
    const ip_host = (@as(u32, ip[0]) << 24) | (@as(u32, ip[1]) << 16) |
                    (@as(u32, ip[2]) << 8)  |  @as(u32, ip[3]);
    const addr = linux.sockaddr.in{
        .family = linux.AF.INET,
        .port   = std.mem.nativeToBig(u16, port),
        .addr   = std.mem.nativeToBig(u32, ip_host),
        .zero   = std.mem.zeroes([8]u8),
    };
    if (linux.connect(fd, @ptrCast(&addr), @sizeOf(linux.sockaddr.in)) != 0) {
        _ = linux.close(fd);
        return error.ConnectFailed;
    }
    const nd: c_int = 1;
    _ = linux.setsockopt(fd, @as(c_int, @intCast(linux.IPPROTO.TCP)),
                         linux.TCP.NODELAY, @ptrCast(&nd), @sizeOf(c_int));
    return fd;
}

fn fdWriteAll(fd: i32, data: []const u8) !void {
    var w: usize = 0;
    while (w < data.len) {
        const n = linux.write(fd, data[w..].ptr, data.len - w);
        if (n == 0 or n > data.len) return error.WriteError;
        w += n;
    }
}

fn fdReadExact(fd: i32, buf: []u8) !void {
    var r: usize = 0;
    while (r < buf.len) {
        const n = linux.read(fd, buf[r..].ptr, buf.len - r);
        if (n == 0) return error.Eof;
        if (n > buf.len) return error.ReadError;
        r += n;
    }
}

// ─── WebSocket frame I/O ─────────────────────────────────────────────────────

const WS_OPCODE_TEXT  = 0x1;
const WS_OPCODE_CLOSE = 0x8;
const WS_OPCODE_PING  = 0x9;
const WS_OPCODE_PONG  = 0xA;

// Sends a single text frame (client→server, masked with a fixed key).
fn wsSend(fd: i32, payload: []const u8) !void {
    // Max header = 2 + 8 + 4 = 14 bytes.
    var hdr: [14]u8 = undefined;
    var hdr_len: usize = 0;

    hdr[0] = 0x80 | WS_OPCODE_TEXT; // FIN + text
    const mask_bit: u8 = 0x80;      // client must mask

    if (payload.len < 126) {
        hdr[1] = mask_bit | @as(u8, @intCast(payload.len));
        hdr_len = 2;
    } else if (payload.len <= 0xFFFF) {
        hdr[1] = mask_bit | 126;
        hdr[2] = @intCast((payload.len >> 8) & 0xFF);
        hdr[3] = @intCast(payload.len & 0xFF);
        hdr_len = 4;
    } else {
        hdr[1] = mask_bit | 127;
        var len = payload.len;
        var i: usize = 9;
        while (i >= 2) : (i -= 1) { hdr[i] = @intCast(len & 0xFF); len >>= 8; }
        hdr_len = 10;
    }
    // Masking key — all zeros → XOR with 0 → no-op, but RFC requires it
    const mask_key = [4]u8{ 0x12, 0x34, 0x56, 0x78 };
    @memcpy(hdr[hdr_len..hdr_len + 4], &mask_key);
    hdr_len += 4;

    try fdWriteAll(fd, hdr[0..hdr_len]);

    // Send masked payload in chunks to avoid large stack allocation
    var chunk_buf: [4096]u8 = undefined;
    var off: usize = 0;
    while (off < payload.len) {
        const end = @min(off + chunk_buf.len, payload.len);
        for (payload[off..end], 0..) |b, i| {
            chunk_buf[i] = b ^ mask_key[(off + i) % 4];
        }
        try fdWriteAll(fd, chunk_buf[0..end - off]);
        off = end;
    }
}

// Reads one WS frame (server→client, no mask). Returns allocated payload slice.
// Caller must free with gpa.free().
fn wsRecv(fd: i32, gpa: std.mem.Allocator) !struct { opcode: u8, data: []u8 } {
    var hdr2: [2]u8 = undefined;
    try fdReadExact(fd, &hdr2);

    const opcode: u8  = hdr2[0] & 0x0F;
    const masked: bool = (hdr2[1] & 0x80) != 0;
    var payload_len: usize = hdr2[1] & 0x7F;

    if (payload_len == 126) {
        var ext: [2]u8 = undefined;
        try fdReadExact(fd, &ext);
        payload_len = (@as(usize, ext[0]) << 8) | ext[1];
    } else if (payload_len == 127) {
        var ext: [8]u8 = undefined;
        try fdReadExact(fd, &ext);
        payload_len = 0;
        for (ext) |b| { payload_len = (payload_len << 8) | b; }
    }

    var mask_key: [4]u8 = .{ 0, 0, 0, 0 };
    if (masked) try fdReadExact(fd, &mask_key);

    const data = try gpa.alloc(u8, payload_len);
    errdefer gpa.free(data);

    // Read in chunks
    var off: usize = 0;
    var chunk_buf: [4096]u8 = undefined;
    while (off < payload_len) {
        const chunk_end = @min(off + chunk_buf.len, payload_len);
        try fdReadExact(fd, chunk_buf[0..chunk_end - off]);
        if (masked) {
            for (chunk_buf[0..chunk_end - off], 0..) |b, i| {
                data[off + i] = b ^ mask_key[(off + i) % 4];
            }
        } else {
            @memcpy(data[off..chunk_end], chunk_buf[0..chunk_end - off]);
        }
        off = chunk_end;
    }

    return .{ .opcode = opcode, .data = data };
}

// ─── WsConn ───────────────────────────────────────────────────────────────────

pub const WsConn = struct {
    fd:      i32,
    host:    []const u8,
    sub_id:  []u8 = &.{},
    gpa:     std.mem.Allocator,

    /// Connect and upgrade to WebSocket. path must start with '/'.
    pub fn init(gpa: std.mem.Allocator, host: []const u8, port: u16, path: []const u8) !WsConn {
        const fd = try tcpConn(host, port);

        // Send HTTP Upgrade
        const req = try std.fmt.allocPrint(gpa,
            "GET {s} HTTP/1.1\r\n" ++
            "Host: {s}:{d}\r\n" ++
            "Upgrade: websocket\r\n" ++
            "Connection: Upgrade\r\n" ++
            "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n" ++
            "Sec-WebSocket-Version: 13\r\n\r\n",
            .{ path, host, port },
        );
        defer gpa.free(req);
        try fdWriteAll(fd, req);

        // Read until \r\n\r\n and check for 101
        var hdr_buf: [2048]u8 = undefined;
        var hdr_len: usize = 0;
        while (hdr_len < hdr_buf.len) {
            const n = linux.read(fd, hdr_buf[hdr_len..].ptr, hdr_buf.len - hdr_len);
            if (n == 0 or n > hdr_buf.len) return error.UpgradeFailed;
            hdr_len += n;
            if (std.mem.indexOf(u8, hdr_buf[0..hdr_len], "\r\n\r\n") != null) break;
        }
        if (std.mem.indexOf(u8, hdr_buf[0..hdr_len], "101") == null)
            return error.UpgradeFailed;

        return WsConn{ .fd = fd, .host = host, .gpa = gpa };
    }

    pub fn deinit(self: *WsConn) void {
        if (self.sub_id.len > 0) self.gpa.free(self.sub_id);
        _ = linux.close(self.fd);
    }

    /// Send eth_subscribe newHeads, store and return subscription ID.
    pub fn subscribeNewHeads(self: *WsConn) ![]const u8 {
        const msg = "{\"id\":1,\"jsonrpc\":\"2.0\",\"method\":\"eth_subscribe\",\"params\":[\"newHeads\"]}";
        try wsSend(self.fd, msg);

        // Read subscription response
        const frame = try wsRecv(self.fd, self.gpa);
        defer self.gpa.free(frame.data);

        // Parse result: {"id":1,"jsonrpc":"2.0","result":"0x1"}
        const sub_id = extractJsonStr(frame.data, "result") orelse return error.NoSubId;
        if (self.sub_id.len > 0) self.gpa.free(self.sub_id);
        self.sub_id = try self.gpa.dupe(u8, sub_id);
        return self.sub_id;
    }

    /// Block until next eth_subscription event; return block number.
    /// Skips ping/pong frames and sends pong on ping.
    pub fn nextBlockNum(self: *WsConn) !u64 {
        while (true) {
            const frame = try wsRecv(self.fd, self.gpa);
            defer self.gpa.free(frame.data);

            switch (frame.opcode) {
                WS_OPCODE_PING => {
                    // Send pong
                    var hdr = [_]u8{ 0x80 | WS_OPCODE_PONG, 0x80, 0, 0, 0, 0 };
                    fdWriteAll(self.fd, &hdr) catch {};
                    continue;
                },
                WS_OPCODE_CLOSE => return error.WsClosed,
                WS_OPCODE_TEXT => {}, // fall through
                else => continue,
            }

            // Parse: {"jsonrpc":"2.0","method":"eth_subscription","params":{"result":{"number":"0x..."}}}
            // Locate "number" → hex string → parse
            const num_str = extractJsonStr(frame.data, "number") orelse continue;
            const hex = if (std.mem.startsWith(u8, num_str, "0x")) num_str[2..] else num_str;
            return std.fmt.parseInt(u64, hex, 16) catch continue;
        }
    }
};

// ─── JSON string extraction (no allocator) ───────────────────────────────────
// Finds `"key":"value"` and returns a slice pointing into the input buffer.

fn extractJsonStr(data: []const u8, key: []const u8) ?[]const u8 {
    var search_buf: [64]u8 = undefined;
    const search = std.fmt.bufPrint(&search_buf, "\"{s}\":\"", .{key}) catch return null;
    const start = std.mem.indexOf(u8, data, search) orelse return null;
    const after = data[start + search.len ..];
    const end = std.mem.indexOfScalar(u8, after, '"') orelse return null;
    return after[0..end];
}
