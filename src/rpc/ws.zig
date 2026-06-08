// WebSocket client (RFC 6455) — raw Linux TCP.
// Supports eth_subscribe newHeads for real-time block notification.
const std = @import("std");
const linux = std.os.linux;

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
    const sockFd = linux.socket(linux.AF.INET, linux.SOCK.STREAM, 0);
    if (sockFd > @as(usize, std.math.maxInt(i32))) return error.SocketFailed;
    const fd: i32 = @intCast(sockFd);
    const ip = ipv4Parts(host);
    const ipHost = (@as(u32, ip[0]) << 24) | (@as(u32, ip[1]) << 16) |
        (@as(u32, ip[2]) << 8) | @as(u32, ip[3]);
    const addr = linux.sockaddr.in{
        .family = linux.AF.INET,
        .port = std.mem.nativeToBig(u16, port),
        .addr = std.mem.nativeToBig(u32, ipHost),
        .zero = std.mem.zeroes([8]u8),
    };
    if (linux.connect(fd, @ptrCast(&addr), @sizeOf(linux.sockaddr.in)) != 0) {
        _ = linux.close(fd);
        return error.ConnectFailed;
    }
    const nd: c_int = 1;
    _ = linux.setsockopt(fd, @as(c_int, @intCast(linux.IPPROTO.TCP)), linux.TCP.NODELAY, @ptrCast(&nd), @sizeOf(c_int));
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

const OPCODE_CONT = 0x0;
const OPCODE_TEXT = 0x1;
const OPCODE_CLOSE = 0x8;
const OPCODE_PING = 0x9;
const OPCODE_PONG = 0xA;

fn wsSend(fd: i32, payload: []const u8) !void {
    var hdr: [14]u8 = undefined;
    var hdrLen: usize = 0;

    hdr[0] = 0x80 | OPCODE_TEXT;
    const maskBit: u8 = 0x80;

    if (payload.len < 126) {
        hdr[1] = maskBit | @as(u8, @intCast(payload.len));
        hdrLen = 2;
    } else if (payload.len <= 0xFFFF) {
        hdr[1] = maskBit | 126;
        hdr[2] = @intCast((payload.len >> 8) & 0xFF);
        hdr[3] = @intCast(payload.len & 0xFF);
        hdrLen = 4;
    } else {
        hdr[1] = maskBit | 127;
        var len = payload.len;
        var i: usize = 9;
        while (i >= 2) : (i -= 1) {
            hdr[i] = @intCast(len & 0xFF);
            len >>= 8;
        }
        hdrLen = 10;
    }
    const maskKey = [4]u8{ 0x12, 0x34, 0x56, 0x78 };
    @memcpy(hdr[hdrLen .. hdrLen + 4], &maskKey);
    hdrLen += 4;

    try fdWriteAll(fd, hdr[0..hdrLen]);

    var chunkBuf: [4096]u8 = undefined;
    var off: usize = 0;
    while (off < payload.len) {
        const end = @min(off + chunkBuf.len, payload.len);
        for (payload[off..end], 0..) |b, i| {
            chunkBuf[i] = b ^ maskKey[(off + i) % 4];
        }
        try fdWriteAll(fd, chunkBuf[0 .. end - off]);
        off = end;
    }
}

fn wsRecv(fd: i32, gpa: std.mem.Allocator) !struct { opcode: u8, fin: bool, data: []u8 } {
    var hdr2: [2]u8 = undefined;
    try fdReadExact(fd, &hdr2);

    const fin: bool = (hdr2[0] & 0x80) != 0;
    const opcode: u8 = hdr2[0] & 0x0F;
    const masked: bool = (hdr2[1] & 0x80) != 0;
    var payloadLen: usize = hdr2[1] & 0x7F;

    if (payloadLen == 126) {
        var ext: [2]u8 = undefined;
        try fdReadExact(fd, &ext);
        payloadLen = (@as(usize, ext[0]) << 8) | ext[1];
    } else if (payloadLen == 127) {
        var ext: [8]u8 = undefined;
        try fdReadExact(fd, &ext);
        payloadLen = 0;
        for (ext) |b| {
            payloadLen = (payloadLen << 8) | b;
        }
    }

    var maskKey: [4]u8 = .{ 0, 0, 0, 0 };
    if (masked) try fdReadExact(fd, &maskKey);

    const data = try gpa.alloc(u8, payloadLen);
    errdefer gpa.free(data);

    var off: usize = 0;
    var chunkBuf: [4096]u8 = undefined;
    while (off < payloadLen) {
        const chunkEnd = @min(off + chunkBuf.len, payloadLen);
        try fdReadExact(fd, chunkBuf[0 .. chunkEnd - off]);
        if (masked) {
            for (chunkBuf[0 .. chunkEnd - off], 0..) |b, i| {
                data[off + i] = b ^ maskKey[(off + i) % 4];
            }
        } else {
            @memcpy(data[off..chunkEnd], chunkBuf[0 .. chunkEnd - off]);
        }
        off = chunkEnd;
    }

    return .{ .opcode = opcode, .fin = fin, .data = data };
}

pub const Conn = struct {
    fd: i32,
    host: []const u8,
    subId: []u8 = &.{},
    gpa: std.mem.Allocator,

    pub fn init(gpa: std.mem.Allocator, host: []const u8, port: u16, path: []const u8) !Conn {
        const fd = try tcpConn(host, port);

        const req = try std.fmt.allocPrint(
            gpa,
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

        var hdrBuf: [2048]u8 = undefined;
        var hdrLen: usize = 0;
        while (hdrLen < hdrBuf.len) {
            const n = linux.read(fd, hdrBuf[hdrLen..].ptr, hdrBuf.len - hdrLen);
            if (n == 0 or n > hdrBuf.len) return error.UpgradeFailed;
            hdrLen += n;
            if (std.mem.indexOf(u8, hdrBuf[0..hdrLen], "\r\n\r\n") != null) break;
        }
        if (std.mem.indexOf(u8, hdrBuf[0..hdrLen], "101") == null)
            return error.UpgradeFailed;

        return Conn{ .fd = fd, .host = host, .gpa = gpa };
    }

    pub fn deinit(self: *Conn) void {
        if (self.subId.len > 0) self.gpa.free(self.subId);
        _ = linux.close(self.fd);
    }

    pub fn subscribeNewHeads(self: *Conn) ![]const u8 {
        const msg = "{\"id\":1,\"jsonrpc\":\"2.0\",\"method\":\"eth_subscribe\",\"params\":[\"newHeads\"]}";
        try wsSend(self.fd, msg);

        const frame = try wsRecv(self.fd, self.gpa);
        defer self.gpa.free(frame.data);

        const subId = extractJsonStr(frame.data, "result") orelse return error.NoSubId;
        if (self.subId.len > 0) self.gpa.free(self.subId);
        self.subId = try self.gpa.dupe(u8, subId);
        return self.subId;
    }

    /// Blocks until the next newHeads notification; returns block number.
    pub fn nextBlockNum(self: *Conn) !u64 {
        var frag: std.ArrayList(u8) = .empty;
        defer frag.deinit(self.gpa);

        while (true) {
            const frame = try wsRecv(self.fd, self.gpa);
            defer self.gpa.free(frame.data);

            switch (frame.opcode) {
                OPCODE_PING => {
                    var pong = [_]u8{ 0x80 | OPCODE_PONG, 0x80, 0, 0, 0, 0 };
                    fdWriteAll(self.fd, &pong) catch {};
                    continue;
                },
                OPCODE_CLOSE => return error.WsClosed,
                OPCODE_CONT => {
                    try frag.appendSlice(self.gpa, frame.data);
                    if (!frame.fin) continue;
                    const numStr = extractJsonStr(frag.items, "number") orelse {
                        frag.clearRetainingCapacity();
                        continue;
                    };
                    frag.clearRetainingCapacity();
                    return parseHexBlockNum(numStr) orelse continue;
                },
                OPCODE_TEXT => {
                    if (frame.fin) {
                        const numStr = extractJsonStr(frame.data, "number") orelse continue;
                        return parseHexBlockNum(numStr) orelse continue;
                    }
                    frag.clearRetainingCapacity();
                    try frag.appendSlice(self.gpa, frame.data);
                },
                else => continue,
            }
        }
    }
};

pub const ParsedUrl = struct {
    host: []const u8,
    port: u16,
    path: []const u8,
};

pub fn parseUrl(url: []const u8) ParsedUrl {
    var s = url;
    if (std.mem.startsWith(u8, s, "ws://")) s = s[5..];
    if (std.mem.startsWith(u8, s, "wss://")) s = s[6..];
    const slash = std.mem.indexOfScalar(u8, s, '/') orelse s.len;
    const path = if (slash < s.len) s[slash..] else "/";
    const hostPort = s[0..slash];
    if (std.mem.lastIndexOfScalar(u8, hostPort, ':')) |c| {
        return .{
            .host = hostPort[0..c],
            .port = std.fmt.parseInt(u16, hostPort[c + 1 ..], 10) catch 8546,
            .path = path,
        };
    }
    return .{ .host = hostPort, .port = 8546, .path = path };
}

fn parseHexBlockNum(numStr: []const u8) ?u64 {
    const hex = if (std.mem.startsWith(u8, numStr, "0x")) numStr[2..] else numStr;
    return std.fmt.parseInt(u64, hex, 16) catch null;
}

fn extractJsonStr(data: []const u8, key: []const u8) ?[]const u8 {
    var searchBuf: [64]u8 = undefined;
    const search = std.fmt.bufPrint(&searchBuf, "\"{s}\":\"", .{key}) catch return null;
    const start = std.mem.indexOf(u8, data, search) orelse return null;
    const after = data[start + search.len ..];
    const end = std.mem.indexOfScalar(u8, after, '"') orelse return null;
    return after[0..end];
}
