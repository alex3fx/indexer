// Redis RESP client — raw Linux TCP, minimal subset: AUTH, SELECT, GET, SET.
// Used for cursor tracking: LATEST_PROCESSED_BLOCK_NUMBER.
const std = @import("std");
const linux = std.os.linux;

fn parseIpv4(host: []const u8) [4]u8 {
    var ip: [4]u8 = .{ 127, 0, 0, 1 };
    var iter = std.mem.splitScalar(u8, host, '.');
    var i: usize = 0;
    while (iter.next()) |p| : (i += 1) {
        if (i >= 4) break;
        ip[i] = std.fmt.parseInt(u8, p, 10) catch 0;
    }
    return ip;
}

fn tcpConnect(host: []const u8, port: u16) !i32 {
    const sockFd = linux.socket(linux.AF.INET, linux.SOCK.STREAM, 0);
    if (sockFd > @as(usize, std.math.maxInt(i32))) return error.SocketFailed;
    const fd: i32 = @intCast(sockFd);

    const ip = parseIpv4(host);
    const ipHost = (@as(u32, ip[0]) << 24) | (@as(u32, ip[1]) << 16) |
        (@as(u32, ip[2]) << 8) | @as(u32, ip[3]);
    const addr = linux.sockaddr.in{
        .family = linux.AF.INET,
        .port = std.mem.nativeToBig(u16, port),
        .addr = std.mem.nativeToBig(u32, ipHost),
        .zero = std.mem.zeroes([8]u8),
    };
    const rc = linux.connect(fd, @ptrCast(&addr), @sizeOf(linux.sockaddr.in));
    if (rc != 0) {
        _ = linux.close(fd);
        return error.ConnectFailed;
    }
    const nodelay: c_int = 1;
    _ = linux.setsockopt(fd, @as(c_int, @intCast(linux.IPPROTO.TCP)), linux.TCP.NODELAY, @ptrCast(&nodelay), @sizeOf(c_int));

    // Bounds GET/SET/SCAN/PING so a half-dead connection can't block the
    // realtime loop forever — see the matching comment in db/pool.zig.
    const tv = linux.timeval{ .sec = 10, .usec = 0 };
    _ = linux.setsockopt(fd, linux.SOL.SOCKET, linux.SO.RCVTIMEO, @ptrCast(&tv), @sizeOf(linux.timeval));
    _ = linux.setsockopt(fd, linux.SOL.SOCKET, linux.SO.SNDTIMEO, @ptrCast(&tv), @sizeOf(linux.timeval));
    return fd;
}

fn fdWrite(fd: i32, data: []const u8) !void {
    var written: usize = 0;
    while (written < data.len) {
        const n = linux.write(fd, data[written..].ptr, data.len - written);
        if (n == 0 or n > data.len) return error.WriteFailed;
        written += n;
    }
}

fn fdReadExact(fd: i32, buf: []u8) !void {
    var pos: usize = 0;
    while (pos < buf.len) {
        const n = linux.read(fd, buf[pos..].ptr, buf.len - pos);
        if (n == 0) return error.ConnectionClosed;
        if (n > buf.len) return error.ReadFailed;
        pos += n;
    }
}

fn fdReadLine(fd: i32, buf: []u8) ![]u8 {
    var pos: usize = 0;
    while (pos < buf.len) {
        var b: [1]u8 = undefined;
        try fdReadExact(fd, &b);
        if (b[0] == '\n') {
            return if (pos > 0 and buf[pos - 1] == '\r') buf[0 .. pos - 1] else buf[0..pos];
        }
        buf[pos] = b[0];
        pos += 1;
    }
    return error.LineTooLong;
}

pub const Conn = struct {
    fd: i32,
    gpa: std.mem.Allocator,

    pub fn init(gpa: std.mem.Allocator, host: []const u8, port: u16) !Conn {
        const fd = try tcpConnect(host, port);
        return Conn{ .fd = fd, .gpa = gpa };
    }

    pub fn deinit(self: *Conn) void {
        _ = linux.close(self.fd);
    }

    /// Lightweight liveness probe — used for periodic health checks in the
    /// realtime loop, distinct from a real GET/SET so it doesn't touch data.
    pub fn ping(self: *Conn) !void {
        try fdWrite(self.fd, "*1\r\n$4\r\nPING\r\n");
        var buf: [64]u8 = undefined;
        const line = try fdReadLine(self.fd, &buf);
        if (!std.mem.eql(u8, line, "+PONG")) return error.RedisPingFailed;
    }

    /// Tears down the current socket and reconnects in place — recovers from
    /// a dropped connection (e.g. Redis/Dragonfly restart) without restarting
    /// the whole indexer process. Callers holding a `*Conn` stay valid.
    pub fn reopen(self: *Conn, host: []const u8, port: u16, password: []const u8, db: u8) !void {
        _ = linux.close(self.fd);
        self.fd = -1;
        self.fd = try tcpConnect(host, port);
        if (password.len > 0) try self.auth(password);
        if (db > 0) try self.selectDb(db);
    }

    pub fn auth(self: *Conn, password: []const u8) !void {
        const cmd = try std.fmt.allocPrint(self.gpa, "*2\r\n$4\r\nAUTH\r\n${d}\r\n{s}\r\n", .{ password.len, password });
        defer self.gpa.free(cmd);
        try fdWrite(self.fd, cmd);
        var buf: [64]u8 = undefined;
        _ = try fdReadLine(self.fd, &buf);
    }

    pub fn selectDb(self: *Conn, db: u8) !void {
        const cmd = try std.fmt.allocPrint(self.gpa, "*2\r\n$6\r\nSELECT\r\n${d}\r\n{d}\r\n", .{ digitLen(db), db });
        defer self.gpa.free(cmd);
        try fdWrite(self.fd, cmd);
        var buf: [64]u8 = undefined;
        _ = try fdReadLine(self.fd, &buf);
    }

    /// Returns null if key does not exist. Caller must free result.
    pub fn get(self: *Conn, key: []const u8) !?[]u8 {
        const cmd = try std.fmt.allocPrint(self.gpa, "*2\r\n$3\r\nGET\r\n${d}\r\n{s}\r\n", .{ key.len, key });
        defer self.gpa.free(cmd);
        try fdWrite(self.fd, cmd);

        var lineBuf: [64]u8 = undefined;
        const line = try fdReadLine(self.fd, &lineBuf);
        if (line.len == 0) return error.EmptyResponse;

        switch (line[0]) {
            '$' => {
                const len = std.fmt.parseInt(i32, line[1..], 10) catch return error.ProtocolError;
                if (len < 0) return null;
                const data = try self.gpa.alloc(u8, @intCast(len));
                errdefer self.gpa.free(data);
                try fdReadExact(self.fd, data);
                var crlf: [2]u8 = undefined;
                try fdReadExact(self.fd, &crlf);
                return data;
            },
            '-' => return error.RedisError,
            else => return null,
        }
    }

    pub fn set(self: *Conn, key: []const u8, value: []const u8) !void {
        const cmd = try std.fmt.allocPrint(self.gpa, "*3\r\n$3\r\nSET\r\n${d}\r\n{s}\r\n${d}\r\n{s}\r\n", .{ key.len, key, value.len, value });
        defer self.gpa.free(cmd);
        try fdWrite(self.fd, cmd);
        var buf: [64]u8 = undefined;
        const line = try fdReadLine(self.fd, &buf);
        if (line.len > 0 and line[0] == '-') return error.RedisError;
    }

    fn readBulkString(self: *Conn) !?[]u8 {
        var lineBuf: [64]u8 = undefined;
        const line = try fdReadLine(self.fd, &lineBuf);
        if (line.len == 0 or line[0] != '$') return error.ProtocolError;
        const len = std.fmt.parseInt(i32, line[1..], 10) catch return error.ProtocolError;
        if (len < 0) return null;
        const data = try self.gpa.alloc(u8, @intCast(len));
        errdefer self.gpa.free(data);
        try fdReadExact(self.fd, data);
        var crlf: [2]u8 = undefined;
        try fdReadExact(self.fd, &crlf);
        return data;
    }

    pub const ScanResult = struct {
        cursor: []u8,
        keys: [][]u8,

        pub fn deinit(self: *ScanResult, gpa: std.mem.Allocator) void {
            gpa.free(self.cursor);
            for (self.keys) |k| gpa.free(k);
            gpa.free(self.keys);
        }
    };

    /// SCAN cursor MATCH pattern COUNT count — caller loops until the returned
    /// cursor is "0". Used only by the offline --erc20-rescan maintenance pass,
    /// never from the hot indexing path.
    pub fn scan(self: *Conn, cursorStr: []const u8, pattern: []const u8, count: u32) !ScanResult {
        var countBuf: [20]u8 = undefined;
        const countStr = std.fmt.bufPrint(&countBuf, "{d}", .{count}) catch unreachable;
        const cmd = try std.fmt.allocPrint(
            self.gpa,
            "*6\r\n$4\r\nSCAN\r\n${d}\r\n{s}\r\n$5\r\nMATCH\r\n${d}\r\n{s}\r\n$5\r\nCOUNT\r\n${d}\r\n{s}\r\n",
            .{ cursorStr.len, cursorStr, pattern.len, pattern, countStr.len, countStr },
        );
        defer self.gpa.free(cmd);
        try fdWrite(self.fd, cmd);

        var lineBuf: [64]u8 = undefined;
        const line = try fdReadLine(self.fd, &lineBuf);
        if (line.len == 0 or line[0] != '*') return error.ProtocolError;
        const nElems = std.fmt.parseInt(i32, line[1..], 10) catch return error.ProtocolError;
        if (nElems != 2) return error.ProtocolError;

        const cursorOut = (try self.readBulkString()) orelse return error.ProtocolError;
        errdefer self.gpa.free(cursorOut);

        var arrLineBuf: [64]u8 = undefined;
        const arrLine = try fdReadLine(self.fd, &arrLineBuf);
        if (arrLine.len == 0 or arrLine[0] != '*') return error.ProtocolError;
        const nKeys = std.fmt.parseInt(i32, arrLine[1..], 10) catch return error.ProtocolError;
        if (nKeys <= 0) return .{ .cursor = cursorOut, .keys = &.{} };

        const keys = try self.gpa.alloc([]u8, @intCast(nKeys));
        var got: usize = 0;
        errdefer {
            for (keys[0..got]) |k| self.gpa.free(k);
            self.gpa.free(keys);
        }
        for (0..@intCast(nKeys)) |i| {
            keys[i] = (try self.readBulkString()) orelse return error.ProtocolError;
            got += 1;
        }
        return .{ .cursor = cursorOut, .keys = keys };
    }

    /// SET with exponential-backoff retry (up to 5 attempts).
    pub fn setWithRetry(self: *Conn, key: []const u8, value: []const u8) !void {
        var attempt: usize = 0;
        while (attempt < 5) : (attempt += 1) {
            self.set(key, value) catch |err| {
                if (attempt == 4) return err;
                const ms: u64 = @as(u64, 50) << @intCast(attempt);
                const ts = linux.timespec{
                    .sec = @intCast(ms / 1000),
                    .nsec = @intCast((ms % 1000) * 1_000_000),
                };
                _ = linux.nanosleep(&ts, null);
                continue;
            };
            return;
        }
    }
};

/// Parse connection URL: redis://:pass@host:port/db
pub const ParsedUrl = struct {
    host: []const u8,
    port: u16,
    password: []const u8,
    db: u8,
};

pub fn parseUrl(url: []const u8) ParsedUrl {
    var result = ParsedUrl{ .host = "127.0.0.1", .port = 6379, .password = "", .db = 0 };
    if (!std.mem.startsWith(u8, url, "redis://")) return result;

    var rest = url[8..];
    if (std.mem.startsWith(u8, rest, ":")) {
        if (std.mem.indexOfScalar(u8, rest, '@')) |at| {
            result.password = rest[1..at];
            rest = rest[at + 1 ..];
        }
    }
    if (std.mem.indexOfScalar(u8, rest, '/')) |slash| {
        result.db = std.fmt.parseInt(u8, rest[slash + 1 ..], 10) catch 0;
        rest = rest[0..slash];
    }
    if (std.mem.lastIndexOfScalar(u8, rest, ':')) |colon| {
        result.host = rest[0..colon];
        result.port = std.fmt.parseInt(u16, rest[colon + 1 ..], 10) catch 6379;
    } else {
        result.host = rest;
    }
    return result;
}

fn digitLen(n: u8) usize {
    if (n == 0) return 1;
    var v = n;
    var d: usize = 0;
    while (v > 0) : (v /= 10) d += 1;
    return d;
}
