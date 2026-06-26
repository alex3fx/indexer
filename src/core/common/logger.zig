const std = @import("std");
const linux = std.os.linux;
const Allocator = std.mem.Allocator;

const core = @import("indexer/core");
const RuntimeFlags = core.RuntimeFlags;

// ─── Date/time ────────────────────────────────────────────────────────────────

const DateTime = struct {
    year: u16,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
    ms: u16,
};

// Howard Hinnant's civil_from_days algorithm.
fn civilFromDays(days: i64) struct { year: i32, month: u8, day: u8 } {
    const z = days + 719468;
    const era: i64 = @divFloor(z, 146097);
    const doe: u32 = @intCast(z - era * 146097);
    const yoe: u32 = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    const y: i32 = @as(i32, @intCast(yoe)) + @as(i32, @intCast(era)) * 400;
    const doy: u32 = doe - (365 * yoe + yoe / 4 - yoe / 100);
    const mp: u32 = (5 * doy + 2) / 153;
    const d: u8 = @intCast(doy - (153 * mp + 2) / 5 + 1);
    const m: u8 = if (mp < 10) @intCast(mp + 3) else @intCast(mp - 9);
    return .{ .year = if (m <= 2) y + 1 else y, .month = m, .day = d };
}

pub fn millisToDateTime(ms: i64, tz_offset_h: i8) DateTime {
    const adjusted = ms + @as(i64, tz_offset_h) * 3_600_000;
    const s = @divFloor(adjusted, 1000);
    const rem_ms: u16 = @intCast(@mod(adjusted, 1000));
    const days = @divFloor(s, 86400);
    const sod: u64 = @intCast(@mod(s, 86400));

    const civil = civilFromDays(days);
    return .{
        .year = @intCast(civil.year),
        .month = civil.month,
        .day = civil.day,
        .hour = @intCast(sod / 3600),
        .minute = @intCast(sod % 3600 / 60),
        .second = @intCast(sod % 60),
        .ms = rem_ms,
    };
}

pub fn formatTimestamp(buf: []u8, ms: i64, tz_offset_h: i8) []u8 {
    const dt = millisToDateTime(ms, tz_offset_h);
    if (tz_offset_h == 0) {
        return std.fmt.bufPrint(
            buf,
            "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.{d:0>3}Z",
            .{ dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, dt.ms },
        ) catch buf[0..0];
    }
    const sign: u8 = if (tz_offset_h > 0) '+' else '-';
    const ah: u8 = if (tz_offset_h > 0) @intCast(tz_offset_h) else @intCast(-tz_offset_h);
    return std.fmt.bufPrint(
        buf,
        "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.{d:0>3}{c}{d:0>2}:00",
        .{ dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, dt.ms, sign, ah },
    ) catch buf[0..0];
}

// ─── TCP helpers ──────────────────────────────────────────────────────────────

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
    const sock = linux.socket(linux.AF.INET, linux.SOCK.STREAM, 0);
    if (sock > @as(usize, std.math.maxInt(i32))) return error.SocketFailed;
    const fd: i32 = @intCast(sock);
    const ip = parseIpv4(host);
    const addr = linux.sockaddr.in{
        .family = linux.AF.INET,
        .port = std.mem.nativeToBig(u16, port),
        .addr = std.mem.nativeToBig(u32, (@as(u32, ip[0]) << 24) | (@as(u32, ip[1]) << 16) | (@as(u32, ip[2]) << 8) | ip[3]),
        .zero = std.mem.zeroes([8]u8),
    };
    if (linux.connect(fd, @ptrCast(&addr), @sizeOf(linux.sockaddr.in)) != 0) {
        _ = linux.close(fd);
        return error.ConnectFailed;
    }

    // sendGelf() is called from every log.* call, including the error paths
    // that report a DB outage — if GrayLog itself is unreachable, an
    // un-timed-out write must not be able to hang those call sites too.
    const tv = linux.timeval{ .sec = 5, .usec = 0 };
    _ = linux.setsockopt(fd, linux.SOL.SOCKET, linux.SO.SNDTIMEO, @ptrCast(&tv), @sizeOf(linux.timeval));
    return fd;
}

fn fdWrite(fd: i32, data: []const u8) !void {
    var done: usize = 0;
    while (done < data.len) {
        const n = linux.write(fd, data[done..].ptr, data.len - done);
        if (n == 0 or n > data.len) return error.WriteFailed;
        done += n;
    }
}

// ─── GELF ─────────────────────────────────────────────────────────────────────

// Syslog severity levels used by GrayLog.
const Level = enum(u3) { err = 3, warn = 4, info = 6 };

fn jsonEscapeAlloc(gpa: Allocator, s: []const u8) ![]u8 {
    var buf: std.ArrayList(u8) = .empty;
    errdefer buf.deinit(gpa);
    for (s) |c| {
        switch (c) {
            '"' => try buf.appendSlice(gpa, "\\\""),
            '\\' => try buf.appendSlice(gpa, "\\\\"),
            '\n' => try buf.appendSlice(gpa, "\\n"),
            '\r' => try buf.appendSlice(gpa, "\\r"),
            '\t' => try buf.appendSlice(gpa, "\\t"),
            else => try buf.append(gpa, c),
        }
    }
    return buf.toOwnedSlice(gpa);
}

fn realtimeMs() i64 {
    var ts: linux.timespec = undefined;
    _ = linux.clock_gettime(.REALTIME, &ts);
    return ts.sec * 1000 + @divTrunc(ts.nsec, 1_000_000);
}

// ─── Logger ───────────────────────────────────────────────────────────────────

pub const Logger = struct {
    gpa: Allocator,
    flags: RuntimeFlags,
    graylog_host: []const u8,
    graylog_port: u16,
    graylog_app: []const u8,
    tz_offset_h: i8,
    fd: i32,

    pub fn init(
        gpa: Allocator,
        flags: RuntimeFlags,
        graylog_host: []const u8,
        graylog_port: u16,
        graylog_app: []const u8,
        tz_offset_h: i8,
    ) Logger {
        return .{
            .gpa = gpa,
            .flags = flags,
            .graylog_host = graylog_host,
            .graylog_port = graylog_port,
            .graylog_app = graylog_app,
            .tz_offset_h = tz_offset_h,
            .fd = if (flags.isDev or flags.isProd) tcpConnect(graylog_host, graylog_port) catch -1 else -1,
        };
    }

    pub fn deinit(self: *Logger) void {
        if (self.fd >= 0) _ = linux.close(self.fd);
    }

    pub fn info(self: *Logger, msg: []const u8) void {
        self.emit(msg, .info, "🔷");
    }

    pub fn warn(self: *Logger, msg: []const u8) void {
        self.emit(msg, .warn, "⚠️");
    }

    pub fn err(self: *Logger, msg: []const u8) void {
        self.emit(msg, .err, "❌");
    }

    pub fn success(self: *Logger, msg: []const u8) void {
        self.emit(msg, .info, "✅");
    }

    fn emit(self: *Logger, msg: []const u8, level: Level, emoji: []const u8) void {
        const ms = realtimeMs();
        if (self.flags.isDev or self.flags.isProd) {
            self.sendGelf(msg, level, ms);
        } else {
            var ts_buf: [32]u8 = undefined;
            const ts = formatTimestamp(&ts_buf, ms, self.tz_offset_h);
            const line = std.fmt.allocPrint(self.gpa, "[{s}] {s} {s}\n", .{ ts, emoji, msg }) catch return;
            defer self.gpa.free(line);
            _ = linux.write(1, line.ptr, line.len); // fd=1 is stdout
        }
    }

    fn sendGelf(self: *Logger, msg: []const u8, level: Level, ms: i64) void {
        const env_str = if (self.flags.isProd) "production" else "development";
        const ts_s = @divTrunc(ms, 1000);
        const ts_frac: u16 = @intCast(@mod(ms, 1000));

        const escaped = jsonEscapeAlloc(self.gpa, msg) catch return;
        defer self.gpa.free(escaped);

        // GELF over TCP: JSON payload terminated by null byte.
        const payload = std.fmt.allocPrint(
            self.gpa,
            "{{\"version\":\"1.1\",\"host\":\"{s}\",\"short_message\":\"{s}\",\"level\":{d},\"timestamp\":{d}.{d:0>3},\"_environment\":\"{s}\"}}\x00",
            .{ self.graylog_app, escaped, @intFromEnum(level), ts_s, ts_frac, env_str },
        ) catch return;
        defer self.gpa.free(payload);

        fdWrite(self.fd, payload) catch {
            // Reconnect once on write failure.
            if (self.fd >= 0) _ = linux.close(self.fd);
            self.fd = tcpConnect(self.graylog_host, self.graylog_port) catch {
                std.debug.print("[graylog unavailable] {s}\n", .{msg});
                return;
            };
            fdWrite(self.fd, payload) catch {
                std.debug.print("[graylog unavailable] {s}\n", .{msg});
            };
        };
    }
};

// ─── Tests ────────────────────────────────────────────────────────────────────

test "millisToDateTime: unix epoch" {
    const dt = millisToDateTime(0, 0);
    try std.testing.expectEqual(@as(u16, 1970), dt.year);
    try std.testing.expectEqual(@as(u8, 1), dt.month);
    try std.testing.expectEqual(@as(u8, 1), dt.day);
    try std.testing.expectEqual(@as(u8, 0), dt.hour);
    try std.testing.expectEqual(@as(u8, 0), dt.minute);
    try std.testing.expectEqual(@as(u8, 0), dt.second);
    try std.testing.expectEqual(@as(u16, 0), dt.ms);
}

test "millisToDateTime: 2026-05-31T17:31:29.555Z" {
    // 2026-05-31 = day 20604 from epoch
    // 00:00:00 = 20604 * 86400 = 1_780_185_600 s
    // 17:31:29 = 63089 s
    // total = 1_780_248_689_555 ms
    const dt = millisToDateTime(1_780_248_689_555, 0);
    try std.testing.expectEqual(@as(u16, 2026), dt.year);
    try std.testing.expectEqual(@as(u8, 5), dt.month);
    try std.testing.expectEqual(@as(u8, 31), dt.day);
    try std.testing.expectEqual(@as(u8, 17), dt.hour);
    try std.testing.expectEqual(@as(u8, 31), dt.minute);
    try std.testing.expectEqual(@as(u8, 29), dt.second);
    try std.testing.expectEqual(@as(u16, 555), dt.ms);
}

test "millisToDateTime: timezone +3 shifts hours" {
    const dt = millisToDateTime(1_780_248_689_555, 3);
    try std.testing.expectEqual(@as(u8, 20), dt.hour); // 17 + 3
    try std.testing.expectEqual(@as(u8, 31), dt.minute);
    try std.testing.expectEqual(@as(u16, 555), dt.ms);
}

test "formatTimestamp: UTC suffix Z" {
    var buf: [32]u8 = undefined;
    const result = formatTimestamp(&buf, 1_780_248_689_555, 0);
    try std.testing.expectEqualStrings("2026-05-31T17:31:29.555Z", result);
}

test "formatTimestamp: positive offset" {
    var buf: [32]u8 = undefined;
    const result = formatTimestamp(&buf, 1_780_248_689_555, 3);
    try std.testing.expectEqualStrings("2026-05-31T20:31:29.555+03:00", result);
}

test "formatTimestamp: negative offset" {
    var buf: [32]u8 = undefined;
    const result = formatTimestamp(&buf, 1_780_248_689_555, -5);
    try std.testing.expectEqualStrings("2026-05-31T12:31:29.555-05:00", result);
}

test "jsonEscapeAlloc: quotes and backslash" {
    const result = try jsonEscapeAlloc(std.testing.allocator, "Say \"hello\\world\"");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("Say \\\"hello\\\\world\\\"", result);
}

test "jsonEscapeAlloc: newlines and tabs" {
    const result = try jsonEscapeAlloc(std.testing.allocator, "line1\nline2\ttab");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("line1\\nline2\\ttab", result);
}

test "Logger local mode: no crash" {
    const flags = RuntimeFlags{ .isLocal = true, .isDev = false, .isProd = false, .evmChainId = 1 };
    var log = Logger.init(std.testing.allocator, flags, "127.0.0.1", 12201, "test", 0);
    defer log.deinit();
    log.info("startup complete");
    log.warn("low memory");
    log.err("connection failed");
    log.success("block saved");
}
