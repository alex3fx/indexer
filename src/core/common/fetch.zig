const std = @import("std");

const Allocator = std.mem.Allocator;

pub const Response = struct {
    status: std.http.Status,
    body: []u8,

    pub fn deinit(self: *Response, allocator: Allocator) void {
        allocator.free(self.body);
        self.* = undefined;
    }

    pub fn ok(self: Response) bool {
        return self.status.class() == .success;
    }
};

pub const Options = struct {
    url: []const u8,
    method: std.http.Method = .GET,
    body: ?[]const u8 = null,
    contentType: ?[]const u8 = null,
    extraHeaders: []const std.http.Header = &.{},
    keepAlive: bool = true,
    responseInitialCapacity: usize = 64 * 1024,
};

pub const Client = struct {
    allocator: Allocator,
    io: std.Io,
    http: std.http.Client,

    pub fn init(allocator: Allocator, io: std.Io) Client {
        return .{
            .allocator = allocator,
            .io = io,
            .http = .{
                .allocator = allocator,
                .io = io,
            },
        };
    }

    pub fn deinit(self: *Client) void {
        self.http.deinit();
        self.* = undefined;
    }

    /// https:// goes through a `curl` subprocess instead of std.http.Client's TLS
    /// stack: Zig 0.17-dev's std.crypto.ml_kem has a vectorized-codegen bug that
    /// SIGILLs during the TLS handshake under -Doptimize=ReleaseFast (confirmed
    /// root cause, see CONTEXT.md task #4 — a local toolchain patch fixes
    /// ReleaseSafe but a second, unfound codegen bug still crashes ReleaseFast,
    /// which is the only mode the production binary can use). HTTPS is only ever
    /// the backup/public-RPC fallback tier here, never the hot path, so the extra
    /// subprocess overhead is irrelevant and far simpler than chasing a compiler bug.
    pub fn fetch(self: *Client, options: Options) !Response {
        if (std.mem.startsWith(u8, options.url, "https://"))
            return fetchViaCurl(self.allocator, self.io, options);
        return fetchWithHttpClient(&self.http, self.allocator, options);
    }
};

// extraHeaders/keepAlive aren't used by any https:// caller today (only contentType
// and body are), so they're silently ignored here rather than wired through to curl.
fn fetchViaCurl(allocator: Allocator, io: std.Io, options: Options) !Response {
    var timeoutBuf: [16]u8 = undefined;
    const timeoutStr = std.fmt.bufPrint(&timeoutBuf, "{d}", .{30}) catch "30";

    var argv: std.ArrayList([]const u8) = .empty;
    defer argv.deinit(allocator);

    var headerBuf: ?[]u8 = null;
    defer if (headerBuf) |h| allocator.free(h);
    if (options.contentType) |ct| headerBuf = try std.fmt.allocPrint(allocator, "Content-Type: {s}", .{ct});

    try argv.appendSlice(allocator, &.{ "curl", "-sS", "-D", "-", "--max-time", timeoutStr, "-X", @tagName(options.method) });
    if (headerBuf) |h| try argv.appendSlice(allocator, &.{ "-H", h });
    if (options.body) |b| try argv.appendSlice(allocator, &.{ "-d", b });
    try argv.append(allocator, options.url);

    const result = try std.process.run(allocator, io, .{ .argv = argv.items });
    defer allocator.free(result.stderr);
    errdefer allocator.free(result.stdout);

    const exitedClean = switch (result.term) {
        .exited => |code| code == 0,
        else => false,
    };
    if (!exitedClean) {
        std.debug.print("[curl] {s} failed: term={any} stderr={s}\n", .{ options.url, result.term, result.stderr });
        return error.CurlExecFailed;
    }

    // `-D -` dumps response headers to stdout, followed by a blank line, then the
    // body. Our JSON-RPC request bodies are always well under curl's 1024-byte
    // "Expect: 100-continue" threshold, so there's exactly one header block here —
    // never an intermediate 100-continue block to skip past.
    const sep = "\r\n\r\n";
    const splitAt = std.mem.indexOf(u8, result.stdout, sep) orelse return error.CurlBadResponse;
    const headerBlock = result.stdout[0..splitAt];
    const bodyStart = splitAt + sep.len;

    const statusLine = headerBlock[0..(std.mem.indexOf(u8, headerBlock, "\r\n") orelse headerBlock.len)];
    var statusCode: u16 = 502;
    if (std.mem.indexOf(u8, statusLine, " ")) |sp| {
        const rest = statusLine[sp + 1 ..];
        const codeEnd = std.mem.indexOf(u8, rest, " ") orelse rest.len;
        statusCode = std.fmt.parseInt(u16, rest[0..codeEnd], 10) catch 502;
    }

    const body = try allocator.dupe(u8, result.stdout[bodyStart..]);
    allocator.free(result.stdout);

    return .{ .status = @enumFromInt(statusCode), .body = body };
}

pub fn fetchWithHttpClient(client: *std.http.Client, allocator: Allocator, options: Options) !Response {
    var body = if (options.responseInitialCapacity == 0)
        std.Io.Writer.Allocating.init(allocator)
    else
        try std.Io.Writer.Allocating.initCapacity(allocator, options.responseInitialCapacity);
    errdefer body.deinit();

    const result = try client.fetch(.{
        .location = .{ .url = options.url },
        .method = options.method,
        .payload = options.body,
        .response_writer = &body.writer,
        .headers = .{
            .content_type = if (options.contentType) |value| .{ .override = value } else .default,
        },
        .extra_headers = options.extraHeaders,
        .keep_alive = options.keepAlive,
    });

    return .{
        .status = result.status,
        .body = try body.toOwnedSlice(),
    };
}
