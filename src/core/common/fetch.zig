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
    http: std.http.Client,

    pub fn init(allocator: Allocator, io: std.Io) Client {
        return .{
            .allocator = allocator,
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

    pub fn fetch(self: *Client, options: Options) !Response {
        return fetchWithHttpClient(&self.http, self.allocator, options);
    }
};

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
