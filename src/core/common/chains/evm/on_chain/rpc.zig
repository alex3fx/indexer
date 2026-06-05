const std = @import("std");

const core = @import("indexer/core");
const utils = @import("indexer/utils");

const Allocator = std.mem.Allocator;
const EvmRpcNodeConfig = core.structures.EvmRpcNodeConfig;
const EvmRpcNodesConfig = core.structures.EvmRpcNodesConfig;
const FetchClient = core.fetch.Client;

pub const Options = struct {
    rpcNodes: EvmRpcNodesConfig,
    number: []const u8,
};

pub const NodeOptions = struct {
    rpcNode: EvmRpcNodeConfig,
    number: []const u8,
};

pub const Response = struct {
    body: []u8,
    result: []const u8,

    pub fn deinit(self: *Response, allocator: Allocator) void {
        allocator.free(self.body);
        self.* = undefined;
    }
};

pub const Method = enum {
    getBlockWithTransactionsByNumber,
    getBlockReceipts,
    getBlockTraces,
};

pub const Task = struct {
    thread: std.Thread,
    context: *ThreadContext,

    pub fn join(self: *Task) !?Response {
        self.thread.join();

        const context = self.context;
        defer context.allocator.destroy(context);
        defer self.* = undefined;

        const result = context.result orelse return error.ThreadResultMissing;
        return result;
    }
};

const ThreadContext = struct {
    allocator: Allocator,
    client: *FetchClient,
    method: Method,
    options: WorkerOptions,
    result: ?(anyerror!?Response) = null,
};

const WorkerOptions = struct {
    rpcNode: EvmRpcNodeConfig,
    number: []u8,
};

pub fn request(
    allocator: Allocator,
    client: *FetchClient,
    method: Method,
    options: Options,
) !Task {
    return requestWithRpcNode(allocator, client, method, .{
        .rpcNode = selectRpcNode(options.rpcNodes),
        .number = options.number,
    });
}

pub fn requestWithRpcNode(
    allocator: Allocator,
    client: *FetchClient,
    method: Method,
    options: NodeOptions,
) !Task {
    const number = try allocator.dupe(u8, options.number);
    errdefer allocator.free(number);

    const context = try allocator.create(ThreadContext);
    errdefer allocator.destroy(context);

    context.* = .{
        .allocator = allocator,
        .client = client,
        .method = method,
        .options = .{
            .rpcNode = options.rpcNode,
            .number = number,
        },
    };

    return .{
        .thread = try std.Thread.spawn(.{}, runRequest, .{context}),
        .context = context,
    };
}

fn runRequest(context: *ThreadContext) void {
    defer context.allocator.free(context.options.number);

    context.result = requestSync(
        context.allocator,
        context.client,
        context.method,
        context.options,
    );
}

fn requestSync(
    allocator: Allocator,
    client: *FetchClient,
    method: Method,
    options: WorkerOptions,
) !?Response {
    if (!utils.isHex(options.number)) return error.InvalidBlockNumberHex;

    const rpcNode = options.rpcNode;
    const payload = try makePayload(allocator, method, rpcNode, options.number);
    defer allocator.free(payload);

    var response = try client.fetch(.{
        .url = rpcNode.https,
        .method = .POST,
        .body = payload,
        .contentType = "application/json",
        .responseInitialCapacity = 512 * 1024,
    });
    errdefer response.deinit(allocator);

    if (!response.ok()) {
        response.deinit(allocator);
        return null;
    }

    const result = jsonRpcResultSlice(response.body) orelse {
        response.deinit(allocator);
        return null;
    };

    return .{
        .body = response.body,
        .result = result,
    };
}

fn selectRpcNode(rpcNodes: EvmRpcNodesConfig) EvmRpcNodeConfig {
    return rpcNodes.lotosArchiveNode;
}

fn makePayload(
    allocator: Allocator,
    method: Method,
    rpcNode: EvmRpcNodeConfig,
    number: []const u8,
) ![]u8 {
    return switch (method) {
        .getBlockWithTransactionsByNumber => try std.fmt.allocPrint(
            allocator,
            "{{\"id\":1,\"jsonrpc\":\"2.0\",\"method\":\"eth_getBlockByNumber\",\"params\":[\"{s}\",true]}}",
            .{number},
        ),
        .getBlockReceipts => try std.fmt.allocPrint(
            allocator,
            "{{\"id\":1,\"jsonrpc\":\"2.0\",\"method\":\"eth_getBlockReceipts\",\"params\":[\"{s}\"]}}",
            .{number},
        ),
        .getBlockTraces => switch (rpcNode.type) {
            .ERIGON, .RETH => try std.fmt.allocPrint(
                allocator,
                "{{\"id\":1,\"jsonrpc\":\"2.0\",\"method\":\"trace_block\",\"params\":[\"{s}\"]}}",
                .{number},
            ),
            // TODO: Geth needs debug_traceBlock with the final tracer/options model.
            .GETH => error.DebugTraceBlockNotImplemented,
        },
    };
}

fn jsonRpcResultSlice(body: []const u8) ?[]const u8 {
    const needle = "\"result\":";
    const pos = std.mem.indexOf(u8, body, needle) orelse return null;
    var i = pos + needle.len;
    while (i < body.len) : (i += 1) {
        switch (body[i]) {
            ' ', '\t', '\n', '\r' => {},
            else => break,
        }
    }
    if (i >= body.len) return null;
    if (body[i] == 'n') return null; // null result → block not yet available
    return body[i..];
}

pub fn jsonObjectFieldSlice(allocator: Allocator, body: []const u8, field: []const u8) !?[]const u8 {
    var scanner = std.json.Scanner.initCompleteInput(allocator, body);
    defer scanner.deinit();

    switch (try scanner.next()) {
        .object_begin => {},
        else => return null,
    }

    while (true) {
        const token = try scanner.nextAlloc(allocator, .alloc_if_needed);
        defer freeJsonToken(allocator, token);

        switch (token) {
            .object_end => return null,
            .string, .allocated_string => {},
            else => return null,
        }

        const key = jsonTokenText(token).?;
        const valueStart = skipJsonValuePrefix(body, scanner.cursor);

        if (std.mem.eql(u8, key, field)) {
            if ((try scanner.peekNextTokenType()) == .null) {
                try scanner.skipValue();
                return null;
            }

            try scanner.skipValue();
            return body[valueStart..scanner.cursor];
        }

        try scanner.skipValue();
    }
}

fn skipJsonWhitespace(body: []const u8, start: usize) usize {
    var index = start;

    while (index < body.len) : (index += 1) {
        switch (body[index]) {
            ' ', '\n', '\r', '\t' => {},
            else => return index,
        }
    }

    return index;
}

fn skipJsonValuePrefix(body: []const u8, start: usize) usize {
    var index = skipJsonWhitespace(body, start);

    if (index < body.len and body[index] == ':') {
        index = skipJsonWhitespace(body, index + 1);
    }

    return index;
}

fn jsonTokenText(token: std.json.Token) ?[]const u8 {
    return switch (token) {
        .string => |value| value,
        .allocated_string => |value| value,
        else => null,
    };
}

fn freeJsonToken(allocator: Allocator, token: std.json.Token) void {
    switch (token) {
        .allocated_string => |value| allocator.free(value),
        .allocated_number => |value| allocator.free(value),
        else => {},
    }
}
