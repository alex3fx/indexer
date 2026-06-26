const std = @import("std");
const linux = std.os.linux;

const core = @import("indexer/core");
const utils = core.utils;
const node_probe = @import("node_probe.zig");

// std.time.nanoTimestamp doesn't exist in this Zig 0.17-dev snapshot — same
// raw-syscall pattern used elsewhere in this codebase (e.g. pipeline.zig's nowNs()).
fn nowNs() i64 {
    var ts: linux.timespec = undefined;
    _ = linux.clock_gettime(.MONOTONIC, &ts);
    return ts.sec * 1_000_000_000 + ts.nsec;
}

const Allocator = std.mem.Allocator;
const EvmRpcNodeConfig = core.structures.EvmRpcNodeConfig;
const EvmRpcNodesConfig = core.structures.EvmRpcNodesConfig;
const FetchClient = core.fetch.Client;

pub const TraceMethod = node_probe.TraceMethod;

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

    /// Like `join`, but gives up after `timeout_ns` instead of blocking forever.
    /// `std.http.Client`'s underlying connection can wedge (observed in production:
    /// a worker stuck >1.5h on a single block after a transient ConnectionRefused,
    /// with zero retry-log output — the request thread never returned control to
    /// the retry loop). `Task.join()` has no way to interrupt a syscall blocked
    /// inside another thread, so on timeout this ABANDONS the spawned thread
    /// (never joined, never freed — context/number/response leak deliberately)
    /// rather than risk touching memory a still-running thread might write to.
    /// Caller MUST treat the client passed to this request as tainted afterward
    /// (deinit+reinit it) — `std.http.Client` only guarantees individual Requests
    /// are non-threadsafe, and an abandoned in-flight request plus a fresh one on
    /// the same Client would be exactly that.
    pub fn joinTimeout(self: *Task, timeout_ns: u64) !?Response {
        const context = self.context;
        const deadline = nowNs() + @as(i64, @intCast(timeout_ns));
        while (!context.done.load(.acquire)) {
            if (nowNs() >= deadline) {
                // detach so the OS reclaims the thread's resources whenever it
                // does eventually finish/unblock, instead of leaving it in a
                // permanently-joinable (never reaped) state.
                self.thread.detach();
                self.* = undefined; // caller must not touch this Task again — thread abandoned
                return error.FetchTimeout;
            }
            const ts = linux.timespec{ .sec = 0, .nsec = 50_000_000 };
            _ = linux.nanosleep(&ts, null);
        }
        return self.join();
    }
};

const ThreadContext = struct {
    allocator: Allocator,
    client: *FetchClient,
    method: Method,
    options: WorkerOptions,
    result: ?(anyerror!?Response) = null,
    // Set after `result` is written, with release ordering, so a thread polling
    // `done` via acquire is guaranteed to see the `result` write — `joinTimeout`
    // reads `result` (indirectly, via `join`) without going through `thread.join()`
    // first, which is the usual happens-before edge for this pattern.
    done: std.atomic.Value(bool) = .init(false),
};

pub const WorkerOptions = struct {
    rpcNode: EvmRpcNodeConfig,
    number: []const u8,
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
    defer context.done.store(true, .release);

    context.result = requestSync(
        context.allocator,
        context.client,
        context.method,
        context.options,
    );
}

pub fn requestSync(
    allocator: Allocator,
    client: *FetchClient,
    method: Method,
    options: WorkerOptions,
) !?Response {
    if (!utils.isHex(options.number)) return error.InvalidBlockNumberHex;

    const rpcNode = options.rpcNode;

    // Detect trace method on first call per URL, cached thereafter (no HTTP on cache hit)
    const traceMethod: node_probe.TraceMethod = if (method == .getBlockTraces)
        node_probe.detect(allocator, client, rpcNode.https)
    else
        .trace_block;

    const payload = try makePayload(allocator, method, traceMethod, options.number);
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
    traceMethod: node_probe.TraceMethod,
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
        .getBlockTraces => switch (traceMethod) {
            .trace_block => try std.fmt.allocPrint(
                allocator,
                "{{\"id\":1,\"jsonrpc\":\"2.0\",\"method\":\"trace_block\",\"params\":[\"{s}\"]}}",
                .{number},
            ),
            .debug_trace_block => try std.fmt.allocPrint(
                allocator,
                "{{\"id\":1,\"jsonrpc\":\"2.0\",\"method\":\"debug_traceBlockByNumber\",\"params\":[\"{s}\",{{\"tracer\":\"callTracer\",\"tracerConfig\":{{\"onlyTopCall\":false}}}}]}}",
                .{number},
            ),
        },
    };
}

pub fn jsonRpcResultSlice(body: []const u8) ?[]const u8 {
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
