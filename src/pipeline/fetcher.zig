const std = @import("std");

const core = @import("indexer/core");
const utils = core.utils;
const rpc = @import("indexer/rpc").client;
const pool = @import("indexer/rpc").pool;

const Allocator = std.mem.Allocator;
const EvmRpcNodeConfig = core.structures.EvmRpcNodeConfig;
const FetchClient = core.fetch.Client;

pub const Options = struct {
    rpcNode: EvmRpcNodeConfig,
    blockNumber: u64,
    blockClient: ?*FetchClient = null,
    receiptsClient: ?*FetchClient = null,
    tracesClient: ?*FetchClient = null,
    httpPool: ?*pool.HttpPool = null, // if set, use pool instead of thread spawn
    skipLogs: bool = false,
};

pub const Response = struct {
    height: u64,
    parallelFetchElapsedNs: u128,
    block: rpc.Response,
    transactions: []const u8,
    receipts: rpc.Response,
    logs: []u8,
    traces: rpc.Response,

    pub fn deinit(self: *Response, allocator: Allocator) void {
        self.block.deinit(allocator);
        self.receipts.deinit(allocator);
        allocator.free(self.logs);
        self.traces.deinit(allocator);
        self.* = undefined;
    }
};

/// BlockBundle is an alias for Response (same data, clearer name in context).
pub const BlockBundle = Response;

const ResponseSet = struct {
    parallelFetchElapsedNs: u128,
    block: rpc.Response,
    receipts: rpc.Response,
    traces: rpc.Response,

    fn deinit(self: *ResponseSet, allocator: Allocator) void {
        self.block.deinit(allocator);
        self.receipts.deinit(allocator);
        self.traces.deinit(allocator);
        self.* = undefined;
    }
};

// Generous enough for legitimately slow conditions (measured up to ~16s per call
// during real disk-saturation incidents on one RPC node) while still bounding the
// "stuck worker" failure mode to a fixed amount of time instead of forever.
const FETCH_TIMEOUT_NS: u64 = 45 * std.time.ns_per_s;

fn reinitClient(client: *FetchClient, allocator: Allocator, io: std.Io) void {
    client.deinit();
    client.* = FetchClient.init(allocator, io);
}

pub fn getConsistentBlockData(
    allocator: Allocator,
    io: std.Io,
    options: Options,
) !?Response {
    const block_number = try utils.toHex(allocator, options.blockNumber);
    defer allocator.free(block_number);

    // Try twice (retry on null response = block not yet available).
    for (0..2) |_| {
        const maybe_set = if (options.httpPool) |p|
            try fetchResponseSetPooled(allocator, p, options.rpcNode, block_number, options.blockClient, options.receiptsClient, options.tracesClient)
        else
            try fetchResponseSet(allocator, io, options.rpcNode, block_number, options.blockClient, options.receiptsClient, options.tracesClient);

        if (maybe_set) |set| return try buildResponse(allocator, options.blockNumber, set, options.skipLogs);
    }
    return null;
}

fn buildResponse(allocator: Allocator, height: u64, set: ResponseSet, skipLogs: bool) !Response {
    var responses = set;
    errdefer responses.deinit(allocator);

    const logs: []u8 = if (skipLogs) &.{} else blk: {
        const l = try flattenReceiptLogs(allocator, responses.receipts.result);
        errdefer allocator.free(l);
        break :blk l;
    };

    return .{
        .height = height,
        .parallelFetchElapsedNs = responses.parallelFetchElapsedNs,
        .block = responses.block,
        .transactions = &.{},
        .receipts = responses.receipts,
        .logs = logs,
        .traces = responses.traces,
    };
}

fn fetchResponseSetPooled(
    allocator: Allocator,
    httpPool: *pool.HttpPool,
    rpcNode: EvmRpcNodeConfig,
    blockNumber: []const u8,
    ext_block: ?*FetchClient,
    ext_receipts: ?*FetchClient,
    ext_traces: ?*FetchClient,
) !?ResponseSet {
    var tmp_block = FetchClient.init(allocator, undefined);
    var tmp_receipts = FetchClient.init(allocator, undefined);
    var tmp_traces = FetchClient.init(allocator, undefined);
    defer if (ext_block == null) tmp_block.deinit();
    defer if (ext_receipts == null) tmp_receipts.deinit();
    defer if (ext_traces == null) tmp_traces.deinit();

    const bc = ext_block orelse &tmp_block;
    const rc = ext_receipts orelse &tmp_receipts;
    const tc = ext_traces orelse &tmp_traces;

    const started_at_ns = monotonicNs();

    const results = try httpPool.requestThree(
        allocator,
        .{ bc, rc, tc },
        .{
            .getBlockWithTransactionsByNumber,
            .getBlockReceipts,
            .getBlockTraces,
        },
        rpcNode,
        blockNumber,
    );

    const parallel_fetch_elapsed_ns = elapsedNs(started_at_ns);

    const maybe_block = results[0] catch |err| {
        deinitResponseResult(allocator, results[1]);
        deinitResponseResult(allocator, results[2]);
        return err;
    };
    const maybe_receipts = results[1] catch |err| {
        deinitOptionalResponse(allocator, maybe_block);
        deinitResponseResult(allocator, results[2]);
        return err;
    };
    const maybe_traces = results[2] catch |err| {
        deinitOptionalResponse(allocator, maybe_block);
        deinitOptionalResponse(allocator, maybe_receipts);
        return err;
    };

    if (maybe_block == null or maybe_receipts == null or maybe_traces == null) {
        deinitOptionalResponse(allocator, maybe_block);
        deinitOptionalResponse(allocator, maybe_receipts);
        deinitOptionalResponse(allocator, maybe_traces);
        return null;
    }

    return .{
        .parallelFetchElapsedNs = parallel_fetch_elapsed_ns,
        .block = maybe_block.?,
        .receipts = maybe_receipts.?,
        .traces = maybe_traces.?,
    };
}

fn fetchResponseSet(
    allocator: Allocator,
    io: std.Io,
    rpcNode: EvmRpcNodeConfig,
    blockNumber: []const u8,
    ext_block: ?*FetchClient,
    ext_receipts: ?*FetchClient,
    ext_traces: ?*FetchClient,
) !?ResponseSet {
    const started_at_ns = monotonicNs();

    var tmp_block = FetchClient.init(allocator, io);
    var tmp_receipts = FetchClient.init(allocator, io);
    var tmp_traces = FetchClient.init(allocator, io);
    defer if (ext_block == null) tmp_block.deinit();
    defer if (ext_receipts == null) tmp_receipts.deinit();
    defer if (ext_traces == null) tmp_traces.deinit();

    const block_client = ext_block orelse &tmp_block;
    const receipts_client = ext_receipts orelse &tmp_receipts;
    const traces_client = ext_traces orelse &tmp_traces;

    var block_task = try rpc.requestWithRpcNode(allocator, block_client, .getBlockWithTransactionsByNumber, .{
        .rpcNode = rpcNode,
        .number = blockNumber,
    });
    var block_task_pending = true;
    errdefer if (block_task_pending) discardTask(&block_task, allocator);

    var receipts_task = try rpc.requestWithRpcNode(allocator, receipts_client, .getBlockReceipts, .{
        .rpcNode = rpcNode,
        .number = blockNumber,
    });
    var receipts_task_pending = true;
    errdefer if (receipts_task_pending) discardTask(&receipts_task, allocator);

    var traces_task = try rpc.requestWithRpcNode(allocator, traces_client, .getBlockTraces, .{
        .rpcNode = rpcNode,
        .number = blockNumber,
    });
    var traces_task_pending = true;
    errdefer if (traces_task_pending) discardTask(&traces_task, allocator);

    // joinTimeout, not join: a hung connection inside one of these threads has
    // no way to be interrupted, so without a deadline a single wedged request
    // permanently parks this worker (observed in production: >1.5h stuck on one
    // block after a transient ConnectionRefused). On timeout the thread is
    // abandoned (leaked, not corrupted — see Task.joinTimeout) and the
    // client it was using must be discarded, since std.http.Client only
    // guarantees individual Requests are non-threadsafe and a still-running
    // abandoned request plus a fresh one on the same Client would violate that.
    const block_result = block_task.joinTimeout(FETCH_TIMEOUT_NS);
    block_task_pending = false;
    if (block_result == error.FetchTimeout) reinitClient(block_client, allocator, io);

    const receipts_result = receipts_task.joinTimeout(FETCH_TIMEOUT_NS);
    receipts_task_pending = false;
    if (receipts_result == error.FetchTimeout) reinitClient(receipts_client, allocator, io);

    const traces_result = traces_task.joinTimeout(FETCH_TIMEOUT_NS);
    traces_task_pending = false;
    if (traces_result == error.FetchTimeout) reinitClient(traces_client, allocator, io);

    const parallel_fetch_elapsed_ns = elapsedNs(started_at_ns);

    const maybe_block = block_result catch |err| {
        deinitResponseResult(allocator, receipts_result);
        deinitResponseResult(allocator, traces_result);
        return err;
    };

    const maybe_receipts = receipts_result catch |err| {
        deinitOptionalResponse(allocator, maybe_block);
        deinitResponseResult(allocator, traces_result);
        return err;
    };

    const maybe_traces = traces_result catch |err| {
        deinitOptionalResponse(allocator, maybe_block);
        deinitOptionalResponse(allocator, maybe_receipts);
        return err;
    };

    if (maybe_block == null or maybe_receipts == null or maybe_traces == null) {
        deinitOptionalResponse(allocator, maybe_block);
        deinitOptionalResponse(allocator, maybe_receipts);
        deinitOptionalResponse(allocator, maybe_traces);
        return null;
    }

    return .{
        .parallelFetchElapsedNs = parallel_fetch_elapsed_ns,
        .block = maybe_block.?,
        .receipts = maybe_receipts.?,
        .traces = maybe_traces.?,
    };
}

fn flattenReceiptLogs(allocator: Allocator, receipts: []const u8) ![]u8 {
    var output = try std.Io.Writer.Allocating.initCapacity(allocator, @min(receipts.len, 64 * 1024));
    errdefer output.deinit();

    try output.writer.writeByte('[');

    var scanner = std.json.Scanner.initCompleteInput(allocator, receipts);
    defer scanner.deinit();

    switch (try scanner.next()) {
        .array_begin => {},
        else => return error.ReceiptsResultNotArray,
    }

    var has_logs = false;

    while (true) {
        const token_type = try scanner.peekNextTokenType();

        switch (token_type) {
            .array_end => {
                _ = try scanner.next();
                break;
            },
            else => {
                const receipt_start = scanner.cursor;
                try scanner.skipValue();

                const receipt = receipts[receipt_start..scanner.cursor];
                const logs = try rpc.jsonObjectFieldSlice(allocator, receipt, "logs") orelse "[]";
                try appendJsonArrayItems(allocator, &output.writer, logs, &has_logs);
            },
        }
    }

    try output.writer.writeByte(']');
    return try output.toOwnedSlice();
}

fn appendJsonArrayItems(
    allocator: Allocator,
    writer: *std.Io.Writer,
    array: []const u8,
    hasItems: *bool,
) !void {
    var scanner = std.json.Scanner.initCompleteInput(allocator, array);
    defer scanner.deinit();

    switch (try scanner.next()) {
        .array_begin => {},
        else => return error.LogsResultNotArray,
    }

    while (true) {
        const token_type = try scanner.peekNextTokenType();

        switch (token_type) {
            .array_end => {
                _ = try scanner.next();
                break;
            },
            else => {
                const item_start = scanner.cursor;
                try scanner.skipValue();

                if (hasItems.*) {
                    try writer.writeByte(',');
                }

                try writer.writeAll(array[item_start..scanner.cursor]);
                hasItems.* = true;
            },
        }
    }
}

fn discardTask(task: *rpc.Task, allocator: Allocator) void {
    if (task.join() catch null) |response| {
        var owned = response;
        owned.deinit(allocator);
    }
}

fn deinitOptionalResponse(allocator: Allocator, maybe_response: ?rpc.Response) void {
    if (maybe_response) |response| {
        var owned = response;
        owned.deinit(allocator);
    }
}

fn deinitResponseResult(allocator: Allocator, result: anyerror!?rpc.Response) void {
    if (result catch null) |response| {
        var owned = response;
        owned.deinit(allocator);
    }
}

fn monotonicNs() u128 {
    var timestamp: std.c.timespec = undefined;

    if (std.c.clock_gettime(.MONOTONIC, &timestamp) != 0) {
        return 0;
    }

    return @as(u128, @intCast(timestamp.sec)) * std.time.ns_per_s +
        @as(u128, @intCast(timestamp.nsec));
}

fn elapsedNs(started_at_ns: u128) u128 {
    const finished_at_ns = monotonicNs();

    if (finished_at_ns < started_at_ns) {
        return 0;
    }

    return finished_at_ns - started_at_ns;
}
