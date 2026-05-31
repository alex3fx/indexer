const std = @import("std");

const core = @import("indexer/core");
const utils = @import("indexer/utils");
const rpc = @import("rpc.zig");

const Allocator = std.mem.Allocator;
const EvmRpcNodeConfig = core.structures.EvmRpcNodeConfig;
const FetchClient = core.fetch.Client;

pub const Options = struct {
    rpcNode: EvmRpcNodeConfig,
    blockNumber: u64,
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

pub fn getConsistentBlockData(
    allocator: Allocator,
    io: std.Io,
    options: Options,
) !?Response {
    const block_number = try utils.toHex(allocator, options.blockNumber);
    defer allocator.free(block_number);

    if (try fetchResponseSet(allocator, io, options.rpcNode, block_number)) |set| {
        return try buildResponse(allocator, options.blockNumber, set);
    }

    if (try fetchResponseSet(allocator, io, options.rpcNode, block_number)) |set| {
        return try buildResponse(allocator, options.blockNumber, set);
    }

    return null;
}

fn buildResponse(allocator: Allocator, height: u64, set: ResponseSet) !Response {
    var responses = set;
    errdefer responses.deinit(allocator);

    const transactions = try rpc.jsonObjectFieldSlice(
        allocator,
        responses.block.result,
        "transactions",
    ) orelse return error.BlockTransactionsMissing;

    const logs = try flattenReceiptLogs(allocator, responses.receipts.result);
    errdefer allocator.free(logs);

    return .{
        .height = height,
        .parallelFetchElapsedNs = responses.parallelFetchElapsedNs,
        .block = responses.block,
        .transactions = transactions,
        .receipts = responses.receipts,
        .logs = logs,
        .traces = responses.traces,
    };
}

fn fetchResponseSet(
    allocator: Allocator,
    io: std.Io,
    rpcNode: EvmRpcNodeConfig,
    blockNumber: []const u8,
) !?ResponseSet {
    const started_at_ns = monotonicNs();

    var block_client = FetchClient.init(allocator, io);
    defer block_client.deinit();

    var receipts_client = FetchClient.init(allocator, io);
    defer receipts_client.deinit();

    var traces_client = FetchClient.init(allocator, io);
    defer traces_client.deinit();

    var block_task = try rpc.requestWithRpcNode(allocator, &block_client, .getBlockWithTransactionsByNumber, .{
        .rpcNode = rpcNode,
        .number = blockNumber,
    });
    var block_task_pending = true;
    errdefer if (block_task_pending) discardTask(&block_task, allocator);

    var receipts_task = try rpc.requestWithRpcNode(allocator, &receipts_client, .getBlockReceipts, .{
        .rpcNode = rpcNode,
        .number = blockNumber,
    });
    var receipts_task_pending = true;
    errdefer if (receipts_task_pending) discardTask(&receipts_task, allocator);

    var traces_task = try rpc.requestWithRpcNode(allocator, &traces_client, .getBlockTraces, .{
        .rpcNode = rpcNode,
        .number = blockNumber,
    });
    var traces_task_pending = true;
    errdefer if (traces_task_pending) discardTask(&traces_task, allocator);

    const block_result = block_task.join();
    block_task_pending = false;

    const receipts_result = receipts_task.join();
    receipts_task_pending = false;

    const traces_result = traces_task.join();
    traces_task_pending = false;

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
