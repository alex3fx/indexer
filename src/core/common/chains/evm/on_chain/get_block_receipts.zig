const std = @import("std");

const core = @import("indexer/core");
const rpc = @import("../../../../../rpc/client.zig");

const Allocator = std.mem.Allocator;
const FetchClient = core.fetch.Client;

pub const Options = rpc.Options;
pub const Response = rpc.Response;
pub const Task = rpc.Task;

pub fn getBlockReceipts(
    allocator: Allocator,
    client: *FetchClient,
    options: Options,
) !Task {
    return rpc.request(allocator, client, .getBlockReceipts, options);
}
