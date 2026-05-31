const std = @import("std");
const Init = std.process.Init;

const core = @import("indexer/core");
const enums = core.enums;
const utils = @import("indexer/utils");
const EnvVariable = enums.EnvVariable;

pub fn main(init: Init) !void {
    const runtime = try core.getRuntimeContext(init);
    const config = core.getEvmChainConfig(runtime.details.evmChainId) orelse unreachable;
    const rpc_node = config.rpcNodes.lotosArchiveNode;

    std.debug.print("MODE = {s}\n", .{@tagName(runtime.env.get(EnvVariable.MODE))});
    std.debug.print("RuntimeContext.details.isLocal = {}\n", .{runtime.details.isLocal});
    std.debug.print("config.id = {d}\n", .{config.id});
    std.debug.print("rpc.lotosArchiveNode.https = {s}\n", .{rpc_node.https});

    const block_number: u64 = 10_000_000;
    const block_number_hex = try utils.toHex(init.gpa, block_number);
    defer init.gpa.free(block_number_hex);

    const maybe_data = core.getConsistentBlockData(init.gpa, init.io, .{
        .rpcNode = rpc_node,
        .blockNumber = block_number,
    }) catch |err| {
        std.debug.print("getConsistentBlockData failed: {s}\n", .{@errorName(err)});
        return;
    };

    if (maybe_data) |consistent_data| {
        var data = consistent_data;
        defer data.deinit(init.gpa);
        const elapsed_ms = data.parallelFetchElapsedNs / std.time.ns_per_ms;

        std.debug.print(
            "getConsistentBlockData.parallelFetch block={s} seconds={d}.{d:0>3}\n",
            .{
                block_number_hex,
                elapsed_ms / std.time.ms_per_s,
                elapsed_ms % std.time.ms_per_s,
            },
        );
    } else {
        std.debug.print("consistent block data {s} = null\n", .{block_number_hex});
    }
}
