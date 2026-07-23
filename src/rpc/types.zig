// Ethereum RPC types.
// Used by parser.zig, transformer.zig, fetcher.zig, etc.

pub const RpcBlock = struct {
    number: []const u8 = "",
    hash: []const u8 = "",
    parentHash: []const u8 = "",
    timestamp: []const u8 = "",
    milliTimestamp: ?[]const u8 = null,
    miner: []const u8 = "",
    transactions: []RpcTransaction = &.{},
};

pub const RpcTransaction = struct {
    hash: []const u8 = "",
    transactionIndex: []const u8 = "",
    from: []const u8 = "",
    to: ?[]const u8 = null,
    value: []const u8 = "",
    gas: []const u8 = "",
    gasPrice: []const u8 = "",
    input: []const u8 = "",
    type: []const u8 = "",
    maxPriorityFeePerGas: ?[]const u8 = null,
    maxFeePerGas: ?[]const u8 = null,
};

pub const RpcReceipt = struct {
    transactionHash: []const u8 = "",
    transactionIndex: []const u8 = "",
    gasUsed: []const u8 = "",
    cumulativeGasUsed: []const u8 = "",
    effectiveGasPrice: ?[]const u8 = null,
    contractAddress: ?[]const u8 = null,
    status: []const u8 = "",
    logs: []RpcLog = &.{},
};

pub const RpcLog = struct {
    address: []const u8 = "",
    topics: [][]const u8 = &.{},
    data: []const u8 = "",
    transactionHash: []const u8 = "",
    transactionIndex: []const u8 = "",
    logIndex: []const u8 = "",
    removed: bool = false,
};

pub const RpcTrace = struct {
    transactionHash: ?[]const u8 = null,
    transactionPosition: ?i32 = null,
    // Parity trace_block "type": "call" | "create" | "suicide" | "reward" | "".
    // Geth callTracer frames are normalized to the same values by the parser.
    type: []const u8 = "",
    action: RpcAction = .{},
    result: ?RpcResult = null,
    // "error" field from Parity trace_block — non-null when this sub-call reverted
    // (e.g. "Reverted", "Out of gas"). Named traceErr to avoid Zig keyword collision.
    traceErr: ?[]const u8 = null,
    // Position in the call tree, e.g. [1,2,8,3] for a 4th-level sub-call.
    // Empty slice for top-level calls (traceAddress: []).
    traceAddress: []i32 = &.{},
};

pub const RpcAction = struct {
    from: []const u8 = "",
    to: ?[]const u8 = null,
    value: ?[]const u8 = null,
    init: ?[]const u8 = null,
    input: ?[]const u8 = null,
    creationMethod: ?[]const u8 = null,
    // suicide-only (Parity trace_block "action" shape for type=="suicide"):
    // the contract being destructed and the address receiving its balance.
    address: ?[]const u8 = null,
    refundAddress: ?[]const u8 = null,
};

pub const RpcResult = struct {
    address: ?[]const u8 = null,
    code: ?[]const u8 = null,
};
