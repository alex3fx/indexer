// DB row types and Entities collection.
const std = @import("std");

pub const BlockRow = struct {
    chunk: i32,
    number: i64,
    timestampS: i64,
    timestampMs: i64,
    miner: []const u8,
};

pub const TxRow = struct {
    chunk: i32,
    blockNumber: i64,
    transactionIndex: i32,
    hash: []const u8,
    blockTimestampS: i64,
    blockTimestampMs: i64,
    methodId: []const u8,
    input: []const u8,
    fromAddress: []const u8,
    toAddress: []const u8,
    value: []const u8,
    gasLimit: i64,
    gasPrice: i64,
    gasUsed: i64,
    maxPriorityFee: i64,
    maxFee: i64,
    cumulativeGasUsed: i64,
    effectiveGasPrice: i64,
    contractAddress: []const u8,
    status: i8,
    txType: i8,
};

pub const LogRow = struct {
    chunk: i32,
    blockNumber: i64,
    transactionIndex: i32,
    logIndex: i32,
    blockTimestampS: i64,
    blockTimestampMs: i64,
    address: []const u8,
    data: []const u8,
    topicZeroth: []const u8,
    topicFirst: []const u8,
    topicSecond: []const u8,
    topicThird: []const u8,
    restTopics: [][]const u8,
    transactionHash: []const u8,
    removed: bool,
};

pub const InternalTxRow = struct {
    chunk: i32,
    blockNumber: i64,
    blockTimestampS: i64,
    blockTimestampMs: i64,
    transactionIndex: i32,
    transactionHash: []const u8,
    traceIndex: i32,
    fromAddress: []const u8,
    toAddress: []const u8,
    value: []const u8,
};

pub const ContractRow = struct {
    chunk: i32,
    blockNumber: i64,
    transactionIndex: i32,
    transactionHash: []const u8,
    traceIndex: i32,
    blockTimestampS: i64,
    blockTimestampMs: i64,
    address: []const u8,
    creationMethod: i8,
    creatorAddress: []const u8,
    contractFactory: []const u8,
    creationBytecode: []const u8,
    deployedBytecode: []const u8,
};

pub const ContractByAddrRow = struct {
    address: []const u8,
    creator: []const u8,
    txHash: []const u8,
    blockNumber: i64,
    timestamp: i64,
    contractFactory: []const u8,
    creationBytecode: []const u8,
    deployedBytecode: []const u8,
};

pub const Entities = struct {
    blocks: std.ArrayList(BlockRow),
    txs: std.ArrayList(TxRow),
    logs: std.ArrayList(LogRow),
    internalTxs: std.ArrayList(InternalTxRow),
    contracts: std.ArrayList(ContractRow),
    contractsByAddr: std.ArrayList(ContractByAddrRow),
    lastBlock: u64,
};
