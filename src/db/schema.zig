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

// Produced at CREATE/CREATE2 time from a bytecode selector scan (no RPC yet).
// Queued for downstream Multicall3 resolution (name/symbol/decimals/totalSupply/owner)
// before becoming an Erc20TokenRow.
pub const Erc20Candidate = struct {
    address: []const u8,
    chunk: i32,
    blockNumber: i64,
    blockTimestampS: i64,
    blockTimestampMs: i64,
    hasBalanceOf: bool,
    hasTransfer: bool,
    hasTransferFrom: bool,
    hasApprove: bool,
    hasAllowance: bool,
};

// A CALL trace whose target is already in the bloom filter (suspected known
// ERC-20). Triggers a re-check of on-chain totalSupply()/owner() as of this
// block — never trusts emitted Transfer/Approval logs, since a malicious
// contract can emit arbitrary fake events.
pub const Erc20Touch = struct {
    address: []const u8,
    chunk: i32,
    blockNumber: i64,
    blockTimestampS: i64,
};

// Raw SELFDESTRUCT signal from a trace (type=="suicide"), regardless of whether
// `address` is a known ERC-20. Filtering against the bloom/Redis ERC-20 set
// happens downstream, before this becomes an Erc20SelfDestructRow.
pub const SelfDestructEvent = struct {
    address: []const u8,
    chunk: i32,
    blockNumber: i64,
    blockTimestampS: i64,
};

// decimals: -1 sentinel = decimals() call failed or returned non-standard value.
pub const Erc20TokenRow = struct {
    address: []const u8,
    chainId: i32,
    name: []const u8,
    symbol: []const u8,
    decimals: i16,
    hasBalanceOf: bool,
    hasTransfer: bool,
    hasTransferFrom: bool,
    hasApprove: bool,
    hasAllowance: bool,
    isStandardDecimals: bool,
    isFullyFollowingStandard: bool,
    isMinimallyFollowingStandard: bool,
    isPartiallyFollowingStandard: bool,
    isNotFollowingStandard: bool,
    detectionVersion: i32,
};

// isUpdate selects the write path in batch.zig: false → INSERT (sets initial_*
// and latest_* together, at creation time); true → UPDATE (touches only
// latest_* + updated_at_*, never clobbers the original initial_* with NULL).
pub const Erc20SupplyRow = struct {
    address: []const u8,
    chainId: i32,
    initialTotalSupply: []const u8,
    latestTotalSupply: []const u8,
    updatedAtBlock: i64,
    updatedAtTimestamp: i64,
    isUpdate: bool,
};

pub const Erc20OwnerRow = struct {
    address: []const u8,
    chainId: i32,
    initialOwner: []const u8,
    latestOwner: []const u8,
    isOwnershipRenounced: bool,
    updatedAtBlock: i64,
    updatedAtTimestamp: i64,
    isUpdate: bool,
};

pub const Erc20SelfDestructRow = struct {
    address: []const u8,
    chainId: i32,
    atBlock: i64,
    atTimestamp: i64,
};

pub const Entities = struct {
    blocks: std.ArrayList(BlockRow),
    txs: std.ArrayList(TxRow),
    logs: std.ArrayList(LogRow),
    internalTxs: std.ArrayList(InternalTxRow),
    contracts: std.ArrayList(ContractRow),
    contractsByAddr: std.ArrayList(ContractByAddrRow),
    erc20Candidates: std.ArrayList(Erc20Candidate),
    erc20Touches: std.ArrayList(Erc20Touch),
    selfDestructEvents: std.ArrayList(SelfDestructEvent),
    erc20Tokens: std.ArrayList(Erc20TokenRow),
    erc20Supplies: std.ArrayList(Erc20SupplyRow),
    erc20Owners: std.ArrayList(Erc20OwnerRow),
    erc20SelfDestructs: std.ArrayList(Erc20SelfDestructRow),
    lastBlock: u64,
};
