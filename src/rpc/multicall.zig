// Multicall3.aggregate3(Call3[]) ABI encode/decode + eth_call round trip.
// Used to resolve ERC-20 metadata (name/symbol/decimals/totalSupply/owner) for
// many candidate contracts in a single RPC round trip, pinned to a specific
// block (the contract's creation block, or the current head for re-checks).
//
// Call3 = (address target, bool allowFailure, bytes callData)
// Result = (bool success, bytes returnData)
// Every callData we send is a fixed 4-byte selector (no arguments), so the
// ABI encoding below is regular and can be built without a general encoder.
const std = @import("std");

const core = @import("indexer/core");
const client = @import("client.zig");

const Allocator = std.mem.Allocator;
const FetchClient = core.fetch.Client;

const AGGREGATE3_SELECTOR = [4]u8{ 0x82, 0xad, 0x56, 0xcb };

const NAME_SELECTOR = [4]u8{ 0x06, 0xfd, 0xde, 0x03 };
const SYMBOL_SELECTOR = [4]u8{ 0x95, 0xd8, 0x9b, 0x41 };
const DECIMALS_SELECTOR = [4]u8{ 0x31, 0x3c, 0xe5, 0x67 };
const TOTAL_SUPPLY_SELECTOR = [4]u8{ 0x18, 0x16, 0x0d, 0xdd };
const OWNER_SELECTOR = [4]u8{ 0x8d, 0xa5, 0xcb, 0x5b };

const CALL_SELECTORS = [_][4]u8{ NAME_SELECTOR, SYMBOL_SELECTOR, DECIMALS_SELECTOR, TOTAL_SUPPLY_SELECTOR, OWNER_SELECTOR };
pub const CALLS_PER_ADDRESS: usize = CALL_SELECTORS.len;
const TUPLE_LEN: usize = 5 * 32; // Call3/Result tuple, head(3 or 2 words) + len + data-word

pub const Erc20MulticallResult = struct {
    name: ?[]const u8 = null,
    symbol: ?[]const u8 = null,
    decimals: ?u8 = null,
    totalSupply: ?[]const u8 = null, // raw uint256 as decimal string
    owner: ?[]const u8 = null, // lowercase "0x"-address
};

// ─── hex helpers ────────────────────────────────────────────────────────────

fn hexNibble(c: u8) !u8 {
    return switch (c) {
        '0'...'9' => c - '0',
        'a'...'f' => c - 'a' + 10,
        'A'...'F' => c - 'A' + 10,
        else => error.InvalidHex,
    };
}

fn stripHexPrefix(s: []const u8) []const u8 {
    return if (s.len >= 2 and s[0] == '0' and (s[1] == 'x' or s[1] == 'X')) s[2..] else s;
}

fn parseAddress(addrHex: []const u8) ![20]u8 {
    const s = stripHexPrefix(addrHex);
    if (s.len != 40) return error.InvalidAddress;
    var out: [20]u8 = undefined;
    var i: usize = 0;
    while (i < 20) : (i += 1) {
        out[i] = (try hexNibble(s[i * 2]) << 4) | (try hexNibble(s[i * 2 + 1]));
    }
    return out;
}

fn toHexString(allocator: Allocator, bytes: []const u8) ![]u8 {
    const hexChars = "0123456789abcdef";
    var out = try allocator.alloc(u8, 2 + bytes.len * 2);
    out[0] = '0';
    out[1] = 'x';
    for (bytes, 0..) |b, i| {
        out[2 + i * 2] = hexChars[b >> 4];
        out[2 + i * 2 + 1] = hexChars[b & 0xF];
    }
    return out;
}

fn hexDecode(allocator: Allocator, hexStr: []const u8) ![]u8 {
    const s = stripHexPrefix(hexStr);
    const out = try allocator.alloc(u8, s.len / 2);
    var i: usize = 0;
    while (i < out.len) : (i += 1) {
        out[i] = (try hexNibble(s[i * 2]) << 4) | (try hexNibble(s[i * 2 + 1]));
    }
    return out;
}

fn writeWordU64(dst: *[32]u8, v: u64) void {
    @memset(dst, 0);
    std.mem.writeInt(u64, dst[24..32], v, .big);
}

// ─── aggregate3 calldata encoding ───────────────────────────────────────────
//
// aggregate3(Call3[] calls):
//   [selector:4]
//   [outer offset = 0x20]
//   @0x20: [length N] [N head offsets, relative to start of element area]
//          [N tuples, each: target(32) allowFailure(32) bytesOffset=0x60(32) bytesLen=4(32) bytesData(32)]

fn buildAggregate3Calldata(allocator: Allocator, addresses: []const []const u8) ![]u8 {
    const n = addresses.len * CALLS_PER_ADDRESS;
    const total = 4 + 32 + 32 + n * 32 + n * TUPLE_LEN;
    const buf = try allocator.alloc(u8, total);
    @memset(buf, 0);

    @memcpy(buf[0..4], &AGGREGATE3_SELECTOR);
    writeWordU64(buf[4..36], 0x20);
    writeWordU64(buf[36..68], n);

    const headStart = 68;
    const tailStart = headStart + n * 32;

    var idx: usize = 0;
    for (addresses) |addrHex| {
        const addrBytes = try parseAddress(addrHex);
        for (CALL_SELECTORS) |sel| {
            // physOff: where this tuple physically sits within the tail buffer.
            // headOff: the value written into the head's offset slot — per the
            // ABI spec this is relative to the start of the array's length word,
            // i.e. it must include the head array's own size (n*32) on top of
            // physOff. Confirmed against eth_abi's reference encoding.
            const physOff = idx * TUPLE_LEN;
            const headOff = n * 32 + physOff;
            writeWordU64(buf[headStart + idx * 32 ..][0..32], headOff);

            const tuple = buf[tailStart + physOff ..][0..TUPLE_LEN];
            @memcpy(tuple[12..32], &addrBytes); // target, left-padded
            tuple[63] = 1; // allowFailure = true
            writeWordU64(tuple[64..96], 0x60); // offset to bytes, relative to tuple start
            writeWordU64(tuple[96..128], 4); // bytes length
            @memcpy(tuple[128..132], &sel); // bytes data (4-byte selector, rest zero-padded)

            idx += 1;
        }
    }
    return buf;
}

// stateOverrideCode: when set (hex "0x..." runtime bytecode), injects it as the code
// of `multicall3Address` for the duration of this single call only — lets us call
// aggregate3 on historical blocks that predate Multicall3's actual deployment
// (14_353_601 on ETH mainnet / 25_770_160 on Polygon). Relies on the de-facto
// `eth_call(tx, block, stateOverrideSet)` extension shared by Geth/Reth/Erigon.
fn buildEthCallPayload(
    allocator: Allocator,
    multicall3Address: []const u8,
    calldataHex: []const u8,
    blockNumberHex: []const u8,
    stateOverrideCode: ?[]const u8,
) ![]u8 {
    if (stateOverrideCode) |code| {
        return std.fmt.allocPrint(
            allocator,
            "{{\"id\":1,\"jsonrpc\":\"2.0\",\"method\":\"eth_call\",\"params\":[{{\"to\":\"{s}\",\"data\":\"{s}\"}},\"{s}\",{{\"{s}\":{{\"code\":\"{s}\"}}}}]}}",
            .{ multicall3Address, calldataHex, blockNumberHex, multicall3Address, code },
        );
    }
    return std.fmt.allocPrint(
        allocator,
        "{{\"id\":1,\"jsonrpc\":\"2.0\",\"method\":\"eth_call\",\"params\":[{{\"to\":\"{s}\",\"data\":\"{s}\"}},\"{s}\"]}}",
        .{ multicall3Address, calldataHex, blockNumberHex },
    );
}

fn buildEthGetCodePayload(allocator: Allocator, address: []const u8, blockTag: []const u8) ![]u8 {
    return std.fmt.allocPrint(
        allocator,
        "{{\"id\":1,\"jsonrpc\":\"2.0\",\"method\":\"eth_getCode\",\"params\":[\"{s}\",\"{s}\"]}}",
        .{ address, blockTag },
    );
}

fn buildEthBlockNumberPayload(allocator: Allocator) ![]u8 {
    return std.fmt.allocPrint(allocator, "{{\"id\":1,\"jsonrpc\":\"2.0\",\"method\":\"eth_blockNumber\",\"params\":[]}}", .{});
}

/// eth_blockNumber — used by the --erc20-rescan pass to pin "latest" to a
/// concrete block number for the Redis cursor (so later isStaleOrUnknown
/// numeric comparisons against this address stay meaningful).
pub fn fetchHeadBlockNumber(allocator: Allocator, fetchClient: *FetchClient, rpcUrl: []const u8) !u64 {
    const payload = try buildEthBlockNumberPayload(allocator);
    defer allocator.free(payload);

    var response = try fetchClient.fetch(.{
        .url = rpcUrl,
        .method = .POST,
        .body = payload,
        .contentType = "application/json",
        .responseInitialCapacity = 4096,
    });
    defer response.deinit(allocator);

    if (!response.ok()) return error.MulticallHttpFailed;
    const resultSlice = client.jsonRpcResultSlice(response.body) orelse return error.MulticallNullResult;
    const hexResultStr = extractHexStringResult(resultSlice) orelse return error.MulticallBadResponse;
    return std.fmt.parseInt(u64, stripHexPrefix(hexResultStr), 16);
}

fn extractHexStringResult(resultSlice: []const u8) ?[]const u8 {
    if (resultSlice.len == 0 or resultSlice[0] != '"') return null;
    var i: usize = 1;
    while (i < resultSlice.len and resultSlice[i] != '"') : (i += 1) {}
    if (i >= resultSlice.len) return null;
    return resultSlice[1..i];
}

// ─── aggregate3 result decoding ─────────────────────────────────────────────

fn readWordU64At(data: []const u8, bytePos: usize) u64 {
    if (bytePos + 32 > data.len) return 0;
    return std.mem.readInt(u64, data[bytePos + 24 ..][0..8], .big);
}

fn decodeAbiString(allocator: Allocator, retData: []const u8) ![]const u8 {
    if (retData.len < 64) return error.BadAbiString;
    const off = readWordU64At(retData, 0);
    if (off + 32 > retData.len) return error.BadAbiString;
    const len = readWordU64At(retData, off);
    const dataStart = off + 32;
    if (dataStart + len > retData.len) return error.BadAbiString;
    return try allocator.dupe(u8, retData[dataStart..][0..len]);
}

fn decodeUint8(retData: []const u8) ?u8 {
    if (retData.len < 32) return null;
    return retData[31];
}

fn decodeUint256Decimal(allocator: Allocator, retData: []const u8) ![]const u8 {
    if (retData.len < 32) return error.BadAbiUint;
    var v: u256 = 0;
    for (retData[0..32]) |b| v = (v << 8) | b;
    return std.fmt.allocPrint(allocator, "{d}", .{v});
}

fn decodeAddress(allocator: Allocator, retData: []const u8) ![]const u8 {
    if (retData.len < 32) return error.BadAbiAddress;
    return try toHexString(allocator, retData[12..32]);
}

// data: raw bytes of the decoded "0x..." eth_call result (the ABI-encoded Result[]).
fn decodeAggregate3Result(allocator: Allocator, data: []const u8, addrCount: usize) ![]Erc20MulticallResult {
    const out = try allocator.alloc(Erc20MulticallResult, addrCount);
    for (out) |*o| o.* = .{};
    if (data.len < 32) return out;

    const arrayLenPos = readWordU64At(data, 0);
    if (arrayLenPos + 32 > data.len) return out;
    const n = readWordU64At(data, arrayLenPos);
    const elemHeadStart = arrayLenPos + 32;
    const useN = @min(n, addrCount * CALLS_PER_ADDRESS);

    var i: usize = 0;
    while (i < useN) : (i += 1) {
        const headPos = elemHeadStart + i * 32;
        if (headPos + 32 > data.len) break;
        const relOff = readWordU64At(data, headPos);
        const tuplePos = elemHeadStart + relOff;
        if (tuplePos + 64 > data.len) continue;

        const success = data[tuplePos + 31] != 0;
        if (!success) continue;

        const bytesOffRel = readWordU64At(data, tuplePos + 32);
        const bytesLenPos = tuplePos + bytesOffRel;
        if (bytesLenPos + 32 > data.len) continue;
        const bLen = readWordU64At(data, bytesLenPos);
        const bDataPos = bytesLenPos + 32;
        if (bDataPos + bLen > data.len) continue;
        const retData = data[bDataPos..][0..bLen];

        const addrIdx = i / CALLS_PER_ADDRESS;
        const kind = i % CALLS_PER_ADDRESS;
        const slot = &out[addrIdx];
        switch (kind) {
            0 => slot.name = decodeAbiString(allocator, retData) catch null,
            1 => slot.symbol = decodeAbiString(allocator, retData) catch null,
            2 => slot.decimals = decodeUint8(retData),
            3 => slot.totalSupply = decodeUint256Decimal(allocator, retData) catch null,
            4 => slot.owner = decodeAddress(allocator, retData) catch null,
            else => unreachable,
        }
    }
    return out;
}

// ─── public entry point ─────────────────────────────────────────────────────

/// Generic eth_getCode(address, blockTag) wrapper. Used both to cache Multicall3's
/// own runtime bytecode at startup (for the stateOverride path) and, in the
/// --erc20-rescan maintenance pass, to re-fetch an arbitrary contract's current
/// bytecode for a fresh selector scan.
pub fn fetchBytecode(
    allocator: Allocator,
    fetchClient: *FetchClient,
    rpcUrl: []const u8,
    address: []const u8,
    blockTag: []const u8,
) ![]u8 {
    const payload = try buildEthGetCodePayload(allocator, address, blockTag);
    defer allocator.free(payload);

    var response = try fetchClient.fetch(.{
        .url = rpcUrl,
        .method = .POST,
        .body = payload,
        .contentType = "application/json",
        .responseInitialCapacity = 64 * 1024,
    });
    defer response.deinit(allocator);

    if (!response.ok()) return error.MulticallHttpFailed;
    const resultSlice = client.jsonRpcResultSlice(response.body) orelse return error.MulticallNullResult;
    const hexResultStr = extractHexStringResult(resultSlice) orelse return error.MulticallBadResponse;
    return try allocator.dupe(u8, hexResultStr);
}

/// Resolves name/symbol/decimals/totalSupply/owner for `addresses` as of `blockNumberHex`,
/// via a single eth_call to Multicall3.aggregate3 on `rpcUrl`. Result[i] corresponds to
/// addresses[i]; a null field means the call reverted/was absent (not an ERC-20 standard
/// function on that contract), not a transport error.
///
/// stateOverrideCode: pass the bytecode from fetchMulticall3Bytecode() when blockNumberHex
/// is below the chain's real Multicall3 deployment block, so aggregate3 still works on
/// state that predates the contract's actual on-chain deployment. Pass null otherwise.
pub fn resolveErc20Metadata(
    allocator: Allocator,
    fetchClient: *FetchClient,
    rpcUrl: []const u8,
    multicall3Address: []const u8,
    addresses: []const []const u8,
    blockNumberHex: []const u8,
    stateOverrideCode: ?[]const u8,
) ![]Erc20MulticallResult {
    if (addresses.len == 0) return &.{};

    const calldata = try buildAggregate3Calldata(allocator, addresses);
    defer allocator.free(calldata);
    const calldataHex = try toHexString(allocator, calldata);
    defer allocator.free(calldataHex);

    const payload = try buildEthCallPayload(allocator, multicall3Address, calldataHex, blockNumberHex, stateOverrideCode);
    defer allocator.free(payload);

    var response = try fetchClient.fetch(.{
        .url = rpcUrl,
        .method = .POST,
        .body = payload,
        .contentType = "application/json",
        .responseInitialCapacity = 64 * 1024,
    });
    defer response.deinit(allocator);

    if (!response.ok()) return error.MulticallHttpFailed;

    const resultSlice = client.jsonRpcResultSlice(response.body) orelse return error.MulticallNullResult;
    const hexResultStr = extractHexStringResult(resultSlice) orelse return error.MulticallBadResponse;

    const raw = try hexDecode(allocator, hexResultStr);
    defer allocator.free(raw);

    return try decodeAggregate3Result(allocator, raw, addresses.len);
}
