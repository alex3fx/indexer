const std = @import("std");
const enums = @import("enums.zig");

const EvmCurrencySymbol = enums.EvmCurrencySymbol;
const EvmContract = enums.EvmContract;
const EvmRpcClientType = enums.EvmRpcClientType;
const EvmSystemCurrency = enums.EvmSystemCurrency;

pub const EvmNativeCurrency = struct {
    decimals: u8,
    name: []const u8,
    symbol: []const u8,
};

pub const EvmIndexingOptions = struct {
    batchSize: u64,
    minifiedChunkSize: u64,
};

pub const EvmRpcNodeConfig = struct {
    type: EvmRpcClientType,
    https: []const u8,
    wss: []const u8,
};

pub const EvmRpcNodesConfig = struct {
    lotosArchiveNode: EvmRpcNodeConfig,
    lotosFullNode: EvmRpcNodeConfig,
    mevFullNode: EvmRpcNodeConfig,
    publicArchiveNode: EvmRpcNodeConfig,
};

pub const EvmContractConfig = struct {
    address: []const u8,
    deployedAtBlock: u64,
    deployedAtTimestamp: u64,
};

pub const EvmBlockExplorersConfig = struct {
    scan: []const u8,
    blockscout: ?[]const u8 = null,
    oklink: []const u8,
};

pub const EvmContractsConfig = struct {
    UNISWAP_V2_ROUTER: ?EvmContractConfig = null,
    PANCAKE_V2_ROUTER: ?EvmContractConfig = null,
    UNISWAP_V3_ROUTER: ?EvmContractConfig = null,
    PANCAKE_V3_ROUTER: ?EvmContractConfig = null,
    UNISWAP_V2_FACTORY: ?EvmContractConfig = null,
    PANCAKE_V2_FACTORY: ?EvmContractConfig = null,
    UNISWAP_V3_FACTORY: ?EvmContractConfig = null,
    PANCAKE_V3_FACTORY: ?EvmContractConfig = null,
    UNISWAP_V2_QUOTER: ?EvmContractConfig = null,
    PANCAKE_V2_QUOTER: ?EvmContractConfig = null,
    MULTICALL3: ?EvmContractConfig = null,

    pub fn get(self: EvmContractsConfig, contract: EvmContract) ?EvmContractConfig {
        return switch (contract) {
            .UNISWAP_V2_ROUTER => self.UNISWAP_V2_ROUTER,
            .PANCAKE_V2_ROUTER => self.PANCAKE_V2_ROUTER,
            .UNISWAP_V3_ROUTER => self.UNISWAP_V3_ROUTER,
            .PANCAKE_V3_ROUTER => self.PANCAKE_V3_ROUTER,
            .UNISWAP_V2_FACTORY => self.UNISWAP_V2_FACTORY,
            .PANCAKE_V2_FACTORY => self.PANCAKE_V2_FACTORY,
            .UNISWAP_V3_FACTORY => self.UNISWAP_V3_FACTORY,
            .PANCAKE_V3_FACTORY => self.PANCAKE_V3_FACTORY,
            .UNISWAP_V2_QUOTER => self.UNISWAP_V2_QUOTER,
            .PANCAKE_V2_QUOTER => self.PANCAKE_V2_QUOTER,
            .MULTICALL3 => self.MULTICALL3,
        };
    }
};

pub const EvmSystemCurrencyConfig = struct {
    address: []const u8,
    type: EvmSystemCurrency,
    symbol: EvmCurrencySymbol,
    decimals: u8,
};

pub const EvmSystemCurrenciesConfig = struct {
    currencyWrapper: EvmSystemCurrencyConfig,
    usdt: EvmSystemCurrencyConfig,
    usdc: EvmSystemCurrencyConfig,
    busd: EvmSystemCurrencyConfig,

    pub fn get(self: EvmSystemCurrenciesConfig, symbol: EvmCurrencySymbol) ?EvmSystemCurrencyConfig {
        const currency = switch (symbol) {
            .WETH, .WBNB => self.currencyWrapper,
            .USDT => self.usdt,
            .USDC => self.usdc,
            .BUSD => self.busd,
        };

        return if (currency.symbol == symbol) currency else null;
    }
};

pub const EvmWellKnownBurnAddressesConfig = struct {
    buckets: []const ?[]const u8,

    pub fn is(self: EvmWellKnownBurnAddressesConfig, address: []const u8) bool {
        if (self.buckets.len == 0) return false;

        var normalized: [evm_address_string_len]u8 = undefined;
        const key = normalizeEvmAddress(&normalized, address) orelse return false;

        var index = bucketIndex(key, self.buckets.len);
        var checked: usize = 0;

        while (checked < self.buckets.len) : (checked += 1) {
            const burnAddress = self.buckets[index] orelse return false;
            if (std.mem.eql(u8, burnAddress, key)) return true;

            index = (index + 1) & (self.buckets.len - 1);
        }

        return false;
    }
};

pub fn makeEvmWellKnownBurnAddressesConfig(comptime addresses: anytype) EvmWellKnownBurnAddressesConfig {
    comptime {
        @setEvalBranchQuota(10_000);

        const capacity = nextPowerOfTwo(@max(1, addresses.len * 2));
        var buckets = [_]?[]const u8{null} ** capacity;

        for (addresses) |address| {
            const normalized = validateEvmAddress(address);
            var index = bucketIndex(normalized, capacity);
            var checked: usize = 0;

            while (checked < capacity) : (checked += 1) {
                if (buckets[index]) |existing| {
                    if (std.mem.eql(u8, existing, normalized)) {
                        @compileError("duplicate well-known burn address");
                    }

                    index = (index + 1) & (capacity - 1);
                    continue;
                }

                buckets[index] = normalized;
                break;
            } else {
                @compileError("well-known burn address hash set is full");
            }
        }

        const final_buckets = buckets;
        return .{ .buckets = &final_buckets };
    }
}

const evm_address_string_len = 42;

fn normalizeEvmAddress(buffer: *[evm_address_string_len]u8, address: []const u8) ?[]const u8 {
    if (address.len != evm_address_string_len) return null;
    if (address[0] != '0' or std.ascii.toLower(address[1]) != 'x') return null;

    buffer[0] = '0';
    buffer[1] = 'x';

    for (address[2..], 2..) |char, index| {
        if (hexDigitValue(char) == null) return null;
        buffer[index] = std.ascii.toLower(char);
    }

    return buffer[0..];
}

fn validateEvmAddress(comptime address: []const u8) []const u8 {
    if (address.len != evm_address_string_len) {
        @compileError("EVM address must be 42 characters long");
    }

    if (address[0] != '0' or address[1] != 'x') {
        @compileError("EVM address must start with 0x");
    }

    for (address[2..]) |char| {
        if (hexDigitValue(char) == null) {
            @compileError("EVM address contains a non-hex character");
        }

        if (std.ascii.toLower(char) != char) {
            @compileError("EVM address must be lowercased with lc(...)");
        }
    }

    return address;
}

fn hexDigitValue(char: u8) ?u4 {
    return switch (char) {
        '0'...'9' => @intCast(char - '0'),
        'a'...'f' => @intCast(char - 'a' + 10),
        'A'...'F' => @intCast(char - 'A' + 10),
        else => null,
    };
}

fn bucketIndex(address: []const u8, capacity: usize) usize {
    return @as(usize, @truncate(fnv1a64(address))) & (capacity - 1);
}

fn fnv1a64(bytes: []const u8) u64 {
    var hash: u64 = 0xcbf29ce484222325;

    for (bytes) |byte| {
        hash ^= byte;
        hash *%= 0x100000001b3;
    }

    return hash;
}

fn nextPowerOfTwo(comptime value: usize) usize {
    var result: usize = 1;

    while (result < value) {
        result *= 2;
    }

    return result;
}

pub const EvmNativeCurrencyPriceFeedDetails = struct {
    poolAddress: []const u8,
    cwPosition: u8,
    cwDecimals: u8,
    usdPosition: u8,
    usdDecimals: u8,
    deployedAtBlock: u64,
    deployedAtTimestamp: u64,
};

pub const EvmChainConfig = struct {
    id: u256,
    name: []const u8,
    blockTime: u64,
    nativeCurrency: EvmNativeCurrency,
    indexingOptions: EvmIndexingOptions,
    rpcNodes: EvmRpcNodesConfig,
    blockExplorers: EvmBlockExplorersConfig,
    contracts: EvmContractsConfig,
    systemCurrencies: EvmSystemCurrenciesConfig,
    wellKnownBurnAddresses: EvmWellKnownBurnAddressesConfig,
    nativeCurrencyPriceFeedDetails: EvmNativeCurrencyPriceFeedDetails,
};
