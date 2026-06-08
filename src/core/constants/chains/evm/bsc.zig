const core = @import("indexer/core");
const enums = core.enums;
const structures = core.structures;
const utils = core.utils;
const EvmChainConfig = structures.EvmChainConfig;
const EvmCurrencySymbol = enums.EvmCurrencySymbol;
const EvmSystemCurrency = enums.EvmSystemCurrency;
const lc = utils.lc;

pub const bsc: EvmChainConfig = .{
    .id = 56,
    .name = "BNB Smart Chain",
    .blockTime = 450,
    .nativeCurrency = .{
        .decimals = 18,
        .name = "BNB",
        .symbol = "BNB",
    },
    .indexingOptions = .{
        .workerCount = 64,
        .batchSizeBlocks = 50,
        .batchSizeTxs = 100,
        .batchSizeLogs = 200,
        .batchSizeItxs = 500,
        .batchSizeContracts = 50,
        .minifiedChunkSize = 200,
    },
    .rpcNodes = .{
        .lotosArchiveNode = .{
            .https = "http://3.0.0.0:8545",
            .wss = "ws://3.0.0.0:8546",
        },
        .lotosFullNode = .{
            .https = "http://4.0.0.0:8545",
            .wss = "ws://4.0.0.0:8546",
        },
        .mevFullNode = .{
            .https = "https://bscrpc.pancakeswap.finance",
            .wss = "wss://bscrpc.pancakeswap.finance",
        },
        .publicArchiveNode = .{
            .https = "https://bsc-rpc.publicnode.com",
            .wss = "wss://bsc-rpc.publicnode.com",
        },
    },
    .blockExplorers = .{
        .scan = "https://bscscan.com",
        .oklink = "https://oklink.com/bsc",
    },
    .contracts = .{
        .UNISWAP_V2_ROUTER = .{
            .address = lc("0x4752ba5dbc23f44d87826276bf6fd6b1c372ad24"),
            .deployedAtBlock = 35_962_974,
            .deployedAtTimestamp = 1_707_417_014,
        },
        .PANCAKE_V2_ROUTER = .{
            .address = lc("0x10ed43c718714eb63d5aa57b78b54704e256024e"),
            .deployedAtBlock = 6_810_080,
            .deployedAtTimestamp = 1_619_165_545,
        },
        .UNISWAP_V3_ROUTER = .{
            .address = lc("0xb971ef87ede563556b2ed4b1c0b0019111dd85d2"),
            .deployedAtBlock = 26_324_062,
            .deployedAtTimestamp = 1_678_391_738,
        },
        .PANCAKE_V3_ROUTER = .{
            .address = lc("0x1b81d678ffb9c0263b24a97847620c99d213eb14"),
            .deployedAtBlock = 26_931_952,
            .deployedAtTimestamp = 1_680_234_350,
        },
        .UNISWAP_V2_FACTORY = .{
            .address = lc("0x8909dc15e40173ff4699343b6eb8132c65e18ec6"),
            .deployedAtBlock = 33_496_018,
            .deployedAtTimestamp = 1_699_996_150,
        },
        .PANCAKE_V2_FACTORY = .{
            .address = lc("0xca143ce32fe78f1f7019d7d551a6402fc5350c73"),
            .deployedAtBlock = 6_809_737,
            .deployedAtTimestamp = 1_619_164_516,
        },
        .UNISWAP_V3_FACTORY = .{
            .address = lc("0xdb1d10011ad0ff90774d0c6bb92e5c5c8b4461f7"),
            .deployedAtBlock = 26_324_014,
            .deployedAtTimestamp = 1_678_391_594,
        },
        .PANCAKE_V3_FACTORY = .{
            .address = lc("0x0bfbcf9fa4f9c56b0f40a671ad40e0805a091865"),
            .deployedAtBlock = 26_956_207,
            .deployedAtTimestamp = 1_680_307_494,
        },
        .UNISWAP_V2_QUOTER = .{
            .address = lc("0x78d78e420da98ad378d7799be8f4af69033eb077"),
            .deployedAtBlock = 26_324_058,
            .deployedAtTimestamp = 1_678_391_726,
        },
        .PANCAKE_V2_QUOTER = .{
            .address = lc("0xb048bbc1ee6b733fffcfb9e9cef7375518e25997"),
            .deployedAtBlock = 26_931_962,
            .deployedAtTimestamp = 1_680_234_380,
        },
        .MULTICALL3 = .{
            .address = lc("0xca11bde05977b3631167028862be2a173976ca11"),
            .deployedAtBlock = 15_921_452,
            .deployedAtTimestamp = 1_646_867_874,
        },
    },
    .systemCurrencies = .{
        .currencyWrapper = .{
            .address = lc("0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c"),
            .type = EvmSystemCurrency.NATIVE_WRAPPER,
            .symbol = EvmCurrencySymbol.WBNB,
            .decimals = 18,
        },
        .usdt = .{
            .address = lc("0x55d398326f99059fF775485246999027B3197955"),
            .type = EvmSystemCurrency.USD_BASED_STABLE,
            .symbol = EvmCurrencySymbol.USDT,
            .decimals = 18,
        },
        .usdc = .{
            .address = lc("0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d"),
            .type = EvmSystemCurrency.USD_BASED_STABLE,
            .symbol = EvmCurrencySymbol.USDC,
            .decimals = 18,
        },
        .busd = .{
            .address = lc("0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56"),
            .type = EvmSystemCurrency.USD_BASED_STABLE,
            .symbol = EvmCurrencySymbol.BUSD,
            .decimals = 18,
        },
    },
    .wellKnownBurnAddresses = structures.makeEvmWellKnownBurnAddressesConfig(.{
        lc("0x0000000000000000000000000000000000000000"),
        lc("0x0000000000000000000000000000000000000001"),
        lc("0x0000000000000000000000000000000000000002"),
        lc("0x0000000000000000000000000000000000000003"),
        lc("0x0000000000000000000000000000000000000004"),
        lc("0x0000000000000000000000000000000000000005"),
        lc("0x0000000000000000000000000000000000000006"),
        lc("0x0000000000000000000000000000000000000007"),
        lc("0x0000000000000000000000000000000000000008"),
        lc("0x0000000000000000000000000000000000000009"),
        lc("0x000000000000000000000000000000000000dEaD"),
        lc("0x0123456789012345678901234567890123456789"),
        lc("0x1111111111111111111111111111111111111111"),
        lc("0x1234567890123456789012345678901234567890"),
        lc("0x2222222222222222222222222222222222222222"),
        lc("0x3333333333333333333333333333333333333333"),
        lc("0x4444444444444444444444444444444444444444"),
        lc("0x6666666666666666666666666666666666666666"),
        lc("0x8888888888888888888888888888888888888888"),
        lc("0xbBbBBBBbbBBBbbbBbbBbbbbBBbBbbbbBbBbbBBbB"),
        lc("0xdEAD000000000000000042069420694206942069"),
        lc("0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"),
        lc("0xFFfFfFffFFfffFFfFFfFFFFFffFFFffffFfFFFfF"),
        lc("0xaAaAaAaaAaAaAaaAaAAAAAAAAaaaAaAaAaaAaaAa"),
        lc("0x64b00B5EC6dF675E94736bdcC006DBd9a0B8B00B"),
    }),
    .nativeCurrencyPriceFeedDetails = .{
        .poolAddress = lc("0x16b9a82891338f9bA80E2D6970FddA79D1eb0daE"),
        .cwPosition = 1,
        .cwDecimals = 18,
        .usdPosition = 0,
        .usdDecimals = 18,
        .deployedAtBlock = 6_810_780,
        .deployedAtTimestamp = 1_619_167_645,
    },
};
