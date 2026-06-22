const core = @import("indexer/core");
const enums = core.enums;
const structures = core.structures;
const utils = core.utils;
const EvmChainConfig = structures.EvmChainConfig;
const EvmCurrencySymbol = enums.EvmCurrencySymbol;
const EvmSystemCurrency = enums.EvmSystemCurrency;
const lc = utils.lc;

pub const polygon: EvmChainConfig = .{
    .id = 137,
    .name = "Polygon",
    .blockTime = 2_000,
    .nativeCurrency = .{
        .decimals = 18,
        .name = "POL",
        .symbol = "POL",
    },
    .indexingOptions = .{
        .workerCount = 64,
        .batchSizeBlocks = 50,
        .batchSizeTxs = 100,
        .batchSizeLogs = 200,
        .batchSizeItxs = 200,
        .batchSizeContracts = 50,
        .minifiedChunkSize = 2_000,
    },
    .rpcNodes = .{
        .lotosArchiveNode = .{
            .https = "http://100.64.0.62:8545",
            .wss = "ws://100.64.0.62:8546",
        },
        .lotosFullNode = .{
            .https = "http://100.64.0.62:8545",
            .wss = "ws://100.64.0.62:8546",
        },
        .mevFullNode = .{
            .https = "https://polygon-rpc.com",
            .wss = "wss://polygon-rpc.com",
        },
        .publicArchiveNode = .{
            .https = "https://polygon-rpc.publicnode.com",
            .wss = "wss://polygon-rpc.publicnode.com",
        },
    },
    .blockExplorers = .{
        .scan = "https://polygonscan.com",
        .blockscout = "https://polygon.blockscout.com",
        .oklink = "https://oklink.com/polygon",
    },
    .contracts = .{
        .UNISWAP_V2_ROUTER = .{
            // QuickSwap V2 Router (primary Uniswap V2 fork on Polygon)
            .address = lc("0xa5e0829caced8ffdd4de3c43696c57f7d7a678ff"),
            .deployedAtBlock = 4_931_414,
            .deployedAtTimestamp = 1_617_714_428,
        },
        .PANCAKE_V2_ROUTER = .{
            // PancakeSwap V2 Router on Polygon
            .address = lc("0x10ed43c718714eb63d5aa57b78b54704e256024e"),
            .deployedAtBlock = 45_063_000,
            .deployedAtTimestamp = 1_680_670_000,
        },
        .UNISWAP_V3_ROUTER = .{
            .address = lc("0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45"),
            .deployedAtBlock = 22_757_547,
            .deployedAtTimestamp = 1_638_422_400,
        },
        .PANCAKE_V3_ROUTER = .{
            .address = lc("0x1b81d678ffb9c0263b24a97847620c99d213eb14"),
            .deployedAtBlock = 40_987_190,
            .deployedAtTimestamp = 1_677_000_000,
        },
        .UNISWAP_V2_FACTORY = .{
            // QuickSwap V2 Factory
            .address = lc("0x5757371414417b8c6caad45baef941abc7d3ab32"),
            .deployedAtBlock = 4_931_390,
            .deployedAtTimestamp = 1_617_714_368,
        },
        .PANCAKE_V2_FACTORY = .{
            .address = lc("0x02a84c1b3bbd7401a5f7fa98a384ebc70bb5749e"),
            .deployedAtBlock = 45_063_000,
            .deployedAtTimestamp = 1_680_670_000,
        },
        .UNISWAP_V3_FACTORY = .{
            .address = lc("0x1f98431c8ad98523631ae4a59f267346ea31f984"),
            .deployedAtBlock = 22_757_547,
            .deployedAtTimestamp = 1_638_422_400,
        },
        .PANCAKE_V3_FACTORY = .{
            .address = lc("0x0bfbcf9fa4f9c56b0f40a671ad40e0805a091865"),
            .deployedAtBlock = 45_063_000,
            .deployedAtTimestamp = 1_680_670_000,
        },
        .UNISWAP_V2_QUOTER = .{
            .address = lc("0x61ffe014ba17989e743c5f6cb21bf9697530b21e"),
            .deployedAtBlock = 22_757_547,
            .deployedAtTimestamp = 1_638_422_400,
        },
        .PANCAKE_V2_QUOTER = .{
            .address = lc("0xb048bbc1ee6b733fffcfb9e9cef7375518e25997"),
            .deployedAtBlock = 45_063_000,
            .deployedAtTimestamp = 1_680_670_000,
        },
        .MULTICALL3 = .{
            .address = lc("0xca11bde05977b3631167028862be2a173976ca11"),
            .deployedAtBlock = 25_770_160,
            .deployedAtTimestamp = 1_643_308_803,
        },
    },
    .systemCurrencies = .{
        .currencyWrapper = .{
            .address = lc("0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270"),
            .type = EvmSystemCurrency.NATIVE_WRAPPER,
            .symbol = EvmCurrencySymbol.WETH,
            .decimals = 18,
        },
        .usdt = .{
            .address = lc("0xc2132d05d31c914a87c6611c10748aeb04b58e8f"),
            .type = EvmSystemCurrency.USD_BASED_STABLE,
            .symbol = EvmCurrencySymbol.USDT,
            .decimals = 6,
        },
        .usdc = .{
            .address = lc("0x2791bca1f2de4661ed88a30c99a7a9449aa84174"),
            .type = EvmSystemCurrency.USD_BASED_STABLE,
            .symbol = EvmCurrencySymbol.USDC,
            .decimals = 6,
        },
        .busd = .{
            .address = lc("0x9c9e5fd8bbc25984b178fdce6117defa39d2db39"),
            .type = EvmSystemCurrency.USD_BASED_STABLE,
            .symbol = EvmCurrencySymbol.BUSD,
            .decimals = 18,
        },
    },
    .wellKnownBurnAddresses = structures.makeEvmWellKnownBurnAddressesConfig(.{
        lc("0x000000000000000000000000000000000000dEaD"),
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
        lc("0x1111111111111111111111111111111111111111"),
        lc("0xdEAD000000000000000042069420694206942069"),
        lc("0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"),
        lc("0xFFfFfFffFFfffFFfFFfFFFFFffFFFffffFfFFFfF"),
        lc("0xaAaAaAaaAaAaAaaAaAAAAAAAAaaaAaAaAaaAaaAa"),
    }),
    .nativeCurrencyPriceFeedDetails = .{
        // WMATIC/USDC QuickSwap V2 pool
        .poolAddress = lc("0x6e7a5fafcec6bb1e78bae2a1f0b612012bf14827"),
        .cwPosition = 0,
        .cwDecimals = 18,
        .usdPosition = 1,
        .usdDecimals = 6,
        .deployedAtBlock = 5_484_002,
        .deployedAtTimestamp = 1_619_099_012,
    },
};
