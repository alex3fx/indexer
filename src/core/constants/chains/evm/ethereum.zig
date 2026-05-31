const enums = @import("indexer/core").enums;
const structures = @import("indexer/core").structures;
const utils = @import("indexer/utils");
const EvmChainConfig = structures.EvmChainConfig;
const EvmCurrencySymbol = enums.EvmCurrencySymbol;
const EvmRpcClientType = enums.EvmRpcClientType;
const EvmSystemCurrency = enums.EvmSystemCurrency;
const lc = utils.lc;

pub const ethereum: EvmChainConfig = .{
    .id = 1,
    .name = "Ethereum",
    .blockTime = 12_000,
    .nativeCurrency = .{
        .decimals = 18,
        .name = "Ether",
        .symbol = "ETH",
    },
    .indexingOptions = .{
        .batchSize = 10,
        .minifiedChunkSize = 1_000,
    },
    .rpcNodes = .{
        .lotosArchiveNode = .{
            .type = EvmRpcClientType.RETH,
            .https = "http://100.64.0.7:8545",
            .wss = "ws://100.64.0.7:8546",
        },
        .lotosFullNode = .{
            .type = EvmRpcClientType.RETH,
            .https = "http://2.0.0.0:8545",
            .wss = "ws://2.0.0.0:8546",
        },
        .mevFullNode = .{
            .type = EvmRpcClientType.RETH,
            .https = "https://eth.merkle.io",
            .wss = "wss://eth.merkle.io",
        },
        .publicArchiveNode = .{
            .type = EvmRpcClientType.ERIGON,
            .https = "https://ethereum-rpc.publicnode.com",
            .wss = "wss://ethereum-rpc.publicnode.com",
        },
    },
    .blockExplorers = .{
        .scan = "https://etherscan.io",
        .blockscout = "https://eth.blockscout.com",
        .oklink = "https://oklink.com/ethereum",
    },
    .contracts = .{
        .UNISWAP_V2_ROUTER = .{
            .address = lc("0x7a250d5630b4cf539739df2c5dacb4c659f2488d"),
            .deployedAtBlock = 10_207_858,
            .deployedAtTimestamp = 1_591_388_241,
        },
        .PANCAKE_V2_ROUTER = .{
            .address = lc("0xeff92a263d31888d860bd50809a8d171709b7b1c"),
            .deployedAtBlock = 15_615_793,
            .deployedAtTimestamp = 1_664_174_123,
        },
        .UNISWAP_V3_ROUTER = .{
            .address = lc("0xe592427a0aece92de3edee1f18e0157c05861564"),
            .deployedAtBlock = 12_369_634,
            .deployedAtTimestamp = 1_620_156_641,
        },
        .PANCAKE_V3_ROUTER = .{
            .address = lc("0x1b81d678ffb9c0263b24a97847620c99d213eb14"),
            .deployedAtBlock = 16_944_755,
            .deployedAtTimestamp = 1_680_236_243,
        },
        .UNISWAP_V2_FACTORY = .{
            .address = lc("0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f"),
            .deployedAtBlock = 10_000_835,
            .deployedAtTimestamp = 1_588_610_042,
        },
        .PANCAKE_V2_FACTORY = .{
            .address = lc("0x1097053fd2ea711dad45caccc45eff7548fcb362"),
            .deployedAtBlock = 15_614_590,
            .deployedAtTimestamp = 1_664_159_627,
        },
        .UNISWAP_V3_FACTORY = .{
            .address = lc("0x1f98431c8ad98523631ae4a59f267346ea31f984"),
            .deployedAtBlock = 12_369_621,
            .deployedAtTimestamp = 1_620_156_420,
        },
        .PANCAKE_V3_FACTORY = .{
            .address = lc("0x0bfbcf9fa4f9c56b0f40a671ad40e0805a091865"),
            .deployedAtBlock = 16_950_686,
            .deployedAtTimestamp = 1_680_308_207,
        },
        .UNISWAP_V2_QUOTER = .{
            .address = lc("0x61ffe014ba17989e743c5f6cb21bf9697530b21e"),
            .deployedAtBlock = 13_723_980,
            .deployedAtTimestamp = 1_638_401_744,
        },
        .PANCAKE_V2_QUOTER = .{
            .address = lc("0xb048bbc1ee6b733fffcfb9e9cef7375518e25997"),
            .deployedAtBlock = 16_944_788,
            .deployedAtTimestamp = 1_680_236_639,
        },
        .MULTICALL3 = .{
            .address = lc("0xca11bde05977b3631167028862be2a173976ca11"),
            .deployedAtBlock = 14_353_601,
            .deployedAtTimestamp = 1_646_842_676,
        },
    },
    .systemCurrencies = .{
        .currencyWrapper = .{
            .address = lc("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"),
            .type = EvmSystemCurrency.NATIVE_WRAPPER,
            .symbol = EvmCurrencySymbol.WETH,
            .decimals = 18,
        },
        .usdt = .{
            .address = lc("0xdAC17F958D2ee523a2206206994597C13D831ec7"),
            .type = EvmSystemCurrency.USD_BASED_STABLE,
            .symbol = EvmCurrencySymbol.USDT,
            .decimals = 6,
        },
        .usdc = .{
            .address = lc("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
            .type = EvmSystemCurrency.USD_BASED_STABLE,
            .symbol = EvmCurrencySymbol.USDC,
            .decimals = 6,
        },
        .busd = .{
            .address = lc("0x4Fabb145d64652a948d72533023f6E7A623C7C53"),
            .type = EvmSystemCurrency.USD_BASED_STABLE,
            .symbol = EvmCurrencySymbol.BUSD,
            .decimals = 18,
        },
    },
    .wellKnownBurnAddresses = structures.makeEvmWellKnownBurnAddressesConfig(.{
        lc("0xc387bb73456a0EF6aC61c1F47B80F8492C429cb6"),
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
        lc("0x00000000000000000000045261D4Ee77acdb3286"),
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
        lc("0xffFFfFFffFFffFfFffFFfFfFfFFFFfFfFFFFDead"),
        lc("0xfFFFFFfffFFFfFFfFFfFfffFfFfffFfffFFfedaD"),
        lc("0xFFfFfFffFFfffFFfFFfFFFFFffFFFffffFfFFFfF"),
        lc("0xaAaAaAaaAaAaAaaAaAAAAAAAAaaaAaAaAaaAaaAa"),
        lc("0x7d7445b6e7098efBDEAfA4A24f443847D5dAA262"),
    }),
    .nativeCurrencyPriceFeedDetails = .{
        .poolAddress = lc("0x0d4a11d5EEaaC28EC3F61d100daF4d40471f1852"),
        .cwPosition = 0,
        .cwDecimals = 18,
        .usdPosition = 1,
        .usdDecimals = 6,
        .deployedAtBlock = 10_093_341,
        .deployedAtTimestamp = 1_589_850_429,
    },
};
