// #######################################
// ##              COMMON               ##
// #######################################

pub const AppErrorCode = enum {
    DATA_LEAK_DETECTED,
    MISSING_VALUE,
    EMPTY_VALUE,
    INVALID_VALUE,
    UNSUPPORTED_CHAIN,
};

pub const EvmRpcClientType = enum {
    RETH,
    ERIGON,
    GETH,
};

pub const EvmContract = enum {
    UNISWAP_V2_ROUTER,
    PANCAKE_V2_ROUTER,
    UNISWAP_V3_ROUTER,
    PANCAKE_V3_ROUTER,
    UNISWAP_V2_FACTORY,
    PANCAKE_V2_FACTORY,
    UNISWAP_V3_FACTORY,
    PANCAKE_V3_FACTORY,
    UNISWAP_V2_QUOTER,
    PANCAKE_V2_QUOTER,
    MULTICALL3,
};

pub const EvmSystemCurrency = enum {
    NATIVE_WRAPPER,
    USD_BASED_STABLE,
};

pub const EvmCurrencySymbol = enum {
    WETH,
    WBNB,
    USDT,
    USDC,
    BUSD,
};

// #######################################
// ##              RUNTIME              ##
// #######################################

pub const EnvVariable = enum {
    MODE,
    EVM_CHAIN_ID,
    CM_CONNECTION_URL,
    SCYLLA_DB_HOST,
    SCYLLA_DB_PORT,
    SCYLLA_DB_LOCAL_DATACENTER,
    SCYLLA_DB_USERNAME,
    SCYLLA_DB_PASSWORD,
    LOGS_GRAYLOG_HOST,
    LOGS_GRAYLOG_PORT,
    LOGS_GRAYLOG_APP,
};

pub const Mode = enum {
    production,
    development,
    local,
};
