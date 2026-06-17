const core = @import("indexer/core");
const bsc_chain = @import("evm/bsc.zig");
const ethereum_chain = @import("evm/ethereum.zig");
const polygon_chain = @import("evm/polygon.zig");

const EvmChainConfig = core.structures.EvmChainConfig;

pub const supportedEvmChains = [_]EvmChainConfig{
    ethereum_chain.ethereum,
    bsc_chain.bsc,
    polygon_chain.polygon,
};

pub fn getEvmChainConfig(evmChainId: u256) ?EvmChainConfig {
    if (evmChainId == ethereum_chain.ethereum.id) return ethereum_chain.ethereum;
    if (evmChainId == bsc_chain.bsc.id) return bsc_chain.bsc;
    if (evmChainId == polygon_chain.polygon.id) return polygon_chain.polygon;

    return null;
}
