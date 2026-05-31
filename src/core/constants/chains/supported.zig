const core = @import("indexer/core");
const bsc_chain = @import("evm/bsc.zig");
const ethereum_chain = @import("evm/ethereum.zig");

const EvmChainConfig = core.structures.EvmChainConfig;

pub const supportedEvmChains = [_]EvmChainConfig{
    ethereum_chain.ethereum,
    bsc_chain.bsc,
};

pub fn getEvmChainConfig(evmChainId: u256) ?EvmChainConfig {
    if (evmChainId == ethereum_chain.ethereum.id) return ethereum_chain.ethereum;
    if (evmChainId == bsc_chain.bsc.id) return bsc_chain.bsc;

    return null;
}
