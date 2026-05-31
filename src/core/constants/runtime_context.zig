const std = @import("std");
const Init = std.process.Init;

const core = @import("indexer/core");
const utils = @import("indexer/utils");
const AppErrorCode = core.enums.AppErrorCode;
const Env = utils.Env;
const ParseEnvError = utils.ParseEnvError;

pub const RuntimeFlags = struct {
    isLocal: bool,
    isDev: bool,
    isProd: bool,
    evmChainId: u256,
};

pub const RuntimeContext = struct {
    env: Env,
    details: RuntimeFlags,
};

pub fn getRuntimeContext(init: Init) ParseEnvError!RuntimeContext {
    const env = try utils.get_envs(init);

    _ = core.getEvmChainConfig(env.EVM_CHAIN_ID) orelse {
        std.log.err("{s} ({d})", .{ @tagName(AppErrorCode.UNSUPPORTED_CHAIN), env.EVM_CHAIN_ID });
        return error.UnsupportedChain;
    };

    return .{
        .env = env,
        .details = .{
            .isLocal = env.MODE == .local,
            .isDev = env.MODE == .development,
            .isProd = env.MODE == .production,
            .evmChainId = env.EVM_CHAIN_ID,
        },
    };
}
