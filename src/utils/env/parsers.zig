const std = @import("std");

const enums = @import("indexer/core").enums;
const errors = @import("indexer/core").errors;
const validations = @import("validations.zig");
const EnvVariable = enums.EnvVariable;
const Mode = enums.Mode;
const ParseEnvError = errors.ParseEnvError;

pub fn parseMode(value: []const u8) ParseEnvError!Mode {
    if (std.mem.eql(u8, value, @tagName(Mode.production))) return .production;
    if (std.mem.eql(u8, value, @tagName(Mode.development))) return .development;
    if (std.mem.eql(u8, value, @tagName(Mode.local))) return .local;

    validations.logInvalidValue(.MODE);
    return error.InvalidEnvironmentVariable;
}

pub fn parseEvmChainId(value: []const u8) ParseEnvError!u256 {
    const chain_id = std.fmt.parseInt(u256, value, 10) catch {
        validations.logInvalidValue(.EVM_CHAIN_ID);
        return error.InvalidEnvironmentVariable;
    };

    if (chain_id == 0) {
        validations.logInvalidValue(.EVM_CHAIN_ID);
        return error.InvalidEnvironmentVariable;
    }

    return chain_id;
}

pub fn parseCacheManagerConnectionUrl(value: []const u8) ParseEnvError![]const u8 {
    const url = try validations.nonEmpty(.CM_CONNECTION_URL, value);

    if (!std.mem.startsWith(u8, url, "redis://") and !std.mem.startsWith(u8, url, "rediss://")) {
        validations.logInvalidValue(.CM_CONNECTION_URL);
        return error.InvalidEnvironmentVariable;
    }

    const scheme_end = std.mem.indexOf(u8, url, "://").?;
    if (url.len == scheme_end + 3) {
        validations.logInvalidValue(.CM_CONNECTION_URL);
        return error.InvalidEnvironmentVariable;
    }

    return url;
}

pub fn parseScyllaHost(value: []const u8) ParseEnvError![]const u8 {
    return parseIpv4Env(.SCYLLA_DB_HOST, value);
}

pub fn parseScyllaPort(value: []const u8) ParseEnvError!u16 {
    return parsePortEnv(.SCYLLA_DB_PORT, value);
}

pub fn parseIpv4Env(comptime variable: EnvVariable, value: []const u8) ParseEnvError![]const u8 {
    const host = std.mem.trim(u8, try validations.nonEmpty(variable, value), " \t\r\n");
    try validations.ipv4(variable, host);
    return host;
}

pub fn parsePortEnv(comptime variable: EnvVariable, value: []const u8) ParseEnvError!u16 {
    const port_text = std.mem.trim(u8, try validations.nonEmpty(variable, value), " \t\r\n");
    const port = std.fmt.parseInt(u32, port_text, 10) catch {
        validations.logInvalidValue(variable);
        return error.InvalidEnvironmentVariable;
    };

    try validations.portRange(variable, port);
    return @intCast(port);
}
