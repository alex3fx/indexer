const std = @import("std");
const Init = std.process.Init;

const core = @import("indexer/core");
const errors = core.errors;
const parsers = @import("parsers.zig");
const types = @import("types.zig");
const validations = @import("validations.zig");

pub const Env = types.Env;
pub const EnvValueType = types.EnvValueType;
pub const Mode = types.Mode;
pub const ParseEnvError = errors.ParseEnvError;

const EnvVariable = core.enums.EnvVariable;

pub fn get_envs(init: Init) ParseEnvError!Env {
    return .{
        .MODE = try parsers.parseMode(try validations.required(init, .MODE)),
        .EVM_CHAIN_ID = try parsers.parseEvmChainId(try validations.required(init, .EVM_CHAIN_ID)),
        .CM_CONNECTION_URL = try parsers.parseCacheManagerConnectionUrl(try validations.required(init, .CM_CONNECTION_URL)),
        .SCYLLA_DB_HOST = try parsers.parseScyllaHost(try validations.required(init, .SCYLLA_DB_HOST)),
        .SCYLLA_DB_PORT = try parsers.parseScyllaPort(try validations.required(init, .SCYLLA_DB_PORT)),
        .SCYLLA_DB_LOCAL_DATACENTER = init.environ_map.get(@tagName(EnvVariable.SCYLLA_DB_LOCAL_DATACENTER)) orelse "datacenter1",
        .SCYLLA_DB_KEYSPACE = init.environ_map.get(@tagName(EnvVariable.SCYLLA_DB_KEYSPACE)) orelse "eth",
        .SCYLLA_DB_USERNAME = try validations.nonEmpty(.SCYLLA_DB_USERNAME, try validations.required(init, .SCYLLA_DB_USERNAME)),
        .SCYLLA_DB_PASSWORD = try validations.nonEmpty(.SCYLLA_DB_PASSWORD, try validations.required(init, .SCYLLA_DB_PASSWORD)),
        .LOGS_GRAYLOG_HOST = init.environ_map.get(@tagName(EnvVariable.LOGS_GRAYLOG_HOST)) orelse "127.0.0.1",
        .LOGS_GRAYLOG_PORT = if (init.environ_map.get(@tagName(EnvVariable.LOGS_GRAYLOG_PORT))) |v|
            std.fmt.parseInt(u16, v, 10) catch 12201
        else
            12201,
        .LOGS_GRAYLOG_APP = init.environ_map.get(@tagName(EnvVariable.LOGS_GRAYLOG_APP)) orelse "indexer",
    };
}
