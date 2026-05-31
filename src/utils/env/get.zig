const Init = @import("std").process.Init;

const errors = @import("indexer/core").errors;
const parsers = @import("parsers.zig");
const types = @import("types.zig");
const validations = @import("validations.zig");

pub const Env = types.Env;
pub const EnvValueType = types.EnvValueType;
pub const Mode = types.Mode;
pub const ParseEnvError = errors.ParseEnvError;

pub fn get_envs(init: Init) ParseEnvError!Env {
    return .{
        .MODE = try parsers.parseMode(try validations.required(init, .MODE)),
        .EVM_CHAIN_ID = try parsers.parseEvmChainId(try validations.required(init, .EVM_CHAIN_ID)),
        .CM_CONNECTION_URL = try parsers.parseCacheManagerConnectionUrl(try validations.required(init, .CM_CONNECTION_URL)),
        .SCYLLA_DB_HOST = try parsers.parseScyllaHost(try validations.required(init, .SCYLLA_DB_HOST)),
        .SCYLLA_DB_PORT = try parsers.parseScyllaPort(try validations.required(init, .SCYLLA_DB_PORT)),
        .SCYLLA_DB_LOCAL_DATACENTER = try validations.nonEmpty(.SCYLLA_DB_LOCAL_DATACENTER, try validations.required(init, .SCYLLA_DB_LOCAL_DATACENTER)),
        .SCYLLA_DB_USERNAME = try validations.nonEmpty(.SCYLLA_DB_USERNAME, try validations.required(init, .SCYLLA_DB_USERNAME)),
        .SCYLLA_DB_PASSWORD = try validations.nonEmpty(.SCYLLA_DB_PASSWORD, try validations.required(init, .SCYLLA_DB_PASSWORD)),
        .LOGS_GRAYLOG_HOST = try parsers.parseIpv4Env(.LOGS_GRAYLOG_HOST, try validations.required(init, .LOGS_GRAYLOG_HOST)),
        .LOGS_GRAYLOG_PORT = try parsers.parsePortEnv(.LOGS_GRAYLOG_PORT, try validations.required(init, .LOGS_GRAYLOG_PORT)),
        .LOGS_GRAYLOG_APP = try validations.nonEmpty(.LOGS_GRAYLOG_APP, try validations.required(init, .LOGS_GRAYLOG_APP)),
    };
}
