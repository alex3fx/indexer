const enums = @import("indexer/core").enums;

pub const EnvVariable = enums.EnvVariable;
pub const Mode = enums.Mode;

pub const Env = struct {
    MODE: Mode,
    EVM_CHAIN_ID: u256,
    CM_CONNECTION_URL: []const u8,
    SCYLLA_DB_HOST: []const u8,
    SCYLLA_DB_PORT: u16,
    SCYLLA_DB_LOCAL_DATACENTER: []const u8,
    SCYLLA_DB_KEYSPACE: []const u8,
    SCYLLA_DB_USERNAME: []const u8,
    SCYLLA_DB_PASSWORD: []const u8,
    LOGS_GRAYLOG_HOST: []const u8,
    LOGS_GRAYLOG_PORT: u16,
    LOGS_GRAYLOG_APP: []const u8,
    TIME_ZONE: i8,

    pub fn get(self: Env, comptime variable: EnvVariable) EnvValueType(variable) {
        return switch (variable) {
            .MODE => self.MODE,
            .EVM_CHAIN_ID => self.EVM_CHAIN_ID,
            .CM_CONNECTION_URL => self.CM_CONNECTION_URL,
            .SCYLLA_DB_HOST => self.SCYLLA_DB_HOST,
            .SCYLLA_DB_PORT => self.SCYLLA_DB_PORT,
            .SCYLLA_DB_LOCAL_DATACENTER => self.SCYLLA_DB_LOCAL_DATACENTER,
            .SCYLLA_DB_KEYSPACE => self.SCYLLA_DB_KEYSPACE,
            .SCYLLA_DB_USERNAME => self.SCYLLA_DB_USERNAME,
            .SCYLLA_DB_PASSWORD => self.SCYLLA_DB_PASSWORD,
            .LOGS_GRAYLOG_HOST => self.LOGS_GRAYLOG_HOST,
            .LOGS_GRAYLOG_PORT => self.LOGS_GRAYLOG_PORT,
            .LOGS_GRAYLOG_APP => self.LOGS_GRAYLOG_APP,
            .TIME_ZONE => self.TIME_ZONE,
        };
    }
};

pub fn EnvValueType(comptime variable: EnvVariable) type {
    return switch (variable) {
        .MODE => Mode,
        .EVM_CHAIN_ID => u256,
        .CM_CONNECTION_URL => []const u8,
        .SCYLLA_DB_HOST => []const u8,
        .SCYLLA_DB_PORT => u16,
        .SCYLLA_DB_LOCAL_DATACENTER => []const u8,
        .SCYLLA_DB_KEYSPACE => []const u8,
        .SCYLLA_DB_USERNAME => []const u8,
        .SCYLLA_DB_PASSWORD => []const u8,
        .LOGS_GRAYLOG_HOST => []const u8,
        .LOGS_GRAYLOG_PORT => u16,
        .LOGS_GRAYLOG_APP => []const u8,
        .TIME_ZONE => i8,
    };
}
