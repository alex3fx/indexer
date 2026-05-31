const std = @import("std");
const Init = std.process.Init;

const enums = @import("indexer/core").enums;
const errors = @import("indexer/core").errors;
const AppErrorCode = enums.AppErrorCode;
const EnvVariable = enums.EnvVariable;
const ParseEnvError = errors.ParseEnvError;

pub fn required(init: Init, comptime variable: EnvVariable) ParseEnvError![]const u8 {
    return init.environ_map.get(@tagName(variable)) orelse {
        std.log.err("{s} ({s})", .{ @tagName(AppErrorCode.MISSING_VALUE), @tagName(variable) });
        return error.MissingEnvironmentVariable;
    };
}

pub fn nonEmpty(comptime variable: EnvVariable, value: []const u8) ParseEnvError![]const u8 {
    if (std.mem.trim(u8, value, " \t\r\n").len == 0) {
        std.log.err("{s} ({s})", .{ @tagName(AppErrorCode.EMPTY_VALUE), @tagName(variable) });
        return error.InvalidEnvironmentVariable;
    }

    return value;
}

pub fn ipv4(comptime variable: EnvVariable, value: []const u8) ParseEnvError!void {
    var parts = std.mem.splitScalar(u8, value, '.');
    var count: usize = 0;

    while (parts.next()) |part| {
        count += 1;
        if (count > 4 or part.len == 0 or part.len > 3) {
            logInvalidValue(variable);
            return error.InvalidEnvironmentVariable;
        }

        for (part) |char| {
            if (!std.ascii.isDigit(char)) {
                logInvalidValue(variable);
                return error.InvalidEnvironmentVariable;
            }
        }

        _ = std.fmt.parseInt(u8, part, 10) catch {
            logInvalidValue(variable);
            return error.InvalidEnvironmentVariable;
        };
    }

    if (count != 4) {
        logInvalidValue(variable);
        return error.InvalidEnvironmentVariable;
    }
}

pub fn portRange(comptime variable: EnvVariable, value: u32) ParseEnvError!void {
    if (value > 65535) {
        logInvalidValue(variable);
        return error.InvalidEnvironmentVariable;
    }
}

pub fn logInvalidValue(comptime variable: EnvVariable) void {
    std.log.err("{s} ({s})", .{
        @tagName(AppErrorCode.INVALID_VALUE),
        @tagName(variable),
    });
}
