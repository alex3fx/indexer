const std = @import("std");

pub fn lcAlloc(allocator: std.mem.Allocator, value: []const u8) ![]u8 {
    const result = try allocator.alloc(u8, value.len);

    for (value, result) |source, *target| {
        target.* = std.ascii.toLower(source);
    }

    return result;
}

pub fn lc(comptime value: []const u8) []const u8 {
    return Lowercase(value).bytes[0..value.len];
}

fn Lowercase(comptime value: []const u8) type {
    return struct {
        const bytes = blk: {
            var result: [value.len:0]u8 = undefined;

            for (value, 0..) |char, index| {
                result[index] = std.ascii.toLower(char);
            }

            result[value.len] = 0;
            break :blk result;
        };
    };
}
