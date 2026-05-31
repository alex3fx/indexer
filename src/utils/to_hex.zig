const std = @import("std");

const Allocator = std.mem.Allocator;

pub fn toHex(allocator: Allocator, value: anytype) ![]u8 {
    return std.fmt.allocPrint(allocator, "0x{x}", .{value});
}
