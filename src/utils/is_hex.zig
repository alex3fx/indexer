const std = @import("std");

pub fn isHex(value: []const u8) bool {
    if (value.len <= 2) return false;
    if (value[0] != '0' or std.ascii.toLower(value[1]) != 'x') return false;

    for (value[2..]) |char| {
        _ = switch (char) {
            '0'...'9', 'a'...'f', 'A'...'F' => {},
            else => return false,
        };
    }

    return true;
}
