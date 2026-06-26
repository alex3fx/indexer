// Bloom filter for sha256 hashes of deployed bytecodes.
// Used to avoid duplicate writes to bytecode_store: if the filter says
// "definitely not seen", the bytecode is new and must be stored. If it
// says "might have seen", we skip the bytecode_store INSERT (the row is
// already there — sha256 collisions are not a realistic concern).
//
// Sized for ~10M unique bytecodes: 128Mbit (16MiB), 5 hash functions →
// FPR ≈ 0.001% at 10M elements. On-disk persistence allows warm restart
// without a full bytecode_store scan.
//
// Thread safety: insert/mightContain use atomic byte ops — safe for
// concurrent transform workers (same model as bloom.zig for ERC-20).
const std = @import("std");
const Allocator = std.mem.Allocator;

pub const BytecodeBloom = struct {
    bits: []u8,

    const BLOOM_BITS: usize = 1 << 27; // 128Mbit = 16MiB
    const BLOOM_BYTES: usize = BLOOM_BITS / 8;
    const NUM_HASHES: usize = 5;

    pub fn init(allocator: Allocator) !BytecodeBloom {
        const bits = try allocator.alloc(u8, BLOOM_BYTES);
        @memset(bits, 0);
        return .{ .bits = bits };
    }

    pub fn deinit(self: *BytecodeBloom, allocator: Allocator) void {
        allocator.free(self.bits);
        self.* = undefined;
    }

    // The key is already a 32-byte cryptographic hash (sha256 output), so
    // extracting 8-byte sub-windows at different offsets gives independent
    // enough hash functions without any additional mixing.
    fn hashSlot(hash: *const [32]u8, comptime idx: usize) u64 {
        // Five non-overlapping 8-byte windows: offsets 0, 6, 12, 18, 24.
        const offset = idx * 6;
        return std.mem.readInt(u64, hash[offset..][0..8], .little);
    }

    pub fn insert(self: *BytecodeBloom, hash: *const [32]u8) void {
        inline for (0..NUM_HASHES) |i| {
            const bit = hashSlot(hash, i) % BLOOM_BITS;
            const mask: u8 = @as(u8, 1) << @intCast(bit % 8);
            _ = @atomicRmw(u8, &self.bits[bit / 8], .Or, mask, .monotonic);
        }
    }

    pub fn mightContain(self: *const BytecodeBloom, hash: *const [32]u8) bool {
        inline for (0..NUM_HASHES) |i| {
            const bit = hashSlot(hash, i) % BLOOM_BITS;
            const mask: u8 = @as(u8, 1) << @intCast(bit % 8);
            const v = @atomicLoad(u8, &self.bits[bit / 8], .monotonic);
            if (v & mask == 0) return false;
        }
        return true;
    }

    // Write raw bit array to file. Overwrites if exists.
    pub fn saveToFile(self: *const BytecodeBloom, path: []const u8) !void {
        const f = try std.fs.cwd().createFile(path, .{ .truncate = true });
        defer f.close();
        try f.writeAll(self.bits);
    }

    // Load from file. If missing or wrong size, returns a zeroed (empty) bloom.
    pub fn loadFromFile(allocator: Allocator, path: []const u8) !BytecodeBloom {
        const bits = try allocator.alloc(u8, BLOOM_BYTES);
        errdefer allocator.free(bits);
        const f = std.fs.cwd().openFile(path, .{}) catch |e| switch (e) {
            error.FileNotFound => {
                @memset(bits, 0);
                return .{ .bits = bits };
            },
            else => return e,
        };
        defer f.close();
        const n = try f.readAll(bits);
        if (n != BLOOM_BYTES) @memset(bits, 0); // wrong size — treat as fresh
        return .{ .bits = bits };
    }
};
