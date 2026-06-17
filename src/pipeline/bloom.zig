// Lock-free probabilistic "have we ever seen this address as an ERC-20 candidate?"
// filter. Sized for ETH-scale contract counts (~100M addresses total, a much
// smaller subset are ERC-20 candidates). Inserts/lookups are plain atomic
// byte ops — safe to call concurrently from all transform worker threads
// with no locking, at the cost of eventual (not immediate) cross-thread
// visibility of the very latest insert, which is acceptable here: a missed
// "touch" this block is simply caught on the contract's next touch, or by
// the maintenance rescan.
const std = @import("std");

const Allocator = std.mem.Allocator;

pub const Bloom = struct {
    bits: []u8,

    const BLOOM_BITS: usize = 1 << 26; // 64Mbit = 8MiB
    const BLOOM_BYTES: usize = BLOOM_BITS / 8;
    const NUM_HASHES: usize = 3;

    pub fn init(allocator: Allocator) !Bloom {
        const bits = try allocator.alloc(u8, BLOOM_BYTES);
        @memset(bits, 0);
        return .{ .bits = bits };
    }

    pub fn deinit(self: *Bloom, allocator: Allocator) void {
        allocator.free(self.bits);
        self.* = undefined;
    }

    fn fnv1a64(addr: []const u8, seed: u64) u64 {
        var h: u64 = seed ^ 0xcbf29ce484222325;
        for (addr) |b| {
            h ^= b;
            h *%= 0x100000001b3;
        }
        return h;
    }

    pub fn insert(self: *Bloom, addr: []const u8) void {
        inline for (0..NUM_HASHES) |i| {
            const bit = fnv1a64(addr, i) % BLOOM_BITS;
            const mask: u8 = @as(u8, 1) << @intCast(bit % 8);
            _ = @atomicRmw(u8, &self.bits[bit / 8], .Or, mask, .monotonic);
        }
    }

    pub fn mightContain(self: *const Bloom, addr: []const u8) bool {
        inline for (0..NUM_HASHES) |i| {
            const bit = fnv1a64(addr, i) % BLOOM_BITS;
            const mask: u8 = @as(u8, 1) << @intCast(bit % 8);
            const v = @atomicLoad(u8, &self.bits[bit / 8], .monotonic);
            if (v & mask == 0) return false;
        }
        return true;
    }
};
