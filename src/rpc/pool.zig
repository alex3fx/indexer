// Persistent 3-thread pool for parallel RPC requests in realtime mode.
// Uses Linux pipes for work/done signaling (same pattern as pipeline ResultChan).
// Eliminates std.Thread.spawn overhead (~6ms) per block.
const std   = @import("std");
const linux = std.os.linux;

const core       = @import("indexer/core");
const client_mod = @import("client.zig");

const Allocator        = std.mem.Allocator;
const FetchClient      = core.fetch.Client;
const EvmRpcNodeConfig = core.structures.EvmRpcNodeConfig;
const Response         = client_mod.Response;
const Method           = client_mod.Method;

// Work item: main → worker. Worker frees number and the Work itself.
const Work = struct {
    allocator: Allocator,
    client:    *FetchClient,
    method:    Method,
    rpcNode:   EvmRpcNodeConfig,
    number:    []const u8,
};

// Result: worker → main. Main frees via allocator + destroy.
const WorkResult = struct {
    allocator: Allocator,
    result:    anyerror!?Response,
};

fn pipeWrite8(fd: i32, val: usize) void {
    var v = val;
    var total: usize = 0;
    const buf: [*]const u8 = @ptrCast(&v);
    while (total < 8) {
        const n = linux.write(fd, buf + total, 8 - total);
        if (@as(isize, @bitCast(n)) <= 0) break;
        total += n;
    }
}

fn pipeRead8(fd: i32) ?usize {
    var v: usize = 0;
    var total: usize = 0;
    const buf: [*]u8 = @ptrCast(&v);
    while (total < 8) {
        const n = linux.read(fd, buf + total, 8 - total);
        if (n == 0 or @as(isize, @bitCast(n)) < 0) return null;
        total += n;
    }
    return v;
}

const Slot = struct {
    work_rd: i32,
    work_wr: i32,
    done_rd: i32,
    done_wr: i32,
    thread:  std.Thread,
};

pub const HttpPool = struct {
    slots: [3]Slot,
    gpa:   Allocator,
    io:    std.Io,

    /// Step 1: create pipes only. Does NOT spawn threads.
    /// Call startThreads() after the HttpPool is at its final memory location.
    pub fn init(gpa: Allocator, io: std.Io) !HttpPool {
        var self = HttpPool{
            .slots = undefined,
            .gpa   = gpa,
            .io    = io,
        };
        var n: usize = 0;
        errdefer for (0..n) |i| closeSlot(&self.slots[i]);
        for (0..3) |i| {
            var wfds: [2]i32 = undefined;
            var dfds: [2]i32 = undefined;
            if (linux.pipe(&wfds) != 0) return error.PipeFailed;
            if (linux.pipe(&dfds) != 0) {
                _ = linux.close(wfds[0]); _ = linux.close(wfds[1]);
                return error.PipeFailed;
            }
            self.slots[i] = .{
                .work_rd = wfds[0], .work_wr = wfds[1],
                .done_rd = dfds[0], .done_wr = dfds[1],
                .thread  = undefined,
            };
            n += 1;
        }
        return self;
    }

    /// Step 2: spawn worker threads. Must be called when *self is stable in memory.
    pub fn startThreads(self: *HttpPool) !void {
        for (&self.slots) |*slot| {
            slot.thread = try std.Thread.spawn(
                .{}, slotWorkerFn, .{slot, self.gpa, self.io});
        }
    }

    pub fn deinit(self: *HttpPool) void {
        for (&self.slots) |*slot| {
            _ = linux.close(slot.work_wr);  // EOF → worker exits
            slot.thread.join();
            _ = linux.close(slot.work_rd);
            _ = linux.close(slot.done_rd);
            _ = linux.close(slot.done_wr);
        }
    }

    /// Submit 3 parallel RPC requests and return their results.
    pub fn requestThree(
        self:      *HttpPool,
        allocator: Allocator,
        clients:   [3]*FetchClient,
        methods:   [3]Method,
        rpcNode:   EvmRpcNodeConfig,
        number:    []const u8,
    ) ![3](anyerror!?Response) {
        // Submit work to all 3 slots.
        for (&self.slots, 0..) |*slot, i| {
            const num_copy = try allocator.dupe(u8, number);
            errdefer allocator.free(num_copy);
            const work = try allocator.create(Work);
            work.* = .{
                .allocator = allocator,
                .client    = clients[i],
                .method    = methods[i],
                .rpcNode   = rpcNode,
                .number    = num_copy,
            };
            pipeWrite8(slot.work_wr, @intFromPtr(work));
        }

        // Collect results from all 3 slots.
        var results: [3](anyerror!?Response) = undefined;
        for (&self.slots, 0..) |*slot, i| {
            const ptr = pipeRead8(slot.done_rd) orelse return error.WorkerExited;
            const res: *WorkResult = @ptrFromInt(ptr);
            results[i] = res.result;
            res.allocator.destroy(res);
        }
        return results;
    }
};

fn closeSlot(slot: *Slot) void {
    _ = linux.close(slot.work_rd);
    _ = linux.close(slot.work_wr);
    _ = linux.close(slot.done_rd);
    _ = linux.close(slot.done_wr);
}

fn slotWorkerFn(slot: *Slot, _gpa: Allocator, _io: std.Io) void {
    _ = _gpa;
    _ = _io;

    while (true) {
        const ptr = pipeRead8(slot.work_rd) orelse break;
        const work: *Work = @ptrFromInt(ptr);
        const alloc = work.allocator;

        const r = client_mod.requestSync(alloc, work.client, work.method, .{
            .rpcNode = work.rpcNode,
            .number  = work.number,
        });
        alloc.free(work.number);
        alloc.destroy(work);

        const res = alloc.create(WorkResult) catch {
            // Out of memory: can't signal result cleanly. Close done_wr to signal failure.
            _ = linux.close(slot.done_wr);
            break;
        };
        res.* = .{ .allocator = alloc, .result = r };
        pipeWrite8(slot.done_wr, @intFromPtr(res));
    }
}
