// Run metrics — collected during historical sync, printed and saved to JSON.
const std = @import("std");

fn nowNs() i64 {
    const linux = std.os.linux;
    var ts: linux.timespec = undefined;
    _ = linux.clock_gettime(.MONOTONIC, &ts);
    return ts.sec * 1_000_000_000 + ts.nsec;
}

pub const BlockMetric = struct {
    block_num:     u64,
    fbdr_ms:       f64,
    http_block_ms: f64,
    http_rcpt_ms:  f64,
    http_trc_ms:   f64,
};

pub const BatchMetric = struct {
    from_block:   u64,
    to_block:     u64,
    fbdr_ms:      f64,
    transform_ms: f64,
    tpt_ms:       f64,
    save_ms:      f64,
    total_ms:     f64,
    blocks:       usize,
    txs:          usize,
    logs:         usize,
    internal_txs: usize,
    contracts:    usize,
};

pub const Metrics = struct {
    blocks:  std.ArrayList(BlockMetric) = .empty,
    batches: std.ArrayList(BatchMetric) = .empty,

    pub fn deinit(self: *Metrics, gpa: std.mem.Allocator) void {
        self.blocks.deinit(gpa);
        self.batches.deinit(gpa);
    }

    pub fn print(self: *Metrics) void {
        var fbdr_sum: f64 = 0;
        var fbdr_max: f64 = 0;
        var http_sum: f64 = 0;
        var tpt_total: f64 = 0;
        var save_total: f64 = 0;
        var total_rows: usize = 0;

        for (self.blocks.items) |b| {
            fbdr_sum += b.fbdr_ms;
            if (b.fbdr_ms > fbdr_max) fbdr_max = b.fbdr_ms;
            http_sum += (b.http_block_ms + b.http_rcpt_ms + b.http_trc_ms) / 3.0;
        }
        for (self.batches.items) |b| {
            tpt_total  += b.tpt_ms;
            save_total += b.save_ms;
            total_rows += b.txs + b.logs + b.internal_txs + b.contracts + b.blocks;
        }
        const n = @as(f64, @floatFromInt(self.blocks.items.len));
        const fbdr_avg = if (n > 0) fbdr_sum / n else 0;
        const http_avg = if (n > 0) http_sum / n else 0;
        const tpt_avg  = if (n > 0) tpt_total / n else 0;

        std.debug.print("\n📊 Zig2 Parser Results ({d} blocks, {d} batches)\n",
            .{ self.blocks.items.len, self.batches.items.len });
        std.debug.print("  FBDR avg/block  : {d:.1} ms\n", .{fbdr_avg});
        std.debug.print("  FBDR max        : {d:.1} ms\n", .{fbdr_max});
        std.debug.print("  HTTP avg/req    : {d:.1} ms\n", .{http_avg});
        std.debug.print("  TPT total       : {d:.0} ms\n", .{tpt_total});
        std.debug.print("  TPT avg/block   : {d:.1} ms\n", .{tpt_avg});
        std.debug.print("  Save total      : {d:.0} ms\n", .{save_total});
        std.debug.print("  Total rows      : {d}\n",       .{total_rows});
    }

    pub fn saveJson(self: *Metrics, gpa: std.mem.Allocator, io: std.Io, results_dir: []const u8) void {
        self.writeJsonFile(gpa, io, results_dir) catch |err| {
            std.debug.print("Warning: could not save results JSON: {}\n", .{err});
        };
    }

    fn writeJsonFile(self: *Metrics, gpa: std.mem.Allocator, io: std.Io, results_dir: []const u8) !void {
        var fbdr_sum: f64 = 0; var fbdr_max: f64 = 0; var http_sum: f64 = 0;
        var tpt_total: f64 = 0; var save_total: f64 = 0; var total_rows: usize = 0;
        for (self.blocks.items) |b| {
            fbdr_sum += b.fbdr_ms;
            if (b.fbdr_ms > fbdr_max) fbdr_max = b.fbdr_ms;
            http_sum += (b.http_block_ms + b.http_rcpt_ms + b.http_trc_ms) / 3.0;
        }
        for (self.batches.items) |b| {
            tpt_total += b.tpt_ms; save_total += b.save_ms;
            total_rows += b.txs + b.logs + b.internal_txs + b.contracts + b.blocks;
        }
        const n = @as(f64, @floatFromInt(self.blocks.items.len));
        const fbdr_avg = if (n > 0) fbdr_sum / n else 0;
        const http_avg = if (n > 0) http_sum / n else 0;
        const tpt_avg  = if (n > 0) tpt_total / n else 0;

        const ts_ms = @divTrunc(nowNs(), 1_000_000);
        var aw = std.Io.Writer.Allocating.init(gpa);
        defer aw.deinit();
        const w = &aw.writer;
        try w.print(
            \\{{"run_id":"zig2_{d}","timestamp_ms":{d},"summary":{{
            \\"total_blocks":{d},"total_batches":{d},
            \\"fbdr_avg_ms":{d:.1},"fbdr_max_ms":{d:.1},
            \\"http_avg_ms":{d:.1},
            \\"tpt_total_ms":{d:.0},"tpt_avg_block_ms":{d:.1},
            \\"save_total_ms":{d:.0},"total_rows":{d}
            \\}}}}
        , .{ ts_ms, ts_ms, self.blocks.items.len, self.batches.items.len,
             fbdr_avg, fbdr_max, http_avg, tpt_total, tpt_avg, save_total, total_rows });

        const json_data = aw.written();
        const filename = try std.fmt.allocPrint(gpa, "zigtest2_{d}.json", .{ts_ms});
        defer gpa.free(filename);
        const path = try std.fs.path.join(gpa, &.{ results_dir, filename });
        defer gpa.free(path);

        std.Io.Dir.createDirAbsolute(io, results_dir, .default_dir) catch {};
        const out_file = try std.Io.Dir.createFileAbsolute(io, path, .{});
        defer out_file.close(io);
        try out_file.writeStreamingAll(io, json_data);
        std.debug.print("Results: {s}\n", .{path});
    }
};
