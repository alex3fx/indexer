// Verification mode (VERIFY=1): scans block_completions for gaps in FROM_BLOCK..TO_BLOCK.
// A row in block_completions exists only after all 6 data tables have been written.
// Missing rows indicate blocks that were never fully indexed.
//
// Usage:
//   VERIFY=1 FROM_BLOCK=0 TO_BLOCK=21000000 <connection env vars> ./indexer
//
// Exit code: 0 = all blocks present, 1 = gaps found.
const std    = @import("std");
const db     = @import("db");
const config = @import("config");

// One row from block_completions.
pub const CompletionEntry = struct {
    block_number:   i64 = 0,
    tx_count:       i32 = 0,
    log_count:      i32 = 0,
    itx_count:      i32 = 0,
    contract_count: i32 = 0,
};

// ─── Minimal CQL RESULT frame parser ─────────────────────────────────────────
// Parses: SELECT block_number,tx_count,log_count,itx_count,contract_count FROM block_completions
// Returns rows sorted by block_number ASC (ScyllaDB clustering order guarantee).

fn skipCqlShortString(body: []const u8, pos: usize) !usize {
    if (pos + 2 > body.len) return error.ShortFrame;
    const l = std.mem.readInt(u16, body[pos..][0..2], .big);
    const end = pos + 2 + l;
    if (end > body.len) return error.ShortFrame;
    return end;
}

fn parseCompletionRows(gpa: std.mem.Allocator, body: []const u8) ![]CompletionEntry {
    var pos: usize = 0;

    // kind (int32): 2 = rows
    if (pos + 4 > body.len) return error.ShortFrame;
    const kind = std.mem.readInt(i32, body[pos..][0..4], .big); pos += 4;
    if (kind != 2) return try gpa.alloc(CompletionEntry, 0);

    // flags (int32)
    if (pos + 4 > body.len) return error.ShortFrame;
    const flags = std.mem.readInt(u32, body[pos..][0..4], .big); pos += 4;
    const global_tables_spec = (flags & 0x0001) != 0;
    const no_metadata        = (flags & 0x0004) != 0;

    // columns_count (int32)
    if (pos + 4 > body.len) return error.ShortFrame;
    const columns_count: usize = @intCast(@max(0, std.mem.readInt(i32, body[pos..][0..4], .big))); pos += 4;

    // skip metadata
    if (!no_metadata) {
        if (global_tables_spec) {
            pos = try skipCqlShortString(body, pos); // keyspace
            pos = try skipCqlShortString(body, pos); // table
        }
        for (0..columns_count) |_| {
            if (!global_tables_spec) {
                pos = try skipCqlShortString(body, pos);
                pos = try skipCqlShortString(body, pos);
            }
            pos = try skipCqlShortString(body, pos); // col name
            if (pos + 2 > body.len) return error.ShortFrame;
            pos += 2; // [short] type option id
        }
    }

    // rows_count (int32)
    if (pos + 4 > body.len) return error.ShortFrame;
    const rows_count = std.mem.readInt(i32, body[pos..][0..4], .big); pos += 4;
    if (rows_count <= 0) return try gpa.alloc(CompletionEntry, 0);

    var entries = try gpa.alloc(CompletionEntry, @intCast(rows_count));
    errdefer gpa.free(entries);

    for (0..@intCast(rows_count)) |row| {
        entries[row] = .{};
        for (0..columns_count) |col| {
            if (pos + 4 > body.len) return error.ShortFrame;
            const val_len = std.mem.readInt(i32, body[pos..][0..4], .big); pos += 4;
            if (val_len < 0) continue; // null
            const vlen: usize = @intCast(val_len);
            if (pos + vlen > body.len) return error.ShortFrame;
            const vdata = body[pos..][0..vlen]; pos += vlen;
            switch (col) {
                0 => if (vlen == 8) { entries[row].block_number   = std.mem.readInt(i64, vdata[0..8], .big); },
                1 => if (vlen == 4) { entries[row].tx_count       = std.mem.readInt(i32, vdata[0..4], .big); },
                2 => if (vlen == 4) { entries[row].log_count      = std.mem.readInt(i32, vdata[0..4], .big); },
                3 => if (vlen == 4) { entries[row].itx_count      = std.mem.readInt(i32, vdata[0..4], .big); },
                4 => if (vlen == 4) { entries[row].contract_count = std.mem.readInt(i32, vdata[0..4], .big); },
                else => {},
            }
        }
    }
    return entries;
}

// ─── Per-chunk query ─────────────────────────────────────────────────────────

fn queryChunk(
    gpa:   std.mem.Allocator,
    conn:  *db.CqlConn,
    chunk: i64,
    from:  u64,
    to:    u64,
) ![]CompletionEntry {
    const q = try std.fmt.allocPrint(gpa,
        "SELECT block_number,tx_count,log_count,itx_count,contract_count" ++
        " FROM block_completions WHERE chunk={d} AND block_number>={d} AND block_number<={d}",
        .{ chunk, from, to });
    defer gpa.free(q);

    const body = try conn.queryRaw(q);
    defer gpa.free(body);

    return parseCompletionRows(gpa, body);
}

// ─── Gap-finding helpers ──────────────────────────────────────────────────────

const MAX_LISTED_MISSING: usize = 30;

fn reportMissing(block: u64, count: *u64) void {
    count.* += 1;
    if (count.* <= MAX_LISTED_MISSING) {
        std.debug.print("  [MISSING] block {d}\n", .{block});
    } else if (count.* == MAX_LISTED_MISSING + 1) {
        std.debug.print("  ... (more missing blocks not listed)\n", .{});
    }
}

// Check one chunk in remap_mod mode: chunk c contains blocks where block % remap_mod == c.
// entries must be sorted ascending by block_number (ScyllaDB guarantees this).
fn checkChunkRemap(
    entries:   []const CompletionEntry,
    chunk:     u64,
    remap_mod: u64,
    from:      u64,
    to:        u64,
    missing:   *u64,
) void {
    // First expected block in this chunk >= from
    var expected: u64 = blk: {
        const r = from % remap_mod;
        if (r == chunk) break :blk from;
        const delta = (chunk + remap_mod - r) % remap_mod;
        break :blk from + delta;
    };
    if (expected > to) return;

    for (entries) |e| {
        if (e.block_number < 0) continue;
        const bn: u64 = @intCast(e.block_number);
        // Count all expected blocks before this entry
        while (expected < bn and expected <= to) : (expected += remap_mod) {
            reportMissing(expected, missing);
        }
        if (expected == bn) expected += remap_mod;
    }
    // Count trailing missing blocks after last entry
    while (expected <= to) : (expected += remap_mod) {
        reportMissing(expected, missing);
    }
}

// Check one chunk in classic mode: chunk c contains blocks c*chunk_size..(c+1)*chunk_size-1.
fn checkChunkClassic(
    entries:    []const CompletionEntry,
    chunk_from: u64,
    chunk_to:   u64,
    missing:    *u64,
) void {
    var expected: u64 = chunk_from;
    for (entries) |e| {
        if (e.block_number < 0) continue;
        const bn: u64 = @intCast(e.block_number);
        while (expected < bn and expected <= chunk_to) : (expected += 1) {
            reportMissing(expected, missing);
        }
        if (expected == bn) expected += 1;
    }
    while (expected <= chunk_to) : (expected += 1) {
        reportMissing(expected, missing);
    }
}

// ─── Main entry point ─────────────────────────────────────────────────────────

pub fn runVerify(
    io:   std.Io,
    gpa:  std.mem.Allocator,
    cfg:  *const config.Config,
    from: u64,
    to:   u64,
) !void {
    const total = to - from + 1;
    std.debug.print(
        "Verifying {d} blocks ({d}..{d})  mode={s}\n\n",
        .{ total, from, to, if (cfg.remap_mod > 0) "remap" else "linear" },
    );

    var conn = try db.CqlConn.init(io, gpa, cfg.scylla_host, cfg.scylla_port,
        cfg.scylla_keyspace, cfg.scylla_user, cfg.scylla_pass);
    defer conn.deinit();

    var missing: u64 = 0;

    if (cfg.remap_mod > 0) {
        var chunk: u64 = 0;
        while (chunk < cfg.remap_mod) : (chunk += 1) {
            const entries = queryChunk(gpa, &conn, @intCast(chunk), from, to) catch |err| {
                std.debug.print("[ERROR] chunk {d} query failed: {s}\n", .{ chunk, @errorName(err) });
                // Count all expected blocks in this chunk as missing
                var e: u64 = blk: {
                    const r = from % cfg.remap_mod;
                    if (r == chunk) break :blk from;
                    const delta = (chunk + cfg.remap_mod - r) % cfg.remap_mod;
                    break :blk from + delta;
                };
                while (e <= to) : (e += cfg.remap_mod) missing += 1;
                continue;
            };
            defer gpa.free(entries);
            checkChunkRemap(entries, chunk, cfg.remap_mod, from, to, &missing);
        }
    } else {
        const chunk_size = cfg.chunk_size;
        const from_chunk = from / chunk_size;
        const to_chunk   = to   / chunk_size;
        var chunk = from_chunk;
        while (chunk <= to_chunk) : (chunk += 1) {
            const chunk_from = @max(chunk * chunk_size, from);
            const chunk_to   = @min((chunk + 1) * chunk_size - 1, to);
            const entries = queryChunk(gpa, &conn, @intCast(chunk), chunk_from, chunk_to) catch |err| {
                std.debug.print("[ERROR] chunk {d} query failed: {s}\n", .{ chunk, @errorName(err) });
                missing += chunk_to - chunk_from + 1;
                continue;
            };
            defer gpa.free(entries);
            checkChunkClassic(entries, chunk_from, chunk_to, &missing);
        }
    }

    std.debug.print("\n", .{});
    if (missing == 0) {
        std.debug.print("✅ OK — all {d} blocks indexed\n", .{total});
    } else {
        std.debug.print("❌ FAIL — {d}/{d} blocks missing from block_completions\n",
            .{ missing, total });
        return error.VerificationFailed;
    }
}
