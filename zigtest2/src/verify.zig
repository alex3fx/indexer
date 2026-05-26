// Verification mode (VERIFY=1): scans block_completions for gaps in FROM_BLOCK..TO_BLOCK.
// A row in block_completions exists only after all 6 data tables have been written.
// Missing rows indicate blocks that were never fully indexed.
//
// Two-level strategy:
//   1. Fast: SELECT count(*) per chunk — O(1) per chunk, no paging.
//      If count matches expected → chunk complete → skip.
//   2. Detail: paged SELECT block_number for mismatched chunks — finds exact gaps.
//      Pages through all rows (5000/page) so large chunks (656K rows) work correctly.
//
// Usage:
//   VERIFY=1 FROM_BLOCK=0 TO_BLOCK=21000000 <connection env vars> ./indexer
//
// Exit code: 0 = all blocks present, 1 = gaps found.
const std    = @import("std");
const db     = @import("db");
const config = @import("config");

// ─── CQL short-string skip helper ────────────────────────────────────────────

fn skipCqlShortString(body: []const u8, pos: usize) !usize {
    if (pos + 2 > body.len) return error.ShortFrame;
    const l = std.mem.readInt(u16, body[pos..][0..2], .big);
    const end = pos + 2 + l;
    if (end > body.len) return error.ShortFrame;
    return end;
}

// ─── Parse SELECT count(*) result ────────────────────────────────────────────

fn parseCountResult(body: []const u8) !u64 {
    var pos: usize = 0;
    if (pos + 4 > body.len) return error.ShortFrame;
    const kind = std.mem.readInt(i32, body[pos..][0..4], .big); pos += 4;
    if (kind != 2) return 0;

    if (pos + 4 > body.len) return error.ShortFrame;
    const flags = std.mem.readInt(u32, body[pos..][0..4], .big); pos += 4;
    const global_tables_spec = (flags & 0x0001) != 0;
    const has_more_pages     = (flags & 0x0002) != 0;
    const no_metadata        = (flags & 0x0004) != 0;

    if (pos + 4 > body.len) return error.ShortFrame;
    const columns_count: usize = @intCast(@max(0, std.mem.readInt(i32, body[pos..][0..4], .big))); pos += 4;

    if (has_more_pages) {
        if (pos + 4 > body.len) return error.ShortFrame;
        const ps_len = std.mem.readInt(i32, body[pos..][0..4], .big); pos += 4;
        if (ps_len > 0) {
            if (pos + @as(usize, @intCast(ps_len)) > body.len) return error.ShortFrame;
            pos += @intCast(ps_len);
        }
    }

    if (!no_metadata) {
        if (global_tables_spec) {
            pos = try skipCqlShortString(body, pos);
            pos = try skipCqlShortString(body, pos);
        }
        for (0..columns_count) |_| {
            if (!global_tables_spec) {
                pos = try skipCqlShortString(body, pos);
                pos = try skipCqlShortString(body, pos);
            }
            pos = try skipCqlShortString(body, pos);
            if (pos + 2 > body.len) return error.ShortFrame;
            pos += 2;
        }
    }

    if (pos + 4 > body.len) return error.ShortFrame;
    const rows_count = std.mem.readInt(i32, body[pos..][0..4], .big); pos += 4;
    if (rows_count <= 0) return 0;

    if (pos + 4 > body.len) return error.ShortFrame;
    const val_len = std.mem.readInt(i32, body[pos..][0..4], .big); pos += 4;
    if (val_len != 8) return error.UnexpectedCountValueSize;
    if (pos + 8 > body.len) return error.ShortFrame;
    const count = std.mem.readInt(i64, body[pos..][0..8], .big);
    return if (count < 0) 0 else @intCast(count);
}

// ─── Parse one page of SELECT block_number results ───────────────────────────

const ParsedPage = struct {
    block_numbers: []i64,  // sorted ascending; caller owns
    paging_state:  ?[]u8,  // null = last page; caller owns when not null
};

fn parsePage(gpa: std.mem.Allocator, body: []const u8) !ParsedPage {
    var pos: usize = 0;

    if (pos + 4 > body.len) return error.ShortFrame;
    const kind = std.mem.readInt(i32, body[pos..][0..4], .big); pos += 4;
    if (kind != 2) return ParsedPage{
        .block_numbers = try gpa.alloc(i64, 0),
        .paging_state  = null,
    };

    if (pos + 4 > body.len) return error.ShortFrame;
    const flags = std.mem.readInt(u32, body[pos..][0..4], .big); pos += 4;
    const global_tables_spec = (flags & 0x0001) != 0;
    const has_more_pages     = (flags & 0x0002) != 0;
    const no_metadata        = (flags & 0x0004) != 0;

    if (pos + 4 > body.len) return error.ShortFrame;
    const columns_count: usize = @intCast(@max(0, std.mem.readInt(i32, body[pos..][0..4], .big))); pos += 4;

    var paging_state: ?[]u8 = null;
    errdefer if (paging_state) |ps| gpa.free(ps);

    if (has_more_pages) {
        if (pos + 4 > body.len) return error.ShortFrame;
        const ps_len = std.mem.readInt(i32, body[pos..][0..4], .big); pos += 4;
        if (ps_len > 0) {
            const ps_ulen: usize = @intCast(ps_len);
            if (pos + ps_ulen > body.len) return error.ShortFrame;
            paging_state = try gpa.dupe(u8, body[pos..][0..ps_ulen]);
            pos += ps_ulen;
        }
    }

    if (!no_metadata) {
        if (global_tables_spec) {
            pos = try skipCqlShortString(body, pos);
            pos = try skipCqlShortString(body, pos);
        }
        for (0..columns_count) |_| {
            if (!global_tables_spec) {
                pos = try skipCqlShortString(body, pos);
                pos = try skipCqlShortString(body, pos);
            }
            pos = try skipCqlShortString(body, pos);
            if (pos + 2 > body.len) return error.ShortFrame;
            pos += 2;
        }
    }

    if (pos + 4 > body.len) return error.ShortFrame;
    const rows_count = std.mem.readInt(i32, body[pos..][0..4], .big); pos += 4;
    if (rows_count <= 0) return ParsedPage{
        .block_numbers = try gpa.alloc(i64, 0),
        .paging_state  = paging_state,
    };

    const count: usize = @intCast(rows_count);
    var nums = try gpa.alloc(i64, count);
    errdefer gpa.free(nums);

    for (0..count) |i| {
        for (0..columns_count) |col| {
            if (pos + 4 > body.len) return error.ShortFrame;
            const val_len = std.mem.readInt(i32, body[pos..][0..4], .big); pos += 4;
            if (val_len < 0) continue;
            const vlen: usize = @intCast(val_len);
            if (pos + vlen > body.len) return error.ShortFrame;
            const vdata = body[pos..][0..vlen]; pos += vlen;
            if (col == 0 and vlen == 8) nums[i] = std.mem.readInt(i64, vdata[0..8], .big);
        }
    }
    return ParsedPage{ .block_numbers = nums, .paging_state = paging_state };
}

// ─── Expected block count per chunk ──────────────────────────────────────────

fn expectedCountRemap(chunk: u64, remap_mod: u64, from: u64, to: u64) u64 {
    const first = blk: {
        const r = from % remap_mod;
        if (r == chunk) break :blk from;
        const delta = (chunk + remap_mod - r) % remap_mod;
        break :blk from + delta;
    };
    if (first > to) return 0;
    return (to - first) / remap_mod + 1;
}

fn expectedCountClassic(chunk: u64, chunk_size: u64, from: u64, to: u64) u64 {
    const chunk_from = @max(chunk * chunk_size, from);
    const chunk_to   = @min((chunk + 1) * chunk_size - 1, to);
    if (chunk_from > chunk_to) return 0;
    return chunk_to - chunk_from + 1;
}

// ─── Gap reporting ────────────────────────────────────────────────────────────

const MAX_LISTED_MISSING: usize = 30;

fn reportMissing(block: u64, count: *u64) void {
    count.* += 1;
    if (count.* <= MAX_LISTED_MISSING) {
        std.debug.print("  [MISSING] block {d}\n", .{block});
    } else if (count.* == MAX_LISTED_MISSING + 1) {
        std.debug.print("  ... (more missing blocks not listed)\n", .{});
    }
}

// ─── Fast COUNT check ─────────────────────────────────────────────────────────

fn countChunk(gpa: std.mem.Allocator, conn: *db.CqlConn, chunk: i64, from: u64, to: u64) !u64 {
    const q = try std.fmt.allocPrint(gpa,
        "SELECT count(*) FROM block_completions WHERE chunk={d} AND block_number>={d} AND block_number<={d}",
        .{ chunk, from, to });
    defer gpa.free(q);
    const body = try conn.queryRaw(q);
    defer gpa.free(body);
    return parseCountResult(body);
}

// ─── Paged gap scan ───────────────────────────────────────────────────────────
// q_from/q_to: exact block range for this chunk (already clipped to overall from..to).
// Handles all pages internally; on page error, counts remaining blocks as missing.

fn findGapsForChunk(
    gpa:    std.mem.Allocator,
    conn:   *db.CqlConn,
    chunk:  i64,
    q_from: u64,
    q_to:   u64,
    cfg:    *const config.Config,
    missing: *u64,
) void {
    const PAGE_SIZE: i32 = 5000;
    const step: u64 = if (cfg.remap_mod > 0) cfg.remap_mod else 1;
    const c: u64 = @intCast(chunk);

    var expected: u64 = if (cfg.remap_mod > 0) blk: {
        const r = q_from % cfg.remap_mod;
        if (r == c) break :blk q_from;
        const delta = (c + cfg.remap_mod - r) % cfg.remap_mod;
        break :blk q_from + delta;
    } else q_from;

    if (expected > q_to) return;

    var cur_ps: ?[]u8 = null;
    var done = false;

    while (!done) {
        const q = std.fmt.allocPrint(gpa,
            "SELECT block_number FROM block_completions" ++
            " WHERE chunk={d} AND block_number>={d} AND block_number<={d}",
            .{ chunk, q_from, q_to }) catch {
            if (cur_ps) |ps| gpa.free(ps);
            break;
        };
        defer gpa.free(q);

        const body = conn.queryRawPaged(q, PAGE_SIZE, cur_ps) catch |err| {
            std.debug.print("[WARN] chunk {d} page query failed: {s}\n", .{ chunk, @errorName(err) });
            if (cur_ps) |ps| gpa.free(ps);
            cur_ps = null;
            break;
        };
        defer gpa.free(body);

        const page = parsePage(gpa, body) catch |err| {
            std.debug.print("[WARN] chunk {d} page parse failed: {s}\n", .{ chunk, @errorName(err) });
            if (cur_ps) |ps| gpa.free(ps);
            cur_ps = null;
            break;
        };

        for (page.block_numbers) |bn_i64| {
            if (bn_i64 < 0) continue;
            const bn: u64 = @intCast(bn_i64);
            while (expected < bn and expected <= q_to) : (expected += step) {
                reportMissing(expected, missing);
            }
            if (expected == bn) expected += step;
        }
        gpa.free(page.block_numbers);

        if (cur_ps) |ps| gpa.free(ps);
        cur_ps = page.paging_state;
        if (cur_ps == null) done = true;
    }

    // Count any trailing blocks (including all remaining blocks if scan errored).
    while (expected <= q_to) : (expected += step) {
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
            const exp = expectedCountRemap(chunk, cfg.remap_mod, from, to);
            if (exp == 0) continue;

            const got = countChunk(gpa, &conn, @intCast(chunk), from, to) catch |err| {
                std.debug.print("[ERROR] chunk {d} count query failed: {s}\n", .{ chunk, @errorName(err) });
                missing += exp;
                continue;
            };
            if (got == exp) continue;

            std.debug.print("[GAP] chunk {d}: expected {d}, found {d}\n", .{ chunk, exp, got });
            findGapsForChunk(gpa, &conn, @intCast(chunk), from, to, cfg, &missing);
        }
    } else {
        const chunk_size = cfg.chunk_size;
        const from_chunk = from / chunk_size;
        const to_chunk   = to   / chunk_size;
        var chunk = from_chunk;
        while (chunk <= to_chunk) : (chunk += 1) {
            const exp = expectedCountClassic(chunk, chunk_size, from, to);
            if (exp == 0) continue;
            const chunk_from = @max(chunk * chunk_size, from);
            const chunk_to   = @min((chunk + 1) * chunk_size - 1, to);

            const got = countChunk(gpa, &conn, @intCast(chunk), chunk_from, chunk_to) catch |err| {
                std.debug.print("[ERROR] chunk {d} count query failed: {s}\n", .{ chunk, @errorName(err) });
                missing += exp;
                continue;
            };
            if (got == exp) continue;

            std.debug.print("[GAP] chunk {d}: expected {d}, found {d}\n", .{ chunk, exp, got });
            findGapsForChunk(gpa, &conn, @intCast(chunk), chunk_from, chunk_to, cfg, &missing);
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
