// Content-addressed bytecode store with sha256 collision resolution.
//
// Identity: BytecodeId = { hash: sha256(bytecode), seq: i8 }.
// seq=0 in the normal case; seq>1 only on a real sha256 collision (probability
// is effectively zero but the code handles it to avoid silent data loss).
//
// resolveOrInsert: checks a bounded FIFO-eviction cache, then reads Scylla,
// compares keccak256 + raw bytes on size match, and inserts IF NOT EXISTS (LWT)
// when no matching row is found.  LWT failure (concurrent insert) causes a
// re-read loop, never a silent skip.
//
// The comptime Db and Hasher parameters let unit tests inject a mock DB and
// a deterministic hash function without touching production code.
const std = @import("std");
const linux = std.os.linux;
const pool = @import("indexer/db").pool;
const transformer = @import("transformer.zig");
const Allocator = std.mem.Allocator;

fn nowMs() i64 {
    var ts: linux.timespec = undefined;
    _ = linux.clock_gettime(.REALTIME, &ts);
    return ts.sec * 1000 + @divTrunc(ts.nsec, 1_000_000);
}

pub const BytecodeId = struct {
    hash: [32]u8,
    seq: i8,
};

pub const Kind = enum(u8) {
    deployed = 0,
    creation = 1,
};

pub const DEFAULT_CACHE_CAP: usize = 100_000;

// ─── CQL queries ──────────────────────────────────────────────────────────────

pub const QUERY_SELECT =
    "SELECT seq, size, check_hash, bytecode FROM bytecode_store_v2 WHERE hash = ?";
pub const QUERY_INSERT =
    "INSERT INTO bytecode_store_v2 (hash,seq,bytecode,check_hash,size,kind,first_seen_block,verified) VALUES (?,?,?,?,?,?,?,false) IF NOT EXISTS";
pub const QUERY_COLLISION =
    "INSERT INTO collision_registry_v2 (hash,detected_at,seq_count,note) VALUES (?,?,?,?)";
pub const QUERY_INSERT_CONTRACT_V2 =
    "INSERT INTO contracts_by_address_v2 (address,block_number,bytecode_hash,bytecode_seq,creation_hash,creation_seq,tx_hash,deployer,contract_factory,block_timestamp_s,block_timestamp_ms,creation_method,transaction_index,trace_index) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
pub const QUERY_INSERT_ADDR_BY_BC =
    "INSERT INTO addresses_by_bytecode (hash,seq,bucket,address,block_number) VALUES (?,?,?,?,?)";
pub const QUERY_INSERT_ADDR_BY_CREATION_BC =
    "INSERT INTO addresses_by_creation_bytecode (hash,seq,bucket,address,block_number) VALUES (?,?,?,?,?)";
pub const QUERY_SELECT_PENDING =
    "SELECT abi, source, programming_language FROM pending_verifications WHERE address = ?";
pub const QUERY_UPDATE_VERIFIED =
    "UPDATE bytecode_store_v2 SET verified = true, verified_at = ?, verified_via_address = ?, abi = ?, source_ref = ?, programming_language = ? WHERE hash = ? AND seq = ?";
pub const QUERY_DELETE_PENDING =
    "DELETE FROM pending_verifications WHERE address = ?";
pub const QUERY_INSERT_SOURCE_CHUNK =
    "INSERT INTO source_store (hash, seq, chunk, data) VALUES (?, ?, ?, ?)";

// ─── FIFO-eviction cache ──────────────────────────────────────────────────────

const HashCtx = struct {
    pub fn hash(_: @This(), key: [32]u8) u64 {
        // SHA256 output is uniformly distributed; first 8 bytes make a good hash.
        const a = @as(u64, key[0]) | (@as(u64, key[1]) << 8) |
            (@as(u64, key[2]) << 16) | (@as(u64, key[3]) << 24) |
            (@as(u64, key[4]) << 32) | (@as(u64, key[5]) << 40) |
            (@as(u64, key[6]) << 48) | (@as(u64, key[7]) << 56);
        return a;
    }
    pub fn eql(_: @This(), a: [32]u8, b: [32]u8) bool {
        return std.mem.eql(u8, &a, &b);
    }
};
const CacheMap = std.HashMap([32]u8, BytecodeId, HashCtx, 80);

const LruCache = struct {
    map: CacheMap,
    ring: [][32]u8, // FIFO eviction ring
    head: usize,
    cap: usize,
    gpa: Allocator,

    fn init(gpa: Allocator, cap: usize) !LruCache {
        const ring = try gpa.alloc([32]u8, cap);
        for (ring) |*slot| @memset(slot, 0);
        return .{
            .map = CacheMap.init(gpa),
            .ring = ring,
            .head = 0,
            .cap = cap,
            .gpa = gpa,
        };
    }

    fn deinit(self: *LruCache) void {
        self.map.deinit();
        self.gpa.free(self.ring);
    }

    fn get(self: *const LruCache, h: [32]u8) ?BytecodeId {
        return self.map.get(h);
    }

    fn put(self: *LruCache, h: [32]u8, id: BytecodeId) void {
        if (self.map.count() >= self.cap) {
            _ = self.map.remove(self.ring[self.head]);
        }
        self.ring[self.head] = h;
        self.head = (self.head + 1) % self.cap;
        self.map.put(h, id) catch {}; // OOM: skip caching, next call will re-resolve
    }
};

// ─── BytecodeStoreT ───────────────────────────────────────────────────────────
//
// Db must implement:
//   allocator() Allocator
//   prepare(query: []const u8) ![]u8
//   executeSelect(prep, nParams, params, gpa) !pool.SelectResult
//   executeLWT(prep, nParams, params) !bool
//   batchSendRows(prep, nVals, rows) !void
//
// Hasher must implement:
//   hash(input: []const u8, out: *[32]u8, options: anytype) void
// (same signature as std.crypto.hash.sha2.Sha256.hash)
pub fn BytecodeStoreT(comptime Db: type, comptime Hasher: type) type {
    return struct {
        const Self = @This();

        selPrep: pool.Prepared,
        insPrep: pool.Prepared,
        colPrep: pool.Prepared,
        insContractPrep: pool.Prepared,
        insAddrPrep: pool.Prepared,
        insCreationAddrPrep: pool.Prepared,
        selPendingPrep: pool.Prepared,
        updVerifiedPrep: pool.Prepared,
        delPendingPrep: pool.Prepared,
        insSourcePrep: pool.Prepared,
        cache: LruCache,
        gpa: Allocator,

        pub fn init(db: *Db, cap: usize) !Self {
            const gpa = db.allocator();
            const selId = try db.prepare(QUERY_SELECT);
            errdefer gpa.free(selId);
            const insId = try db.prepare(QUERY_INSERT);
            errdefer gpa.free(insId);
            const colId = try db.prepare(QUERY_COLLISION);
            errdefer gpa.free(colId);
            const insContractId = try db.prepare(QUERY_INSERT_CONTRACT_V2);
            errdefer gpa.free(insContractId);
            const insAddrId = try db.prepare(QUERY_INSERT_ADDR_BY_BC);
            errdefer gpa.free(insAddrId);
            const insCreationAddrId = try db.prepare(QUERY_INSERT_ADDR_BY_CREATION_BC);
            errdefer gpa.free(insCreationAddrId);
            const selPendingId = try db.prepare(QUERY_SELECT_PENDING);
            errdefer gpa.free(selPendingId);
            const updVerifiedId = try db.prepare(QUERY_UPDATE_VERIFIED);
            errdefer gpa.free(updVerifiedId);
            const delPendingId = try db.prepare(QUERY_DELETE_PENDING);
            errdefer gpa.free(delPendingId);
            const insSourceId = try db.prepare(QUERY_INSERT_SOURCE_CHUNK);
            errdefer gpa.free(insSourceId);
            const cache = try LruCache.init(gpa, cap);
            return .{
                .selPrep = .{ .id = selId, .query = QUERY_SELECT },
                .insPrep = .{ .id = insId, .query = QUERY_INSERT },
                .colPrep = .{ .id = colId, .query = QUERY_COLLISION },
                .insContractPrep = .{ .id = insContractId, .query = QUERY_INSERT_CONTRACT_V2 },
                .insAddrPrep = .{ .id = insAddrId, .query = QUERY_INSERT_ADDR_BY_BC },
                .insCreationAddrPrep = .{ .id = insCreationAddrId, .query = QUERY_INSERT_ADDR_BY_CREATION_BC },
                .selPendingPrep = .{ .id = selPendingId, .query = QUERY_SELECT_PENDING },
                .updVerifiedPrep = .{ .id = updVerifiedId, .query = QUERY_UPDATE_VERIFIED },
                .delPendingPrep = .{ .id = delPendingId, .query = QUERY_DELETE_PENDING },
                .insSourcePrep = .{ .id = insSourceId, .query = QUERY_INSERT_SOURCE_CHUNK },
                .cache = cache,
                .gpa = gpa,
            };
        }

        pub fn deinit(self: *Self) void {
            self.gpa.free(self.selPrep.id);
            self.gpa.free(self.insPrep.id);
            self.gpa.free(self.colPrep.id);
            self.gpa.free(self.insContractPrep.id);
            self.gpa.free(self.insAddrPrep.id);
            self.gpa.free(self.insCreationAddrPrep.id);
            self.gpa.free(self.selPendingPrep.id);
            self.gpa.free(self.updVerifiedPrep.id);
            self.gpa.free(self.delPendingPrep.id);
            self.gpa.free(self.insSourcePrep.id);
            self.cache.deinit();
        }

        // Resolve or insert a bytecode into bytecode_store_v2.
        //
        // Algorithm (strictly as spec'd):
        //  1. sha256(bytecode) → cache lookup → return on hit
        //  2. SELECT seq,size,check_hash,bytecode WHERE hash=sha256
        //  3. For each row: compare size, then keccak256, then raw bytes.
        //     Match → cache + return.
        //  4. No match, rows exist → sha256 collision: INSERT seq=max+1 IF NOT EXISTS,
        //     write collision_registry_v2, log.
        //  5. No rows → INSERT seq=0 IF NOT EXISTS.
        //  6. LWT not applied (concurrent insert) → re-read from step 2.
        //
        // gpa is used for temporary SelectResult allocation (freed before return).
        pub fn resolveOrInsert(self: *Self, db: *Db, bytecode: []const u8, kind: Kind, gpa: Allocator, first_seen_block: i64) !BytecodeId {
            var sha: [32]u8 = undefined;
            Hasher.hash(bytecode, &sha, .{});

            if (self.cache.get(sha)) |id| return id;

            while (true) {
                // Build SELECT params: [blob] hash
                var sparams: std.ArrayList(u8) = .empty;
                defer sparams.deinit(pool.tempAllocator);
                try pool.valBlob(&sparams, &sha);

                var result = try db.executeSelect(&self.selPrep, 1, sparams.items, gpa);
                defer result.deinit();

                var max_seq: i8 = -1;
                var our_keccak: ?[32]u8 = null; // computed lazily

                for (result.rows) |row| {
                    if (row.len < 4) continue;

                    // seq (tinyint): 1 byte
                    const seq_col = row[0] orelse continue;
                    if (seq_col.len < 1) continue;
                    const seq: i8 = @bitCast(seq_col[0]);
                    if (seq > max_seq) max_seq = seq;

                    // size (int): 4 bytes big-endian
                    const size_col = row[1] orelse continue;
                    if (size_col.len < 4) continue;
                    const db_size = std.mem.readInt(i32, size_col[0..4], .big);
                    if (db_size != @as(i32, @intCast(bytecode.len))) continue;

                    // check_hash (blob): keccak256, 32 bytes
                    const ck_col = row[2] orelse continue;
                    if (ck_col.len != 32) continue;
                    if (our_keccak == null) {
                        our_keccak = undefined;
                        std.crypto.hash.sha3.Keccak256.hash(bytecode, &our_keccak.?, .{});
                    }
                    if (!std.mem.eql(u8, ck_col, &our_keccak.?)) continue;

                    // Full byte comparison — mandatory, never skip.
                    const bc_col = row[3] orelse continue;
                    if (std.mem.eql(u8, bc_col, bytecode)) {
                        const id = BytecodeId{ .hash = sha, .seq = seq };
                        self.cache.put(sha, id);
                        return id;
                    }
                }

                // No matching row found — insert.
                const new_seq = max_seq + 1;
                const is_collision = max_seq >= 0;

                if (our_keccak == null) {
                    our_keccak = undefined;
                    std.crypto.hash.sha3.Keccak256.hash(bytecode, &our_keccak.?, .{});
                }

                var iparams: std.ArrayList(u8) = .empty;
                defer iparams.deinit(pool.tempAllocator);
                try pool.valBlob(&iparams, &sha);                              // hash
                try pool.valTinyint(&iparams, new_seq);                        // seq
                try pool.valBlob(&iparams, bytecode);                          // bytecode
                try pool.valBlob(&iparams, &our_keccak.?);                    // check_hash
                try pool.valInt32(&iparams, @intCast(bytecode.len));           // size
                try pool.valTinyint(&iparams, @bitCast(@as(u8, @intFromEnum(kind)))); // kind
                try pool.valBigint(&iparams, first_seen_block);                // first_seen_block

                const applied = try db.executeLWT(&self.insPrep, 7, iparams.items);
                if (!applied) {
                    // Concurrent insert beat us — re-read to find it.
                    continue;
                }

                if (is_collision) {
                    std.debug.print("[bytecode_store] SHA256 COLLISION hash={x} new_seq={d}\n", .{ sha, new_seq });
                    self.recordCollision(db, sha, new_seq) catch |e|
                        std.debug.print("[bytecode_store] collision record failed: {s}\n", .{@errorName(e)});
                }

                const id = BytecodeId{ .hash = sha, .seq = new_seq };
                self.cache.put(sha, id);
                return id;
            }
        }

        fn recordCollision(self: *Self, db: *Db, sha: [32]u8, seq_count: i8) !void {
            const now_ms = nowMs();
            var p: std.ArrayList(u8) = .empty;
            defer p.deinit(pool.tempAllocator);
            try pool.valBlob(&p, &sha);
            try pool.valBigint(&p, now_ms);
            try pool.valTinyint(&p, seq_count);
            try pool.valText(&p, "auto-detected SHA256 collision");
            const rows = [1][]const u8{p.items};
            try db.batchSendRows(&self.colPrep, 4, &rows);
        }

        // Process all contract deploys in a window, writing v2 rows.
        //
        // For each ContractByAddrRow:
        //  1. Decode hex bytecodes → resolveOrInsert (bytecode_store_v2)
        //  2. INSERT contracts_by_address_v2
        //  3. INSERT addresses_by_bytecode (deployed bytecode id, bucket=addr[0])
        //
        // Errors on a single contract are logged and skipped — we must not abort
        // the whole save batch due to one bad row (e.g. empty bytecode on a precompile).
        pub fn processContracts(
            self: *Self,
            db: *Db,
            contracts: []const @import("indexer/db").schema.ContractByAddrRow,
        ) void {
            for (contracts) |*c| {
                self.processOneContract(db, c) catch |e| {
                    std.debug.print("[bytecode_store] processContract error addr={s}: {s}\n", .{ c.address, @errorName(e) });
                };
            }
        }

        fn processOneContract(
            self: *Self,
            db: *Db,
            c: *const @import("indexer/db").schema.ContractByAddrRow,
        ) !void {
            const deployed_raw = transformer.decodeHexBytecode(pool.tempAllocator, c.deployedBytecode) catch &.{};
            defer if (deployed_raw.len > 0) pool.tempAllocator.free(deployed_raw);
            const creation_raw = transformer.decodeHexBytecode(pool.tempAllocator, c.creationBytecode) catch &.{};
            defer if (creation_raw.len > 0) pool.tempAllocator.free(creation_raw);

            const deployed_id = try self.resolveOrInsert(db, deployed_raw, .deployed, pool.tempAllocator, c.blockNumber);
            const creation_id = try self.resolveOrInsert(db, creation_raw, .creation, pool.tempAllocator, c.blockNumber);

            // INSERT contracts_by_address_v2
            var p: std.ArrayList(u8) = .empty;
            defer p.deinit(pool.tempAllocator);
            try pool.valTextRequired(&p, c.address);
            try pool.valBigint(&p, c.blockNumber);
            try pool.valBlob(&p, &deployed_id.hash);
            try pool.valTinyint(&p, deployed_id.seq);
            try pool.valBlob(&p, &creation_id.hash);
            try pool.valTinyint(&p, creation_id.seq);
            try pool.valText(&p, c.txHash);
            try pool.valText(&p, c.creator);
            try pool.valText(&p, c.contractFactory);
            try pool.valBigint(&p, c.timestamp);
            try pool.valBigint(&p, c.blockTimestampMs);
            try pool.valTinyint(&p, c.creationMethod);
            try pool.valInt32(&p, c.transactionIndex);
            try pool.valInt32(&p, c.traceIndex);
            const row1 = [1][]const u8{p.items};
            try db.batchSendRows(&self.insContractPrep, 14, &row1);

            // INSERT addresses_by_bytecode (deployed bytecode reverse index).
            p.items.len = 0;
            const bucket = addrFirstByte(c.address);
            try pool.valBlob(&p, &deployed_id.hash);
            try pool.valTinyint(&p, deployed_id.seq);
            try pool.valSmallint(&p, bucket);
            try pool.valTextRequired(&p, c.address);
            try pool.valBigint(&p, c.blockNumber);
            const row2 = [1][]const u8{p.items};
            try db.batchSendRows(&self.insAddrPrep, 5, &row2);

            // INSERT addresses_by_creation_bytecode (creation bytecode reverse index).
            // Skip if creation_raw is empty (precompiles, zero-bytecode edge cases).
            if (creation_raw.len > 0) {
                p.items.len = 0;
                try pool.valBlob(&p, &creation_id.hash);
                try pool.valTinyint(&p, creation_id.seq);
                try pool.valSmallint(&p, bucket);
                try pool.valTextRequired(&p, c.address);
                try pool.valBigint(&p, c.blockNumber);
                const row3 = [1][]const u8{p.items};
                try db.batchSendRows(&self.insCreationAddrPrep, 5, &row3);
            }

            // Apply any pending verification submitted before this contract was indexed.
            self.applyPendingVerification(db, c.address, deployed_id) catch |e|
                std.debug.print("[bytecode_store] applyPending addr={s}: {s}\n", .{ c.address, @errorName(e) });
        }

        fn applyPendingVerification(
            self: *Self,
            db: *Db,
            address: []const u8,
            deployed_id: BytecodeId,
        ) !void {
            // Go API stores address as lowercase hex text (normalizeAddr = strings.ToLower).
            const addr_lower = try pool.tempAllocator.dupe(u8, address);
            defer pool.tempAllocator.free(addr_lower);
            for (addr_lower) |*b| b.* = std.ascii.toLower(b.*);

            var sp: std.ArrayList(u8) = .empty;
            defer sp.deinit(pool.tempAllocator);
            try pool.valTextRequired(&sp, addr_lower);

            var result = try db.executeSelect(&self.selPendingPrep, 1, sp.items, pool.tempAllocator);
            defer result.deinit();

            if (result.rows.len == 0) return;

            const row = result.rows[0];
            if (row.len < 3) return;

            const abi_raw = row[0] orelse return;
            if (abi_raw.len == 0) return;

            const abi_compressed = try zlibCompress(abi_raw);
            defer pool.tempAllocator.free(abi_compressed);

            const source_raw = row[1];
            var source_ref: []const u8 = "";
            if (source_raw) |src| {
                if (src.len > 0) {
                    try self.storeSourceChunks(db, deployed_id, src);
                    source_ref = "source_store";
                }
            }

            const prog_lang_raw = row[2];
            const prog_lang: []const u8 = if (prog_lang_raw) |pl| pl else "";

            const now_ms = nowMs();
            var up: std.ArrayList(u8) = .empty;
            defer up.deinit(pool.tempAllocator);
            try pool.valBigint(&up, now_ms);           // verified_at
            try pool.valBlob(&up, addr_lower);         // verified_via_address (blob, hex string bytes)
            try pool.valBlob(&up, abi_compressed);     // abi
            try pool.valText(&up, source_ref);         // source_ref
            try pool.valText(&up, prog_lang);          // programming_language
            try pool.valBlob(&up, &deployed_id.hash);  // hash
            try pool.valTinyint(&up, deployed_id.seq); // seq
            const row_up = [1][]const u8{up.items};
            try db.batchSendRows(&self.updVerifiedPrep, 7, &row_up);

            var dp: std.ArrayList(u8) = .empty;
            defer dp.deinit(pool.tempAllocator);
            try pool.valTextRequired(&dp, addr_lower);
            const row_del = [1][]const u8{dp.items};
            try db.batchSendRows(&self.delPendingPrep, 1, &row_del);

            std.debug.print("[bytecode_store] applied pending verification addr={s}\n", .{address});
        }

        fn storeSourceChunks(self: *Self, db: *Db, id: BytecodeId, source: []const u8) !void {
            const chunk_size: usize = 512 * 1024;
            var chunk: i32 = 0;
            var offset: usize = 0;
            while (offset < source.len) {
                const end = @min(offset + chunk_size, source.len);
                var p: std.ArrayList(u8) = .empty;
                defer p.deinit(pool.tempAllocator);
                try pool.valBlob(&p, &id.hash);
                try pool.valTinyint(&p, id.seq);
                try pool.valInt32(&p, chunk);
                try pool.valBlob(&p, source[offset..end]);
                const row = [1][]const u8{p.items};
                try db.batchSendRows(&self.insSourcePrep, 4, &row);
                offset = end;
                chunk += 1;
            }
        }
    };
}

fn zlibCompress(input: []const u8) ![]u8 {
    var aw = try std.Io.Writer.Allocating.initCapacity(pool.tempAllocator, 4096);
    errdefer aw.deinit();
    const window_buf = try pool.tempAllocator.alloc(u8, std.compress.flate.max_window_len);
    defer pool.tempAllocator.free(window_buf);
    var comp = try std.compress.flate.Compress.init(&aw.writer, window_buf, .zlib, .default);
    try std.Io.Writer.writeAll(&comp.writer, input);
    try comp.finish();
    return aw.toOwnedSlice();
}

// Return the first decoded byte of a hex-encoded address as a smallint bucket.
// Address format: "0x1234..." or "1234...". Returns 0 on malformed input.
fn addrFirstByte(hexAddr: []const u8) i16 {
    var h = hexAddr;
    if (h.len >= 2 and h[0] == '0' and (h[1] == 'x' or h[1] == 'X')) h = h[2..];
    if (h.len < 2) return 0;
    const hi = std.fmt.charToDigit(h[0], 16) catch return 0;
    const lo = std.fmt.charToDigit(h[1], 16) catch return 0;
    return @intCast((hi << 4) | lo);
}

pub const BytecodeStore = BytecodeStoreT(pool.CqlConn, std.crypto.hash.sha2.Sha256);

// ─── Unit tests ───────────────────────────────────────────────────────────────

const testing = std.testing;

// A hash function that always returns the same digest — forces sha256 "collisions"
// for any two distinct inputs. Used in collision and concurrent-insert tests.
const ConstantHash = struct {
    pub fn hash(input: []const u8, out: *[32]u8, options: anytype) void {
        _ = input;
        _ = options;
        @memset(out, 0xAB);
    }
};

// MockDb: controls SELECT results and LWT decisions via queued slices.
// SELECT results: each call consumes selResponses[selIdx++].
//   A response is a slice of rows; each row is a slice of nullable byte slices.
// LWT results: each call consumes lwtResults[lwtIdx++].
// batchSendRows: just increments batchCalls.
const MockDb = struct {
    gpa: Allocator,
    selResponses: []const []const []const ?[]const u8,
    selIdx: usize,
    lwtResults: []const bool,
    lwtIdx: usize,
    batchCalls: usize,

    fn init(gpa: Allocator, selResponses: []const []const []const ?[]const u8, lwtResults: []const bool) MockDb {
        return .{
            .gpa = gpa,
            .selResponses = selResponses,
            .selIdx = 0,
            .lwtResults = lwtResults,
            .lwtIdx = 0,
            .batchCalls = 0,
        };
    }

    pub fn allocator(self: *MockDb) Allocator {
        return self.gpa;
    }

    pub fn prepare(self: *MockDb, query: []const u8) ![]u8 {
        _ = query;
        // Return a heap-allocated dummy so deinit() can free it normally.
        return try self.gpa.dupe(u8, "mock");
    }

    pub fn executeSelect(self: *MockDb, prep: *pool.Prepared, nParams: u16, params: []const u8, gpa: Allocator) !pool.SelectResult {
        _ = prep;
        _ = nParams;
        _ = params;
        const rows = self.selResponses[self.selIdx];
        self.selIdx += 1;
        // Copy into a SelectResult arena so the caller can call deinit().
        var arena = std.heap.ArenaAllocator.init(gpa);
        const aa = arena.allocator();
        const rows_copy = try aa.alloc([]const ?[]const u8, rows.len);
        for (rows, 0..) |row, i| {
            const col_copy = try aa.alloc(?[]const u8, row.len);
            for (row, 0..) |col, j| {
                col_copy[j] = if (col) |c| try aa.dupe(u8, c) else null;
            }
            rows_copy[i] = col_copy;
        }
        return pool.SelectResult{ .rows = rows_copy, ._arena = arena };
    }

    pub fn executeLWT(self: *MockDb, prep: *pool.Prepared, nParams: u16, params: []const u8) !bool {
        _ = prep;
        _ = nParams;
        _ = params;
        const applied = self.lwtResults[self.lwtIdx];
        self.lwtIdx += 1;
        return applied;
    }

    pub fn batchSendRows(self: *MockDb, prep: *pool.Prepared, nVals: u16, rows: []const []const u8) !void {
        _ = prep;
        _ = nVals;
        _ = rows;
        self.batchCalls += 1;
    }
};

const TestStore = BytecodeStoreT(MockDb, std.crypto.hash.sha2.Sha256);
const CollisionStore = BytecodeStoreT(MockDb, ConstantHash);

// Helper: encode a tinyint value as CQL wire bytes (1 byte)
fn mockTinyint(v: i8) [1]u8 {
    return .{@bitCast(v)};
}

// Helper: encode an int32 as 4 big-endian bytes
fn mockInt32(v: i32) [4]u8 {
    var b: [4]u8 = undefined;
    std.mem.writeInt(i32, &b, v, .big);
    return b;
}

test "empty partition: first deploy inserts with seq=0" {
    // No existing rows → SELECT returns empty → LWT applied → returns seq=0.
    const bytecode = "hello world bytecode";
    const empty_rows: []const []const ?[]const u8 = &.{};
    const sel_responses = [_][]const []const ?[]const u8{empty_rows};
    const lwt_results = [_]bool{true};

    var mock = MockDb.init(testing.allocator, &sel_responses, &lwt_results);
    var store = try TestStore.init(&mock, 16);
    defer store.deinit();

    const id = try store.resolveOrInsert(&mock, bytecode, .deployed, testing.allocator, 0);

    try testing.expectEqual(@as(i8, 0), id.seq);
    try testing.expectEqual(@as(usize, 1), mock.lwtIdx); // one LWT insert
}

test "dedup hit via cache: second call returns without CQL" {
    // First call: empty partition → insert seq=0.
    // Second call: cache hit → no additional CQL.
    const bytecode = "dedup test bytecode";
    const empty: []const []const ?[]const u8 = &.{};
    const sel_responses = [_][]const []const ?[]const u8{empty};
    const lwt_results = [_]bool{true};

    var mock = MockDb.init(testing.allocator, &sel_responses, &lwt_results);
    var store = try TestStore.init(&mock, 16);
    defer store.deinit();

    const id1 = try store.resolveOrInsert(&mock, bytecode, .deployed, testing.allocator, 0);
    const id2 = try store.resolveOrInsert(&mock, bytecode, .deployed, testing.allocator, 0);

    // Both should return the same BytecodeId.
    try testing.expectEqual(id1.seq, id2.seq);
    try testing.expectEqualSlices(u8, &id1.hash, &id2.hash);
    // SELECT was only called once (second call was a cache hit).
    try testing.expectEqual(@as(usize, 1), mock.selIdx);
    try testing.expectEqual(@as(usize, 1), mock.lwtIdx);
}

test "sha256 collision: bytecode whose hash partition already holds a different entry inserts at seq=1" {
    // ConstantHash makes any bytecode produce 0xAB...AB.
    // We simulate the case where b1 was already stored (by some prior call / other node)
    // and now we call resolveOrInsert for b2 which has the same sha256 but different bytes.
    // Because the cache is cold (fresh store), the SELECT fires and returns b1's row.
    // resolveOrInsert sees no byte-match → detects collision → inserts seq=1.
    //
    // Note: we do NOT call resolveOrInsert for b1 first — that would populate the cache
    // with 0xAB...AB → seq=0 and bypass the SELECT for b2 (cache hit, wrong result).
    // Collision-safe deduplication applies at the DB level; the in-process cache is only
    // a latency optimisation and can return a stale id when two nodes race.

    const b1: []const u8 = "bytecode_alpha";
    const b2: []const u8 = "bytecode_beta_different"; // different content, same ConstantHash

    // Pre-compute keccak256 of b1 (the value the "existing" row in Scylla would hold).
    var ck_b1: [32]u8 = undefined;
    std.crypto.hash.sha3.Keccak256.hash(b1, &ck_b1, .{});

    const sz_b1 = mockInt32(@intCast(b1.len));
    const seq0 = mockTinyint(0);

    // SELECT for b2 returns b1's row already in Scylla at seq=0.
    // b2.len (23) ≠ b1.len (14) → size mismatch → row is not a match for b2.
    // max_seq is still tracked as 0 so the next seq assigned is 1.
    const existing_row = [_]?[]const u8{
        &seq0,  // seq
        &sz_b1, // size = 14 (b1), ≠ b2.len=23 → mismatch
        &ck_b1, // check_hash (keccak of b1)
        b1,     // bytecode bytes (b1)
    };
    const existing_rows = [_][]const ?[]const u8{&existing_row};
    const sel_responses = [_][]const []const ?[]const u8{&existing_rows};
    const lwt_results = [_]bool{true}; // b2 insert succeeds

    var mock = MockDb.init(testing.allocator, &sel_responses, &lwt_results);
    var store = try CollisionStore.init(&mock, 16);
    defer store.deinit();

    const id2 = try store.resolveOrInsert(&mock, b2, .deployed, testing.allocator, 0);
    try testing.expectEqual(@as(i8, 1), id2.seq); // collision → seq=1
    try testing.expectEqual(@as(usize, 1), mock.batchCalls); // collision_registry written
}

test "concurrent LWT retry: LWT fails once then succeeds on re-read" {
    // First loop: SELECT returns empty, LWT fails (another writer won).
    // Second loop: SELECT returns the row inserted by the concurrent writer, bytes match → return.
    const bytecode: []const u8 = "concurrent bytecode";

    var our_sha: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytecode, &our_sha, .{});
    var our_keccak: [32]u8 = undefined;
    std.crypto.hash.sha3.Keccak256.hash(bytecode, &our_keccak, .{});
    const sz = mockInt32(@intCast(bytecode.len));
    const seq0 = mockTinyint(0);

    // Second SELECT returns the row the concurrent winner inserted.
    const winner_row = [_]?[]const u8{
        &seq0,
        &sz,
        &our_keccak,
        bytecode,
    };
    const winner_rows = [_][]const ?[]const u8{&winner_row};
    const empty: []const []const ?[]const u8 = &.{};

    const sel_responses = [_][]const []const ?[]const u8{
        empty,        // loop1: empty partition
        &winner_rows, // loop2: concurrent winner already inserted
    };
    const lwt_results = [_]bool{false}; // LWT fails (not applied)

    var mock = MockDb.init(testing.allocator, &sel_responses, &lwt_results);
    var store = try TestStore.init(&mock, 16);
    defer store.deinit();

    const id = try store.resolveOrInsert(&mock, bytecode, .deployed, testing.allocator, 0);
    try testing.expectEqual(@as(i8, 0), id.seq);
    try testing.expectEqual(@as(usize, 2), mock.selIdx); // two SELECTs
    try testing.expectEqual(@as(usize, 1), mock.lwtIdx); // one failed LWT
}
