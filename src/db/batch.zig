// ScyllaDB batch write functions — save* per table, parallel save, realtime connections.
const std = @import("std");

const schema = @import("schema.zig");
const pool = @import("pool.zig");

const CqlConn = pool.CqlConn;
const BatchSizes = pool.BatchSizes;
const tempAllocator = pool.tempAllocator;

pub const Entities = schema.Entities;
const BlockRow = schema.BlockRow;
const TxRow = schema.TxRow;
const LogRow = schema.LogRow;
const InternalTxRow = schema.InternalTxRow;
const ContractRow = schema.ContractRow;
const ContractByAddrRow = schema.ContractByAddrRow;
const Erc20TokenRow = schema.Erc20TokenRow;
const Erc20SupplyRow = schema.Erc20SupplyRow;
const Erc20OwnerRow = schema.Erc20OwnerRow;
const Erc20SelfDestructRow = schema.Erc20SelfDestructRow;

// ─── RealtimeConns ────────────────────────────────────────────────────────────
// 32 persistent CQL connections for parallel per-table writes in realtime mode.
// Split matches historical: 1 blk + 3 txs + 6 logs + 20 itxs + 1 contracts + 1 comp.

pub const ACCUM_TXS_LANES: usize = 3;
pub const ACCUM_LOG_LANES: usize = 6;
pub const ACCUM_ITX_LANES: usize = 20;

pub const RealtimeConns = struct {
    gpa: std.mem.Allocator,
    // Kept for reconnectAll() — connection params, not owned (point into the
    // caller's env-derived strings, which outlive the process).
    host: []const u8,
    port: u16,
    ks: []const u8,
    user: []const u8,
    pass: []const u8,
    blk: CqlConn,
    txs: []CqlConn,
    logs: []CqlConn,
    itxs: []CqlConn,
    contracts: CqlConn,
    comp: CqlConn,
    erc20: CqlConn,

    pub fn init(
        gpa: std.mem.Allocator,
        host: []const u8,
        port: u16,
        ks: []const u8,
        user: []const u8,
        pass: []const u8,
        txsN: usize,
        logsN: usize,
        itxsN: usize,
    ) !RealtimeConns {
        const txs = try gpa.alloc(CqlConn, txsN);
        errdefer gpa.free(txs);
        const logs = try gpa.alloc(CqlConn, logsN);
        errdefer gpa.free(logs);
        const itxs = try gpa.alloc(CqlConn, itxsN);
        errdefer gpa.free(itxs);

        var blk = try CqlConn.init(gpa, host, port, ks, user, pass);
        errdefer blk.deinit();
        var txsN2: usize = 0;
        errdefer for (txs[0..txsN2]) |*c| c.deinit();
        for (txs) |*c| { c.* = try CqlConn.init(gpa, host, port, ks, user, pass); txsN2 += 1; }
        var logsN2: usize = 0;
        errdefer for (logs[0..logsN2]) |*c| c.deinit();
        for (logs) |*c| { c.* = try CqlConn.init(gpa, host, port, ks, user, pass); logsN2 += 1; }
        var itxsN2: usize = 0;
        errdefer for (itxs[0..itxsN2]) |*c| c.deinit();
        for (itxs) |*c| { c.* = try CqlConn.init(gpa, host, port, ks, user, pass); itxsN2 += 1; }
        var contracts = try CqlConn.init(gpa, host, port, ks, user, pass);
        errdefer contracts.deinit();
        var comp = try CqlConn.init(gpa, host, port, ks, user, pass);
        errdefer comp.deinit();
        const erc20 = try CqlConn.init(gpa, host, port, ks, user, pass);
        return .{
            .gpa = gpa,
            .host = host,
            .port = port,
            .ks = ks,
            .user = user,
            .pass = pass,
            .blk = blk,
            .txs = txs,
            .logs = logs,
            .itxs = itxs,
            .contracts = contracts,
            .comp = comp,
            .erc20 = erc20,
        };
    }

    pub fn deinit(self: *RealtimeConns) void {
        self.blk.deinit();
        for (self.txs) |*c| c.deinit();
        for (self.logs) |*c| c.deinit();
        for (self.itxs) |*c| c.deinit();
        self.contracts.deinit();
        self.comp.deinit();
        self.erc20.deinit();
        self.gpa.free(self.txs);
        self.gpa.free(self.logs);
        self.gpa.free(self.itxs);
    }

    /// True only if every connection responds to a CQL OPTIONS probe.
    pub fn pingAll(self: *RealtimeConns) bool {
        self.blk.ping() catch return false;
        for (self.txs) |*c| c.ping() catch return false;
        for (self.logs) |*c| c.ping() catch return false;
        for (self.itxs) |*c| c.ping() catch return false;
        self.contracts.ping() catch return false;
        self.comp.ping() catch return false;
        self.erc20.ping() catch return false;
        return true;
    }

    /// Reconnects every connection (e.g. after a Scylla restart broke them
    /// all at once). Best-effort: attempts all of them even if some fail,
    /// then returns the first error seen (if any) so the caller can log it.
    pub fn reconnectAll(self: *RealtimeConns) !void {
        var firstErr: ?anyerror = null;
        self.blk.reopen(self.host, self.port, self.ks, self.user, self.pass) catch |e| {
            firstErr = firstErr orelse e;
        };
        for (self.txs) |*c| c.reopen(self.host, self.port, self.ks, self.user, self.pass) catch |e| {
            firstErr = firstErr orelse e;
        };
        for (self.logs) |*c| c.reopen(self.host, self.port, self.ks, self.user, self.pass) catch |e| {
            firstErr = firstErr orelse e;
        };
        for (self.itxs) |*c| c.reopen(self.host, self.port, self.ks, self.user, self.pass) catch |e| {
            firstErr = firstErr orelse e;
        };
        self.contracts.reopen(self.host, self.port, self.ks, self.user, self.pass) catch |e| {
            firstErr = firstErr orelse e;
        };
        self.comp.reopen(self.host, self.port, self.ks, self.user, self.pass) catch |e| {
            firstErr = firstErr orelse e;
        };
        self.erc20.reopen(self.host, self.port, self.ks, self.user, self.pass) catch |e| {
            firstErr = firstErr orelse e;
        };
        if (firstErr) |e| return e;
    }
};

pub fn saveBlockRt(conns: *RealtimeConns, ent: *const Entities, bs: BatchSizes) !void {
    const ents = [1]*const Entities{ent};
    try saveEntitiesParallel(
        &conns.blk,
        conns.txs,
        &conns.contracts,
        conns.logs,
        conns.itxs,
        &conns.comp,
        &conns.erc20,
        @constCast(&ents),
        bs,
    );
}

fn preallocBuf(est: usize) std.ArrayList(u8) {
    var v: std.ArrayList(u8) = .empty;
    v.ensureTotalCapacity(tempAllocator, est) catch {};
    return v;
}

// Max rows per batch — stack arrays sized to this to avoid heap allocs.
const MAX_BS: usize = 2000;

// ─── Save a single block's entities ──────────────────────────────────────────

pub fn saveBlock(conn: *CqlConn, ent: *const Entities, bs: BatchSizes) !void {
    try saveBlocks(conn, ent.blocks.items, bs.blocks);
    try saveTxs(conn, ent.txs.items, bs.txs);
    try saveLogs(conn, ent.logs.items, bs.logs);
    try saveInternalTxs(conn, ent.internalTxs.items, bs.itxs);
    try saveContracts(conn, ent.contracts.items, bs.contracts);
    try saveContractsByAddr(conn, ent.contractsByAddr.items, bs.contracts);
    try saveErc20Tokens(conn, ent.erc20Tokens.items, bs.contracts);
    try saveErc20Supplies(conn, ent.erc20Supplies.items, bs.contracts);
    try saveErc20Owners(conn, ent.erc20Owners.items, bs.contracts);
    try saveErc20SelfDestructs(conn, ent.erc20SelfDestructs.items, bs.contracts);
    try saveBlockCompletions(conn, ent);
}

// ─── Parallel save: N entities over 32 CQL connections ───────────────────────

pub const TableSave = struct {
    conn: *CqlConn,
    ents: []*const Entities,
    bs: BatchSizes,
    err: ?anyerror = null,
};

pub fn saveBlockRowsForEntities(g: *TableSave) void {
    for (g.ents) |ent| {
        saveBlocks(g.conn, ent.blocks.items, g.bs.blocks) catch |e| {
            g.err = e;
            return;
        };
    }
}

pub fn saveContractRowsForEntities(g: *TableSave) void {
    for (g.ents) |ent| {
        saveContracts(g.conn, ent.contracts.items, g.bs.contracts) catch |e| {
            g.err = e;
            return;
        };
        saveContractsByAddr(g.conn, ent.contractsByAddr.items, g.bs.contracts) catch |e| {
            g.err = e;
            return;
        };
    }
}

pub fn saveErc20RowsForEntities(g: *TableSave) void {
    for (g.ents) |ent| {
        saveErc20Tokens(g.conn, ent.erc20Tokens.items, g.bs.contracts) catch |e| {
            g.err = e;
            return;
        };
        saveErc20Supplies(g.conn, ent.erc20Supplies.items, g.bs.contracts) catch |e| {
            g.err = e;
            return;
        };
        saveErc20Owners(g.conn, ent.erc20Owners.items, g.bs.contracts) catch |e| {
            g.err = e;
            return;
        };
        saveErc20SelfDestructs(g.conn, ent.erc20SelfDestructs.items, g.bs.contracts) catch |e| {
            g.err = e;
            return;
        };
    }
}

pub const TableLaneSave = struct {
    conn: *CqlConn,
    ents: []*const Entities,
    bs: BatchSizes,
    lane: usize,
    nLanes: usize,
    err: ?anyerror = null,
};

pub fn saveLogRowsForLane(g: *TableLaneSave) void {
    var rows: std.ArrayList(LogRow) = .empty;
    defer rows.deinit(tempAllocator);
    for (g.ents) |ent| {
        const all = ent.logs.items;
        const start = all.len * g.lane / g.nLanes;
        const end = all.len * (g.lane + 1) / g.nLanes;
        rows.appendSlice(tempAllocator, all[start..end]) catch |e| {
            g.err = e;
            return;
        };
    }
    saveLogs(g.conn, rows.items, g.bs.logs) catch |e| {
        g.err = e;
        return;
    };
}

pub fn saveItxRowsForLane(g: *TableLaneSave) void {
    var rows: std.ArrayList(InternalTxRow) = .empty;
    defer rows.deinit(tempAllocator);
    for (g.ents) |ent| {
        const all = ent.internalTxs.items;
        const start = all.len * g.lane / g.nLanes;
        const end = all.len * (g.lane + 1) / g.nLanes;
        rows.appendSlice(tempAllocator, all[start..end]) catch |e| {
            g.err = e;
            return;
        };
    }
    saveInternalTxs(g.conn, rows.items, g.bs.itxs) catch |e| {
        g.err = e;
        return;
    };
}

pub fn saveTxRowsForLane(g: *TableLaneSave) void {
    var rows: std.ArrayList(TxRow) = .empty;
    defer rows.deinit(tempAllocator);
    for (g.ents) |ent| {
        const all = ent.txs.items;
        const start = all.len * g.lane / g.nLanes;
        const end = all.len * (g.lane + 1) / g.nLanes;
        rows.appendSlice(tempAllocator, all[start..end]) catch |e| {
            g.err = e;
            return;
        };
    }
    saveTxs(g.conn, rows.items, g.bs.txs) catch |e| {
        g.err = e;
        return;
    };
}

// Batch-write all completion rows for a slice of entities in one CQL round-trip.
pub fn saveBlockCompletionsBatch(conn: *CqlConn, ents: []*const Entities) !void {
    if (ents.len == 0) return;
    const BATCH: usize = 50;
    var rowBuf: std.ArrayList(u8) = preallocBuf(64);
    defer rowBuf.deinit(tempAllocator);
    var batchBuf: std.ArrayList(u8) = .empty;
    defer batchBuf.deinit(tempAllocator);
    var starts: [BATCH + 1]usize = undefined;
    var ptrs: [BATCH][]const u8 = undefined;
    var i: usize = 0;
    while (i < ents.len) {
        const end = @min(i + BATCH, ents.len);
        batchBuf.items.len = 0;
        var enc: usize = 0;
        for (i..end) |j| {
            const ent = ents[j];
            if (ent.blocks.items.len == 0) continue;
            const b = ent.blocks.items[0];
            rowBuf.items.len = 0;
            starts[enc] = batchBuf.items.len;
            try pool.valInt32(&rowBuf, b.chunk);
            try pool.valBigint(&rowBuf, b.number);
            try pool.valInt32(&rowBuf, @as(i32, @intCast(ent.txs.items.len)));
            try pool.valInt32(&rowBuf, @as(i32, @intCast(ent.logs.items.len)));
            try pool.valInt32(&rowBuf, @as(i32, @intCast(ent.internalTxs.items.len)));
            try pool.valInt32(&rowBuf, @as(i32, @intCast(ent.contracts.items.len)));
            try batchBuf.appendSlice(tempAllocator, rowBuf.items);
            enc += 1;
        }
        if (enc > 0) {
            starts[enc] = batchBuf.items.len;
            for (0..enc) |k| ptrs[k] = batchBuf.items[starts[k]..starts[k + 1]];
            try conn.batchSendRows(conn.prepIds.blockCompletions, COMPLETION_COLS, ptrs[0..enc]);
        }
        i = end;
    }
}

// ─── saveEntitiesParallel ─────────────────────────────────────────────────────

pub fn saveEntitiesParallel(
    connBlocks: *CqlConn,
    connTxs: []CqlConn,
    connContracts: *CqlConn,
    connLogs: []CqlConn,
    connItxs: []CqlConn,
    connComp: *CqlConn,
    connErc20: *CqlConn,
    ents: []*const Entities,
    bs: BatchSizes,
) !void {
    if (ents.len == 0) return;

    const txsN = connTxs.len;
    const logsN = connLogs.len;
    const itxsN = connItxs.len;

    var gBlocks = TableSave{ .conn = connBlocks, .ents = ents, .bs = bs };
    var gContracts = TableSave{ .conn = connContracts, .ents = ents, .bs = bs };
    var gErc20 = TableSave{ .conn = connErc20, .ents = ents, .bs = bs };
    const gTxs = try tempAllocator.alloc(TableLaneSave, txsN);
    defer tempAllocator.free(gTxs);
    const gLogs = try tempAllocator.alloc(TableLaneSave, logsN);
    defer tempAllocator.free(gLogs);
    const gItxs = try tempAllocator.alloc(TableLaneSave, itxsN);
    defer tempAllocator.free(gItxs);

    for (0..txsN) |i|
        gTxs[i] = .{ .conn = &connTxs[i], .ents = ents, .bs = bs, .lane = i, .nLanes = txsN };
    for (0..logsN) |i|
        gLogs[i] = .{ .conn = &connLogs[i], .ents = ents, .bs = bs, .lane = i, .nLanes = logsN };
    for (0..itxsN) |i|
        gItxs[i] = .{ .conn = &connItxs[i], .ents = ents, .bs = bs, .lane = i, .nLanes = itxsN };

    // 1 (blocks) + 1 (erc20) + txsN (txs) + logsN (logs) + itxsN (itxs); contracts run inline.
    const nSpawn = 2 + txsN + logsN + itxsN;
    const threads = try tempAllocator.alloc(std.Thread, nSpawn);
    defer tempAllocator.free(threads);
    var spawned: usize = 0;
    // Only for spawn failures below — once the real join below succeeds,
    // `joined` stops this from running again on the same handles. Calling
    // pthread_join twice on the same thread is UB and segfaults (verified
    // live: a lane's post-join `return e` — e.g. a write to a connection
    // that died mid-save — re-triggered this errdefer on already-joined
    // threads, crashing inside __pthread_clockjoin_ex).
    var joined = false;
    errdefer if (!joined) for (threads[0..spawned]) |t| t.join();

    for (0..txsN) |i| {
        threads[spawned] = try std.Thread.spawn(.{}, saveTxRowsForLane, .{&gTxs[i]});
        spawned += 1;
    }
    for (0..logsN) |i| {
        threads[spawned] = try std.Thread.spawn(.{}, saveLogRowsForLane, .{&gLogs[i]});
        spawned += 1;
    }
    for (0..itxsN) |i| {
        threads[spawned] = try std.Thread.spawn(.{}, saveItxRowsForLane, .{&gItxs[i]});
        spawned += 1;
    }
    threads[spawned] = try std.Thread.spawn(.{}, saveBlockRowsForEntities, .{&gBlocks});
    spawned += 1;
    threads[spawned] = try std.Thread.spawn(.{}, saveErc20RowsForEntities, .{&gErc20});
    spawned += 1;

    saveContractRowsForEntities(&gContracts);

    for (threads[0..spawned]) |t| t.join();
    joined = true;

    if (gBlocks.err) |e| return e;
    if (gContracts.err) |e| return e;
    if (gErc20.err) |e| return e;
    for (gTxs) |g| if (g.err) |e| return e;
    for (gLogs) |g| if (g.err) |e| return e;
    for (gItxs) |g| if (g.err) |e| return e;

    try saveBlockCompletionsBatch(connComp, ents);
}

// ─── Low-level table writers ──────────────────────────────────────────────────
const BLOCK_COLS: u16 = 5;
const TX_COLS: u16 = 21;
const LOG_COLS: u16 = 15;
const ITX_COLS: u16 = 10;
const CONTRACT_COLS: u16 = 13;
const CONTRACT_CBA_COLS: u16 = 8;
const COMPLETION_COLS: u16 = 6;
const ERC20_TOKEN_COLS: u16 = 16;
const ERC20_SUPPLY_INSERT_COLS: u16 = 6;
const ERC20_SUPPLY_UPDATE_COLS: u16 = 4;
const ERC20_OWNER_INSERT_COLS: u16 = 7;
const ERC20_OWNER_UPDATE_COLS: u16 = 5;
const ERC20_SELFDESTRUCT_COLS: u16 = 4;

fn saveBlocks(conn: *CqlConn, rows: []const BlockRow, bs: usize) !void {
    if (rows.len == 0) return;
    const chunk = @min(bs, MAX_BS);
    var v = preallocBuf(96);
    defer v.deinit(tempAllocator);
    var bd = preallocBuf(chunk *% 96);
    defer bd.deinit(tempAllocator);
    var starts: [MAX_BS + 1]usize = undefined;
    var ptrs: [MAX_BS][]const u8 = undefined;
    var i: usize = 0;
    while (i < rows.len) {
        const end = @min(i + chunk, rows.len);
        bd.items.len = 0;
        var enc: usize = 0;
        for (i..end) |j| {
            v.items.len = 0;
            starts[enc] = bd.items.len;
            const r = rows[j];
            try pool.valInt32(&v, r.chunk);
            try pool.valBigint(&v, r.number);
            try pool.valBigint(&v, r.timestampS);
            try pool.valBigint(&v, r.timestampMs);
            try pool.valTextRequired(&v, r.miner);
            try bd.appendSlice(tempAllocator, v.items);
            enc += 1;
        }
        starts[enc] = bd.items.len;
        for (0..enc) |k| ptrs[k] = bd.items[starts[k]..starts[k + 1]];
        try conn.batchSendRows(conn.prepIds.blocks, BLOCK_COLS, ptrs[0..enc]);
        i = end;
    }
}

fn saveTxs(conn: *CqlConn, rows: []const TxRow, bs: usize) !void {
    if (rows.len == 0) return;
    const chunk = @min(bs, MAX_BS);
    var v = preallocBuf(512);
    defer v.deinit(tempAllocator);
    var bd = preallocBuf(chunk *% 512);
    defer bd.deinit(tempAllocator);
    var starts: [MAX_BS + 1]usize = undefined;
    var ptrs: [MAX_BS][]const u8 = undefined;
    var i: usize = 0;
    while (i < rows.len) {
        const end = @min(i + chunk, rows.len);
        bd.items.len = 0;
        var enc: usize = 0;
        for (i..end) |j| {
            v.items.len = 0;
            starts[enc] = bd.items.len;
            const r = rows[j];
            try pool.valInt32(&v, r.chunk);
            try pool.valBigint(&v, r.blockNumber);
            try pool.valInt32(&v, r.transactionIndex);
            try pool.valTextRequired(&v, r.hash);
            try pool.valBigint(&v, r.blockTimestampS);
            try pool.valBigint(&v, r.blockTimestampMs);
            try pool.valText(&v, r.methodId);
            try pool.valText(&v, r.input);
            try pool.valTextRequired(&v, r.fromAddress);
            try pool.valText(&v, r.toAddress);
            try pool.valVarint(&v, r.value);
            try pool.valBigint(&v, r.gasLimit);
            try pool.valBigint(&v, r.gasPrice);
            try pool.valBigint(&v, r.gasUsed);
            try pool.valBigint(&v, r.maxPriorityFee);
            try pool.valBigint(&v, r.maxFee);
            try pool.valBigint(&v, r.cumulativeGasUsed);
            try pool.valBigint(&v, r.effectiveGasPrice);
            try pool.valText(&v, r.contractAddress);
            try pool.valTinyint(&v, r.status);
            try pool.valTinyint(&v, r.txType);
            try bd.appendSlice(tempAllocator, v.items);
            enc += 1;
        }
        starts[enc] = bd.items.len;
        for (0..enc) |k| ptrs[k] = bd.items[starts[k]..starts[k + 1]];
        try conn.batchSendRows(conn.prepIds.transactions, TX_COLS, ptrs[0..enc]);
        i = end;
    }
}

fn saveLogs(conn: *CqlConn, rows: []const LogRow, bs: usize) !void {
    if (rows.len == 0) return;
    const chunk = @min(bs, MAX_BS);
    var v = preallocBuf(512);
    defer v.deinit(tempAllocator);
    var bd = preallocBuf(chunk *% 512);
    defer bd.deinit(tempAllocator);
    var starts: [MAX_BS + 1]usize = undefined;
    var ptrs: [MAX_BS][]const u8 = undefined;
    var i: usize = 0;
    while (i < rows.len) {
        const end = @min(i + chunk, rows.len);
        bd.items.len = 0;
        var enc: usize = 0;
        for (i..end) |j| {
            v.items.len = 0;
            starts[enc] = bd.items.len;
            const r = rows[j];
            try pool.valInt32(&v, r.chunk);
            try pool.valBigint(&v, r.blockNumber);
            try pool.valInt32(&v, r.transactionIndex);
            try pool.valInt32(&v, r.logIndex);
            try pool.valBigint(&v, r.blockTimestampS);
            try pool.valBigint(&v, r.blockTimestampMs);
            try pool.valTextRequired(&v, r.address);
            try pool.valTextRequired(&v, r.data);
            try pool.valText(&v, r.topicZeroth);
            try pool.valText(&v, r.topicFirst);
            try pool.valText(&v, r.topicSecond);
            try pool.valText(&v, r.topicThird);
            try pool.valListText(&v, r.restTopics);
            try pool.valTextRequired(&v, r.transactionHash);
            try pool.valBool(&v, r.removed);
            try bd.appendSlice(tempAllocator, v.items);
            enc += 1;
        }
        starts[enc] = bd.items.len;
        for (0..enc) |k| ptrs[k] = bd.items[starts[k]..starts[k + 1]];
        try conn.batchSendRows(conn.prepIds.logs, LOG_COLS, ptrs[0..enc]);
        i = end;
    }
}

fn saveInternalTxs(conn: *CqlConn, rows: []const InternalTxRow, bs: usize) !void {
    if (rows.len == 0) return;
    const chunk = @min(bs, MAX_BS);
    var v = preallocBuf(192);
    defer v.deinit(tempAllocator);
    var bd = preallocBuf(chunk *% 192);
    defer bd.deinit(tempAllocator);
    var starts: [MAX_BS + 1]usize = undefined;
    var ptrs: [MAX_BS][]const u8 = undefined;
    var i: usize = 0;
    while (i < rows.len) {
        const end = @min(i + chunk, rows.len);
        bd.items.len = 0;
        var enc: usize = 0;
        for (i..end) |j| {
            v.items.len = 0;
            starts[enc] = bd.items.len;
            const r = rows[j];
            try pool.valInt32(&v, r.chunk);
            try pool.valBigint(&v, r.blockNumber);
            try pool.valBigint(&v, r.blockTimestampS);
            try pool.valBigint(&v, r.blockTimestampMs);
            try pool.valInt32(&v, r.transactionIndex);
            try pool.valTextRequired(&v, r.transactionHash);
            try pool.valInt32(&v, r.traceIndex);
            try pool.valTextRequired(&v, r.fromAddress);
            try pool.valTextRequired(&v, r.toAddress);
            try pool.valVarint(&v, r.value);
            try bd.appendSlice(tempAllocator, v.items);
            enc += 1;
        }
        starts[enc] = bd.items.len;
        for (0..enc) |k| ptrs[k] = bd.items[starts[k]..starts[k + 1]];
        try conn.batchSendRows(conn.prepIds.internalTxs, ITX_COLS, ptrs[0..enc]);
        i = end;
    }
}

fn saveContracts(conn: *CqlConn, rows: []const ContractRow, bs: usize) !void {
    if (rows.len == 0) return;
    const chunk = @min(bs, MAX_BS);
    var v = preallocBuf(1024);
    defer v.deinit(tempAllocator);
    var bd = preallocBuf(chunk *% @as(usize, 1024));
    defer bd.deinit(tempAllocator);
    var starts: [MAX_BS + 1]usize = undefined;
    var ptrs: [MAX_BS][]const u8 = undefined;
    var i: usize = 0;
    while (i < rows.len) {
        const end = @min(i + chunk, rows.len);
        bd.items.len = 0;
        var enc: usize = 0;
        for (i..end) |j| {
            v.items.len = 0;
            starts[enc] = bd.items.len;
            const r = rows[j];
            try pool.valInt32(&v, r.chunk);
            try pool.valBigint(&v, r.blockNumber);
            try pool.valInt32(&v, r.transactionIndex);
            try pool.valTextRequired(&v, r.transactionHash);
            try pool.valInt32(&v, r.traceIndex);
            try pool.valBigint(&v, r.blockTimestampS);
            try pool.valBigint(&v, r.blockTimestampMs);
            try pool.valTextRequired(&v, r.address);
            try pool.valTinyint(&v, r.creationMethod);
            try pool.valTextRequired(&v, r.creatorAddress);
            try pool.valText(&v, r.contractFactory);
            try pool.valTextRequired(&v, r.creationBytecode);
            try pool.valTextRequired(&v, r.deployedBytecode);
            try bd.appendSlice(tempAllocator, v.items);
            enc += 1;
        }
        starts[enc] = bd.items.len;
        for (0..enc) |k| ptrs[k] = bd.items[starts[k]..starts[k + 1]];
        try conn.batchSendRows(conn.prepIds.contracts, CONTRACT_COLS, ptrs[0..enc]);
        i = end;
    }
}

fn saveContractsByAddr(conn: *CqlConn, rows: []const ContractByAddrRow, bs: usize) !void {
    if (rows.len == 0) return;
    const chunk = @min(bs, MAX_BS);
    var v = preallocBuf(512);
    defer v.deinit(tempAllocator);
    var bd = preallocBuf(chunk *% 512);
    defer bd.deinit(tempAllocator);
    var starts: [MAX_BS + 1]usize = undefined;
    var ptrs: [MAX_BS][]const u8 = undefined;
    var i: usize = 0;
    while (i < rows.len) {
        const end = @min(i + chunk, rows.len);
        bd.items.len = 0;
        var enc: usize = 0;
        for (i..end) |j| {
            v.items.len = 0;
            starts[enc] = bd.items.len;
            const r = rows[j];
            try pool.valTextRequired(&v, r.address);
            try pool.valTextRequired(&v, r.creator);
            try pool.valTextRequired(&v, r.txHash);
            try pool.valBigint(&v, r.blockNumber);
            try pool.valBigint(&v, r.timestamp);
            try pool.valText(&v, r.contractFactory);
            try pool.valTextRequired(&v, r.creationBytecode);
            try pool.valTextRequired(&v, r.deployedBytecode);
            try bd.appendSlice(tempAllocator, v.items);
            enc += 1;
        }
        starts[enc] = bd.items.len;
        for (0..enc) |k| ptrs[k] = bd.items[starts[k]..starts[k + 1]];
        try conn.batchSendRows(conn.prepIds.contractsByAddr, CONTRACT_CBA_COLS, ptrs[0..enc]);
        i = end;
    }
}

fn saveErc20Tokens(conn: *CqlConn, rows: []const Erc20TokenRow, bs: usize) !void {
    if (rows.len == 0) return;
    const chunk = @min(bs, MAX_BS);
    var v = preallocBuf(256);
    defer v.deinit(tempAllocator);
    var bd = preallocBuf(chunk *% 256);
    defer bd.deinit(tempAllocator);
    var starts: [MAX_BS + 1]usize = undefined;
    var ptrs: [MAX_BS][]const u8 = undefined;
    var i: usize = 0;
    while (i < rows.len) {
        const end = @min(i + chunk, rows.len);
        bd.items.len = 0;
        var enc: usize = 0;
        for (i..end) |j| {
            v.items.len = 0;
            starts[enc] = bd.items.len;
            const r = rows[j];
            try pool.valTextRequired(&v, r.address);
            try pool.valInt32(&v, r.chainId);
            try pool.valText(&v, r.name);
            try pool.valText(&v, r.symbol);
            if (r.decimals < 0) try pool.valNull(&v) else try pool.valSmallint(&v, r.decimals);
            try pool.valBool(&v, r.hasBalanceOf);
            try pool.valBool(&v, r.hasTransfer);
            try pool.valBool(&v, r.hasTransferFrom);
            try pool.valBool(&v, r.hasApprove);
            try pool.valBool(&v, r.hasAllowance);
            try pool.valBool(&v, r.isStandardDecimals);
            try pool.valBool(&v, r.isFullyFollowingStandard);
            try pool.valBool(&v, r.isMinimallyFollowingStandard);
            try pool.valBool(&v, r.isPartiallyFollowingStandard);
            try pool.valBool(&v, r.isNotFollowingStandard);
            try pool.valInt32(&v, r.detectionVersion);
            try bd.appendSlice(tempAllocator, v.items);
            enc += 1;
        }
        starts[enc] = bd.items.len;
        for (0..enc) |k| ptrs[k] = bd.items[starts[k]..starts[k + 1]];
        try conn.batchSendRows(conn.prepIds.erc20Tokens, ERC20_TOKEN_COLS, ptrs[0..enc]);
        i = end;
    }
}

fn saveErc20Supplies(conn: *CqlConn, rows: []const Erc20SupplyRow, bs: usize) !void {
    if (rows.len == 0) return;
    const chunk = @min(bs, MAX_BS);
    var v = preallocBuf(128);
    defer v.deinit(tempAllocator);
    var bd = preallocBuf(chunk *% 128);
    defer bd.deinit(tempAllocator);
    var starts: [MAX_BS + 1]usize = undefined;
    var ptrs: [MAX_BS][]const u8 = undefined;

    var i: usize = 0;
    while (i < rows.len) {
        const end = @min(i + chunk, rows.len);
        bd.items.len = 0;
        var enc: usize = 0;
        for (i..end) |j| {
            const r = rows[j];
            if (r.isUpdate) continue;
            v.items.len = 0;
            starts[enc] = bd.items.len;
            try pool.valTextRequired(&v, r.address);
            try pool.valInt32(&v, r.chainId);
            try pool.valText(&v, r.initialTotalSupply);
            try pool.valText(&v, r.latestTotalSupply);
            try pool.valBigint(&v, r.updatedAtBlock);
            try pool.valBigint(&v, r.updatedAtTimestamp);
            try bd.appendSlice(tempAllocator, v.items);
            enc += 1;
        }
        if (enc > 0) {
            starts[enc] = bd.items.len;
            for (0..enc) |k| ptrs[k] = bd.items[starts[k]..starts[k + 1]];
            try conn.batchSendRows(conn.prepIds.erc20SupplyInsert, ERC20_SUPPLY_INSERT_COLS, ptrs[0..enc]);
        }
        i = end;
    }

    i = 0;
    while (i < rows.len) {
        const end = @min(i + chunk, rows.len);
        bd.items.len = 0;
        var enc: usize = 0;
        for (i..end) |j| {
            const r = rows[j];
            if (!r.isUpdate) continue;
            v.items.len = 0;
            starts[enc] = bd.items.len;
            try pool.valText(&v, r.latestTotalSupply);
            try pool.valBigint(&v, r.updatedAtBlock);
            try pool.valBigint(&v, r.updatedAtTimestamp);
            try pool.valTextRequired(&v, r.address);
            try bd.appendSlice(tempAllocator, v.items);
            enc += 1;
        }
        if (enc > 0) {
            starts[enc] = bd.items.len;
            for (0..enc) |k| ptrs[k] = bd.items[starts[k]..starts[k + 1]];
            try conn.batchSendRows(conn.prepIds.erc20SupplyUpdate, ERC20_SUPPLY_UPDATE_COLS, ptrs[0..enc]);
        }
        i = end;
    }
}

fn saveErc20Owners(conn: *CqlConn, rows: []const Erc20OwnerRow, bs: usize) !void {
    if (rows.len == 0) return;
    const chunk = @min(bs, MAX_BS);
    var v = preallocBuf(128);
    defer v.deinit(tempAllocator);
    var bd = preallocBuf(chunk *% 128);
    defer bd.deinit(tempAllocator);
    var starts: [MAX_BS + 1]usize = undefined;
    var ptrs: [MAX_BS][]const u8 = undefined;

    var i: usize = 0;
    while (i < rows.len) {
        const end = @min(i + chunk, rows.len);
        bd.items.len = 0;
        var enc: usize = 0;
        for (i..end) |j| {
            const r = rows[j];
            if (r.isUpdate) continue;
            v.items.len = 0;
            starts[enc] = bd.items.len;
            try pool.valTextRequired(&v, r.address);
            try pool.valInt32(&v, r.chainId);
            try pool.valText(&v, r.initialOwner);
            try pool.valText(&v, r.latestOwner);
            try pool.valBool(&v, r.isOwnershipRenounced);
            try pool.valBigint(&v, r.updatedAtBlock);
            try pool.valBigint(&v, r.updatedAtTimestamp);
            try bd.appendSlice(tempAllocator, v.items);
            enc += 1;
        }
        if (enc > 0) {
            starts[enc] = bd.items.len;
            for (0..enc) |k| ptrs[k] = bd.items[starts[k]..starts[k + 1]];
            try conn.batchSendRows(conn.prepIds.erc20OwnerInsert, ERC20_OWNER_INSERT_COLS, ptrs[0..enc]);
        }
        i = end;
    }

    i = 0;
    while (i < rows.len) {
        const end = @min(i + chunk, rows.len);
        bd.items.len = 0;
        var enc: usize = 0;
        for (i..end) |j| {
            const r = rows[j];
            if (!r.isUpdate) continue;
            v.items.len = 0;
            starts[enc] = bd.items.len;
            try pool.valText(&v, r.latestOwner);
            try pool.valBool(&v, r.isOwnershipRenounced);
            try pool.valBigint(&v, r.updatedAtBlock);
            try pool.valBigint(&v, r.updatedAtTimestamp);
            try pool.valTextRequired(&v, r.address);
            try bd.appendSlice(tempAllocator, v.items);
            enc += 1;
        }
        if (enc > 0) {
            starts[enc] = bd.items.len;
            for (0..enc) |k| ptrs[k] = bd.items[starts[k]..starts[k + 1]];
            try conn.batchSendRows(conn.prepIds.erc20OwnerUpdate, ERC20_OWNER_UPDATE_COLS, ptrs[0..enc]);
        }
        i = end;
    }
}

fn saveErc20SelfDestructs(conn: *CqlConn, rows: []const Erc20SelfDestructRow, bs: usize) !void {
    if (rows.len == 0) return;
    const chunk = @min(bs, MAX_BS);
    var v = preallocBuf(64);
    defer v.deinit(tempAllocator);
    var bd = preallocBuf(chunk *% 64);
    defer bd.deinit(tempAllocator);
    var starts: [MAX_BS + 1]usize = undefined;
    var ptrs: [MAX_BS][]const u8 = undefined;
    var i: usize = 0;
    while (i < rows.len) {
        const end = @min(i + chunk, rows.len);
        bd.items.len = 0;
        var enc: usize = 0;
        for (i..end) |j| {
            v.items.len = 0;
            starts[enc] = bd.items.len;
            const r = rows[j];
            try pool.valTextRequired(&v, r.address);
            try pool.valInt32(&v, r.chainId);
            try pool.valBigint(&v, r.atBlock);
            try pool.valBigint(&v, r.atTimestamp);
            try bd.appendSlice(tempAllocator, v.items);
            enc += 1;
        }
        starts[enc] = bd.items.len;
        for (0..enc) |k| ptrs[k] = bd.items[starts[k]..starts[k + 1]];
        try conn.batchSendRows(conn.prepIds.erc20SelfDestruct, ERC20_SELFDESTRUCT_COLS, ptrs[0..enc]);
        i = end;
    }
}

fn saveBlockCompletions(conn: *CqlConn, ent: *const Entities) !void {
    if (ent.blocks.items.len == 0) return;
    const bs = 50;
    var v = preallocBuf(64);
    defer v.deinit(tempAllocator);
    var bd: std.ArrayList(u8) = .empty;
    defer bd.deinit(tempAllocator);
    var starts: [bs + 1]usize = undefined;
    var ptrs: [bs][]const u8 = undefined;
    var txIdx: usize = 0;
    var logIdx: usize = 0;
    var itxIdx: usize = 0;
    var conIdx: usize = 0;
    var i: usize = 0;
    while (i < ent.blocks.items.len) {
        const bEnd = @min(i + bs, ent.blocks.items.len);
        bd.items.len = 0;
        var enc: usize = 0;
        for (i..bEnd) |j| {
            const b = ent.blocks.items[j];
            var txCnt: i32 = 0;
            var logCnt: i32 = 0;
            var itxCnt: i32 = 0;
            var conCnt: i32 = 0;
            while (txIdx < ent.txs.items.len and ent.txs.items[txIdx].blockNumber == b.number) : (txIdx += 1) txCnt += 1;
            while (logIdx < ent.logs.items.len and ent.logs.items[logIdx].blockNumber == b.number) : (logIdx += 1) logCnt += 1;
            while (itxIdx < ent.internalTxs.items.len and ent.internalTxs.items[itxIdx].blockNumber == b.number) : (itxIdx += 1) itxCnt += 1;
            while (conIdx < ent.contracts.items.len and ent.contracts.items[conIdx].blockNumber == b.number) : (conIdx += 1) conCnt += 1;
            v.items.len = 0;
            starts[enc] = bd.items.len;
            try pool.valInt32(&v, b.chunk);
            try pool.valBigint(&v, b.number);
            try pool.valInt32(&v, txCnt);
            try pool.valInt32(&v, logCnt);
            try pool.valInt32(&v, itxCnt);
            try pool.valInt32(&v, conCnt);
            try bd.appendSlice(tempAllocator, v.items);
            enc += 1;
        }
        starts[enc] = bd.items.len;
        for (0..enc) |k| ptrs[k] = bd.items[starts[k]..starts[k + 1]];
        try conn.batchSendRows(conn.prepIds.blockCompletions, COMPLETION_COLS, ptrs[0..enc]);
        i = bEnd;
    }
}
