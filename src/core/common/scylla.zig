// ScyllaDB client — CQL v4 binary protocol, raw Linux TCP, no connection pool.
// One CqlConn per worker thread. Caller owns connection lifecycle.
const std = @import("std");
const linux = std.os.linux;
const transform = @import("chains/evm/transform.zig");
const structures = @import("indexer/core").structures;

pub const BatchSizes = struct {
    blocks:    usize,
    txs:       usize,
    logs:      usize,
    itxs:      usize,
    contracts: usize,

    pub fn fromChain(opts: structures.EvmIndexingOptions) BatchSizes {
        return .{
            .blocks    = @intCast(opts.batchSizeBlocks),
            .txs       = @intCast(opts.batchSizeTxs),
            .logs      = @intCast(opts.batchSizeLogs),
            .itxs      = @intCast(opts.batchSizeItxs),
            .contracts = @intCast(opts.batchSizeContracts),
        };
    }
};

// ─── TCP helpers ──────────────────────────────────────────────────────────────

fn parseIpv4(host: []const u8) [4]u8 {
    var ip: [4]u8 = .{ 127, 0, 0, 1 };
    var iter = std.mem.splitScalar(u8, host, '.');
    var i: usize = 0;
    while (iter.next()) |p| : (i += 1) {
        if (i >= 4) break;
        ip[i] = std.fmt.parseInt(u8, p, 10) catch 0;
    }
    return ip;
}

fn tcpConnect(host: []const u8, port: u16) !i32 {
    const sockFd = linux.socket(linux.AF.INET, linux.SOCK.STREAM, 0);
    if (sockFd > @as(usize, std.math.maxInt(i32))) return error.SocketFailed;
    const fd: i32 = @intCast(sockFd);

    const ip = parseIpv4(host);
    const ipHost = (@as(u32, ip[0]) << 24) | (@as(u32, ip[1]) << 16) |
                   (@as(u32, ip[2]) << 8)  |  @as(u32, ip[3]);
    const addr = linux.sockaddr.in{
        .family = linux.AF.INET,
        .port   = std.mem.nativeToBig(u16, port),
        .addr   = std.mem.nativeToBig(u32, ipHost),
        .zero   = std.mem.zeroes([8]u8),
    };
    const rc = linux.connect(fd, @ptrCast(&addr), @sizeOf(linux.sockaddr.in));
    if (rc != 0) { _ = linux.close(fd); return error.ConnectFailed; }

    const nodelay: c_int = 1;
    _ = linux.setsockopt(fd, @as(c_int, @intCast(linux.IPPROTO.TCP)),
        linux.TCP.NODELAY, @ptrCast(&nodelay), @sizeOf(c_int));
    return fd;
}

fn tcpWrite(fd: i32, data: []const u8) !void {
    var written: usize = 0;
    while (written < data.len) {
        const n = linux.write(fd, data[written..].ptr, data.len - written);
        if (n == 0 or n > data.len) return error.WriteFailed;
        written += n;
    }
}

fn tcpReadExact(fd: i32, buf: []u8) !void {
    var pos: usize = 0;
    while (pos < buf.len) {
        const n = linux.read(fd, buf[pos..].ptr, buf.len - pos);
        if (n == 0) return error.ConnectionClosed;
        if (n > buf.len) return error.ReadFailed;
        pos += n;
    }
}

// ─── CQL v4 frame constants ───────────────────────────────────────────────────

const CQL_VERSION:     u8  = 0x04;
const OPCODE_STARTUP:  u8  = 0x01;
const OPCODE_AUTH_RESP: u8 = 0x0F;
const OPCODE_QUERY:    u8  = 0x07;
const OPCODE_PREPARE:  u8  = 0x09;
const OPCODE_BATCH:    u8  = 0x0D;
const OPCODE_READY:    u8  = 0x02;
const OPCODE_AUTH:     u8  = 0x03;
const OPCODE_AUTH_OK:  u8  = 0x10;
const OPCODE_RESULT:   u8  = 0x08;
const OPCODE_ERROR:    u8  = 0x00;
const CONSISTENCY_ONE: u16 = 0x0001;

// Temporary allocator for CQL frame buffers. Lifetime: within each function call.
const tempAllocator = std.heap.page_allocator;

// ─── CQL value encoding ───────────────────────────────────────────────────────

fn appendShort(list: *std.ArrayList(u8), v: u16) !void {
    var b: [2]u8 = undefined; std.mem.writeInt(u16, &b, v, .big);
    try list.appendSlice(tempAllocator, &b);
}

fn appendCqlString(list: *std.ArrayList(u8), s: []const u8) !void {
    try appendShort(list, @intCast(s.len));
    try list.appendSlice(tempAllocator, s);
}

fn appendLongString(list: *std.ArrayList(u8), s: []const u8) !void {
    var b: [4]u8 = undefined; std.mem.writeInt(u32, &b, @intCast(s.len), .big);
    try list.appendSlice(tempAllocator, &b);
    try list.appendSlice(tempAllocator, s);
}

fn appendBytes(list: *std.ArrayList(u8), data: []const u8) !void {
    var b: [4]u8 = undefined; std.mem.writeInt(i32, &b, @intCast(data.len), .big);
    try list.appendSlice(tempAllocator, &b);
    try list.appendSlice(tempAllocator, data);
}

pub fn valNull(list: *std.ArrayList(u8)) !void {
    try list.appendSlice(tempAllocator, &.{ 0xFF, 0xFF, 0xFF, 0xFF });
}

pub fn valBigint(list: *std.ArrayList(u8), v: i64) !void {
    var b: [12]u8 = undefined;
    std.mem.writeInt(i32, b[0..4], 8, .big);
    std.mem.writeInt(i64, b[4..12], v, .big);
    try list.appendSlice(tempAllocator, &b);
}

pub fn valVarint(list: *std.ArrayList(u8), hexS: []const u8) !void {
    var hex = hexS;
    if (std.mem.startsWith(u8, hex, "0x") or std.mem.startsWith(u8, hex, "0X"))
        hex = hex[2..];
    if (hex.len == 0) return list.appendSlice(tempAllocator, &.{ 0, 0, 0, 1, 0 });

    const nbytesMax = (hex.len + 1) / 2;
    const oldLen   = list.items.len;
    const outStart = oldLen + 4;
    try list.ensureTotalCapacity(tempAllocator, oldLen + 4 + 1 + nbytesMax);
    list.items.len = oldLen + 4 + 1 + nbytesMax;

    var wi = outStart;
    var i: usize = 0;
    if (hex.len % 2 != 0) { list.items[wi] = try hexNibble(hex[0]); wi += 1; i = 1; }
    while (i < hex.len) : ({ i += 2; wi += 1; }) {
        list.items[wi] = (try hexNibble(hex[i]) << 4) | (try hexNibble(hex[i + 1]));
    }

    const rawLen = wi - outStart;
    var trim: usize = 0;
    while (trim + 1 < rawLen and list.items[outStart + trim] == 0) : (trim += 1) {}
    const trimmedLen = rawLen - trim;
    if (trim > 0)
        std.mem.copyForwards(u8, list.items[outStart..][0..trimmedLen],
                                 list.items[outStart + trim..][0..trimmedLen]);

    const needPrefix = list.items[outStart] >= 0x80;
    const payloadLen = trimmedLen + if (needPrefix) @as(usize, 1) else 0;
    if (needPrefix) {
        std.mem.copyBackwards(u8, list.items[outStart + 1..][0..trimmedLen],
                                  list.items[outStart..][0..trimmedLen]);
        list.items[outStart] = 0x00;
    }
    std.mem.writeInt(i32, list.items[oldLen..][0..4], @intCast(payloadLen), .big);
    list.items.len = oldLen + 4 + payloadLen;
}

inline fn hexNibble(c: u8) !u8 {
    return switch (c) {
        '0'...'9' => c - '0',
        'a'...'f' => c - 'a' + 10,
        'A'...'F' => c - 'A' + 10,
        else => error.InvalidHex,
    };
}

pub fn valInt32(list: *std.ArrayList(u8), v: i32) !void {
    var b: [8]u8 = undefined;
    std.mem.writeInt(i32, b[0..4], 4, .big);
    std.mem.writeInt(i32, b[4..8], v, .big);
    try list.appendSlice(tempAllocator, &b);
}

pub fn valTinyint(list: *std.ArrayList(u8), v: i8) !void {
    var b: [5]u8 = undefined;
    std.mem.writeInt(i32, b[0..4], 1, .big);
    b[4] = @bitCast(v);
    try list.appendSlice(tempAllocator, &b);
}

pub fn valText(list: *std.ArrayList(u8), s: []const u8) !void {
    if (s.len == 0) return valNull(list);
    var b: [4]u8 = undefined;
    std.mem.writeInt(i32, &b, @intCast(s.len), .big);
    try list.appendSlice(tempAllocator, &b);
    try list.appendSlice(tempAllocator, s);
}

pub fn valTextRequired(list: *std.ArrayList(u8), s: []const u8) !void {
    var b: [4]u8 = undefined;
    std.mem.writeInt(i32, &b, @intCast(s.len), .big);
    try list.appendSlice(tempAllocator, &b);
    try list.appendSlice(tempAllocator, s);
}

pub fn valBool(list: *std.ArrayList(u8), v: bool) !void {
    var b: [5]u8 = undefined;
    std.mem.writeInt(i32, b[0..4], 1, .big);
    b[4] = if (v) 1 else 0;
    try list.appendSlice(tempAllocator, &b);
}

pub fn valListText(list: *std.ArrayList(u8), items: []const []const u8) !void {
    var total: usize = 4;
    for (items) |item| total += 4 + item.len;
    var b: [4]u8 = undefined;
    std.mem.writeInt(i32, &b, @intCast(total), .big);
    try list.appendSlice(tempAllocator, &b);
    std.mem.writeInt(i32, &b, @intCast(items.len), .big);
    try list.appendSlice(tempAllocator, &b);
    for (items) |item| {
        std.mem.writeInt(i32, &b, @intCast(item.len), .big);
        try list.appendSlice(tempAllocator, &b);
        try list.appendSlice(tempAllocator, item);
    }
}

// ─── Prepared statement IDs ───────────────────────────────────────────────────

pub const PreparedIds = struct {
    blocks: []u8,
    transactions: []u8,
    logs: []u8,
    internalTxs: []u8,
    contracts: []u8,
    contractsByAddr: []u8,
    blockCompletions: []u8,
};

const INSERT_BLOCKS          = "INSERT INTO blocks (chunk,number,timestamp_s,timestamp_ms,miner) VALUES (?,?,?,?,?)";
const INSERT_TXS             = "INSERT INTO transactions (chunk,block_number,transaction_index,hash,block_timestamp_s,block_timestamp_ms,method_id,input,from_address,to_address,value,gas_limit,gas_price,gas_used,max_priority_fee_per_gas,max_fee_per_gas,cumulative_gas_used,effective_gas_price,contract_address,status,type) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
const INSERT_LOGS             = "INSERT INTO logs (chunk,block_number,transaction_index,log_index,block_timestamp_s,block_timestamp_ms,address,data,topic_zeroth,topic_first,topic_second,topic_third,rest_topics,transaction_hash,removed) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
const INSERT_INT_TXS          = "INSERT INTO internal_transactions (chunk,block_number,block_timestamp_s,block_timestamp_ms,transaction_index,transaction_hash,trace_index,from_address,to_address,value) VALUES (?,?,?,?,?,?,?,?,?,?)";
const INSERT_CONTRACTS        = "INSERT INTO contracts (chunk,block_number,transaction_index,transaction_hash,trace_index,block_timestamp_s,block_timestamp_ms,address,creation_method,creator_address,contract_factory,creation_bytecode,deployed_bytecode) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)";
const INSERT_CONTRACTS_BY_ADDR = "INSERT INTO contracts_by_addresses (address,creator,tx_hash,block_number,timestamp,contract_factory,creation_bytecode,deployed_bytecode) VALUES (?,?,?,?,?,?,?,?)";
const INSERT_BLOCK_COMPLETIONS = "INSERT INTO block_completions (chunk,block_number,tx_count,log_count,itx_count,contract_count) VALUES (?,?,?,?,?,?)";

// ─── CqlConn ──────────────────────────────────────────────────────────────────

pub const CqlConn = struct {
    fd: i32,
    gpa: std.mem.Allocator,
    prepIds: PreparedIds = undefined,

    pub fn init(
        gpa: std.mem.Allocator,
        host: []const u8,
        port: u16,
        keyspace: []const u8,
        user: []const u8,
        pass: []const u8,
    ) !CqlConn {
        const fd = try tcpConnect(host, port);
        var self = CqlConn{ .fd = fd, .gpa = gpa };
        errdefer _ = linux.close(self.fd);

        try self.sendStartup();
        {
            const r = try self.recvFrame();
            self.gpa.free(r.body);
            if (r.opcode == OPCODE_AUTH) {
                try self.sendAuthResponse(user, pass);
                const ar = try self.recvFrame();
                self.gpa.free(ar.body);
                if (ar.opcode != OPCODE_AUTH_OK) return error.CqlAuthFailed;
            } else if (r.opcode != OPCODE_READY) {
                return error.CqlUnexpectedOpcode;
            }
        }

        const useQuery = try std.fmt.allocPrint(gpa, "USE {s}", .{keyspace});
        defer gpa.free(useQuery);
        try self.sendQuery(useQuery);
        {
            const r = try self.recvFrame();
            self.gpa.free(r.body);
            if (r.opcode == OPCODE_ERROR) return error.CqlUseKeyspaceFailed;
        }

        self.prepIds = try prepareAll(&self);
        return self;
    }

    pub fn deinit(self: *CqlConn) void {
        self.gpa.free(self.prepIds.blocks);
        self.gpa.free(self.prepIds.transactions);
        self.gpa.free(self.prepIds.logs);
        self.gpa.free(self.prepIds.internalTxs);
        self.gpa.free(self.prepIds.contracts);
        self.gpa.free(self.prepIds.contractsByAddr);
        self.gpa.free(self.prepIds.blockCompletions);
        _ = linux.close(self.fd);
    }

    fn sendFrame(self: *CqlConn, opcode: u8, body: []const u8) !void {
        var header: [9]u8 = undefined;
        header[0] = CQL_VERSION;
        header[1] = 0x00;
        std.mem.writeInt(u16, header[2..4], 1, .big);
        header[4] = opcode;
        std.mem.writeInt(u32, header[5..9], @intCast(body.len), .big);
        try tcpWrite(self.fd, &header);
        if (body.len > 0) try tcpWrite(self.fd, body);
    }

    fn recvFrame(self: *CqlConn) !struct { opcode: u8, body: []u8 } {
        var header: [9]u8 = undefined;
        try tcpReadExact(self.fd, &header);
        const opcode  = header[4];
        const bodyLen = std.mem.readInt(u32, header[5..9], .big);
        if (bodyLen == 0) return .{ .opcode = opcode, .body = try self.gpa.alloc(u8, 0) };
        const body = try self.gpa.alloc(u8, bodyLen);
        errdefer self.gpa.free(body);
        try tcpReadExact(self.fd, body);
        return .{ .opcode = opcode, .body = body };
    }

    fn sendStartup(self: *CqlConn) !void {
        var body: std.ArrayList(u8) = .empty;
        defer body.deinit(tempAllocator);
        try appendShort(&body, 1);
        try appendCqlString(&body, "CQL_VERSION");
        try appendCqlString(&body, "3.0.0");
        try self.sendFrame(OPCODE_STARTUP, body.items);
    }

    fn sendAuthResponse(self: *CqlConn, user: []const u8, pass: []const u8) !void {
        var sasl: std.ArrayList(u8) = .empty;
        defer sasl.deinit(tempAllocator);
        try sasl.append(tempAllocator, 0);
        try sasl.appendSlice(tempAllocator, user);
        try sasl.append(tempAllocator, 0);
        try sasl.appendSlice(tempAllocator, pass);
        var body: std.ArrayList(u8) = .empty;
        defer body.deinit(tempAllocator);
        try appendBytes(&body, sasl.items);
        try self.sendFrame(OPCODE_AUTH_RESP, body.items);
    }

    fn sendQuery(self: *CqlConn, query: []const u8) !void {
        var body: std.ArrayList(u8) = .empty;
        defer body.deinit(tempAllocator);
        try appendLongString(&body, query);
        try appendShort(&body, CONSISTENCY_ONE);
        try body.append(tempAllocator, 0x00);
        try self.sendFrame(OPCODE_QUERY, body.items);
    }

    pub fn prepare(self: *CqlConn, query: []const u8) ![]u8 {
        var body: std.ArrayList(u8) = .empty;
        defer body.deinit(tempAllocator);
        try appendLongString(&body, query);
        try body.appendSlice(tempAllocator, &[4]u8{ 0, 0, 0, 0 });
        try self.sendFrame(OPCODE_PREPARE, body.items);

        const resp = try self.recvFrame();
        defer self.gpa.free(resp.body);
        if (resp.opcode == OPCODE_ERROR) {
            if (resp.body.len >= 6) {
                const code   = std.mem.readInt(i32, resp.body[0..4], .big);
                const msgLen = std.mem.readInt(u16, resp.body[4..6], .big);
                const msg    = resp.body[6..@min(6 + @as(usize, msgLen), resp.body.len)];
                std.debug.print("[CQL PREPARE ERROR] code=0x{x:0>4} msg={s}\nquery={s}\n",
                    .{ code, msg, query });
            }
            return error.CqlPrepareError;
        }
        if (resp.opcode != OPCODE_RESULT) return error.CqlUnexpectedOpcode;
        if (resp.body.len < 6) return error.CqlMalformedResult;
        const kind  = std.mem.readInt(i32, resp.body[0..4], .big);
        if (kind != 4) return error.CqlNotPrepared;
        const idLen = std.mem.readInt(u16, resp.body[4..6], .big);
        if (resp.body.len < 6 + idLen) return error.CqlMalformedResult;
        return try self.gpa.dupe(u8, resp.body[6..][0..idLen]);
    }

    fn recvFrameCheck(self: *CqlConn) !void {
        var header: [9]u8 = undefined;
        try tcpReadExact(self.fd, &header);
        const opcode  = header[4];
        const bodyLen = std.mem.readInt(u32, header[5..9], .big);
        if (bodyLen == 0) {
            if (opcode == OPCODE_ERROR) return error.CqlError;
            return;
        }
        var tmp: [512]u8 = undefined;
        const readLen = @min(bodyLen, tmp.len);
        try tcpReadExact(self.fd, tmp[0..readLen]);
        if (opcode == OPCODE_ERROR) {
            const code   = if (readLen >= 4) std.mem.readInt(i32, tmp[0..4], .big) else -1;
            const msgLen = if (readLen >= 6) std.mem.readInt(u16, tmp[4..6], .big) else 0;
            const msg    = tmp[6..@min(6 + @as(usize, msgLen), readLen)];
            std.debug.print("[CQL ERROR] code=0x{x:0>4} msg={s}\n", .{ code, msg });
            return error.CqlError;
        }
        var remain: usize = bodyLen -| readLen;
        var discard: [128]u8 = undefined;
        while (remain > 0) {
            const n = @min(remain, discard.len);
            try tcpReadExact(self.fd, discard[0..n]);
            remain -= n;
        }
    }

    pub fn batchSendRows(self: *CqlConn, prepId: []const u8, nVals: u16, rowBufs: []const []const u8) !void {
        if (rowBufs.len == 0) return;
        var frame: std.ArrayList(u8) = .empty;
        defer frame.deinit(tempAllocator);
        frame.ensureTotalCapacity(tempAllocator, 10 + rowBufs.len * (5 + prepId.len + 300)) catch {};

        var tmp: [2]u8 = undefined;
        try frame.append(tempAllocator, 0x01); // UNLOGGED
        std.mem.writeInt(u16, &tmp, @intCast(rowBufs.len), .big);
        try frame.appendSlice(tempAllocator, &tmp);
        for (rowBufs) |rb| {
            try frame.append(tempAllocator, 0x01); // kind=PREPARED
            std.mem.writeInt(u16, &tmp, @intCast(prepId.len), .big);
            try frame.appendSlice(tempAllocator, &tmp);
            try frame.appendSlice(tempAllocator, prepId);
            std.mem.writeInt(u16, &tmp, nVals, .big);
            try frame.appendSlice(tempAllocator, &tmp);
            try frame.appendSlice(tempAllocator, rb);
        }
        std.mem.writeInt(u16, &tmp, CONSISTENCY_ONE, .big);
        try frame.appendSlice(tempAllocator, &tmp);
        try frame.append(tempAllocator, 0x00);

        if (frame.items.len > 400 * 1024 and rowBufs.len > 1) {
            const mid = rowBufs.len / 2;
            try self.batchSendRows(prepId, nVals, rowBufs[0..mid]);
            try self.batchSendRows(prepId, nVals, rowBufs[mid..]);
            return;
        }
        try self.sendFrame(OPCODE_BATCH, frame.items);
        try self.recvFrameCheck();
    }
};

// ─── RealtimeConns ────────────────────────────────────────────────────────────
// 32 persistent CQL connections for parallel per-table writes in realtime mode.
// Split matches historical: 1 blk + 3 txs + 6 logs + 20 itxs + 1 contracts + 1 comp.

pub const RealtimeConns = struct {
    blk:       CqlConn,
    txs:       [ACCUM_TXS_LANES]CqlConn,
    logs:      [ACCUM_LOG_LANES]CqlConn,
    itxs:      [ACCUM_ITX_LANES]CqlConn,
    contracts: CqlConn,
    comp:      CqlConn,

    pub fn init(
        gpa:  std.mem.Allocator,
        host: []const u8, port: u16,
        ks:   []const u8,
        user: []const u8, pass: []const u8,
    ) !RealtimeConns {
        var self: RealtimeConns = undefined;
        self.blk       = try CqlConn.init(gpa, host, port, ks, user, pass);
        for (&self.txs)  |*c| c.* = try CqlConn.init(gpa, host, port, ks, user, pass);
        for (&self.logs) |*c| c.* = try CqlConn.init(gpa, host, port, ks, user, pass);
        for (&self.itxs) |*c| c.* = try CqlConn.init(gpa, host, port, ks, user, pass);
        self.contracts = try CqlConn.init(gpa, host, port, ks, user, pass);
        self.comp      = try CqlConn.init(gpa, host, port, ks, user, pass);
        return self;
    }

    pub fn deinit(self: *RealtimeConns) void {
        self.blk.deinit();
        for (&self.txs)  |*c| c.deinit();
        for (&self.logs) |*c| c.deinit();
        for (&self.itxs) |*c| c.deinit();
        self.contracts.deinit();
        self.comp.deinit();
    }
};

pub fn saveBlockRt(conns: *RealtimeConns, ent: *const transform.Entities, bs: BatchSizes) !void {
    const ents = [1]*const transform.Entities{ent};
    try saveEntitiesParallel(
        &conns.blk, &conns.txs, &conns.contracts,
        &conns.logs, &conns.itxs, &conns.comp,
        @constCast(&ents),
        bs,
    );
}

fn prepareAll(conn: *CqlConn) !PreparedIds {
    return .{
        .blocks           = try conn.prepare(INSERT_BLOCKS),
        .transactions     = try conn.prepare(INSERT_TXS),
        .logs             = try conn.prepare(INSERT_LOGS),
        .internalTxs      = try conn.prepare(INSERT_INT_TXS),
        .contracts        = try conn.prepare(INSERT_CONTRACTS),
        .contractsByAddr  = try conn.prepare(INSERT_CONTRACTS_BY_ADDR),
        .blockCompletions = try conn.prepare(INSERT_BLOCK_COMPLETIONS),
    };
}

fn preallocBuf(est: usize) std.ArrayList(u8) {
    var v: std.ArrayList(u8) = .empty;
    v.ensureTotalCapacity(tempAllocator, est) catch {};
    return v;
}

// Max rows per batch — stack arrays sized to this to avoid heap allocs.
const MAX_BS: usize = 512;

// ─── Save a single block's entities ──────────────────────────────────────────

pub fn saveBlock(conn: *CqlConn, ent: *const transform.Entities, bs: BatchSizes) !void {
    try saveBlocks(conn, ent.blocks.items, bs.blocks);
    try saveTxs(conn, ent.txs.items, bs.txs);
    try saveLogs(conn, ent.logs.items, bs.logs);
    try saveInternalTxs(conn, ent.internalTxs.items, bs.itxs);
    try saveContracts(conn, ent.contracts.items, bs.contracts);
    try saveContractsByAddr(conn, ent.contractsByAddr.items, bs.contracts);
    try saveBlockCompletions(conn, ent);
}

// ─── Parallel save: N entities over 32 CQL connections ───────────────────────
// TableSave: blocks or contracts — sequential over all entities on one connection.
// TableLaneSave: txs/logs/itxs — each lane handles 1/N of the rows in parallel.

const TableSave = struct {
    conn: *CqlConn,
    ents: []*const transform.Entities,
    bs:   BatchSizes,
    err:  ?anyerror = null,
};

fn saveBlockRowsForEntities(g: *TableSave) void {
    for (g.ents) |ent| {
        saveBlocks(g.conn, ent.blocks.items, g.bs.blocks) catch |e| { g.err = e; return; };
    }
}

fn saveContractRowsForEntities(g: *TableSave) void {
    for (g.ents) |ent| {
        saveContracts(g.conn, ent.contracts.items, g.bs.contracts) catch |e| { g.err = e; return; };
        saveContractsByAddr(g.conn, ent.contractsByAddr.items, g.bs.contracts) catch |e| { g.err = e; return; };
    }
}

const TableLaneSave = struct {
    conn:   *CqlConn,
    ents:   []*const transform.Entities,
    bs:     BatchSizes,
    lane:   usize,
    nLanes: usize,
    err:    ?anyerror = null,
};

fn saveLogRowsForLane(g: *TableLaneSave) void {
    var rows: std.ArrayList(transform.LogRow) = .empty;
    defer rows.deinit(tempAllocator);
    for (g.ents) |ent| {
        const all   = ent.logs.items;
        const start = all.len * g.lane / g.nLanes;
        const end   = all.len * (g.lane + 1) / g.nLanes;
        rows.appendSlice(tempAllocator, all[start..end]) catch |e| { g.err = e; return; };
    }
    saveLogs(g.conn, rows.items, g.bs.logs) catch |e| { g.err = e; return; };
}

fn saveItxRowsForLane(g: *TableLaneSave) void {
    var rows: std.ArrayList(transform.InternalTxRow) = .empty;
    defer rows.deinit(tempAllocator);
    for (g.ents) |ent| {
        const all   = ent.internalTxs.items;
        const start = all.len * g.lane / g.nLanes;
        const end   = all.len * (g.lane + 1) / g.nLanes;
        rows.appendSlice(tempAllocator, all[start..end]) catch |e| { g.err = e; return; };
    }
    saveInternalTxs(g.conn, rows.items, g.bs.itxs) catch |e| { g.err = e; return; };
}

fn saveTxRowsForLane(g: *TableLaneSave) void {
    var rows: std.ArrayList(transform.TxRow) = .empty;
    defer rows.deinit(tempAllocator);
    for (g.ents) |ent| {
        const all   = ent.txs.items;
        const start = all.len * g.lane / g.nLanes;
        const end   = all.len * (g.lane + 1) / g.nLanes;
        rows.appendSlice(tempAllocator, all[start..end]) catch |e| { g.err = e; return; };
    }
    saveTxs(g.conn, rows.items, g.bs.txs) catch |e| { g.err = e; return; };
}

// Batch-write all completion rows for a slice of entities in one CQL round-trip.
// Each entity has exactly one block; sends at most 50 rows per batch frame.
fn saveBlockCompletionsBatch(conn: *CqlConn, ents: []*const transform.Entities) !void {
    if (ents.len == 0) return;
    const BATCH: usize = 50;
    var rowBuf:   std.ArrayList(u8) = preallocBuf(64); defer rowBuf.deinit(tempAllocator);
    var batchBuf: std.ArrayList(u8) = .empty;          defer batchBuf.deinit(tempAllocator);
    var starts: [BATCH + 1]usize = undefined;
    var ptrs:   [BATCH][]const u8 = undefined;
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
            try valInt32(&rowBuf, b.chunk);
            try valBigint(&rowBuf, b.number);
            try valInt32(&rowBuf, @as(i32, @intCast(ent.txs.items.len)));
            try valInt32(&rowBuf, @as(i32, @intCast(ent.logs.items.len)));
            try valInt32(&rowBuf, @as(i32, @intCast(ent.internalTxs.items.len)));
            try valInt32(&rowBuf, @as(i32, @intCast(ent.contracts.items.len)));
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
// Connection split matching production SPLIT=1,3,6,20,1,1.

pub const ACCUM_TXS_LANES: usize = 3;
pub const ACCUM_LOG_LANES: usize = 6;
pub const ACCUM_ITX_LANES: usize = 20;

pub fn saveEntitiesParallel(
    connBlocks:    *CqlConn,
    connTxs:       *[ACCUM_TXS_LANES]CqlConn,
    connContracts: *CqlConn,
    connLogs:      *[ACCUM_LOG_LANES]CqlConn,
    connItxs:      *[ACCUM_ITX_LANES]CqlConn,
    connComp:      *CqlConn,
    ents:          []*const transform.Entities,
    bs:            BatchSizes,
) !void {
    if (ents.len == 0) return;

    var gBlocks    = TableSave{ .conn = connBlocks,    .ents = ents, .bs = bs };
    var gContracts = TableSave{ .conn = connContracts, .ents = ents, .bs = bs };
    var gTxs:  [ACCUM_TXS_LANES]TableLaneSave = undefined;
    var gLogs: [ACCUM_LOG_LANES]TableLaneSave = undefined;
    var gItxs: [ACCUM_ITX_LANES]TableLaneSave = undefined;

    for (0..ACCUM_TXS_LANES) |i|
        gTxs[i]  = .{ .conn = &connTxs[i],  .ents = ents, .bs = bs, .lane = i, .nLanes = ACCUM_TXS_LANES };
    for (0..ACCUM_LOG_LANES) |i|
        gLogs[i] = .{ .conn = &connLogs[i], .ents = ents, .bs = bs, .lane = i, .nLanes = ACCUM_LOG_LANES };
    for (0..ACCUM_ITX_LANES) |i|
        gItxs[i] = .{ .conn = &connItxs[i], .ents = ents, .bs = bs, .lane = i, .nLanes = ACCUM_ITX_LANES };

    // Spawns 30 threads: 1 (blocks) + 3 (txs) + 6 (logs) + 20 (itxs).
    // Contracts run inline on the caller's thread (few rows, fast path).
    const nSpawn = 1 + ACCUM_TXS_LANES + ACCUM_LOG_LANES + ACCUM_ITX_LANES;
    var threads: [nSpawn]std.Thread = undefined;
    var spawned: usize = 0;
    errdefer for (threads[0..spawned]) |t| t.join();

    for (0..ACCUM_TXS_LANES) |i| {
        threads[spawned] = try std.Thread.spawn(.{}, saveTxRowsForLane, .{&gTxs[i]});
        spawned += 1;
    }
    for (0..ACCUM_LOG_LANES) |i| {
        threads[spawned] = try std.Thread.spawn(.{}, saveLogRowsForLane, .{&gLogs[i]});
        spawned += 1;
    }
    for (0..ACCUM_ITX_LANES) |i| {
        threads[spawned] = try std.Thread.spawn(.{}, saveItxRowsForLane, .{&gItxs[i]});
        spawned += 1;
    }
    threads[spawned] = try std.Thread.spawn(.{}, saveBlockRowsForEntities, .{&gBlocks});
    spawned += 1;

    saveContractRowsForEntities(&gContracts);

    for (threads[0..spawned]) |t| t.join();

    if (gBlocks.err)    |e| return e;
    if (gContracts.err) |e| return e;
    for (gTxs)  |g| if (g.err) |e| return e;
    for (gLogs) |g| if (g.err) |e| return e;
    for (gItxs) |g| if (g.err) |e| return e;

    try saveBlockCompletionsBatch(connComp, ents);
}

// ─── Low-level table writers ──────────────────────────────────────────────────
// Column counts match the prepared statement parameter counts (schema order).
const BLOCK_COLS:        u16 = 5;
const TX_COLS:           u16 = 21;
const LOG_COLS:          u16 = 15;
const ITX_COLS:          u16 = 10;
const CONTRACT_COLS:     u16 = 13;
const CONTRACT_CBA_COLS: u16 = 8;
const COMPLETION_COLS:   u16 = 6;

fn saveBlocks(conn: *CqlConn, rows: []const transform.BlockRow, bs: usize) !void {
    if (rows.len == 0) return;
    const chunk = @min(bs, MAX_BS);
    var v  = preallocBuf(96);           defer v.deinit(tempAllocator);
    var bd = preallocBuf(chunk *% 96);  defer bd.deinit(tempAllocator);
    var starts: [MAX_BS + 1]usize = undefined;
    var ptrs:   [MAX_BS][]const u8 = undefined;
    var i: usize = 0;
    while (i < rows.len) {
        const end = @min(i + chunk, rows.len); bd.items.len = 0; var enc: usize = 0;
        for (i..end) |j| {
            v.items.len = 0; starts[enc] = bd.items.len;
            const r = rows[j];
            try valInt32(&v, r.chunk); try valBigint(&v, r.number);
            try valBigint(&v, r.timestampS); try valBigint(&v, r.timestampMs);
            try valTextRequired(&v, r.miner);
            try bd.appendSlice(tempAllocator, v.items); enc += 1;
        }
        starts[enc] = bd.items.len;
        for (0..enc) |k| ptrs[k] = bd.items[starts[k]..starts[k + 1]];
        try conn.batchSendRows(conn.prepIds.blocks, BLOCK_COLS, ptrs[0..enc]);
        i = end;
    }
}

fn saveTxs(conn: *CqlConn, rows: []const transform.TxRow, bs: usize) !void {
    if (rows.len == 0) return;
    const chunk = @min(bs, MAX_BS);
    var v  = preallocBuf(512);           defer v.deinit(tempAllocator);
    var bd = preallocBuf(chunk *% 512);  defer bd.deinit(tempAllocator);
    var starts: [MAX_BS + 1]usize = undefined;
    var ptrs:   [MAX_BS][]const u8 = undefined;
    var i: usize = 0;
    while (i < rows.len) {
        const end = @min(i + chunk, rows.len); bd.items.len = 0; var enc: usize = 0;
        for (i..end) |j| {
            v.items.len = 0; starts[enc] = bd.items.len;
            const r = rows[j];
            try valInt32(&v, r.chunk); try valBigint(&v, r.blockNumber);
            try valInt32(&v, r.transactionIndex); try valTextRequired(&v, r.hash);
            try valBigint(&v, r.blockTimestampS); try valBigint(&v, r.blockTimestampMs);
            try valText(&v, r.methodId); try valText(&v, r.input);
            try valTextRequired(&v, r.fromAddress); try valText(&v, r.toAddress);
            try valVarint(&v, r.value);
            try valBigint(&v, r.gasLimit); try valBigint(&v, r.gasPrice);
            try valBigint(&v, r.gasUsed); try valBigint(&v, r.maxPriorityFee);
            try valBigint(&v, r.maxFee); try valBigint(&v, r.cumulativeGasUsed);
            try valBigint(&v, r.effectiveGasPrice); try valText(&v, r.contractAddress);
            try valTinyint(&v, r.status); try valTinyint(&v, r.txType);
            try bd.appendSlice(tempAllocator, v.items); enc += 1;
        }
        starts[enc] = bd.items.len;
        for (0..enc) |k| ptrs[k] = bd.items[starts[k]..starts[k + 1]];
        try conn.batchSendRows(conn.prepIds.transactions, TX_COLS, ptrs[0..enc]);
        i = end;
    }
}

fn saveLogs(conn: *CqlConn, rows: []const transform.LogRow, bs: usize) !void {
    if (rows.len == 0) return;
    const chunk = @min(bs, MAX_BS);
    var v  = preallocBuf(512);           defer v.deinit(tempAllocator);
    var bd = preallocBuf(chunk *% 512);  defer bd.deinit(tempAllocator);
    var starts: [MAX_BS + 1]usize = undefined;
    var ptrs:   [MAX_BS][]const u8 = undefined;
    var i: usize = 0;
    while (i < rows.len) {
        const end = @min(i + chunk, rows.len); bd.items.len = 0; var enc: usize = 0;
        for (i..end) |j| {
            v.items.len = 0; starts[enc] = bd.items.len;
            const r = rows[j];
            try valInt32(&v, r.chunk); try valBigint(&v, r.blockNumber);
            try valInt32(&v, r.transactionIndex); try valInt32(&v, r.logIndex);
            try valBigint(&v, r.blockTimestampS); try valBigint(&v, r.blockTimestampMs);
            try valTextRequired(&v, r.address); try valTextRequired(&v, r.data);
            try valText(&v, r.topicZeroth); try valText(&v, r.topicFirst);
            try valText(&v, r.topicSecond); try valText(&v, r.topicThird);
            try valListText(&v, r.restTopics); try valTextRequired(&v, r.transactionHash);
            try valBool(&v, r.removed);
            try bd.appendSlice(tempAllocator, v.items); enc += 1;
        }
        starts[enc] = bd.items.len;
        for (0..enc) |k| ptrs[k] = bd.items[starts[k]..starts[k + 1]];
        try conn.batchSendRows(conn.prepIds.logs, LOG_COLS, ptrs[0..enc]);
        i = end;
    }
}

fn saveInternalTxs(conn: *CqlConn, rows: []const transform.InternalTxRow, bs: usize) !void {
    if (rows.len == 0) return;
    const chunk = @min(bs, MAX_BS);
    var v  = preallocBuf(192);           defer v.deinit(tempAllocator);
    var bd = preallocBuf(chunk *% 192);  defer bd.deinit(tempAllocator);
    var starts: [MAX_BS + 1]usize = undefined;
    var ptrs:   [MAX_BS][]const u8 = undefined;
    var i: usize = 0;
    while (i < rows.len) {
        const end = @min(i + chunk, rows.len); bd.items.len = 0; var enc: usize = 0;
        for (i..end) |j| {
            v.items.len = 0; starts[enc] = bd.items.len;
            const r = rows[j];
            try valInt32(&v, r.chunk); try valBigint(&v, r.blockNumber);
            try valBigint(&v, r.blockTimestampS); try valBigint(&v, r.blockTimestampMs);
            try valInt32(&v, r.transactionIndex); try valTextRequired(&v, r.transactionHash);
            try valInt32(&v, r.traceIndex);
            try valTextRequired(&v, r.fromAddress); try valTextRequired(&v, r.toAddress);
            try valVarint(&v, r.value);
            try bd.appendSlice(tempAllocator, v.items); enc += 1;
        }
        starts[enc] = bd.items.len;
        for (0..enc) |k| ptrs[k] = bd.items[starts[k]..starts[k + 1]];
        try conn.batchSendRows(conn.prepIds.internalTxs, ITX_COLS, ptrs[0..enc]);
        i = end;
    }
}

fn saveContracts(conn: *CqlConn, rows: []const transform.ContractRow, bs: usize) !void {
    if (rows.len == 0) return;
    const chunk = @min(bs, MAX_BS);
    var v  = preallocBuf(1024);                        defer v.deinit(tempAllocator);
    var bd = preallocBuf(chunk *% @as(usize, 1024));   defer bd.deinit(tempAllocator);
    var starts: [MAX_BS + 1]usize = undefined;
    var ptrs:   [MAX_BS][]const u8 = undefined;
    var i: usize = 0;
    while (i < rows.len) {
        const end = @min(i + chunk, rows.len); bd.items.len = 0; var enc: usize = 0;
        for (i..end) |j| {
            v.items.len = 0; starts[enc] = bd.items.len;
            const r = rows[j];
            try valInt32(&v, r.chunk); try valBigint(&v, r.blockNumber);
            try valInt32(&v, r.transactionIndex); try valTextRequired(&v, r.transactionHash);
            try valInt32(&v, r.traceIndex);
            try valBigint(&v, r.blockTimestampS); try valBigint(&v, r.blockTimestampMs);
            try valTextRequired(&v, r.address); try valTinyint(&v, r.creationMethod);
            try valTextRequired(&v, r.creatorAddress); try valText(&v, r.contractFactory);
            try valTextRequired(&v, r.creationBytecode); try valTextRequired(&v, r.deployedBytecode);
            try bd.appendSlice(tempAllocator, v.items); enc += 1;
        }
        starts[enc] = bd.items.len;
        for (0..enc) |k| ptrs[k] = bd.items[starts[k]..starts[k + 1]];
        try conn.batchSendRows(conn.prepIds.contracts, CONTRACT_COLS, ptrs[0..enc]);
        i = end;
    }
}

fn saveContractsByAddr(conn: *CqlConn, rows: []const transform.ContractByAddrRow, bs: usize) !void {
    if (rows.len == 0) return;
    const chunk = @min(bs, MAX_BS);
    var v  = preallocBuf(512);           defer v.deinit(tempAllocator);
    var bd = preallocBuf(chunk *% 512);  defer bd.deinit(tempAllocator);
    var starts: [MAX_BS + 1]usize = undefined;
    var ptrs:   [MAX_BS][]const u8 = undefined;
    var i: usize = 0;
    while (i < rows.len) {
        const end = @min(i + chunk, rows.len); bd.items.len = 0; var enc: usize = 0;
        for (i..end) |j| {
            v.items.len = 0; starts[enc] = bd.items.len;
            const r = rows[j];
            try valTextRequired(&v, r.address); try valTextRequired(&v, r.creator);
            try valTextRequired(&v, r.txHash); try valBigint(&v, r.blockNumber);
            try valBigint(&v, r.timestamp); try valText(&v, r.contractFactory);
            try valTextRequired(&v, r.creationBytecode); try valTextRequired(&v, r.deployedBytecode);
            try bd.appendSlice(tempAllocator, v.items); enc += 1;
        }
        starts[enc] = bd.items.len;
        for (0..enc) |k| ptrs[k] = bd.items[starts[k]..starts[k + 1]];
        try conn.batchSendRows(conn.prepIds.contractsByAddr, CONTRACT_CBA_COLS, ptrs[0..enc]);
        i = end;
    }
}

fn saveBlockCompletions(conn: *CqlConn, ent: *const transform.Entities) !void {
    if (ent.blocks.items.len == 0) return;
    const bs = 50;
    var v  = preallocBuf(64); defer v.deinit(tempAllocator);
    var bd: std.ArrayList(u8) = .empty; defer bd.deinit(tempAllocator);
    var starts: [bs + 1]usize = undefined;
    var ptrs:   [bs][]const u8 = undefined;
    var txIdx: usize = 0; var logIdx: usize = 0;
    var itxIdx: usize = 0; var conIdx: usize = 0;
    var i: usize = 0;
    while (i < ent.blocks.items.len) {
        const bEnd = @min(i + bs, ent.blocks.items.len);
        bd.items.len = 0; var enc: usize = 0;
        for (i..bEnd) |j| {
            const b = ent.blocks.items[j];
            var txCnt: i32 = 0; var logCnt: i32 = 0;
            var itxCnt: i32 = 0; var conCnt: i32 = 0;
            while (txIdx  < ent.txs.items.len         and ent.txs.items[txIdx].blockNumber         == b.number) : (txIdx  += 1) txCnt  += 1;
            while (logIdx < ent.logs.items.len         and ent.logs.items[logIdx].blockNumber       == b.number) : (logIdx += 1) logCnt += 1;
            while (itxIdx < ent.internalTxs.items.len  and ent.internalTxs.items[itxIdx].blockNumber == b.number) : (itxIdx += 1) itxCnt += 1;
            while (conIdx < ent.contracts.items.len    and ent.contracts.items[conIdx].blockNumber  == b.number) : (conIdx += 1) conCnt += 1;
            v.items.len = 0; starts[enc] = bd.items.len;
            try valInt32(&v, b.chunk); try valBigint(&v, b.number);
            try valInt32(&v, txCnt); try valInt32(&v, logCnt);
            try valInt32(&v, itxCnt); try valInt32(&v, conCnt);
            try bd.appendSlice(tempAllocator, v.items); enc += 1;
        }
        starts[enc] = bd.items.len;
        for (0..enc) |k| ptrs[k] = bd.items[starts[k]..starts[k + 1]];
        try conn.batchSendRows(conn.prepIds.blockCompletions, COMPLETION_COLS, ptrs[0..enc]);
        i = bEnd;
    }
}
