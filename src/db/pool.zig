// ScyllaDB client — CQL v4 binary protocol, raw Linux TCP, no connection pool.
// One CqlConn per worker thread. Caller owns connection lifecycle.
const std = @import("std");
const linux = std.os.linux;
const core = @import("indexer/core");

const structures = core.structures;

pub const BatchSizes = struct {
    blocks: usize,
    txs: usize,
    logs: usize,
    itxs: usize,
    contracts: usize,

    pub fn fromChain(opts: structures.EvmIndexingOptions) BatchSizes {
        return .{
            .blocks = @intCast(opts.batchSizeBlocks),
            .txs = @intCast(opts.batchSizeTxs),
            .logs = @intCast(opts.batchSizeLogs),
            .itxs = @intCast(opts.batchSizeItxs),
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
        (@as(u32, ip[2]) << 8) | @as(u32, ip[3]);
    const addr = linux.sockaddr.in{
        .family = linux.AF.INET,
        .port = std.mem.nativeToBig(u16, port),
        .addr = std.mem.nativeToBig(u32, ipHost),
        .zero = std.mem.zeroes([8]u8),
    };
    const rc = linux.connect(fd, @ptrCast(&addr), @sizeOf(linux.sockaddr.in));
    if (rc != 0) {
        _ = linux.close(fd);
        return error.ConnectFailed;
    }

    const nodelay: c_int = 1;
    _ = linux.setsockopt(fd, @as(c_int, @intCast(linux.IPPROTO.TCP)), linux.TCP.NODELAY, @ptrCast(&nodelay), @sizeOf(c_int));
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

const CQL_VERSION: u8 = 0x04;
const OPCODE_STARTUP: u8 = 0x01;
const OPCODE_AUTH_RESP: u8 = 0x0F;
const OPCODE_QUERY: u8 = 0x07;
const OPCODE_PREPARE: u8 = 0x09;
const OPCODE_EXECUTE: u8 = 0x0A;
const OPCODE_BATCH: u8 = 0x0D;
const OPCODE_READY: u8 = 0x02;
const OPCODE_AUTH: u8 = 0x03;
const OPCODE_AUTH_OK: u8 = 0x10;
const OPCODE_RESULT: u8 = 0x08;
const OPCODE_ERROR: u8 = 0x00;
const CONSISTENCY_ONE: u16 = 0x0001;

// Temporary allocator for CQL frame buffers. Lifetime: within each function call.
pub const tempAllocator = std.heap.page_allocator;

// ─── Best-effort GELF alert for CqlError ──────────────────────────────────────
// CqlConn has no Logger (would need a TCP socket per save-worker thread); instead
// main.zig sets these once at startup, before any worker threads are spawned, and
// every connection's recvFrameCheck fires a one-shot GELF send on CqlError. This is
// the only place "Batch too large" (and any other Scylla-side rejection) becomes
// visible in GrayLog — previously it only ever reached the raw stdout log file.
var alertHost: []const u8 = "127.0.0.1";
var alertPort: u16 = 12201;
var alertApp: []const u8 = "indexer";
var alertEnabled: bool = false;

pub fn configureAlerts(host: []const u8, port: u16, app: []const u8, enabled: bool) void {
    alertHost = host;
    alertPort = port;
    alertApp = app;
    alertEnabled = enabled;
}

// ─── pol.skipped_blocks — explicit, visible record of "gave up on this block" ──
// Set once at startup (main.zig), read by recordSkippedBlock() from any worker
// thread. A block only reaches recordSkippedBlock after exhausting primary AND
// neighbor RPC nodes — at that point pipeline.zig marks it ok=true with empty
// entities (so the watermark can advance past it instead of blocking the whole
// run forever), and this is the durable, queryable trail of which blocks that
// happened to. See docs/INTEGRITY_CHECKS.md.
var scyllaHost: []const u8 = "127.0.0.1";
var scyllaPort: u16 = 9042;
var scyllaKeyspace: []const u8 = "";
var scyllaUser: []const u8 = "";
var scyllaPass: []const u8 = "";

pub fn configureScylla(host: []const u8, port: u16, keyspace: []const u8, user: []const u8, pass: []const u8) void {
    scyllaHost = host;
    scyllaPort = port;
    scyllaKeyspace = keyspace;
    scyllaUser = user;
    scyllaPass = pass;
}

/// Best-effort: opens a one-shot connection, inserts one row, closes. Failure to
/// record is logged but never propagated — losing the audit trail is bad, but
/// must never be worse than the gap it's recording.
pub fn recordSkippedBlock(gpa: std.mem.Allocator, blockNumber: i64, chunk: i32, reason: []const u8) void {
    var conn = CqlConn.init(gpa, scyllaHost, scyllaPort, scyllaKeyspace, scyllaUser, scyllaPass) catch |e| {
        std.debug.print("[skipped_blocks] connect failed: {s}\n", .{@errorName(e)});
        return;
    };
    defer conn.deinit();

    var escBuf: [400]u8 = undefined;
    var escLen: usize = 0;
    for (reason) |c| {
        if (escLen + 2 > escBuf.len) break;
        switch (c) {
            '\'' => {
                escBuf[escLen] = '\'';
                escBuf[escLen + 1] = '\'';
                escLen += 2;
            },
            '\n', '\r' => {},
            else => {
                escBuf[escLen] = c;
                escLen += 1;
            },
        }
    }

    var queryBuf: [700]u8 = undefined;
    const query = std.fmt.bufPrint(
        &queryBuf,
        "INSERT INTO skipped_blocks (block_number, chunk, skipped_at, reason, resolved) VALUES ({d}, {d}, toTimestamp(now()), '{s}', false)",
        .{ blockNumber, chunk, escBuf[0..escLen] },
    ) catch {
        std.debug.print("[skipped_blocks] query format failed for block={d}\n", .{blockNumber});
        return;
    };

    conn.sendQuery(query) catch |e| {
        std.debug.print("[skipped_blocks] insert failed for block={d}: {s}\n", .{ blockNumber, @errorName(e) });
        return;
    };
    conn.recvFrameCheck() catch |e| {
        std.debug.print("[skipped_blocks] insert response error for block={d}: {s}\n", .{ blockNumber, @errorName(e) });
    };
}

fn alertCqlError(code: i32, msg: []const u8) void {
    if (!alertEnabled) return;
    const fd = tcpConnect(alertHost, alertPort) catch return;
    defer _ = linux.close(fd);

    var ts: linux.timespec = undefined;
    _ = linux.clock_gettime(.REALTIME, &ts);
    const tsFrac: u32 = @intCast(@divTrunc(ts.nsec, 1_000_000));

    var escBuf: [400]u8 = undefined;
    var escLen: usize = 0;
    for (msg) |c| {
        if (escLen + 2 > escBuf.len) break;
        switch (c) {
            '"' => {
                escBuf[escLen] = '\\';
                escBuf[escLen + 1] = '"';
                escLen += 2;
            },
            '\\' => {
                escBuf[escLen] = '\\';
                escBuf[escLen + 1] = '\\';
                escLen += 2;
            },
            '\n', '\r', '\t' => {},
            else => {
                escBuf[escLen] = c;
                escLen += 1;
            },
        }
    }

    var payloadBuf: [600]u8 = undefined;
    const payload = std.fmt.bufPrint(
        &payloadBuf,
        "{{\"version\":\"1.1\",\"host\":\"{s}\",\"short_message\":\"CqlError code=0x{x:0>4}: {s}\",\"level\":3,\"timestamp\":{d}.{d:0>3}}}\x00",
        .{ alertApp, code, escBuf[0..escLen], ts.sec, tsFrac },
    ) catch return;
    tcpWrite(fd, payload) catch {};
}

// ─── CQL value encoding ───────────────────────────────────────────────────────

fn appendShort(list: *std.ArrayList(u8), v: u16) !void {
    var b: [2]u8 = undefined;
    std.mem.writeInt(u16, &b, v, .big);
    try list.appendSlice(tempAllocator, &b);
}

fn appendCqlString(list: *std.ArrayList(u8), s: []const u8) !void {
    try appendShort(list, @intCast(s.len));
    try list.appendSlice(tempAllocator, s);
}

fn appendLongString(list: *std.ArrayList(u8), s: []const u8) !void {
    var b: [4]u8 = undefined;
    std.mem.writeInt(u32, &b, @intCast(s.len), .big);
    try list.appendSlice(tempAllocator, &b);
    try list.appendSlice(tempAllocator, s);
}

fn appendBytes(list: *std.ArrayList(u8), data: []const u8) !void {
    var b: [4]u8 = undefined;
    std.mem.writeInt(i32, &b, @intCast(data.len), .big);
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
    const oldLen = list.items.len;
    const outStart = oldLen + 4;
    try list.ensureTotalCapacity(tempAllocator, oldLen + 4 + 1 + nbytesMax);
    list.items.len = oldLen + 4 + 1 + nbytesMax;

    var wi = outStart;
    var i: usize = 0;
    if (hex.len % 2 != 0) {
        list.items[wi] = try hexNibble(hex[0]);
        wi += 1;
        i = 1;
    }
    while (i < hex.len) : ({
        i += 2;
        wi += 1;
    }) {
        list.items[wi] = (try hexNibble(hex[i]) << 4) | (try hexNibble(hex[i + 1]));
    }

    const rawLen = wi - outStart;
    var trim: usize = 0;
    while (trim + 1 < rawLen and list.items[outStart + trim] == 0) : (trim += 1) {}
    const trimmedLen = rawLen - trim;
    if (trim > 0)
        std.mem.copyForwards(u8, list.items[outStart..][0..trimmedLen], list.items[outStart + trim ..][0..trimmedLen]);

    const needPrefix = list.items[outStart] >= 0x80;
    const payloadLen = trimmedLen + if (needPrefix) @as(usize, 1) else 0;
    if (needPrefix) {
        std.mem.copyBackwards(u8, list.items[outStart + 1 ..][0..trimmedLen], list.items[outStart..][0..trimmedLen]);
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

const INSERT_BLOCKS = "INSERT INTO blocks (chunk,number,timestamp_s,timestamp_ms,miner) VALUES (?,?,?,?,?)";
const INSERT_TXS = "INSERT INTO transactions (chunk,block_number,transaction_index,hash,block_timestamp_s,block_timestamp_ms,method_id,input,from_address,to_address,value,gas_limit,gas_price,gas_used,max_priority_fee_per_gas,max_fee_per_gas,cumulative_gas_used,effective_gas_price,contract_address,status,type) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
const INSERT_LOGS = "INSERT INTO logs (chunk,block_number,transaction_index,log_index,block_timestamp_s,block_timestamp_ms,address,data,topic_zeroth,topic_first,topic_second,topic_third,rest_topics,transaction_hash,removed) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
const INSERT_INT_TXS = "INSERT INTO internal_transactions (chunk,block_number,block_timestamp_s,block_timestamp_ms,transaction_index,transaction_hash,trace_index,from_address,to_address,value) VALUES (?,?,?,?,?,?,?,?,?,?)";
const INSERT_CONTRACTS = "INSERT INTO contracts (chunk,block_number,transaction_index,transaction_hash,trace_index,block_timestamp_s,block_timestamp_ms,address,creation_method,creator_address,contract_factory,creation_bytecode,deployed_bytecode) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)";
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
        const opcode = header[4];
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

    pub fn sendQuery(self: *CqlConn, query: []const u8) !void {
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
                const code = std.mem.readInt(i32, resp.body[0..4], .big);
                const msgLen = std.mem.readInt(u16, resp.body[4..6], .big);
                const msg = resp.body[6..@min(6 + @as(usize, msgLen), resp.body.len)];
                std.debug.print("[CQL PREPARE ERROR] code=0x{x:0>4} msg={s}\nquery={s}\n", .{ code, msg, query });
            }
            return error.CqlPrepareError;
        }
        if (resp.opcode != OPCODE_RESULT) return error.CqlUnexpectedOpcode;
        if (resp.body.len < 6) return error.CqlMalformedResult;
        const kind = std.mem.readInt(i32, resp.body[0..4], .big);
        if (kind != 4) return error.CqlNotPrepared;
        const idLen = std.mem.readInt(u16, resp.body[4..6], .big);
        if (resp.body.len < 6 + idLen) return error.CqlMalformedResult;
        return try self.gpa.dupe(u8, resp.body[6..][0..idLen]);
    }

    pub fn recvFrameCheck(self: *CqlConn) !void {
        var header: [9]u8 = undefined;
        try tcpReadExact(self.fd, &header);
        const opcode = header[4];
        const bodyLen = std.mem.readInt(u32, header[5..9], .big);
        if (bodyLen == 0) {
            if (opcode == OPCODE_ERROR) {
                alertCqlError(-1, "");
                return error.CqlError;
            }
            return;
        }
        var tmp: [512]u8 = undefined;
        const readLen = @min(bodyLen, tmp.len);
        try tcpReadExact(self.fd, tmp[0..readLen]);
        if (opcode == OPCODE_ERROR) {
            const code = if (readLen >= 4) std.mem.readInt(i32, tmp[0..4], .big) else -1;
            const msgLen = if (readLen >= 6) std.mem.readInt(u16, tmp[4..6], .big) else 0;
            const msg = tmp[6..@min(6 + @as(usize, msgLen), readLen)];
            std.debug.print("[CQL ERROR] code=0x{x:0>4} msg={s}\n", .{ code, msg });
            alertCqlError(code, msg);
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

    // Single-statement EXECUTE — not subject to Scylla's batch_size_fail_threshold_in_kb,
    // which applies even to a BATCH containing just one statement. Used as the floor for
    // batchSendRows' size-based split, so one pathologically large row (e.g. contract
    // bytecode) can never be rejected by the batch-size limit, only the (much larger)
    // native protocol frame size limit.
    pub fn executeRow(self: *CqlConn, prepId: []const u8, nVals: u16, rowBuf: []const u8) !void {
        var frame: std.ArrayList(u8) = .empty;
        defer frame.deinit(tempAllocator);
        var tmp: [2]u8 = undefined;
        std.mem.writeInt(u16, &tmp, @intCast(prepId.len), .big);
        try frame.appendSlice(tempAllocator, &tmp);
        try frame.appendSlice(tempAllocator, prepId);
        std.mem.writeInt(u16, &tmp, CONSISTENCY_ONE, .big);
        try frame.appendSlice(tempAllocator, &tmp);
        try frame.append(tempAllocator, 0x01); // flags: VALUES present
        std.mem.writeInt(u16, &tmp, nVals, .big);
        try frame.appendSlice(tempAllocator, &tmp);
        try frame.appendSlice(tempAllocator, rowBuf);
        try self.sendFrame(OPCODE_EXECUTE, frame.items);
        try self.recvFrameCheck();
    }

    pub fn batchSendRows(self: *CqlConn, prepId: []const u8, nVals: u16, rowBufs: []const []const u8) !void {
        if (rowBufs.len == 0) return;
        if (rowBufs.len == 1) return self.executeRow(prepId, nVals, rowBufs[0]);
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

pub fn prepareAll(conn: *CqlConn) !PreparedIds {
    return .{
        .blocks = try conn.prepare(INSERT_BLOCKS),
        .transactions = try conn.prepare(INSERT_TXS),
        .logs = try conn.prepare(INSERT_LOGS),
        .internalTxs = try conn.prepare(INSERT_INT_TXS),
        .contracts = try conn.prepare(INSERT_CONTRACTS),
        .contractsByAddr = try conn.prepare(INSERT_CONTRACTS_BY_ADDR),
        .blockCompletions = try conn.prepare(INSERT_BLOCK_COMPLETIONS),
    };
}
