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

    // Without this, a half-dead connection (e.g. the remote restarted but
    // didn't get to send a clean RST/FIN, or a network partition) blocks
    // read()/write() forever — no application-level reconnect logic ever
    // gets a chance to run because the syscall never returns. CQL is a
    // tight request/response protocol, so a short bound is safe.
    const tv = linux.timeval{ .sec = 10, .usec = 0 };
    _ = linux.setsockopt(fd, linux.SOL.SOCKET, linux.SO.RCVTIMEO, @ptrCast(&tv), @sizeOf(linux.timeval));
    _ = linux.setsockopt(fd, linux.SOL.SOCKET, linux.SO.SNDTIMEO, @ptrCast(&tv), @sizeOf(linux.timeval));
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
const OPCODE_OPTIONS: u8 = 0x05;
const OPCODE_QUERY: u8 = 0x07;
const OPCODE_PREPARE: u8 = 0x09;
const OPCODE_BATCH: u8 = 0x0D;
const OPCODE_READY: u8 = 0x02;
const OPCODE_AUTH: u8 = 0x03;
const OPCODE_AUTH_OK: u8 = 0x10;
const OPCODE_SUPPORTED: u8 = 0x06;
const OPCODE_RESULT: u8 = 0x08;
const OPCODE_ERROR: u8 = 0x00;
const CONSISTENCY_ONE: u16 = 0x0001;

// Temporary allocator for CQL frame buffers. Lifetime: within each function call.
pub const tempAllocator = std.heap.page_allocator;

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

pub fn valSmallint(list: *std.ArrayList(u8), v: i16) !void {
    var b: [6]u8 = undefined;
    std.mem.writeInt(i32, b[0..4], 2, .big);
    std.mem.writeInt(i16, b[4..6], v, .big);
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
    erc20Tokens: []u8,
    erc20SupplyInsert: []u8,
    erc20SupplyUpdate: []u8,
    erc20OwnerInsert: []u8,
    erc20OwnerUpdate: []u8,
    erc20SelfDestruct: []u8,
};

const INSERT_BLOCKS = "INSERT INTO blocks (chunk,number,timestamp_s,timestamp_ms,miner) VALUES (?,?,?,?,?)";
const INSERT_TXS = "INSERT INTO transactions (chunk,block_number,transaction_index,hash,block_timestamp_s,block_timestamp_ms,method_id,input,from_address,to_address,value,gas_limit,gas_price,gas_used,max_priority_fee_per_gas,max_fee_per_gas,cumulative_gas_used,effective_gas_price,contract_address,status,type) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
const INSERT_LOGS = "INSERT INTO logs (chunk,block_number,transaction_index,log_index,block_timestamp_s,block_timestamp_ms,address,data,topic_zeroth,topic_first,topic_second,topic_third,rest_topics,transaction_hash,removed) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
const INSERT_INT_TXS = "INSERT INTO internal_transactions (chunk,block_number,block_timestamp_s,block_timestamp_ms,transaction_index,transaction_hash,trace_index,from_address,to_address,value) VALUES (?,?,?,?,?,?,?,?,?,?)";
const INSERT_CONTRACTS = "INSERT INTO contracts (chunk,block_number,transaction_index,transaction_hash,trace_index,block_timestamp_s,block_timestamp_ms,address,creation_method,creator_address,contract_factory,creation_bytecode,deployed_bytecode) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)";
const INSERT_CONTRACTS_BY_ADDR = "INSERT INTO contracts_by_addresses (address,creator,tx_hash,block_number,timestamp,contract_factory,creation_bytecode,deployed_bytecode) VALUES (?,?,?,?,?,?,?,?)";
const INSERT_BLOCK_COMPLETIONS = "INSERT INTO block_completions (chunk,block_number,tx_count,log_count,itx_count,contract_count) VALUES (?,?,?,?,?,?)";
const INSERT_ERC20_TOKENS = "INSERT INTO erc20_tokens (address,chain_id,name,symbol,decimals,has_balance_of,has_transfer,has_transfer_from,has_approve,has_allowance,is_standard_decimals,is_fully_following_standard,is_minimally_following_standard,is_partially_following_standard,is_not_following_standard,detection_version) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
const INSERT_ERC20_SUPPLY = "INSERT INTO erc20_total_supplies (address,chain_id,initial_total_supply,latest_total_supply,updated_at_block,updated_at_timestamp) VALUES (?,?,?,?,?,?)";
const UPDATE_ERC20_SUPPLY = "UPDATE erc20_total_supplies SET latest_total_supply=?,updated_at_block=?,updated_at_timestamp=? WHERE address=?";
const INSERT_ERC20_OWNER = "INSERT INTO erc20_owners (address,chain_id,initial_owner,latest_owner,is_ownership_renounced,updated_at_block,updated_at_timestamp) VALUES (?,?,?,?,?,?,?)";
const UPDATE_ERC20_OWNER = "UPDATE erc20_owners SET latest_owner=?,is_ownership_renounced=?,updated_at_block=?,updated_at_timestamp=? WHERE address=?";
const INSERT_ERC20_SELFDESTRUCT = "INSERT INTO erc20_self_destructed (address,chain_id,at_block,at_timestamp) VALUES (?,?,?,?)";

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
        freePreparedIds(self.gpa, self.prepIds);
        _ = linux.close(self.fd);
    }

    /// Lightweight liveness probe (CQL OPTIONS/SUPPORTED) — no query execution,
    /// just confirms the socket is still accepted by the server. Used for
    /// periodic health checks in the realtime loop.
    pub fn ping(self: *CqlConn) !void {
        try self.sendFrame(OPCODE_OPTIONS, &.{});
        const resp = try self.recvFrame();
        defer self.gpa.free(resp.body);
        if (resp.opcode != OPCODE_SUPPORTED) return error.CqlPingFailed;
    }

    /// Tears down the current socket/prepared statements and reconnects in
    /// place — callers holding a `*CqlConn` keep a valid connection after this
    /// returns, without needing to know the pointer changed underneath them.
    /// Used to recover from a dropped connection (e.g. Scylla restart) without
    /// restarting the whole indexer process.
    pub fn reopen(
        self: *CqlConn,
        host: []const u8,
        port: u16,
        keyspace: []const u8,
        user: []const u8,
        pass: []const u8,
    ) !void {
        _ = linux.close(self.fd);
        self.fd = -1; // leave the connection visibly dead until reinit succeeds —
        // never let a stale fd number (reusable by the OS after close()) be
        // mistaken for a live one if init() below fails and returns early.
        freePreparedIds(self.gpa, self.prepIds);
        self.prepIds = undefined;
        const fresh = try CqlConn.init(self.gpa, host, port, keyspace, user, pass);
        self.* = fresh;
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
            if (opcode == OPCODE_ERROR) return error.CqlError;
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

fn freePreparedIds(gpa: std.mem.Allocator, ids: PreparedIds) void {
    gpa.free(ids.blocks);
    gpa.free(ids.transactions);
    gpa.free(ids.logs);
    gpa.free(ids.internalTxs);
    gpa.free(ids.contracts);
    gpa.free(ids.contractsByAddr);
    gpa.free(ids.blockCompletions);
    gpa.free(ids.erc20Tokens);
    gpa.free(ids.erc20SupplyInsert);
    gpa.free(ids.erc20SupplyUpdate);
    gpa.free(ids.erc20OwnerInsert);
    gpa.free(ids.erc20OwnerUpdate);
    gpa.free(ids.erc20SelfDestruct);
}

pub fn prepareAll(conn: *CqlConn) !PreparedIds {
    return .{
        .blocks = try conn.prepare(INSERT_BLOCKS),
        .transactions = try conn.prepare(INSERT_TXS),
        .logs = try conn.prepare(INSERT_LOGS),
        .internalTxs = try conn.prepare(INSERT_INT_TXS),
        .contracts = try conn.prepare(INSERT_CONTRACTS),
        .contractsByAddr = try conn.prepare(INSERT_CONTRACTS_BY_ADDR),
        .blockCompletions = try conn.prepare(INSERT_BLOCK_COMPLETIONS),
        .erc20Tokens = try conn.prepare(INSERT_ERC20_TOKENS),
        .erc20SupplyInsert = try conn.prepare(INSERT_ERC20_SUPPLY),
        .erc20SupplyUpdate = try conn.prepare(UPDATE_ERC20_SUPPLY),
        .erc20OwnerInsert = try conn.prepare(INSERT_ERC20_OWNER),
        .erc20OwnerUpdate = try conn.prepare(UPDATE_ERC20_OWNER),
        .erc20SelfDestruct = try conn.prepare(INSERT_ERC20_SELFDESTRUCT),
    };
}
