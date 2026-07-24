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
const OPCODE_EXECUTE: u8 = 0x0A;
const CONSISTENCY_ONE: u16 = 0x0001;
const CONSISTENCY_LOCAL_SERIAL: u16 = 0x0009;

// Temporary allocator for CQL frame buffers. Lifetime: within each function call.
pub const tempAllocator = std.heap.page_allocator;

// ─── SELECT result ────────────────────────────────────────────────────────────

// Rows from a CQL SELECT. Each row is a slice of nullable byte columns.
// All memory lives in an internal arena; call deinit() when done.
pub const SelectResult = struct {
    rows: []const []const ?[]const u8,
    _arena: std.heap.ArenaAllocator,

    pub fn deinit(self: *SelectResult) void {
        self._arena.deinit();
    }
};

// Skip a CQL [short string]: [u16 len] + len bytes.
fn skipShortStr(body: []const u8, pos: usize) usize {
    if (pos + 2 > body.len) return body.len;
    const len = std.mem.readInt(u16, body[pos..][0..2], .big);
    return pos + 2 + len;
}

// Skip a CQL [option]: [u16 type_code] + optional extra bytes.
fn skipCqlOption(body: []const u8, pos: usize) usize {
    if (pos + 2 > body.len) return body.len;
    const code = std.mem.readInt(u16, body[pos..][0..2], .big);
    var p = pos + 2;
    switch (code) {
        0x0000 => p = skipShortStr(body, p), // CUSTOM: class name
        0x0020, 0x0022 => p = skipCqlOption(body, p), // LIST, SET: element type
        0x0021 => { // MAP: key type + value type
            p = skipCqlOption(body, p);
            p = skipCqlOption(body, p);
        },
        0x0030 => { // UDT
            p = skipShortStr(body, p); // keyspace
            p = skipShortStr(body, p); // type name
            if (p + 2 > body.len) return body.len;
            const n = std.mem.readInt(u16, body[p..][0..2], .big);
            p += 2;
            for (0..n) |_| {
                p = skipShortStr(body, p);
                p = skipCqlOption(body, p);
            }
        },
        0x0031 => { // TUPLE
            if (p + 2 > body.len) return body.len;
            const n = std.mem.readInt(u16, body[p..][0..2], .big);
            p += 2;
            for (0..n) |_| p = skipCqlOption(body, p);
        },
        else => {}, // all simple types: no extra bytes
    }
    return p;
}

fn parseRowsResult(body: []const u8, gpa: std.mem.Allocator) !SelectResult {
    if (body.len < 12) return error.CqlMalformedResult;
    const kind = std.mem.readInt(i32, body[0..4], .big);
    if (kind != 2) return error.CqlNotRows;
    const flags = std.mem.readInt(i32, body[4..8], .big);
    const columns_count: usize = @intCast(std.mem.readInt(i32, body[8..12], .big));
    var pos: usize = 12;

    // Skip paging state
    if (flags & 0x02 != 0) {
        if (pos + 4 > body.len) return error.CqlMalformedResult;
        const ps_len = std.mem.readInt(i32, body[pos..][0..4], .big);
        pos += 4;
        if (ps_len > 0) pos += @intCast(ps_len);
    }

    // Skip column metadata
    if (flags & 0x04 == 0) { // has metadata
        if (flags & 0x01 != 0) { // global table spec
            pos = skipShortStr(body, pos);
            pos = skipShortStr(body, pos);
        }
        for (0..columns_count) |_| {
            if (flags & 0x01 == 0) {
                pos = skipShortStr(body, pos);
                pos = skipShortStr(body, pos);
            }
            pos = skipShortStr(body, pos); // column name
            pos = skipCqlOption(body, pos); // column type
        }
    }

    if (pos + 4 > body.len) return error.CqlMalformedResult;
    const rows_count: usize = @intCast(std.mem.readInt(i32, body[pos..][0..4], .big));
    pos += 4;

    var arena = std.heap.ArenaAllocator.init(gpa);
    errdefer arena.deinit();
    const aa = arena.allocator();

    const rows = try aa.alloc([]const ?[]const u8, rows_count);
    for (0..rows_count) |i| {
        const cols = try aa.alloc(?[]const u8, columns_count);
        for (0..columns_count) |j| {
            if (pos + 4 > body.len) return error.CqlMalformedResult;
            const vlen = std.mem.readInt(i32, body[pos..][0..4], .big);
            pos += 4;
            if (vlen < 0) {
                cols[j] = null;
            } else {
                const ulen: usize = @intCast(vlen);
                if (pos + ulen > body.len) return error.CqlMalformedResult;
                cols[j] = try aa.dupe(u8, body[pos..][0..ulen]);
                pos += ulen;
            }
        }
        rows[i] = cols;
    }
    return .{ .rows = rows, ._arena = arena };
}

// Parse [applied] boolean from an LWT INSERT IF NOT EXISTS result.
// First column of first row is the boolean.
fn parseLWTResult(body: []const u8) !bool {
    if (body.len < 12) return error.CqlMalformedResult;
    const kind = std.mem.readInt(i32, body[0..4], .big);
    if (kind != 2) return error.CqlNotRows;
    const flags = std.mem.readInt(i32, body[4..8], .big);
    const columns_count: usize = @intCast(std.mem.readInt(i32, body[8..12], .big));
    var pos: usize = 12;

    if (flags & 0x02 != 0) {
        if (pos + 4 > body.len) return error.CqlMalformedResult;
        const ps_len = std.mem.readInt(i32, body[pos..][0..4], .big);
        pos += 4;
        if (ps_len > 0) pos += @intCast(ps_len);
    }
    if (flags & 0x04 == 0) {
        if (flags & 0x01 != 0) {
            pos = skipShortStr(body, pos);
            pos = skipShortStr(body, pos);
        }
        for (0..columns_count) |_| {
            if (flags & 0x01 == 0) {
                pos = skipShortStr(body, pos);
                pos = skipShortStr(body, pos);
            }
            pos = skipShortStr(body, pos);
            pos = skipCqlOption(body, pos);
        }
    }
    if (pos + 4 > body.len) return error.CqlMalformedResult;
    const rows_count = std.mem.readInt(i32, body[pos..][0..4], .big);
    pos += 4;
    if (rows_count == 0) return false;
    // First row, first column = [applied] boolean
    if (pos + 4 > body.len) return error.CqlMalformedResult;
    const vlen = std.mem.readInt(i32, body[pos..][0..4], .big);
    pos += 4;
    if (vlen < 1) return false;
    if (pos >= body.len) return error.CqlMalformedResult;
    return body[pos] != 0;
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

// CQL blob: 4-byte length prefix + raw bytes. Same wire encoding as text.
pub fn valBlob(list: *std.ArrayList(u8), data: []const u8) !void {
    var b: [4]u8 = undefined;
    std.mem.writeInt(i32, &b, @intCast(data.len), .big);
    try list.appendSlice(tempAllocator, &b);
    try list.appendSlice(tempAllocator, data);
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

// Scylla can evict a prepared statement from its cache under pressure (e.g.
// many concurrent connections each preparing the same query) without the
// connection itself dying — the server replies UNPREPARED (code 0x2500) on
// the next EXECUTE/BATCH using that id. The CQL-correct response is to
// transparently re-PREPARE and retry, not fail the whole batch — so each
// cached id carries its source query text for that purpose.
pub const Prepared = struct {
    id: []u8,
    query: []const u8,
};

pub const PreparedIds = struct {
    blocks: Prepared,
    transactions: Prepared,
    logs: Prepared,
    internalTxs: Prepared,
    blockCompletions: Prepared,
    forkedBlock: Prepared,
    erc20Tokens: Prepared,
    erc20SupplyInsert: Prepared,
    erc20SupplyUpdate: Prepared,
    erc20OwnerInsert: Prepared,
    erc20OwnerUpdate: Prepared,
    erc20SelfDestruct: Prepared,
    bcScan: Prepared,
    verifiedErasAll: Prepared,
    verifiedErasInsert: Prepared,
};

const INSERT_BLOCKS = "INSERT INTO blocks (chunk,number,timestamp_s,timestamp_ms,miner) VALUES (?,?,?,?,?)";
const INSERT_TXS = "INSERT INTO transactions (chunk,block_number,transaction_index,hash,block_timestamp_s,block_timestamp_ms,method_id,input,from_address,to_address,value,gas_limit,gas_price,gas_used,max_priority_fee_per_gas,max_fee_per_gas,cumulative_gas_used,effective_gas_price,contract_address,status,type) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
const INSERT_LOGS = "INSERT INTO logs (chunk,block_number,transaction_index,log_index,block_timestamp_s,block_timestamp_ms,address,data,topic_zeroth,topic_first,topic_second,topic_third,rest_topics,transaction_hash,removed) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
const INSERT_INT_TXS = "INSERT INTO internal_transactions (chunk,block_number,block_timestamp_s,block_timestamp_ms,transaction_index,transaction_hash,trace_index,from_address,to_address,value) VALUES (?,?,?,?,?,?,?,?,?,?)";
const INSERT_BLOCK_COMPLETIONS = "INSERT INTO block_completions (chunk,block_number,tx_count,log_count,itx_count,contract_count,block_hash) VALUES (?,?,?,?,?,?,?)";
const INSERT_FORKED_BLOCK = "INSERT INTO forked_blocks (block_number,block_hash,miner,block_timestamp,era,depth,reorg_group_id,affected_txns_count_orphan,affected_txns_count_lost,affected_logs_count_orphan,affected_logs_count_lost,affected_traces_count_orphan,affected_traces_count_lost) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)";
const INSERT_ERC20_TOKENS = "INSERT INTO erc20_tokens (address,chain_id,name,symbol,decimals,has_balance_of,has_transfer,has_transfer_from,has_approve,has_allowance,is_standard_decimals,is_fully_following_standard,is_minimally_following_standard,is_partially_following_standard,is_not_following_standard,detection_version) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
const INSERT_ERC20_SUPPLY = "INSERT INTO erc20_total_supplies (address,chain_id,initial_total_supply,latest_total_supply,updated_at_block,updated_at_timestamp) VALUES (?,?,?,?,?,?)";
const UPDATE_ERC20_SUPPLY = "UPDATE erc20_total_supplies SET latest_total_supply=?,updated_at_block=?,updated_at_timestamp=? WHERE address=?";
const INSERT_ERC20_OWNER = "INSERT INTO erc20_owners (address,chain_id,initial_owner,latest_owner,is_ownership_renounced,updated_at_block,updated_at_timestamp) VALUES (?,?,?,?,?,?,?)";
const UPDATE_ERC20_OWNER = "UPDATE erc20_owners SET latest_owner=?,is_ownership_renounced=?,updated_at_block=?,updated_at_timestamp=? WHERE address=?";
const INSERT_ERC20_SELFDESTRUCT = "INSERT INTO erc20_self_destructed (address,chain_id,at_block,at_timestamp) VALUES (?,?,?,?)";

const SELECT_BC_SCAN = "SELECT block_number FROM block_completions WHERE chunk=? AND block_number>=? AND block_number<=?";
const SELECT_VERIFIED_ERA_ALL = "SELECT era FROM verified_eras";
const INSERT_VERIFIED_ERA = "INSERT INTO verified_eras (era,verified_at_ms,missing_found,missing_reindexed) VALUES (?,?,?,?)";

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
        // 64 MB sanity cap: a corrupt bodyLen (e.g. 4 GB from frame mis-alignment)
        // causes SmpAllocator → PageAllocator.map → mmap(4 GB), then tcpReadExact
        // physically backs those pages at network speed → OOM kill in ~30s.
        const max_body_len: u32 = 64 * 1024 * 1024;
        if (bodyLen > max_body_len) {
            std.debug.print("[CQL] recvFrame: oversized bodyLen={d} opcode=0x{x:0>2} fd={d} — aborting\n", .{ bodyLen, opcode, self.fd });
            return error.CqlFrameTooBig;
        }
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
            // 0x2500 = Unprepared — the server's prepared-statement cache
            // evicted this id (normal under cache pressure, not a dead
            // connection). Distinguish it so callers can re-PREPARE and
            // retry instead of treating it as fatal.
            if (code == 0x2500) return error.CqlUnprepared;
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

    /// Sends a batch using `prep.id`; on Unprepared (0x2500) — re-PREPAREs
    /// `prep.query` on this connection, updates `prep.id` in place, and
    /// retries once. Scylla can evict a prepared statement from cache under
    /// pressure without the connection dying, so this is expected/recoverable,
    /// not a fatal error (verified live: a long historical run aborted on
    /// "No prepared statement with ID ... found" while otherwise healthy).
    pub fn batchSendRows(self: *CqlConn, prep: *Prepared, nVals: u16, rowBufs: []const []const u8) !void {
        self.sendBatchOnce(prep.id, nVals, rowBufs) catch |err| {
            if (err != error.CqlUnprepared) return err;
            const newId = try self.prepare(prep.query);
            self.gpa.free(prep.id);
            prep.id = newId;
            try self.sendBatchOnce(prep.id, nVals, rowBufs);
        };
    }

    pub fn allocator(self: *CqlConn) std.mem.Allocator {
        return self.gpa;
    }

    // Build and send a CQL EXECUTE frame.
    // params: pre-encoded values (concatenated valBlob/valInt32/etc. output).
    // serialConsistency: non-null enables the WITH_SERIAL_CONSISTENCY flag (for LWT).
    fn sendExecuteFrame(self: *CqlConn, prepId: []const u8, consistency: u16, nParams: u16, params: []const u8, serialConsistency: ?u16) !void {
        var body: std.ArrayList(u8) = .empty;
        defer body.deinit(tempAllocator);

        // [short bytes] id
        try appendShort(&body, @intCast(prepId.len));
        try body.appendSlice(tempAllocator, prepId);
        // [short] consistency
        try appendShort(&body, consistency);
        // [byte] flags: 0x01=VALUES, 0x10=WITH_SERIAL_CONSISTENCY
        const flags: u8 = if (serialConsistency != null) 0x11 else 0x01;
        try body.append(tempAllocator, flags);
        // [short] n_values + values
        try appendShort(&body, nParams);
        try body.appendSlice(tempAllocator, params);
        // [short] serial_consistency (if LWT)
        if (serialConsistency) |sc| try appendShort(&body, sc);

        try self.sendFrame(OPCODE_EXECUTE, body.items);
    }

    fn executeSelectOnce(self: *CqlConn, prepId: []const u8, nParams: u16, params: []const u8, gpa: std.mem.Allocator) !SelectResult {
        try self.sendExecuteFrame(prepId, CONSISTENCY_ONE, nParams, params, null);
        const resp = try self.recvFrame();
        defer self.gpa.free(resp.body);
        if (resp.opcode == OPCODE_ERROR) {
            if (resp.body.len >= 4 and std.mem.readInt(i32, resp.body[0..4], .big) == 0x2500)
                return error.CqlUnprepared;
            return error.CqlError;
        }
        if (resp.opcode != OPCODE_RESULT) return error.CqlUnexpectedOpcode;
        return parseRowsResult(resp.body, gpa);
    }

    /// Execute a SELECT prepared statement and return all rows.
    /// On UNPREPARED (cache eviction): re-PREPAREs and retries once.
    /// Caller owns the returned SelectResult; call result.deinit() when done.
    pub fn executeSelect(self: *CqlConn, prep: *Prepared, nParams: u16, params: []const u8, gpa: std.mem.Allocator) !SelectResult {
        return self.executeSelectOnce(prep.id, nParams, params, gpa) catch |err| {
            if (err != error.CqlUnprepared) return err;
            const newId = try self.prepare(prep.query);
            self.gpa.free(prep.id);
            prep.id = newId;
            return self.executeSelectOnce(prep.id, nParams, params, gpa);
        };
    }

    fn executeLWTOnce(self: *CqlConn, prepId: []const u8, nParams: u16, params: []const u8) !bool {
        try self.sendExecuteFrame(prepId, CONSISTENCY_ONE, nParams, params, CONSISTENCY_LOCAL_SERIAL);
        const resp = try self.recvFrame();
        defer self.gpa.free(resp.body);
        if (resp.opcode == OPCODE_ERROR) {
            if (resp.body.len >= 4 and std.mem.readInt(i32, resp.body[0..4], .big) == 0x2500)
                return error.CqlUnprepared;
            return error.CqlError;
        }
        if (resp.opcode != OPCODE_RESULT) return error.CqlUnexpectedOpcode;
        return parseLWTResult(resp.body);
    }

    /// Execute a prepared INSERT IF NOT EXISTS. Returns true if applied.
    /// On UNPREPARED: re-PREPAREs and retries once.
    pub fn executeLWT(self: *CqlConn, prep: *Prepared, nParams: u16, params: []const u8) !bool {
        return self.executeLWTOnce(prep.id, nParams, params) catch |err| {
            if (err != error.CqlUnprepared) return err;
            const newId = try self.prepare(prep.query);
            self.gpa.free(prep.id);
            prep.id = newId;
            return self.executeLWTOnce(prep.id, nParams, params);
        };
    }

    fn sendBatchOnce(self: *CqlConn, prepId: []const u8, nVals: u16, rowBufs: []const []const u8) !void {
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
            try self.sendBatchOnce(prepId, nVals, rowBufs[0..mid]);
            try self.sendBatchOnce(prepId, nVals, rowBufs[mid..]);
            return;
        }
        try self.sendFrame(OPCODE_BATCH, frame.items);
        try self.recvFrameCheck();
    }
};

fn freePreparedIds(gpa: std.mem.Allocator, ids: PreparedIds) void {
    gpa.free(ids.blocks.id);
    gpa.free(ids.transactions.id);
    gpa.free(ids.logs.id);
    gpa.free(ids.internalTxs.id);
    gpa.free(ids.blockCompletions.id);
    gpa.free(ids.forkedBlock.id);
    gpa.free(ids.erc20Tokens.id);
    gpa.free(ids.erc20SupplyInsert.id);
    gpa.free(ids.erc20SupplyUpdate.id);
    gpa.free(ids.erc20OwnerInsert.id);
    gpa.free(ids.erc20OwnerUpdate.id);
    gpa.free(ids.erc20SelfDestruct.id);
    gpa.free(ids.bcScan.id);
    gpa.free(ids.verifiedErasAll.id);
    gpa.free(ids.verifiedErasInsert.id);
}

fn makePrepared(conn: *CqlConn, query: []const u8) !Prepared {
    return .{ .id = try conn.prepare(query), .query = query };
}

pub fn prepareAll(conn: *CqlConn) !PreparedIds {
    return .{
        .blocks = try makePrepared(conn, INSERT_BLOCKS),
        .transactions = try makePrepared(conn, INSERT_TXS),
        .logs = try makePrepared(conn, INSERT_LOGS),
        .internalTxs = try makePrepared(conn, INSERT_INT_TXS),
        .blockCompletions = try makePrepared(conn, INSERT_BLOCK_COMPLETIONS),
        .forkedBlock = try makePrepared(conn, INSERT_FORKED_BLOCK),
        .erc20Tokens = try makePrepared(conn, INSERT_ERC20_TOKENS),
        .erc20SupplyInsert = try makePrepared(conn, INSERT_ERC20_SUPPLY),
        .erc20SupplyUpdate = try makePrepared(conn, UPDATE_ERC20_SUPPLY),
        .erc20OwnerInsert = try makePrepared(conn, INSERT_ERC20_OWNER),
        .erc20OwnerUpdate = try makePrepared(conn, UPDATE_ERC20_OWNER),
        .erc20SelfDestruct = try makePrepared(conn, INSERT_ERC20_SELFDESTRUCT),
        .bcScan = try makePrepared(conn, SELECT_BC_SCAN),
        .verifiedErasAll = try makePrepared(conn, SELECT_VERIFIED_ERA_ALL),
        .verifiedErasInsert = try makePrepared(conn, INSERT_VERIFIED_ERA),
    };
}
