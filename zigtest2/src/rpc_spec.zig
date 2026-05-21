// Specialized zero-allocation JSON parser for Ethereum JSON-RPC responses.
// Replaces std.json.parseFromSlice + deepCopy with a single schema-specific pass.
//
// Design:
//   - Hand-written recursive-descent scanner
//   - Skips unknown fields without allocating DOM nodes
//   - Writes directly into the caller's allocator (dc_alloc / arena)
//   - No intermediate representation — one pass, one allocation per needed string
//
// Expected speedup vs std.json + deepCopy: 5–15× for trace_block responses
// (which contain many fields we don't need: type, subtraces, traceAddress, etc.)

const std = @import("std");
const rpc = @import("rpc");

const Al = std.mem.Allocator;

// ─── Scanner ──────────────────────────────────────────────────────────────────

const P = struct {
    s: []const u8,
    i: usize = 0,

    fn init(s: []const u8) P { return .{ .s = s, .i = 0 }; }

    // Skip ASCII whitespace in-place.
    inline fn ws(p: *P) void {
        while (p.i < p.s.len) : (p.i += 1) {
            switch (p.s[p.i]) {
                ' ', '\t', '\n', '\r' => {},
                else => return,
            }
        }
    }

    inline fn peek(p: *P) u8 {
        p.ws();
        return if (p.i < p.s.len) p.s[p.i] else 0;
    }

    // Consume char c (after skipping ws). Returns true if consumed.
    inline fn eat(p: *P, c: u8) bool {
        p.ws();
        if (p.i < p.s.len and p.s[p.i] == c) { p.i += 1; return true; }
        return false;
    }

    // Read a JSON string. Returns a ZERO-COPY slice into p.s (no escape processing).
    // Ethereum hex strings never contain escapes, so this is safe for our use case.
    fn str(p: *P) []const u8 {
        p.ws();
        if (p.i >= p.s.len or p.s[p.i] != '"') return "";
        p.i += 1;
        const start = p.i;
        while (p.i < p.s.len) : (p.i += 1) {
            switch (p.s[p.i]) {
                '\\' => p.i += 1, // skip next char
                '"' => {
                    const v = p.s[start..p.i];
                    p.i += 1;
                    return v;
                },
                else => {},
            }
        }
        return p.s[start..];
    }

    // Read optional string: null → null, else string content.
    fn optStr(p: *P) ?[]const u8 {
        p.ws();
        if (p.i + 3 < p.s.len and p.s[p.i] == 'n' and
            p.s[p.i+1] == 'u' and p.s[p.i+2] == 'l' and p.s[p.i+3] == 'l')
        {
            p.i += 4;
            return null;
        }
        const v = p.str();
        return if (v.len > 0 or (p.s.len > 0)) v else null;
    }

    // Read JSON boolean.
    fn boolean(p: *P) bool {
        p.ws();
        if (p.i + 3 < p.s.len and p.s[p.i] == 't') { p.i += 4; return true; }
        if (p.i + 4 < p.s.len and p.s[p.i] == 'f') { p.i += 5; return false; }
        return false;
    }

    // Skip any JSON value without parsing it.
    fn skip(p: *P) void {
        p.ws();
        if (p.i >= p.s.len) return;
        switch (p.s[p.i]) {
            '"' => _ = p.str(),
            '{' => {
                p.i += 1;
                while (p.i < p.s.len) {
                    p.ws();
                    if (p.s[p.i] == '}') { p.i += 1; return; }
                    if (p.s[p.i] == ',') { p.i += 1; continue; }
                    _ = p.str(); // key
                    _ = p.eat(':');
                    p.skip(); // value
                }
            },
            '[' => {
                p.i += 1;
                while (p.i < p.s.len) {
                    p.ws();
                    if (p.s[p.i] == ']') { p.i += 1; return; }
                    if (p.s[p.i] == ',') { p.i += 1; continue; }
                    p.skip();
                }
            },
            else => {
                // number, bool, null: scan until JSON delimiter
                while (p.i < p.s.len) : (p.i += 1) {
                    switch (p.s[p.i]) {
                        ',', '}', ']', ' ', '\t', '\n', '\r' => return,
                        else => {},
                    }
                }
            },
        }
    }

    // Jump to "key": in the remaining input using SIMD-backed indexOf.
    // Positions p.i just after the ':'.
    fn jumpTo(p: *P, comptime key: []const u8) bool {
        const needle = "\"" ++ key ++ "\":";
        const pos = std.mem.indexOf(u8, p.s[p.i..], needle) orelse return false;
        p.i += pos + needle.len;
        return true;
    }
};

inline fn eql(a: []const u8, b: []const u8) bool {
    return std.mem.eql(u8, a, b);
}

fn readInt(p: *P) !usize {
    p.ws();
    var n: usize = 0;
    var found = false;
    while (p.i < p.s.len) : (p.i += 1) {
        const c = p.s[p.i];
        if (c >= '0' and c <= '9') { n = n * 10 + (c - '0'); found = true; }
        else break;
    }
    return if (found) n else error.NotAnInt;
}

// ─── String helpers ───────────────────────────────────────────────────────────
// zc=true: zero-copy slice into raw buffer (buffer must outlive the result).
// zc=false: arena.dupe copy (for modes where raw buffer is freed after parse).

inline fn S(p: *P, arena: Al, comptime zc: bool) ![]const u8 {
    const s = p.str();
    return if (zc) s else try arena.dupe(u8, s);
}

inline fn OS(p: *P, arena: Al, comptime zc: bool) !?[]const u8 {
    const s = p.optStr() orelse return null;
    return if (zc) s else try arena.dupe(u8, s);
}

// ─── Block parser ─────────────────────────────────────────────────────────────

fn parseBlockObj(p: *P, arena: Al, comptime zc: bool) !rpc.RpcBlock {
    _ = p.eat('{');
    var blk = rpc.RpcBlock{};
    var txs_al: std.ArrayList(rpc.RpcTransaction) = .empty;
    while (p.i < p.s.len) {
        p.ws();
        if (p.s[p.i] == '}') { p.i += 1; break; }
        if (p.s[p.i] == ',') { p.i += 1; continue; }
        const key = p.str();
        _ = p.eat(':');
        if (eql(key, "number")) {
            blk.number = try S(p, arena, zc);
        } else if (eql(key, "timestamp")) {
            blk.timestamp = try S(p, arena, zc);
        } else if (eql(key, "miner")) {
            blk.miner = try S(p, arena, zc);
        } else if (eql(key, "milliTimestamp")) {
            blk.milliTimestamp = try OS(p, arena, zc);
        } else if (eql(key, "transactions")) {
            try parseTxArray(p, arena, &txs_al, zc);
        } else {
            p.skip();
        }
    }
    blk.transactions = try txs_al.toOwnedSlice(arena);
    return blk;
}

pub fn parseBlockResp(raw: []const u8, arena: Al) !?rpc.RpcBlock {
    var p = P.init(raw);
    if (!p.jumpTo("result")) return null;
    p.ws();
    if (p.peek() == 'n') return null;
    return try parseBlockObj(&p, arena, false);
}

pub fn parseBlockRespZC(raw: []const u8, arena: Al) !?rpc.RpcBlock {
    var p = P.init(raw);
    if (!p.jumpTo("result")) return null;
    p.ws();
    if (p.peek() == 'n') return null;
    return try parseBlockObj(&p, arena, true);
}

fn parseTxArray(p: *P, arena: Al, out: *std.ArrayList(rpc.RpcTransaction), comptime zc: bool) !void {
    p.ws();
    if (!p.eat('[')) return;
    try out.ensureTotalCapacity(arena, 256);
    while (p.i < p.s.len) {
        p.ws();
        if (p.s[p.i] == ']') { p.i += 1; return; }
        if (p.s[p.i] == ',') { p.i += 1; continue; }
        if (p.s[p.i] != '{') { p.skip(); continue; }
        try out.append(arena, try parseTx(p, arena, zc));
    }
}

fn parseTx(p: *P, arena: Al, comptime zc: bool) !rpc.RpcTransaction {
    _ = p.eat('{');
    var tx = rpc.RpcTransaction{};
    while (p.i < p.s.len) {
        p.ws();
        if (p.s[p.i] == '}') { p.i += 1; break; }
        if (p.s[p.i] == ',') { p.i += 1; continue; }
        const key = p.str();
        _ = p.eat(':');
        if (eql(key, "hash")) {
            tx.hash = try S(p, arena, zc);
        } else if (eql(key, "transactionIndex")) {
            tx.transactionIndex = try S(p, arena, zc);
        } else if (eql(key, "from")) {
            tx.from = try S(p, arena, zc);
        } else if (eql(key, "to")) {
            tx.to = try OS(p, arena, zc);
        } else if (eql(key, "value")) {
            tx.value = try S(p, arena, zc);
        } else if (eql(key, "gas")) {
            tx.gas = try S(p, arena, zc);
        } else if (eql(key, "gasPrice")) {
            tx.gasPrice = try S(p, arena, zc);
        } else if (eql(key, "input")) {
            tx.input = try S(p, arena, zc);
        } else if (eql(key, "type")) {
            tx.@"type" = try S(p, arena, zc);
        } else if (eql(key, "maxPriorityFeePerGas")) {
            tx.maxPriorityFeePerGas = try OS(p, arena, zc);
        } else if (eql(key, "maxFeePerGas")) {
            tx.maxFeePerGas = try OS(p, arena, zc);
        } else {
            p.skip();
        }
    }
    return tx;
}

// ─── Receipts parser ──────────────────────────────────────────────────────────

fn parseReceiptsArr(p: *P, arena: Al, comptime zc: bool) ![]rpc.RpcReceipt {
    if (!p.eat('[')) return &.{};
    var rcpts = try std.ArrayList(rpc.RpcReceipt).initCapacity(arena, 256);
    while (p.i < p.s.len) {
        p.ws();
        if (p.s[p.i] == ']') { p.i += 1; break; }
        if (p.s[p.i] == ',') { p.i += 1; continue; }
        if (p.s[p.i] != '{') { p.skip(); continue; }
        try rcpts.append(arena, try parseReceipt(p, arena, zc));
    }
    return try rcpts.toOwnedSlice(arena);
}

pub fn parseReceiptsResp(raw: []const u8, arena: Al) !?[]rpc.RpcReceipt {
    var p = P.init(raw);
    if (!p.jumpTo("result")) return null;
    p.ws();
    if (p.peek() == 'n') return null;
    return try parseReceiptsArr(&p, arena, false);
}

pub fn parseReceiptsRespZC(raw: []const u8, arena: Al) !?[]rpc.RpcReceipt {
    var p = P.init(raw);
    if (!p.jumpTo("result")) return null;
    p.ws();
    if (p.peek() == 'n') return null;
    return try parseReceiptsArr(&p, arena, true);
}

fn parseReceipt(p: *P, arena: Al, comptime zc: bool) !rpc.RpcReceipt {
    _ = p.eat('{');
    var rcpt = rpc.RpcReceipt{};
    var logs_al: std.ArrayList(rpc.RpcLog) = .empty;
    while (p.i < p.s.len) {
        p.ws();
        if (p.s[p.i] == '}') { p.i += 1; break; }
        if (p.s[p.i] == ',') { p.i += 1; continue; }
        const key = p.str();
        _ = p.eat(':');
        if (eql(key, "transactionHash")) {
            rcpt.transactionHash = try S(p, arena, zc);
        } else if (eql(key, "transactionIndex")) {
            rcpt.transactionIndex = try S(p, arena, zc);
        } else if (eql(key, "gasUsed")) {
            rcpt.gasUsed = try S(p, arena, zc);
        } else if (eql(key, "cumulativeGasUsed")) {
            rcpt.cumulativeGasUsed = try S(p, arena, zc);
        } else if (eql(key, "effectiveGasPrice")) {
            rcpt.effectiveGasPrice = try OS(p, arena, zc);
        } else if (eql(key, "contractAddress")) {
            rcpt.contractAddress = try OS(p, arena, zc);
        } else if (eql(key, "status")) {
            rcpt.status = try S(p, arena, zc);
        } else if (eql(key, "logs")) {
            try parseLogArray(p, arena, &logs_al, zc);
        } else {
            p.skip();
        }
    }
    rcpt.logs = try logs_al.toOwnedSlice(arena);
    return rcpt;
}

fn parseLogArray(p: *P, arena: Al, out: *std.ArrayList(rpc.RpcLog), comptime zc: bool) !void {
    p.ws();
    if (!p.eat('[')) return;
    while (p.i < p.s.len) {
        p.ws();
        if (p.s[p.i] == ']') { p.i += 1; return; }
        if (p.s[p.i] == ',') { p.i += 1; continue; }
        if (p.s[p.i] != '{') { p.skip(); continue; }
        try out.append(arena, try parseLog(p, arena, zc));
    }
}

fn parseLog(p: *P, arena: Al, comptime zc: bool) !rpc.RpcLog {
    _ = p.eat('{');
    var log = rpc.RpcLog{};
    var topics_al: std.ArrayList([]const u8) = .empty;
    while (p.i < p.s.len) {
        p.ws();
        if (p.s[p.i] == '}') { p.i += 1; break; }
        if (p.s[p.i] == ',') { p.i += 1; continue; }
        const key = p.str();
        _ = p.eat(':');
        if (eql(key, "address")) {
            log.address = try S(p, arena, zc);
        } else if (eql(key, "data")) {
            log.data = try S(p, arena, zc);
        } else if (eql(key, "transactionHash")) {
            log.transactionHash = try S(p, arena, zc);
        } else if (eql(key, "transactionIndex")) {
            log.transactionIndex = try S(p, arena, zc);
        } else if (eql(key, "logIndex")) {
            log.logIndex = try S(p, arena, zc);
        } else if (eql(key, "removed")) {
            log.removed = p.boolean();
        } else if (eql(key, "topics")) {
            try parseTopics(p, arena, &topics_al, zc);
        } else {
            p.skip();
        }
    }
    log.topics = try topics_al.toOwnedSlice(arena);
    return log;
}

fn parseTopics(p: *P, arena: Al, out: *std.ArrayList([]const u8), comptime zc: bool) !void {
    p.ws();
    if (!p.eat('[')) return;
    try out.ensureTotalCapacity(arena, 4); // typical: 2-4 topics per log
    while (p.i < p.s.len) {
        p.ws();
        if (p.s[p.i] == ']') { p.i += 1; return; }
        if (p.s[p.i] == ',') { p.i += 1; continue; }
        try out.append(arena, try S(p, arena, zc));
    }
}

// ─── Traces parser ────────────────────────────────────────────────────────────

fn parseTracesArr(p: *P, arena: Al, comptime zc: bool) ![]rpc.RpcTrace {
    if (!p.eat('[')) return &.{};
    var traces = try std.ArrayList(rpc.RpcTrace).initCapacity(arena, 2048);
    while (p.i < p.s.len) {
        p.ws();
        if (p.s[p.i] == ']') { p.i += 1; break; }
        if (p.s[p.i] == ',') { p.i += 1; continue; }
        if (p.s[p.i] != '{') { p.skip(); continue; }
        try traces.append(arena, try parseTrace(p, arena, zc));
    }
    return try traces.toOwnedSlice(arena);
}

pub fn parseTracesResp(raw: []const u8, arena: Al) !?[]rpc.RpcTrace {
    var p = P.init(raw);
    if (!p.jumpTo("result")) return null;
    p.ws();
    if (p.peek() == 'n') return null;
    return try parseTracesArr(&p, arena, false);
}

pub fn parseTracesRespZC(raw: []const u8, arena: Al) !?[]rpc.RpcTrace {
    var p = P.init(raw);
    if (!p.jumpTo("result")) return null;
    p.ws();
    if (p.peek() == 'n') return null;
    return try parseTracesArr(&p, arena, true);
}

fn parseTrace(p: *P, arena: Al, comptime zc: bool) !rpc.RpcTrace {
    _ = p.eat('{');
    var trace = rpc.RpcTrace{};
    while (p.i < p.s.len) {
        p.ws();
        if (p.s[p.i] == '}') { p.i += 1; break; }
        if (p.s[p.i] == ',') { p.i += 1; continue; }
        const key = p.str();
        _ = p.eat(':');
        if (eql(key, "transactionHash")) {
            trace.transactionHash = try OS(p, arena, zc);
        } else if (eql(key, "transactionPosition")) {
            const s = p.str();
            if (s.len > 2 and s[0] == '0' and s[1] == 'x')
                trace.transactionPosition = std.fmt.parseInt(i32, s[2..], 16) catch null
            else
                trace.transactionPosition = std.fmt.parseInt(i32, s, 10) catch null;
        } else if (eql(key, "action")) {
            trace.action = try parseAction(p, arena, zc);
        } else if (eql(key, "result")) {
            p.ws();
            if (p.s[p.i] == 'n') { p.skip(); }
            else { trace.result = try parseResult(p, arena, zc); }
        } else {
            p.skip();
        }
    }
    return trace;
}

fn parseAction(p: *P, arena: Al, comptime zc: bool) !rpc.RpcAction {
    _ = p.eat('{');
    var action = rpc.RpcAction{};
    while (p.i < p.s.len) {
        p.ws();
        if (p.s[p.i] == '}') { p.i += 1; break; }
        if (p.s[p.i] == ',') { p.i += 1; continue; }
        const key = p.str();
        _ = p.eat(':');
        if (eql(key, "from")) {
            action.from = try S(p, arena, zc);
        } else if (eql(key, "to")) {
            action.to = try OS(p, arena, zc);
        } else if (eql(key, "value")) {
            action.value = try OS(p, arena, zc);
        } else if (eql(key, "init")) {
            action.init = try OS(p, arena, zc);
        } else if (eql(key, "input")) {
            action.input = try OS(p, arena, zc);
        } else if (eql(key, "creationMethod")) {
            action.creationMethod = try OS(p, arena, zc);
        } else {
            p.skip();
        }
    }
    return action;
}

fn parseResult(p: *P, arena: Al, comptime zc: bool) !rpc.RpcResult {
    _ = p.eat('{');
    var res = rpc.RpcResult{};

    while (p.i < p.s.len) {
        p.ws();
        if (p.s[p.i] == '}') { p.i += 1; break; }
        if (p.s[p.i] == ',') { p.i += 1; continue; }

        const key = p.str();
        _ = p.eat(':');

        if (eql(key, "address")) {
            res.address = try OS(p, arena, zc);
        } else if (eql(key, "code")) {
            res.code = try OS(p, arena, zc);
        } else {
            p.skip();
        }
    }

    return res;
}

// ─── Batch response parsers ───────────────────────────────────────────────────
// Batch JSON-RPC response: [{jsonrpc, id, result}, ...]
// Responses may arrive in any order — id (= block index in batch) routes each result.
// Assumes id appears before result in each response object (standard for all Ethereum nodes).

pub fn parseBatchBlockResp(raw: []const u8, out: []?rpc.RpcBlock, arena: Al) !void {
    var p = P.init(raw);
    p.ws();
    if (!p.eat('[')) return;
    while (p.i < p.s.len) {
        p.ws();
        if (p.s[p.i] == ']') break;
        if (p.s[p.i] == ',') { p.i += 1; continue; }
        if (!p.eat('{')) { p.skip(); continue; }
        var id: ?usize = null;
        var result: ?rpc.RpcBlock = null;
        while (p.i < p.s.len) {
            p.ws();
            if (p.s[p.i] == '}') { p.i += 1; break; }
            if (p.s[p.i] == ',') { p.i += 1; continue; }
            const key = p.str();
            _ = p.eat(':');
            if (eql(key, "id")) {
                id = readInt(&p) catch null;
            } else if (eql(key, "result")) {
                p.ws();
                if (p.i < p.s.len and p.s[p.i] != 'n') {
                    result = parseBlockObj(&p, arena, false) catch null;
                } else p.skip();
            } else p.skip();
        }
        if (id) |i| { if (i < out.len) out[i] = result; }
    }
}

pub fn parseBatchReceiptsResp(raw: []const u8, out: []?[]rpc.RpcReceipt, arena: Al) !void {
    var p = P.init(raw);
    p.ws();
    if (!p.eat('[')) return;
    while (p.i < p.s.len) {
        p.ws();
        if (p.s[p.i] == ']') break;
        if (p.s[p.i] == ',') { p.i += 1; continue; }
        if (!p.eat('{')) { p.skip(); continue; }
        var id: ?usize = null;
        var result: ?[]rpc.RpcReceipt = null;
        while (p.i < p.s.len) {
            p.ws();
            if (p.s[p.i] == '}') { p.i += 1; break; }
            if (p.s[p.i] == ',') { p.i += 1; continue; }
            const key = p.str();
            _ = p.eat(':');
            if (eql(key, "id")) {
                id = readInt(&p) catch null;
            } else if (eql(key, "result")) {
                p.ws();
                if (p.i < p.s.len and p.s[p.i] == '[') {
                    result = parseReceiptsArr(&p, arena, false) catch null;
                } else p.skip();
            } else p.skip();
        }
        if (id) |i| { if (i < out.len) out[i] = result; }
    }
}

pub fn parseBatchTracesResp(raw: []const u8, out: []?[]rpc.RpcTrace, arena: Al) !void {
    var p = P.init(raw);
    p.ws();
    if (!p.eat('[')) return;
    while (p.i < p.s.len) {
        p.ws();
        if (p.s[p.i] == ']') break;
        if (p.s[p.i] == ',') { p.i += 1; continue; }
        if (!p.eat('{')) { p.skip(); continue; }
        var id: ?usize = null;
        var result: ?[]rpc.RpcTrace = null;
        while (p.i < p.s.len) {
            p.ws();
            if (p.s[p.i] == '}') { p.i += 1; break; }
            if (p.s[p.i] == ',') { p.i += 1; continue; }
            const key = p.str();
            _ = p.eat(':');
            if (eql(key, "id")) {
                id = readInt(&p) catch null;
            } else if (eql(key, "result")) {
                p.ws();
                if (p.i < p.s.len and p.s[p.i] == '[') {
                    result = parseTracesArr(&p, arena, false) catch null;
                } else p.skip();
            } else p.skip();
        }
        if (id) |i| { if (i < out.len) out[i] = result; }
    }
}

// ─── Per-block batch parser ───────────────────────────────────────────────────
// Parses [{id:0,result:{block}},{id:1,result:[receipts]},{id:2,result:[traces]}]
// id=0→block, id=1→receipts, id=2→traces (matches fetchBlockBatch request order).
pub fn parseBlockBatchResp(
    raw: []const u8,
    blk: *?rpc.RpcBlock,
    rcpts: *?[]rpc.RpcReceipt,
    trcs: *?[]rpc.RpcTrace,
    arena: Al,
) !void {
    var p = P.init(raw);
    p.ws();
    if (!p.eat('[')) return;
    while (p.i < p.s.len) {
        p.ws();
        if (p.s[p.i] == ']') break;
        if (p.s[p.i] == ',') { p.i += 1; continue; }
        if (!p.eat('{')) { p.skip(); continue; }
        var id: ?usize = null;
        while (p.i < p.s.len) {
            p.ws();
            if (p.s[p.i] == '}') { p.i += 1; break; }
            if (p.s[p.i] == ',') { p.i += 1; continue; }
            const key = p.str();
            _ = p.eat(':');
            if (eql(key, "id")) {
                id = readInt(&p) catch null;
            } else if (eql(key, "result")) {
                p.ws();
                switch (id orelse 99) {
                    0 => if (p.i < p.s.len and p.s[p.i] != 'n') {
                        blk.* = parseBlockObj(&p, arena, false) catch null;
                    } else { p.skip(); },
                    1 => if (p.i < p.s.len and p.s[p.i] == '[') {
                        rcpts.* = parseReceiptsArr(&p, arena, false) catch null;
                    } else { p.skip(); },
                    2 => if (p.i < p.s.len and p.s[p.i] == '[') {
                        trcs.* = parseTracesArr(&p, arena, false) catch null;
                    } else { p.skip(); },
                    else => p.skip(),
                }
            } else p.skip();
        }
    }
}
