// Zero-copy JSON parser for Ethereum JSON-RPC responses.
// Hand-written recursive-descent scanner — skips unknown fields without DOM allocation.
// ZC variants return slices directly into the raw buffer (zero copy, zero alloc per string).
const std = @import("std");

const types = @import("indexer/rpc").types;

pub const RpcBlock = types.RpcBlock;
pub const RpcTransaction = types.RpcTransaction;
pub const RpcReceipt = types.RpcReceipt;
pub const RpcLog = types.RpcLog;
pub const RpcTrace = types.RpcTrace;
pub const RpcAction = types.RpcAction;
pub const RpcResult = types.RpcResult;

const Al = std.mem.Allocator;

// ─── Scanner ──────────────────────────────────────────────────────────────────

const P = struct {
    s: []const u8,
    i: usize = 0,

    fn init(s: []const u8) P {
        return .{ .s = s, .i = 0 };
    }

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

    inline fn eat(p: *P, c: u8) bool {
        p.ws();
        if (p.i < p.s.len and p.s[p.i] == c) {
            p.i += 1;
            return true;
        }
        return false;
    }

    fn str(p: *P) []const u8 {
        p.ws();
        if (p.i >= p.s.len or p.s[p.i] != '"') return "";
        p.i += 1;
        const start = p.i;
        while (p.i < p.s.len) : (p.i += 1) {
            switch (p.s[p.i]) {
                '\\' => p.i += 1,
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

    fn optStr(p: *P) ?[]const u8 {
        p.ws();
        if (p.i + 3 < p.s.len and p.s[p.i] == 'n' and
            p.s[p.i + 1] == 'u' and p.s[p.i + 2] == 'l' and p.s[p.i + 3] == 'l')
        {
            p.i += 4;
            return null;
        }
        const v = p.str();
        return if (v.len > 0 or (p.s.len > 0)) v else null;
    }

    fn boolean(p: *P) bool {
        p.ws();
        if (p.i + 3 < p.s.len and p.s[p.i] == 't') {
            p.i += 4;
            return true;
        }
        if (p.i + 4 < p.s.len and p.s[p.i] == 'f') {
            p.i += 5;
            return false;
        }
        return false;
    }

    fn skip(p: *P) void {
        p.ws();
        if (p.i >= p.s.len) return;
        switch (p.s[p.i]) {
            '"' => _ = p.str(),
            '{' => {
                p.i += 1;
                while (p.i < p.s.len) {
                    p.ws();
                    if (p.s[p.i] == '}') {
                        p.i += 1;
                        return;
                    }
                    if (p.s[p.i] == ',') {
                        p.i += 1;
                        continue;
                    }
                    _ = p.str();
                    _ = p.eat(':');
                    p.skip();
                }
            },
            '[' => {
                p.i += 1;
                while (p.i < p.s.len) {
                    p.ws();
                    if (p.s[p.i] == ']') {
                        p.i += 1;
                        return;
                    }
                    if (p.s[p.i] == ',') {
                        p.i += 1;
                        continue;
                    }
                    p.skip();
                }
            },
            else => {
                while (p.i < p.s.len) : (p.i += 1) {
                    switch (p.s[p.i]) {
                        ',', '}', ']', ' ', '\t', '\n', '\r' => return,
                        else => {},
                    }
                }
            },
        }
    }

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
        if (c >= '0' and c <= '9') {
            n = n * 10 + (c - '0');
            found = true;
        } else break;
    }
    return if (found) n else error.NotAnInt;
}

inline fn S(p: *P, arena: Al, comptime zc: bool) ![]const u8 {
    const s = p.str();
    return if (zc) s else try arena.dupe(u8, s);
}

inline fn OS(p: *P, arena: Al, comptime zc: bool) !?[]const u8 {
    const s = p.optStr() orelse return null;
    return if (zc) s else try arena.dupe(u8, s);
}

// ─── Block parser ─────────────────────────────────────────────────────────────

fn parseBlockObj(p: *P, arena: Al, comptime zc: bool) !RpcBlock {
    _ = p.eat('{');
    var blk = RpcBlock{};
    var txs_al: std.ArrayList(RpcTransaction) = .empty;
    while (p.i < p.s.len) {
        p.ws();
        if (p.s[p.i] == '}') {
            p.i += 1;
            break;
        }
        if (p.s[p.i] == ',') {
            p.i += 1;
            continue;
        }
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

// Non-ZC: copies all strings into arena (safe when source buffer will be freed).
pub fn parseBlockResp(raw: []const u8, arena: Al) !?RpcBlock {
    var p = P.init(raw);
    if (!p.jumpTo("result")) return null;
    p.ws();
    if (p.peek() == 'n') return null;
    return try parseBlockObj(&p, arena, false);
}

pub fn parseBlockRespZC(raw: []const u8, arena: Al) !?RpcBlock {
    var p = P.init(raw);
    if (!p.jumpTo("result")) return null;
    p.ws();
    if (p.peek() == 'n') return null;
    return try parseBlockObj(&p, arena, true);
}

fn parseTxArray(p: *P, arena: Al, out: *std.ArrayList(RpcTransaction), comptime zc: bool) !void {
    p.ws();
    if (!p.eat('[')) return;
    try out.ensureTotalCapacity(arena, 256);
    while (p.i < p.s.len) {
        p.ws();
        if (p.s[p.i] == ']') {
            p.i += 1;
            return;
        }
        if (p.s[p.i] == ',') {
            p.i += 1;
            continue;
        }
        if (p.s[p.i] != '{') {
            p.skip();
            continue;
        }
        try out.append(arena, try parseTx(p, arena, zc));
    }
}

fn parseTx(p: *P, arena: Al, comptime zc: bool) !RpcTransaction {
    _ = p.eat('{');
    var tx = RpcTransaction{};
    while (p.i < p.s.len) {
        p.ws();
        if (p.s[p.i] == '}') {
            p.i += 1;
            break;
        }
        if (p.s[p.i] == ',') {
            p.i += 1;
            continue;
        }
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
            tx.type = try S(p, arena, zc);
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

fn parseReceiptsArr(p: *P, arena: Al, comptime zc: bool) ![]RpcReceipt {
    if (!p.eat('[')) return &.{};
    var rcpts = try std.ArrayList(RpcReceipt).initCapacity(arena, 256);
    while (p.i < p.s.len) {
        p.ws();
        if (p.s[p.i] == ']') {
            p.i += 1;
            break;
        }
        if (p.s[p.i] == ',') {
            p.i += 1;
            continue;
        }
        if (p.s[p.i] != '{') {
            p.skip();
            continue;
        }
        try rcpts.append(arena, try parseReceipt(p, arena, zc));
    }
    return try rcpts.toOwnedSlice(arena);
}

pub fn parseReceiptsResp(raw: []const u8, arena: Al) !?[]RpcReceipt {
    var p = P.init(raw);
    if (!p.jumpTo("result")) return null;
    p.ws();
    if (p.peek() == 'n') return null;
    return try parseReceiptsArr(&p, arena, false);
}

pub fn parseReceiptsRespZC(raw: []const u8, arena: Al) !?[]RpcReceipt {
    var p = P.init(raw);
    if (!p.jumpTo("result")) return null;
    p.ws();
    if (p.peek() == 'n') return null;
    return try parseReceiptsArr(&p, arena, true);
}

fn parseReceipt(p: *P, arena: Al, comptime zc: bool) !RpcReceipt {
    _ = p.eat('{');
    var rcpt = RpcReceipt{};
    var logs_al: std.ArrayList(RpcLog) = .empty;
    while (p.i < p.s.len) {
        p.ws();
        if (p.s[p.i] == '}') {
            p.i += 1;
            break;
        }
        if (p.s[p.i] == ',') {
            p.i += 1;
            continue;
        }
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

fn parseLogArray(p: *P, arena: Al, out: *std.ArrayList(RpcLog), comptime zc: bool) !void {
    p.ws();
    if (!p.eat('[')) return;
    while (p.i < p.s.len) {
        p.ws();
        if (p.s[p.i] == ']') {
            p.i += 1;
            return;
        }
        if (p.s[p.i] == ',') {
            p.i += 1;
            continue;
        }
        if (p.s[p.i] != '{') {
            p.skip();
            continue;
        }
        try out.append(arena, try parseLog(p, arena, zc));
    }
}

fn parseLog(p: *P, arena: Al, comptime zc: bool) !RpcLog {
    _ = p.eat('{');
    var log = RpcLog{};
    var topics_al: std.ArrayList([]const u8) = .empty;
    while (p.i < p.s.len) {
        p.ws();
        if (p.s[p.i] == '}') {
            p.i += 1;
            break;
        }
        if (p.s[p.i] == ',') {
            p.i += 1;
            continue;
        }
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
    try out.ensureTotalCapacity(arena, 4);
    while (p.i < p.s.len) {
        p.ws();
        if (p.s[p.i] == ']') {
            p.i += 1;
            return;
        }
        if (p.s[p.i] == ',') {
            p.i += 1;
            continue;
        }
        try out.append(arena, try S(p, arena, zc));
    }
}

// ─── Traces parser ────────────────────────────────────────────────────────────

fn parseTracesArr(p: *P, arena: Al, comptime zc: bool) ![]RpcTrace {
    if (!p.eat('[')) return &.{};
    var traces = try std.ArrayList(RpcTrace).initCapacity(arena, 2048);
    while (p.i < p.s.len) {
        p.ws();
        if (p.s[p.i] == ']') {
            p.i += 1;
            break;
        }
        if (p.s[p.i] == ',') {
            p.i += 1;
            continue;
        }
        if (p.s[p.i] != '{') {
            p.skip();
            continue;
        }
        try traces.append(arena, try parseTrace(p, arena, zc));
    }
    return try traces.toOwnedSlice(arena);
}

pub fn parseTracesResp(raw: []const u8, arena: Al) !?[]RpcTrace {
    var p = P.init(raw);
    if (!p.jumpTo("result")) return null;
    p.ws();
    if (p.peek() == 'n') return null;
    return try parseTracesArr(&p, arena, false);
}

pub fn parseTracesRespZC(raw: []const u8, arena: Al) !?[]RpcTrace {
    var p = P.init(raw);
    if (!p.jumpTo("result")) return null;
    p.ws();
    if (p.peek() == 'n') return null;
    return try parseTracesArr(&p, arena, true);
}

fn parseTrace(p: *P, arena: Al, comptime zc: bool) !RpcTrace {
    _ = p.eat('{');
    var trace = RpcTrace{};
    while (p.i < p.s.len) {
        p.ws();
        if (p.s[p.i] == '}') {
            p.i += 1;
            break;
        }
        if (p.s[p.i] == ',') {
            p.i += 1;
            continue;
        }
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
            if (p.s[p.i] == 'n') {
                p.skip();
            } else {
                trace.result = try parseResult(p, arena, zc);
            }
        } else {
            p.skip();
        }
    }
    return trace;
}

fn parseAction(p: *P, arena: Al, comptime zc: bool) !RpcAction {
    _ = p.eat('{');
    var action = RpcAction{};
    while (p.i < p.s.len) {
        p.ws();
        if (p.s[p.i] == '}') {
            p.i += 1;
            break;
        }
        if (p.s[p.i] == ',') {
            p.i += 1;
            continue;
        }
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

fn parseResult(p: *P, arena: Al, comptime zc: bool) !RpcResult {
    _ = p.eat('{');
    var res = RpcResult{};
    while (p.i < p.s.len) {
        p.ws();
        if (p.s[p.i] == '}') {
            p.i += 1;
            break;
        }
        if (p.s[p.i] == ',') {
            p.i += 1;
            continue;
        }
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

// ─── GETH callTracer parser ───────────────────────────────────────────────────
// Parses debug_traceBlockByNumber (callTracer) response.
// Format: {"result":[{"txHash":"0x...","result":{CallFrame}}, ...]}
// CallFrame: {from, to, type, value, input, calls:[CallFrame,...]}
// Flattens the nested call tree into a flat []RpcTrace (same output as parseTracesRespZC).

pub fn parseGethTracesRespZC(raw: []const u8, arena: Al) !?[]RpcTrace {
    var p = P.init(raw);
    if (!p.jumpTo("result")) return null;
    p.ws();
    if (p.peek() == 'n') return null;
    return try parseGethTracesArr(&p, arena);
}

fn parseGethTracesArr(p: *P, arena: Al) ![]RpcTrace {
    var out: std.ArrayList(RpcTrace) = .empty;
    if (!p.eat('[')) return out.toOwnedSlice(arena);
    var tx_pos: i32 = 0;
    while (p.i < p.s.len) {
        p.ws();
        if (p.s[p.i] == ']') {
            p.i += 1;
            break;
        }
        if (p.s[p.i] == ',') {
            p.i += 1;
            continue;
        }
        if (p.s[p.i] == '{') {
            try parseGethTxEntry(p, arena, tx_pos, &out);
            tx_pos += 1;
        } else {
            p.skip();
        }
    }
    return out.toOwnedSlice(arena);
}

fn parseGethTxEntry(p: *P, arena: Al, tx_pos: i32, out: *std.ArrayList(RpcTrace)) !void {
    _ = p.eat('{');
    var tx_hash: []const u8 = "";
    var result_start: usize = 0;
    var result_end: usize = 0;
    var has_result = false;
    while (p.i < p.s.len) {
        p.ws();
        if (p.s[p.i] == '}') {
            p.i += 1;
            break;
        }
        if (p.s[p.i] == ',') {
            p.i += 1;
            continue;
        }
        const key = p.str();
        _ = p.eat(':');
        if (eql(key, "txHash") or eql(key, "transactionHash")) {
            tx_hash = p.str();
        } else if (eql(key, "result")) {
            p.ws();
            if (p.i < p.s.len and p.s[p.i] == '{') {
                result_start = p.i;
                p.skip();
                result_end = p.i;
                has_result = true;
            } else {
                p.skip();
            }
        } else {
            p.skip();
        }
    }
    if (!has_result) return;
    var fp = P.init(p.s[result_start..result_end]);
    try flattenCallFrame(&fp, arena, tx_hash, tx_pos, out);
}

fn flattenCallFrame(
    p: *P,
    arena: Al,
    tx_hash: []const u8,
    tx_pos: i32,
    out: *std.ArrayList(RpcTrace),
) !void {
    if (p.peek() != '{') return;
    _ = p.eat('{');

    var from: []const u8 = "";
    var to: ?[]const u8 = null;
    var value: ?[]const u8 = null;
    var frame_type: []const u8 = "";
    var input: ?[]const u8 = null;
    var calls_start: usize = 0;
    var calls_end: usize = 0;
    var has_calls = false;

    while (p.i < p.s.len) {
        p.ws();
        if (p.s[p.i] == '}') {
            p.i += 1;
            break;
        }
        if (p.s[p.i] == ',') {
            p.i += 1;
            continue;
        }
        const key = p.str();
        _ = p.eat(':');
        if (eql(key, "from")) {
            from = p.str();
        } else if (eql(key, "to")) {
            const v = p.str();
            to = if (v.len > 0) v else null;
        } else if (eql(key, "value")) {
            const v = p.str();
            value = if (v.len > 0) v else null;
        } else if (eql(key, "type")) {
            frame_type = p.str();
        } else if (eql(key, "input")) {
            const v = p.str();
            input = if (v.len > 0) v else null;
        } else if (eql(key, "calls")) {
            p.ws();
            if (p.i < p.s.len and p.s[p.i] == '[') {
                calls_start = p.i;
                p.skip();
                calls_end = p.i;
                has_calls = true;
            } else {
                p.skip();
            }
        } else {
            p.skip();
        }
    }

    const is_create = eql(frame_type, "CREATE") or eql(frame_type, "CREATE2");
    const is_selfdestruct = eql(frame_type, "SELFDESTRUCT");

    if (!is_selfdestruct) {
        var trace = RpcTrace{
            .transactionHash = if (tx_hash.len > 0) tx_hash else null,
            .transactionPosition = tx_pos,
        };
        if (is_create) {
            trace.action.from = from;
            trace.action.value = value;
            trace.action.input = input;
            trace.action.creationMethod = if (eql(frame_type, "CREATE2")) "create2" else "create";
            if (to) |addr| trace.result = .{ .address = addr };
        } else {
            trace.action.from = from;
            trace.action.to = to;
            trace.action.value = value;
        }
        try out.append(arena, trace);
    }

    if (has_calls) {
        var sp = P.init(p.s[calls_start..calls_end]);
        _ = sp.eat('[');
        while (sp.i < sp.s.len) {
            sp.ws();
            if (sp.i >= sp.s.len) break;
            if (sp.s[sp.i] == ']') {
                sp.i += 1;
                break;
            }
            if (sp.s[sp.i] == ',') {
                sp.i += 1;
                continue;
            }
            if (sp.s[sp.i] == '{') {
                try flattenCallFrame(&sp, arena, tx_hash, tx_pos, out);
            } else {
                sp.skip();
            }
        }
    }
}

// suppress unused warning for readInt
const _readInt = readInt;
