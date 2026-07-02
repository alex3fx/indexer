// Test runner: imports core and pipeline modules so all test blocks are compiled.
const core = @import("indexer/core");
const bytecode_store = @import("pipeline/bytecode_store.zig");
comptime {
    _ = core.logger;
    _ = bytecode_store;
}
