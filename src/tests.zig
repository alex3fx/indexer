// Test runner: imports core so all transitive test blocks are compiled and run.
const core = @import("indexer/core");
comptime {
    _ = core.logger;
}
