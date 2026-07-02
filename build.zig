const std = @import("std");
const Build = std.Build;

pub fn build(b: *Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const utils_mod = b.createModule(.{
        .root_source_file = b.path("src/utils/_root.zig"),
        .target = target,
        .optimize = optimize,
    });

    const rpc_mod = b.createModule(.{
        .root_source_file = b.path("src/rpc/_root.zig"),
        .target = target,
        .optimize = optimize,
    });

    const db_mod = b.createModule(.{
        .root_source_file = b.path("src/db/_root.zig"),
        .target = target,
        .optimize = optimize,
    });

    const core_mod = b.createModule(.{
        .root_source_file = b.path("src/core/_root.zig"),
        .target = target,
        .optimize = optimize,
    });

    // core imports itself (so internal files can @import("indexer/core"))
    // and utils (so _root.zig can re-export it as pub const utils = ...)
    core_mod.addImport("indexer/core", core_mod);
    core_mod.addImport("indexer/utils", utils_mod);
    core_mod.addImport("indexer/rpc", rpc_mod);

    // utils imports core (env files use @import("indexer/core") for enums/errors)
    utils_mod.addImport("indexer/core", core_mod);

    // rpc imports core (for structures, fetch.Client, etc.)
    rpc_mod.addImport("indexer/core", core_mod);

    // db imports core (pool.zig uses core structures)
    db_mod.addImport("indexer/core", core_mod);

    const exe = b.addExecutable(.{
        .name = "raw",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    exe.root_module.addImport("indexer/core", core_mod);
    exe.root_module.addImport("indexer/rpc", rpc_mod);
    exe.root_module.addImport("indexer/db", db_mod);
    exe.root_module.link_libc = true;
    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    if (b.args) |args| run_cmd.addArgs(args);

    const step = b.step("start", "Build and run raw");
    step.dependOn(b.getInstallStep());
    step.dependOn(&run_cmd.step);

    const tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/tests.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    tests.root_module.addImport("indexer/core", core_mod);
    tests.root_module.addImport("indexer/db", db_mod);
    tests.root_module.link_libc = true;

    const test_step = b.step("test", "Run logger tests");
    test_step.dependOn(&b.addRunArtifact(tests).step);
}
