const std = @import("std");

pub fn build(b: *std.Build) void {
    const target   = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // ── Tuning options ────────────────────────────────────────────────────────
    const pool_size = b.option(u32, "pool_size", "CQL connection pool size (default 32)") orelse 32;
    const pipeline  = b.option(u32, "pipeline",  "CQL pipeline depth per chunk (default 256)") orelse 256;
    const split_str = b.option([]const u8, "split",
        "Connection split across 6 tables (default \"1,3,6,20,1,1\")")
        orelse "1,3,6,20,1,1";

    const tuning = b.addOptions();
    tuning.addOption(u32,        "pool_size", pool_size);
    tuning.addOption(u32,        "pipeline",  pipeline);
    tuning.addOption([]const u8, "split",     split_str);
    const tuning_mod = tuning.createModule();

    // ── Source modules ────────────────────────────────────────────────────────
    const rpc_spec_mod = b.createModule(.{
        .root_source_file = b.path("src/rpc_spec.zig"),
        .target = target, .optimize = optimize,
    });
    const rpc_mod = b.createModule(.{
        .root_source_file = b.path("src/rpc.zig"),
        .target = target, .optimize = optimize,
        .imports = &.{ .{ .name = "rpc_spec", .module = rpc_spec_mod } },
    });
    rpc_spec_mod.addImport("rpc", rpc_mod);

    const transform_mod = b.createModule(.{
        .root_source_file = b.path("src/transform.zig"),
        .target = target, .optimize = optimize,
        .imports = &.{ .{ .name = "rpc", .module = rpc_mod } },
    });
    const db_mod = b.createModule(.{
        .root_source_file = b.path("src/db.zig"),
        .target = target, .optimize = optimize,
        .imports = &.{
            .{ .name = "transform", .module = transform_mod },
            .{ .name = "cfg",       .module = tuning_mod },
        },
    });
    const config_mod = b.createModule(.{
        .root_source_file = b.path("src/config.zig"),
        .target = target, .optimize = optimize,
        .imports = &.{ .{ .name = "rpc", .module = rpc_mod } },
    });
    const metrics_mod = b.createModule(.{
        .root_source_file = b.path("src/metrics.zig"),
        .target = target, .optimize = optimize,
    });
    const pipeline_mod = b.createModule(.{
        .root_source_file = b.path("src/pipeline.zig"),
        .target = target, .optimize = optimize,
        .imports = &.{
            .{ .name = "rpc",       .module = rpc_mod },
            .{ .name = "transform", .module = transform_mod },
            .{ .name = "db",        .module = db_mod },
            .{ .name = "config",    .module = config_mod },
            .{ .name = "metrics",   .module = metrics_mod },
        },
    });
    const realtime_mod = b.createModule(.{
        .root_source_file = b.path("src/realtime.zig"),
        .target = target, .optimize = optimize,
        .imports = &.{
            .{ .name = "rpc",       .module = rpc_mod },
            .{ .name = "transform", .module = transform_mod },
            .{ .name = "db",        .module = db_mod },
            .{ .name = "config",    .module = config_mod },
        },
    });

    const exe = b.addExecutable(.{
        .name = "zigparser2",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target, .optimize = optimize,
            .imports = &.{
                .{ .name = "rpc",       .module = rpc_mod },
                .{ .name = "db",        .module = db_mod },
                .{ .name = "config",    .module = config_mod },
                .{ .name = "metrics",   .module = metrics_mod },
                .{ .name = "pipeline",  .module = pipeline_mod },
                .{ .name = "realtime",  .module = realtime_mod },
            },
        }),
    });
    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| run_cmd.addArgs(args);
    b.step("run", "Run").dependOn(&run_cmd.step);
}
