const std = @import("std");
const Build = std.Build;

const executables = .{
    .{ "start", "raw", "src/entrypoints/raw.zig" },
};

const modules = .{
    .{ "indexer/utils", "src/utils/_root.zig" },
    .{ "indexer/core", "src/core/_root.zig" },
};

pub fn build(b: *Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    inline for (executables) |d| {
        var module_refs: [modules.len]*Build.Module = undefined;
        inline for (modules, 0..) |mod, i| {
            module_refs[i] = b.createModule(.{
                .root_source_file = b.path(mod[1]),
                .target = target,
                .optimize = optimize,
            });
        }

        inline for (module_refs) |module| {
            inline for (modules, module_refs) |mod, import_module| {
                module.addImport(mod[0], import_module);
            }
        }

        const exe = b.addExecutable(.{
            .name = d[1],
            .root_module = b.createModule(.{
                .root_source_file = b.path(d[2]),
                .target = target,
                .optimize = optimize,
            }),
        });

        inline for (modules, module_refs) |mod, module| {
            exe.root_module.addImport(mod[0], module);
        }

        b.installArtifact(exe);

        const run_cmd = b.addRunArtifact(exe);
        if (b.args) |args| run_cmd.addArgs(args);

        const step = b.step(d[0], b.fmt("Build and run {s}", .{d[1]}));
        step.dependOn(b.getInstallStep());
        step.dependOn(&run_cmd.step);
    }
}
