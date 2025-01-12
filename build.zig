const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const tardy = b.addModule("tardy", .{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });

    add_example(b, "basic", target, optimize, tardy);
    add_example(b, "channel", target, optimize, tardy);
    add_example(b, "echo", target, optimize, tardy);
    add_example(b, "http", target, optimize, tardy);
    add_example(b, "cat", target, optimize, tardy);
    add_example(b, "shove", target, optimize, tardy);
    add_example(b, "rmdir", target, optimize, tardy);
    add_example(b, "stat", target, optimize, tardy);

    add_example(b, "integ", target, optimize, tardy);
}

fn add_example(
    b: *std.Build,
    name: []const u8,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.Mode,
    tardy_module: *std.Build.Module,
) void {
    const example = b.addExecutable(.{
        .name = b.fmt("{s}", .{name}),
        .root_source_file = b.path(b.fmt("examples/{s}/main.zig", .{name})),
        .target = target,
        .optimize = optimize,
        .strip = false,
    });

    if (target.result.os.tag == .windows) {
        example.linkLibC();
    }

    example.root_module.addImport("tardy", tardy_module);
    const install_artifact = b.addInstallArtifact(example, .{});
    b.getInstallStep().dependOn(&install_artifact.step);

    const build_step = b.step(b.fmt("{s}", .{name}), b.fmt("Build tardy example ({s})", .{name}));
    build_step.dependOn(&install_artifact.step);

    const run_artifact = b.addRunArtifact(example);
    run_artifact.step.dependOn(&install_artifact.step);

    const run_step = b.step(b.fmt("run_{s}", .{name}), b.fmt("Run tardy example ({s})", .{name}));
    run_step.dependOn(&install_artifact.step);
    run_step.dependOn(&run_artifact.step);
}
