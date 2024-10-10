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

    const tests = b.addTest(.{
        .name = "tests",
        .root_source_file = b.path("./src/test.zig"),
    });

    const run_test = b.addRunArtifact(tests);
    run_test.step.dependOn(&tests.step);

    const test_step = b.step("test", "Run general unit tests");
    test_step.dependOn(&run_test.step);
}

fn add_example(
    b: *std.Build,
    name: []const u8,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.Mode,
    tardy_module: *std.Build.Module,
) void {
    const example = b.addExecutable(.{
        .name = b.fmt("tardy_example_{s}", .{name}),
        .root_source_file = b.path(b.fmt("examples/{s}/main.zig", .{name})),
        .target = target,
        .optimize = optimize,
    });

    example.root_module.addImport("tardy", tardy_module);
    const install_artifact = b.addInstallArtifact(example, .{});

    const run_cmd = b.addRunArtifact(example);
    run_cmd.step.dependOn(&install_artifact.step);
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_step = b.step(b.fmt("run_{s}", .{name}), b.fmt("Run tardy example ({s})", .{name}));
    run_step.dependOn(&run_cmd.step);
}
