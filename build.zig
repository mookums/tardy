const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;

const zig_version = std.SemanticVersion{ .major = 0, .minor = 14, .patch = 0 };

comptime {
    // Compare versions while allowing different pre/patch metadata.
    const zig_version_eq = zig_version.major == builtin.zig_version.major and
        zig_version.minor == builtin.zig_version.minor and
        zig_version.patch == builtin.zig_version.patch;
    if (!zig_version_eq) {
        @compileError(std.fmt.comptimePrint(
            "unsupported zig version: expected {}, found {}",
            .{ zig_version, builtin.zig_version },
        ));
    }
}

const Example = enum {
    none,
    all,

    basic,
    cat,
    channel,
    echo,
    http,
    rmdir,
    shove,
    stat,
    stream,

    fn toString(ex: Example) []const u8 {
        const ex_string = switch (ex) {
            .basic => "basic",
            .cat => "cat",
            .channel => "channel",
            .echo => "echo",
            .http => "http",
            .rmdir => "rmdir",
            .shove => "shove",
            .stat => "stat",
            .stream => "stream",

            else => "",
        };

        return ex_string;
    }
};

pub fn build(b: *std.Build) void {

    // Top-level steps you can invoke on the command line.
    const build_steps = .{
        .run = b.step("run", "Run a Tardy Program/Example"),
    };

    // Build options passed with `-D` flags.
    const build_options = .{
        .example = b.option(Example, "example", "example name") orelse .none,
    };

    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // create a public tardy module
    const tardy = b.addModule("tardy", .{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });

    build_examples(b, .{
        .run = build_steps.run,
        .install = b.getInstallStep(),
    }, .{
        .tardy_mod = tardy,
        .example = build_options.example,
        .optimize = optimize,
        .target = target,
    });

    const tests = b.addTest(.{
        .name = "tests",
        .root_source_file = b.path("./src/tests.zig"),
    });

    const run_test = b.addRunArtifact(tests);
    run_test.step.dependOn(&tests.step);

    const test_step = b.step("test", "Run general unit tests");
    test_step.dependOn(&run_test.step);

    add_test(b, "e2e", target, optimize, tardy);
}

fn build_examples(
    b: *std.Build,
    steps: struct {
        run: *std.Build.Step,
        install: *std.Build.Step,
    },
    options: struct {
        tardy_mod: *std.Build.Module,
        example: Example,
        target: std.Build.ResolvedTarget,
        optimize: std.builtin.OptimizeMode,
    },
) void {
    if (options.example == .none) {
        return;
    }

    if (options.example != .none and options.example != .all) {
        build_example_exe(
            b,
            .{
                .run = steps.run,
                .install = steps.install,
            },
            .{
                .tardy_mod = options.tardy_mod,
                .example = options.example,
                .optimize = options.optimize,
                .target = options.target,
                .allExamples = false,
            },
        );

        return;
    }

    if (options.example == .all) {
        inline for (std.meta.fields(Example)) |f| {
            if (f.value == 1 or f.value == 0) {
                continue;
            }

            build_example_exe(
                b,
                .{
                    .run = steps.run,
                    .install = steps.install,
                },
                .{
                    .tardy_mod = options.tardy_mod,
                    .example = @as(Example, @enumFromInt(f.value)),
                    .optimize = options.optimize,
                    .target = options.target,
                    .allExamples = true,
                },
            );
        }
        return;
    }
}

fn build_example_module(
    b: *std.Build,
    options: struct {
        tardy_mod: *std.Build.Module,
        example: Example,
        target: std.Build.ResolvedTarget,
        optimize: std.builtin.OptimizeMode,
    },
) *std.Build.Module {
    assert(options.example != .none);
    assert(options.example != .all);

    // create a private example module
    const example_mod = b.createModule(.{
        .root_source_file = b.path(b.fmt("examples/{s}/main.zig", .{options.example.toString()})),
        .target = options.target,
        .optimize = options.optimize,
    });

    example_mod.addImport("tardy", options.tardy_mod);

    if (options.target.result.os.tag == .windows) {
        example_mod.link_libc = true;
    }

    return example_mod;
}

fn build_example_exe(
    b: *std.Build,
    steps: struct {
        run: *std.Build.Step,
        install: *std.Build.Step,
    },
    options: struct {
        tardy_mod: *std.Build.Module,
        example: Example,
        allExamples: bool,
        target: std.Build.ResolvedTarget,
        optimize: std.builtin.OptimizeMode,
    },
) void {
    const example_mod = build_example_module(b, .{
        .tardy_mod = options.tardy_mod,
        .example = options.example,
        .optimize = options.optimize,
        .target = options.target,
    });

    const example_exe = b.addExecutable(.{
        .name = options.example.toString(),
        .root_module = example_mod,
    });

    const install_artifact = b.addInstallArtifact(example_exe, .{});
    b.getInstallStep().dependOn(&install_artifact.step);

    // build
    steps.install.dependOn(&install_artifact.step);

    // Should not run all examples at the same time
    if (options.allExamples) {
        return;
    }

    // run
    const run_artifact = b.addRunArtifact(example_exe);
    run_artifact.step.dependOn(&install_artifact.step);

    steps.run.dependOn(&install_artifact.step);
    steps.run.dependOn(&run_artifact.step);
}

const AsyncKind = @import("src/aio/lib.zig").AsyncKind;

fn add_test(
    b: *std.Build,
    name: []const u8,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.Mode,
    tardy_module: *std.Build.Module,
) void {
    const exe = b.addExecutable(.{
        .name = b.fmt("{s}", .{name}),
        .root_source_file = b.path(b.fmt("test/{s}/main.zig", .{name})),
        .target = target,
        .optimize = optimize,
        .strip = false,
    });

    if (target.result.os.tag == .windows) exe.linkLibC();

    const async_option = b.option(AsyncKind, "async", "What async backend you want to compile support for") orelse .auto;
    const options = b.addOptions();
    options.addOption(AsyncKind, "async_option", async_option);
    exe.root_module.addOptions("options", options);

    exe.root_module.addImport("tardy", tardy_module);
    const install_artifact = b.addInstallArtifact(exe, .{});
    b.getInstallStep().dependOn(&install_artifact.step);

    const build_step = b.step(b.fmt("{s}", .{name}), b.fmt("Build tardy test ({s})", .{name}));
    build_step.dependOn(&install_artifact.step);

    const run_artifact = b.addRunArtifact(exe);
    run_artifact.step.dependOn(&install_artifact.step);

    if (b.args) |args| run_artifact.addArgs(args);

    const run_step = b.step(b.fmt("test_{s}", .{name}), b.fmt("Run tardy test ({s})", .{name}));
    run_step.dependOn(&install_artifact.step);
    run_step.dependOn(&run_artifact.step);
}
