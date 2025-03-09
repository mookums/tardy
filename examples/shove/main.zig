const std = @import("std");
const log = std.log.scoped(.@"tardy/example/shove");

const Runtime = @import("tardy").Runtime;
const Task = @import("tardy").Task;
const Tardy = @import("tardy").Tardy(.auto);
const Cross = @import("tardy").Cross;

const File = @import("tardy").File;
const Dir = @import("tardy").Dir;

const OpenFileResult = @import("tardy").OpenFileResult;
const ReadResult = @import("tardy").ReadResult;
const WriteResult = @import("tardy").WriteResult;

pub const std_options: std.Options = .{ .log_level = .debug };

fn main_frame(rt: *Runtime, name: [:0]const u8) !void {
    const file = try Dir.cwd().create_file(rt, name, .{});
    for (0..8) |_| _ = try file.write_all(rt, "*shoved*\n", null);

    const stat = try file.stat(rt);
    std.debug.print("size: {d}\n", .{stat.size});

    try file.close(rt);
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .thread_safe = true }){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    var tardy = try Tardy.init(allocator, .{
        .threading = .single,
        .pooling = .grow,
        .size_tasks_initial = 1,
        .size_aio_reap_max = 1,
    });
    defer tardy.deinit();

    var i: usize = 0;
    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    const file_name: [:0]const u8 = blk: {
        while (args.next()) |arg| : (i += 1) if (i == 1) break :blk arg;
        try std.io.getStdOut().writeAll("file name not passed in: ./shove [file name]");
        return;
    };

    try tardy.entry(
        file_name,
        struct {
            fn start(rt: *Runtime, name: [:0]const u8) !void {
                try rt.spawn(.{ rt, name }, main_frame, 1024 * 1024 * 2);
            }
        }.start,
    );
}
