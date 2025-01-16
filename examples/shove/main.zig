const std = @import("std");
const log = std.log.scoped(.@"tardy/example/shove");

const Runtime = @import("tardy").Runtime;
const Task = @import("tardy").Task;
const Tardy = @import("tardy").Tardy(.auto);
const Cross = @import("tardy").Cross;

const File = @import("tardy").File;

const OpenFileResult = @import("tardy").OpenFileResult;
const ReadResult = @import("tardy").ReadResult;
const WriteResult = @import("tardy").WriteResult;

pub const std_options = .{
    .log_level = .debug,
};

fn create_task(rt: *Runtime, result: OpenFileResult, _: void) !void {
    const file = try result.unwrap();
    try rt.storage.store_alloc("file_fd", file.handle);
    for (0..8) |_| try file.write_all(rt, {}, end_task, "*shoved*\n", null);
}

fn end_task(_: *Runtime, res: WriteResult, _: void) !void {
    _ = try res.unwrap();
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .thread_safe = true }){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    var tardy = try Tardy.init(allocator, .{
        .threading = .single,
        .pooling = .grow,
        .size_tasks_initial = 1,
        .size_aio_reap_max = 256,
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
                try File.create(rt, {}, create_task, .{ .rel = .{
                    .dir = std.posix.AT.FDCWD,
                    .path = name,
                } }, .{});
            }
        }.start,
        {},
        struct {
            fn end(rt: *Runtime, _: void) !void {
                const fd = rt.storage.get("file_fd", std.posix.fd_t);
                std.posix.close(fd);
            }
        }.end,
    );
}
