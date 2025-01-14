const std = @import("std");
const log = std.log.scoped(.@"tardy/example/cat");

const Runtime = @import("tardy").Runtime;
const Task = @import("tardy").Task;
const Tardy = @import("tardy").Tardy(.auto);
const Cross = @import("tardy").Cross;

const Dir = @import("tardy").Dir;
const File = @import("tardy").File;

const OpenFileResult = @import("tardy").OpenFileResult;
const ReadResult = @import("tardy").ReadResult;
const WriteResult = @import("tardy").WriteResult;

const ZeroCopy = @import("tardy").ZeroCopy;

pub const std_options = .{ .log_level = .err };

const Context = struct {
    std_out: File = File.std_out(),
    file: File = undefined,
    buffer: []u8,
};

fn open_task(rt: *Runtime, result: OpenFileResult, context: *Context) !void {
    context.file = try result.unwrap();
    try context.file.read_all(rt, context, read_task, context.buffer, null);
}

fn read_task(rt: *Runtime, result: ReadResult, context: *Context) !void {
    const length = try result.unwrap();

    if (length != context.buffer.len) {
        try context.std_out.write_all(rt, context, write_done_task, context.buffer[0..length], null);
    } else {
        try context.std_out.write_all(rt, context, write_task, context.buffer, null);
    }
}

fn write_task(rt: *Runtime, result: WriteResult, context: *Context) !void {
    _ = try result.unwrap();
    try context.file.read_all(rt, context, read_task, context.buffer, null);
}

fn write_done_task(rt: *Runtime, result: WriteResult, context: *Context) !void {
    _ = try result.unwrap();
    context.file.close_blocking();
    rt.stop();
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .thread_safe = true }){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    var tardy = try Tardy.init(allocator, .{
        // The way this is written will only support
        // single-threaded execution.
        .threading = .single,
        .size_tasks_max = 1,
        .size_aio_jobs_max = 1,
        .size_aio_reap_max = 1,
    });
    defer tardy.deinit();

    var i: usize = 0;
    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    const file_name: [:0]const u8 = blk: {
        while (args.next()) |arg| : (i += 1) {
            if (i == 1) break :blk arg;
        }

        try std.io.getStdOut().writeAll("file name not passed in: ./cat [file name]");
        return;
    };

    const EntryParams = struct {
        file_name: [:0]const u8,
        context: Context,
    };

    // 32kB buffer :)
    var buf: [1024 * 32]u8 = undefined;
    var params: EntryParams = .{
        .file_name = file_name,
        .context = .{ .buffer = &buf },
    };

    try tardy.entry(
        &params,
        struct {
            fn start(rt: *Runtime, p: *EntryParams) !void {
                try Dir.cwd().open_file(rt, &p.context, open_task, p.file_name, .{});
            }
        }.start,
        {},
        struct {
            fn end(_: *Runtime, _: void) !void {}
        }.end,
    );
}
