const std = @import("std");
const log = std.log.scoped(.@"tardy/example/cat");

const Runtime = @import("tardy").Runtime;
const Task = @import("tardy").Task;
const Tardy = @import("tardy").Tardy(.auto);
const Cross = @import("tardy").Cross;

const File = @import("tardy").File;

const InnerOpenResult = @import("tardy").InnerOpenResult;
const ReadResult = @import("tardy").ReadResult;
const WriteResult = @import("tardy").WriteResult;

pub const std_options = .{
    .log_level = .info,
};

const FileProvision = struct {
    std_out: File = undefined,
    file: File = undefined,
    buffer: []u8,
    written: usize = 0,
    read: usize = 0,
};

fn open_task(rt: *Runtime, result: InnerOpenResult, provision: *FileProvision) !void {
    provision.file = try result.unwrap();
    provision.std_out = .{ .handle = try Cross.get_std_out() };

    try provision.file.read(rt, provision, read_task, provision.buffer);
}

fn read_task(rt: *Runtime, result: ReadResult, provision: *FileProvision) !void {
    const length = result.unwrap() catch |e| {
        switch (e) {
            error.EndOfFile => {
                return try provision.file.close(rt, provision, close_task);
            },
            else => {
                std.debug.print("Unexpected Error: {}\n", .{e});
                return;
            },
        }
    };

    provision.read += @intCast(length);
    try provision.std_out.write(rt, provision, write_task, provision.buffer[0..length]);
}

fn write_task(rt: *Runtime, result: WriteResult, provision: *FileProvision) !void {
    const written = result.unwrap() catch |e| {
        std.debug.print("Unexpected Error: {}\n", .{e});
        return try provision.file.close(rt, provision, close_task);
    };

    provision.written += written;

    if (provision.written < provision.read) {
        const remaining_slice = provision.buffer[provision.read - provision.written ..];
        try provision.std_out.write(rt, provision, write_task, remaining_slice);
    } else {
        try provision.file.read(rt, provision, read_task, provision.buffer);
    }
}

fn close_task(rt: *Runtime, _: void, _: *FileProvision) !void {
    log.debug("all done!", .{});
    rt.stop();
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .thread_safe = true }){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    var tardy = try Tardy.init(.{
        .allocator = allocator,
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
        provision: FileProvision,
        file_name: [:0]const u8,
    };

    var buffer: [512]u8 = undefined;

    var params: EntryParams = .{
        .file_name = file_name,
        .provision = .{ .buffer = &buffer },
    };

    try tardy.entry(
        &params,
        struct {
            fn start(rt: *Runtime, parameters: *EntryParams) !void {
                try File.open(
                    rt,
                    &parameters.provision,
                    open_task,
                    .{
                        .rel = .{
                            .dir = std.posix.AT.FDCWD,
                            .path = parameters.file_name,
                        },
                    },
                    .{},
                );
            }
        }.start,
        {},
        struct {
            fn end(_: *Runtime, _: void) !void {}
        }.end,
    );
}
