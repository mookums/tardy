const std = @import("std");
const log = std.log.scoped(.@"tardy/example/file");

const Runtime = @import("tardy").Runtime;
const Task = @import("tardy").Task;
const Tardy = @import("tardy").Tardy(.auto);

const FileProvision = struct {
    fd: std.posix.fd_t,
    buffer: []u8,
    offset: usize,
};

fn open_task(rt: *Runtime, t: *const Task, ctx: ?*anyopaque) !void {
    const provision: *FileProvision = @ptrCast(@alignCast(ctx.?));
    const fd: std.posix.fd_t = t.result.?.fd;
    provision.fd = fd;

    if (fd <= 0) {
        try std.io.getStdOut().writeAll("No such file or directory");
        rt.stop();
        return;
    }

    try rt.fs.read(.{
        .fd = fd,
        .buffer = provision.buffer,
        .offset = provision.offset,
        .func = read_task,
        .ctx = ctx,
    });
}

fn read_task(rt: *Runtime, t: *const Task, ctx: ?*anyopaque) !void {
    const provision: *FileProvision = @ptrCast(@alignCast(ctx.?));
    const length: i32 = t.result.?.value;
    provision.offset += @intCast(length);

    // either done OR we have read EOF.
    if (length <= 0 or provision.buffer[@intCast(length - 1)] == 0x04) {
        try rt.fs.close(.{
            .fd = provision.fd,
            .func = close_task,
            .ctx = ctx,
        });

        return;
    }

    try rt.fs.write(.{
        .fd = std.posix.STDOUT_FILENO,
        .buffer = provision.buffer[0..@intCast(length)],
        .offset = provision.offset,
        .func = write_task,
        .ctx = ctx,
    });
}

fn write_task(rt: *Runtime, t: *const Task, ctx: ?*anyopaque) !void {
    const provision: *FileProvision = @ptrCast(@alignCast(ctx.?));
    const length: i32 = t.result.?.value;

    if (length <= 0) {
        try rt.fs.close(.{
            .fd = provision.fd,
            .func = close_task,
            .ctx = ctx,
        });

        return;
    }

    try rt.fs.read(.{
        .fd = provision.fd,
        .buffer = provision.buffer,
        .offset = provision.offset,
        .func = read_task,
        .ctx = ctx,
    });
}

fn close_task(rt: *Runtime, _: *const Task, _: ?*anyopaque) !void {
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
    var args = std.process.args();
    const file_name: []const u8 = blk: {
        while (args.next()) |arg| : (i += 1) {
            if (i == 1) break :blk arg;
        }

        try std.io.getStdOut().writeAll("file name not passed in: ./cat [file name]");
        return;
    };

    const EntryParams = struct {
        provision: FileProvision,
        file_name: []const u8,
    };

    const buffer = try allocator.alloc(u8, 512);
    defer allocator.free(buffer);

    var params: EntryParams = .{
        .file_name = file_name,
        .provision = .{
            .fd = 0,
            .buffer = buffer,
            .offset = 0,
        },
    };

    try tardy.entry(struct {
        fn start(rt: *Runtime, _: std.mem.Allocator, parameters: *EntryParams) !void {
            try rt.fs.open(.{
                .path = parameters.file_name,
                .func = open_task,
                .ctx = &parameters.provision,
            });
        }
    }.start, &params);
}
