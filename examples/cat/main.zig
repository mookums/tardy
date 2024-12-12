const std = @import("std");
const log = std.log.scoped(.@"tardy/example/file");

const Runtime = @import("tardy").Runtime;
const Task = @import("tardy").Task;
const Tardy = @import("tardy").Tardy(.auto);
const Cross = @import("tardy").Cross;

const OpenResult = @import("tardy").OpenResult;
const ReadResult = @import("tardy").ReadResult;
const WriteResult = @import("tardy").WriteResult;

const FileProvision = struct {
    fd: std.posix.fd_t = undefined,
    buffer: []u8,
    offset: usize,
};

fn open_task(rt: *Runtime, result: OpenResult, provision: *FileProvision) !void {
    const fd = try result.unwrap();
    provision.fd = fd;

    if (!Cross.fd.is_valid(fd)) {
        try std.io.getStdOut().writeAll("No such file or directory");
        rt.stop();
        return;
    }

    try rt.fs.read(
        provision,
        read_task,
        fd,
        provision.buffer,
        provision.offset,
    );
}

fn read_task(rt: *Runtime, result: ReadResult, provision: *FileProvision) !void {
    const length = result.unwrap() catch |e| {
        switch (e) {
            error.EndOfFile => {
                try rt.fs.close(provision, close_task, provision.fd);
                return;
            },
            else => {
                std.debug.print("Unexpected Error: {}\n", .{e});
                return;
            },
        }
    };

    provision.offset += @intCast(length);

    try rt.fs.write(
        provision,
        write_task,
        try Cross.get_std_out(),
        provision.buffer,
        provision.offset,
    );
}

fn write_task(rt: *Runtime, result: WriteResult, provision: *FileProvision) !void {
    _ = result.unwrap() catch |e| {
        std.debug.print("Unexpected Error: {}\n", .{e});
        try rt.fs.close(provision, close_task, provision.fd);
        return;
    };

    try rt.fs.read(
        provision,
        read_task,
        provision.fd,
        provision.buffer,
        provision.offset,
    );
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

    const buffer = try allocator.alloc(u8, 512);
    defer allocator.free(buffer);

    var params: EntryParams = .{
        .file_name = file_name,
        .provision = .{
            .buffer = buffer,
            .offset = 0,
        },
    };

    try tardy.entry(
        &params,
        struct {
            fn start(rt: *Runtime, parameters: *EntryParams) !void {
                try rt.fs.open(&parameters.provision, open_task, parameters.file_name);
            }
        }.start,
        {},
        struct {
            fn end(_: *Runtime, _: void) !void {}
        }.end,
    );
}
