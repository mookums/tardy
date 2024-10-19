const std = @import("std");
const log = std.log.scoped(.@"tardy/example/file");

const tardy = @import("tardy");
const Runtime = tardy.Runtime;
const Task = tardy.Task;
const Tardy = tardy.Tardy(.auto);

// Using the higher level File API.
const File = tardy.File;

var buffer: [256]u8 = undefined;

fn open_cb(file: File, _: *Runtime, _: *const Task, _: ?*anyopaque) !void {
    log.debug("File: {d}", .{file.handle});

    try file.read(buffer[0..], null, read_cb);
}

fn read_cb(bytes: []const u8, file: File, _: *Runtime, _: *const Task, _: ?*anyopaque) !void {
    if (bytes.len > 0) {
        try std.io.getStdOut().writeAll(bytes);
        try file.read(buffer[0..], null, read_cb);
    } else {
        try file.close(null, close_cb);
    }
}

fn close_cb(rt: *Runtime, _: *const Task, _: ?*anyopaque) !void {
    log.debug("file closed!", .{});
    rt.stop();
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    var t = try Tardy.init(.{
        .allocator = allocator,
        .threading = .single,
        .size_tasks_max = 1,
        .size_aio_jobs_max = 1,
        .size_aio_reap_max = 1,
    });
    defer t.deinit();

    try t.entry(
        struct {
            fn init(rt: *Runtime, _: std.mem.Allocator, _: anytype) !void {
                const std_file = try std.fs.cwd().openFile("./README.md", .{});
                const file = File.from_std(rt, std_file);
                try file.read(buffer[0..], null, read_cb);
            }
        }.init,
        void,
        struct {
            fn deinit(_: *Runtime, _: std.mem.Allocator, _: anytype) void {}
        }.deinit,
        void,
    );
}
