const std = @import("std");
const log = std.log.scoped(.@"tardy/example/stat");

const Runtime = @import("tardy").Runtime;
const Task = @import("tardy").Task;
const Tardy = @import("tardy").Tardy(.auto);
const Stat = @import("tardy").Stat;

fn stat_task(rt: *Runtime, stat: Stat, _: void) !void {
    try std.io.getStdOut().writer().print("size: {d}", .{stat.size});
    rt.stop();
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    var tardy = try Tardy.init(.{
        .allocator = allocator,
        .threading = .single,
    });
    defer tardy.deinit();

    var i: usize = 0;
    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    const file_name: [:0]const u8 = blk: {
        while (args.next()) |arg| : (i += 1) {
            if (i == 1) break :blk arg;
        }

        try std.io.getStdOut().writeAll("file name not passed in: ./stat [file name]");
        return;
    };

    try tardy.entry(
        struct {
            fn init(rt: *Runtime, _: std.mem.Allocator, path: [:0]const u8) !void {
                const file = try std.fs.cwd().openFileZ(path, .{});
                try rt.fs.stat({}, stat_task, file.handle);
            }
        }.init,
        file_name,
        struct {
            fn deinit(_: *Runtime, _: std.mem.Allocator, _: anytype) void {}
        }.deinit,
        void,
    );
}
