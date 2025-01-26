const std = @import("std");
const log = std.log.scoped(.@"tardy/example/stat");

const Runtime = @import("tardy").Runtime;
const Task = @import("tardy").Task;
const Tardy = @import("tardy").Tardy(.auto);

const Dir = @import("tardy").Dir;
const File = @import("tardy").File;
const Stat = @import("tardy").Stat;
const StatResult = @import("tardy").StatResult;

fn main_frame(rt: *Runtime, name: [:0]const u8) !void {
    const file = try Dir.cwd().open_file(rt, name, .{});
    defer file.close_blocking();

    const stat = try file.stat(rt);
    std.debug.print("stat: {any}\n", .{stat});
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    var tardy = try Tardy.init(allocator, .{
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
        file_name,
        struct {
            fn init(rt: *Runtime, path: [:0]const u8) !void {
                try rt.spawn(.{ rt, path }, main_frame, 1024 * 1024 * 2);
            }
        }.init,
    );
}
