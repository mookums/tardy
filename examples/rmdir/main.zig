const std = @import("std");
const log = std.log.scoped(.@"tardy/example/rmdir");

const Runtime = @import("tardy").Runtime;
const Task = @import("tardy").Task;
const Tardy = @import("tardy").Tardy(.auto);
const Cross = @import("tardy").Cross;

const Dir = @import("tardy").Dir;

const OpenFileResult = @import("tardy").OpenFileResult;
const ReadResult = @import("tardy").ReadResult;
const WriteResult = @import("tardy").WriteResult;

const DeleteTreeResult = @import("tardy").DeleteTreeResult;

fn end_task(rt: *Runtime, res: DeleteTreeResult, _: void) !void {
    try res.unwrap();
    log.debug("deleted tree :)", .{});
    rt.stop();
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .thread_safe = true }){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    var tardy = try Tardy.init(allocator, .{
        .threading = .single,
        .size_tasks_max = 1,
        .size_aio_jobs_max = 1,
        .size_aio_reap_max = 1,
    });
    defer tardy.deinit();

    var i: usize = 0;
    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    const tree_name: [:0]const u8 = blk: {
        while (args.next()) |arg| : (i += 1) {
            if (i == 1) break :blk arg;
        }

        try std.io.getStdOut().writeAll("tree name not passed in: ./rmdir [tree name]");
        return;
    };

    try tardy.entry(
        tree_name,
        struct {
            fn start(rt: *Runtime, name: [:0]const u8) !void {
                const dir = Dir.from_std(std.fs.cwd());
                try dir.delete_tree(rt, {}, end_task, name, 32);
            }
        }.start,
        {},
        struct {
            fn end(_: *Runtime, _: void) !void {}
        }.end,
    );
}
