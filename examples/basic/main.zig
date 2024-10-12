const std = @import("std");
const log = std.log.scoped(.@"tardy/example/basic");

const Runtime = @import("tardy").Runtime;
const Task = @import("tardy").Task;
const Tardy = @import("tardy").Tardy(.auto);

fn log_task(rt: *Runtime, _: *Task, _: ?*anyopaque) void {
    log.debug("{d} - tardy example", .{std.time.milliTimestamp()});
    std.time.sleep(1 * std.time.ns_per_s);
    rt.spawn(.{ .func = log_task }) catch unreachable;
}

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    var tardy = try Tardy.init(.{
        .allocator = allocator,
        .threading = .single_threaded,
    });
    defer tardy.deinit();

    try tardy.entry(struct {
        fn start(rt: *Runtime, _: std.mem.Allocator, _: anytype) !void {
            try rt.spawn(.{ .func = log_task });
        }
    }.start, void);
}
