const std = @import("std");
const assert = std.debug.assert;
const tardy = @import("tardy");
const log = std.log.scoped(.@"tardy/example/basic");

const Runtime = tardy.Runtime(.auto);
const Task = Runtime.Task;

fn log_task(rt: *Runtime, _: *Task, _: ?*anyopaque) void {
    log.debug("{d} - tardy example", .{std.time.milliTimestamp()});
    std.time.sleep(1 * std.time.ns_per_s);
    rt.spawn(.{ .func = log_task }) catch unreachable;
}

pub fn main() !void {
    const allocator = std.heap.page_allocator;
    var runtime = try Runtime.init(.{ .allocator = allocator });
    defer runtime.deinit();

    try runtime.spawn(.{ .func = log_task });
    try runtime.run();
}
