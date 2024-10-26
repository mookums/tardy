const std = @import("std");
const log = std.log.scoped(.@"tardy/example/basic");

const Runtime = @import("tardy").Runtime;
const Task = @import("tardy").Task;
const Tardy = @import("tardy").Tardy(.auto);

const Counter = packed struct {
    count: u32 = 0,

    pub fn increment(self: Counter) Counter {
        return .{ .count = self.count + 1 };
    }
};

fn log_task(rt: *Runtime, _: *const Task, count: i8) !void {
    log.debug("{d} - tardy example | {d}", .{ std.time.milliTimestamp(), count });
    rt.spawn_delay(i8, log_task, count + 1, .{ .seconds = 1 }) catch unreachable;
}

fn log_task_struct(rt: *Runtime, _: *const Task, counter: Counter) !void {
    const count = counter.increment().count;
    log.debug("{d} - tardy example | {d}", .{ std.time.milliTimestamp(), count });
    rt.spawn_delay(Counter, log_task_struct, Counter{ .count = count }, .{ .seconds = 1 }) catch unreachable;
}

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    var tardy = try Tardy.init(.{
        .allocator = allocator,
        .threading = .single,
    });
    defer tardy.deinit();

    try tardy.entry(
        struct {
            fn init(rt: *Runtime, _: std.mem.Allocator, _: anytype) !void {
                try rt.spawn(i8, log_task, std.math.minInt(i8));
                try rt.spawn(Counter, log_task_struct, Counter{ .count = 0 });
            }
        }.init,
        void,
        struct {
            fn deinit(_: *Runtime, _: std.mem.Allocator, _: anytype) void {}
        }.deinit,
        void,
    );
}
