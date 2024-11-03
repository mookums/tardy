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

fn log_task(rt: *Runtime, _: void, count: i8) !void {
    log.debug("{d} - tardy example | {d}", .{ std.time.milliTimestamp(), count });
    try rt.spawn_delay(void, count + 1, log_task, .{ .seconds = 1 });
}

fn log_task_struct(rt: *Runtime, _: void, counter: Counter) !void {
    const count = counter.increment().count;
    log.debug("{d} - tardy example | {d}", .{ std.time.milliTimestamp(), count });
    try rt.spawn_delay(void, Counter{ .count = count }, log_task_struct, .{ .seconds = 1 });
}

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    var tardy = try Tardy.init(.{
        .allocator = allocator,
        .threading = .single,
    });
    defer tardy.deinit();

    try tardy.entry(
        {},
        struct {
            fn init(rt: *Runtime, _: void) !void {
                try rt.spawn(void, @as(i8, std.math.minInt(i8)), log_task);
                try rt.spawn(void, Counter{ .count = 0 }, log_task_struct);
            }
        }.init,
        {},
        struct {
            fn deinit(_: *Runtime, _: void) !void {}
        }.deinit,
    );
}
