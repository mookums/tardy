const std = @import("std");
const log = std.log.scoped(.@"tardy/example/basic");

const Runtime = @import("tardy").Runtime;
const Task = @import("tardy").Task;

const Timer = @import("tardy").Timer;

const Tardy = @import("tardy").Tardy(.auto);

const Counter = packed struct {
    count: usize = 0,

    pub fn increment(self: Counter) usize {
        return self.count + 1;
    }
};

fn log_frame(rt: *Runtime) !void {
    var count: usize = 0;

    while (count < 10) : (count += 1) {
        log.debug("{d} - tardy example | {d}", .{ std.time.milliTimestamp(), count });
        try Timer.delay(.{ .seconds = 1 }).resolve(rt);
    }
}

fn log_task_struct(rt: *Runtime, _: void, counter: Counter) !void {
    const count = counter.increment();
    log.debug("{d} - tardy example | {d}", .{ std.time.milliTimestamp(), count });
    try Timer.delay(.{ .seconds = 1 }).callback(rt, Counter{ .count = count }, log_task_struct);
}

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    var tardy = try Tardy.init(allocator, .{
        .threading = .single,
        .pooling = .static,
        .size_tasks_initial = 2,
        .size_aio_reap_max = 2,
    });
    defer tardy.deinit();

    try tardy.entry(
        {},
        struct {
            fn init(rt: *Runtime, _: void) !void {
                try rt.spawn_frame(.{rt}, log_frame, 1024 * 16);
                //try rt.spawn_task(void, Counter{ .count = 0 }, log_task_struct);
            }
        }.init,
        {},
        struct {
            fn deinit(_: *Runtime, _: void) !void {}
        }.deinit,
    );
}
