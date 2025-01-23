const std = @import("std");
const log = std.log.scoped(.@"tardy/example/basic");

const Runtime = @import("tardy").Runtime;
const Task = @import("tardy").Task;

const Timer = @import("tardy").Timer;

const Tardy = @import("tardy").Tardy(.auto);

fn log_frame(rt: *Runtime) !void {
    var count: usize = 0;

    while (count < 10) : (count += 1) {
        log.debug("{d} - tardy example | {d}", .{ std.time.milliTimestamp(), count });
        try Timer.delay(rt, .{ .seconds = 1 });
    }
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
                try rt.spawn(.{rt}, log_frame, 1024 * 16);
            }
        }.init,
        {},
        struct {
            fn deinit(_: *Runtime, _: void) !void {}
        }.deinit,
    );
}
