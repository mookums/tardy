const std = @import("std");
const log = std.log.scoped(.@"tardy/example/resolve");

const Runtime = @import("tardy").Runtime;
const Frame = @import("tardy").Frame;
const Task = @import("tardy").Task;

const Timer = @import("tardy").Timer;
const Tardy = @import("tardy").Tardy(.auto);

fn log_task(rt: *Runtime, _: void, count: i8) !void {
    log.debug("{d} - tardy example | {d}", .{ std.time.milliTimestamp(), count });
    try Timer.delay(.{ .seconds = 1 }).callback(rt, count + 1, log_task);
}

fn first_frame(rt: *Runtime) !void {
    var y: usize = 100;
    log.debug("running the first frame! (y={d})", .{y});
    y += 10;

    // set to waiting
    log.debug("setting timer and waiting for timer", .{});
    try Timer.delay(.{ .seconds = 5 }).resolve(rt);

    log.debug("continuing the first frame! (y={d})", .{y});
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
                try rt.spawn_frame(.{rt}, first_frame, 1024 * 32);
                //try rt.spawn_task(void, @as(i8, std.math.minInt(i8)), log_task);
            }
        }.init,
        {},
        struct {
            fn deinit(_: *Runtime, _: void) !void {}
        }.deinit,
    );
}
