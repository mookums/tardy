const std = @import("std");
const log = std.log.scoped(.@"tardy/example/event");

const Runtime = @import("tardy").Runtime;
const Task = @import("tardy").Task;
const Timer = @import("tardy").Timer;
const Tardy = @import("tardy").Tardy(.auto);

pub const std_options = .{ .log_level = .debug };

fn first_frame(rt: *Runtime) !void {
    const sub = try rt.events.subscribe(.abc);
    std.log.debug("subscribed to topic...", .{});
    const value = try sub.recv(rt, u32);
    std.log.debug("sub 1 received value {d} from event topic", .{value});
}

fn second_frame(rt: *Runtime) !void {
    const sub = try rt.events.subscribe(.abc);
    std.log.debug("subscribed to topic...", .{});
    const value = try sub.recv(rt, u32);
    std.log.debug("sub 2 received value {d} from event topic", .{value});
}

fn third_frame(rt: *Runtime) !void {
    // Race-conditiony but lets just wait a few seconds for the other threads to start first.
    try Timer.delay(rt, .{ .seconds = 2 });
    std.log.debug("pushing to test topic...", .{});
    try rt.events.push(.abc, u32, 59);
    std.log.debug("pushed to test topic", .{});
}

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    var tardy = try Tardy.init(allocator, .{
        .threading = .{ .multi = 3 },
        .pooling = .static,
        .size_tasks_initial = 1,
        .size_aio_reap_max = 1,
    });
    defer tardy.deinit();

    try tardy.entry(
        {},
        struct {
            fn init(rt: *Runtime, _: void) !void {
                switch (rt.id) {
                    0 => try rt.spawn(.{rt}, first_frame, 1024 * 32),
                    1 => try rt.spawn(.{rt}, second_frame, 1024 * 32),
                    2 => try rt.spawn(.{rt}, third_frame, 1024 * 32),
                    else => unreachable,
                }
            }
        }.init,
    );
}
