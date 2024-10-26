const std = @import("std");
const log = std.log.scoped(.@"tardy/example/basic");

const Runtime = @import("tardy").Runtime;
const Task = @import("tardy").Task;
const Tardy = @import("tardy").Tardy(.auto);

fn log_task(rt: *Runtime, _: *const Task, ctx: *void) !void {
    log.debug("{d} - tardy example", .{std.time.milliTimestamp()});
    rt.spawn_delay(void, log_task, ctx, .{ .seconds = 1 }) catch unreachable;
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
                try rt.spawn(void, log_task, @constCast(&{}));
            }
        }.init,
        void,
        struct {
            fn deinit(_: *Runtime, _: std.mem.Allocator, _: anytype) void {}
        }.deinit,
        void,
    );
}
