const std = @import("std");
const log = std.log.scoped(.@"tardy/example/channel");

const Runtime = @import("tardy").Runtime;
const Task = @import("tardy").Task;
const Timer = @import("tardy").Timer;
const Tardy = @import("tardy").Tardy(.auto);

const Spsc = @import("tardy").Spsc;

pub const std_options = .{ .log_level = .err };

const MAX_COUNT = 10;

fn producer_frame(rt: *Runtime, producer: Spsc(usize).Producer) !void {
    var count: usize = 0;

    while (count <= MAX_COUNT) {
        producer.send(count) catch |e| switch (e) {
            error.NotReady => {
                log.debug("not ready!", .{});
                continue;
            },
            else => return e,
        };
        count += 1;

        try Timer.delay(rt, .{ .nanos = std.time.ns_per_ms * 10 });
    }
}

fn consumer_frame(_: *Runtime, consumer: Spsc(usize).Consumer) !void {
    var count: usize = 0;

    while (count < MAX_COUNT) {
        count = consumer.recv() catch |e| switch (e) {
            error.ChannelClosed => return,
            error.NotReady => {
                log.debug("not ready!", .{});
                continue;
            },
            else => return e,
        };
        log.err("{d} - tardy example | {d}", .{ std.time.milliTimestamp(), count });
    }
}

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    var tardy = try Tardy.init(allocator, .{
        .threading = .{ .multi = 2 },
        .pooling = .static,
        .size_tasks_initial = 1,
        .size_aio_reap_max = 1,
    });
    defer tardy.deinit();

    var channel = try Spsc(usize).init(allocator, 1);
    defer channel.deinit();

    try tardy.entry(
        &channel,
        struct {
            fn init(rt: *Runtime, spsc: *Spsc(usize)) !void {
                switch (rt.id) {
                    0 => try rt.spawn(.{ rt, spsc.producer(rt) }, producer_frame, 1024 * 32),
                    1 => try rt.spawn(.{ rt, spsc.consumer(rt) }, consumer_frame, 1024 * 32),
                    else => unreachable,
                }
            }
        }.init,
    );
}
