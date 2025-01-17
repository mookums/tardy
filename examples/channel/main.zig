const std = @import("std");
const log = std.log.scoped(.@"tardy/example/channel");

const Tardy = @import("tardy").Tardy(.auto);
const Broadcast = @import("tardy").Broadcast;
const Runtime = @import("tardy").Runtime;
const Task = @import("tardy").Task;
const Channel = @import("tardy").Channel;

const Timer = @import("tardy").Timer;

const Atomic = std.atomic.Value;

fn write_channel_task(rt: *Runtime, _: void, bc: *Broadcast(usize)) !void {
    const num: usize = @intCast(std.time.timestamp());
    try bc.send(num);
    try Timer.delay(rt, bc, write_channel_task, .{ .seconds = 1 });
}

fn read_channel_task(_: *Runtime, read: ?*const usize, chan: *Channel(usize)) !void {
    if (read) |data| {
        log.debug("tardy channel recv: {d}", .{data.*});
        try chan.recv(chan, read_channel_task);
    } else {
        log.debug("tardy channel closed", .{});
    }
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    var tardy = try Tardy.init(allocator, .{
        .threading = .{ .multi = 3 },
        .pooling = .static,
    });
    defer tardy.deinit();

    var f = Atomic(bool).init(false);
    var b = try Broadcast(usize).init(allocator, 10);
    defer b.deinit();

    const Params = struct {
        broadcast: *Broadcast(usize),
        flag: *Atomic(bool),
    };

    try tardy.entry(
        Params{ .broadcast = &b, .flag = &f },
        struct {
            fn init(rt: *Runtime, params: Params) !void {
                const broadcast = params.broadcast;

                // Spawn only one writer.
                if (!params.flag.swap(true, .acq_rel)) {
                    log.debug("spawned write task", .{});
                    try rt.spawn(void, broadcast, write_channel_task);
                }

                const chan: *Channel(usize) = try broadcast.subscribe(rt, 1);
                try chan.recv(chan, read_channel_task);
            }
        }.init,
        {},
        struct {
            fn deinit(_: *Runtime, _: void) !void {}
        }.deinit,
    );
}
