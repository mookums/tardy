const std = @import("std");
const log = std.log.scoped(.@"tardy/example/channel");

const Channel = @import("tardy").Channel;
const Runtime = @import("tardy").Runtime;
const Task = @import("tardy").Task;
const Tardy = @import("tardy").Tardy(.auto);

fn write_channel_task(rt: *Runtime, _: void, channel: *Channel(usize)) !void {
    const num: usize = @intCast(std.time.timestamp());
    try channel.send(num);
    try rt.spawn_delay(void, channel, write_channel_task, .{ .seconds = 1 });
}

fn read_channel_task(_: *Runtime, read: *const usize, channel: *Channel(usize)) !void {
    log.debug("tardy channel recv: {d}", .{read.*});
    try channel.recv(channel, read_channel_task);
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    var tardy = try Tardy.init(.{
        .allocator = allocator,
        .threading = .single,
    });
    defer tardy.deinit();

    try tardy.entry(
        struct {
            fn init(rt: *Runtime, alloc: std.mem.Allocator, _: void) !void {
                const channel = try alloc.create(Channel(usize));
                channel.* = try Channel(usize).init(alloc, rt, 1);
                try rt.storage.store_ptr("channel", channel);

                try rt.spawn(void, channel, write_channel_task);
                try channel.recv(channel, read_channel_task);
            }
        }.init,
        {},
        struct {
            fn deinit(rt: *Runtime, alloc: std.mem.Allocator, _: void) void {
                const chan = rt.storage.get_ptr("channel", Channel(usize));
                chan.deinit();
                alloc.destroy(chan);
            }
        }.deinit,
        {},
    );
}
