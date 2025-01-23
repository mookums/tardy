const std = @import("std");
const Ring = @import("../core/ring.zig").Ring;
const Runtime = @import("lib.zig").Runtime;
const Channel = @import("channel.zig").Channel;
const Pool = @import("../core/pool.zig").Pool;

/// A broadcast is a SPMC abstraction that
/// allows you to send data across runtimes.
///
/// This wraps the Channel type and is thread-safe.
pub fn Broadcast(comptime T: type) type {
    return struct {
        const Self = @This();
        allocator: std.mem.Allocator,
        mutex: std.Thread.Mutex = .{},
        channels: Pool(Channel(T)),

        pub fn init(allocator: std.mem.Allocator, channel_count: usize) !Self {
            return .{
                .allocator = allocator,
                .channels = try Pool(Channel(T)).init(allocator, channel_count, .static),
            };
        }

        pub fn deinit(self: *Self) void {
            self.mutex.lock();
            defer self.mutex.unlock();

            var iter = self.channels.iterator();
            while (iter.next_ptr()) |chan| {
                chan.deinit();
            }

            self.channels.deinit();
        }

        pub fn subscribe(self: *Self, runtime: *Runtime, channel_size: usize) !*Channel(T) {
            self.mutex.lock();
            defer self.mutex.unlock();

            const index = try self.channels.borrow();
            const item = self.channels.get_ptr(index);
            item.* = try Channel(T).init(self.allocator, runtime, channel_size);
            item.id = index;
            return item;
        }

        pub fn unsubscribe(self: *Self, chan: *Channel(T)) void {
            self.mutex.lock();
            defer self.mutex.unlock();
            self.channels.release(chan.id);
            chan.deinit();
        }

        pub fn send(self: *Self, message: T) !void {
            self.mutex.lock();
            defer self.mutex.unlock();

            // If we have no channels, why are we sending?
            if (self.channels.empty()) return error.BroadcastNoReceiver;

            // Try to send to each channel, mark closed ones for removal
            var iter = self.channels.iterator();
            while (iter.next_ptr()) |channel| {
                channel.send(message) catch |err| switch (err) {
                    error.ChannelClosed => self.channels.release(channel.id),
                    else => return err,
                };
            }
        }
    };
}
