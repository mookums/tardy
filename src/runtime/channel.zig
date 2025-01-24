const std = @import("std");

const Ring = @import("../core/ring.zig").Ring;
const TaskFn = @import("task.zig").TaskFn;
const Runtime = @import("lib.zig").Runtime;

/// A channel is a SPSC abstraction that
/// allows you to send data across runtimes.
///
/// This is thread-safe.
pub fn Channel(comptime T: type) type {
    return struct {
        const Self = @This();
        /// Used internally for tracking.
        id: usize = 0,
        mutex: std.Thread.Mutex = .{},
        ring: Ring(T),
        runtime: *Runtime,
        closed: bool = false,

        pub fn init(allocator: std.mem.Allocator, runtime: *Runtime, size: usize) !Self {
            return .{
                .ring = try Ring(T).init(allocator, size),
                .runtime = runtime,
            };
        }

        pub fn deinit(self: *Self) void {
            self.mutex.lock();
            defer self.mutex.unlock();

            if (!self.closed) {
                self.ring.deinit();
                self.closed = true;

                // Awaken the runtime to ensure
                // that the channel closes without
                // waiting on I/O.
                if (self.runtime.asleep()) {
                    self.runtime.wake() catch unreachable;
                }
            }
        }

        pub fn send(self: *Self, message: T) !void {
            self.mutex.lock();
            defer self.mutex.unlock();

            if (self.closed) return error.ChannelClosed;

            try self.ring.send(message);
            if (self.runtime.asleep()) {
                try self.runtime.wake();
            }
        }

        fn channel_gen(ctx: *anyopaque) ?*anyopaque {
            const channel: *Self = @ptrCast(@alignCast(ctx));
            channel.mutex.lock();
            defer channel.mutex.unlock();

            if (channel.closed) return null;
            return channel.ring.recv_ptr() catch unreachable;
        }

        fn channel_check(ctx: *anyopaque) bool {
            const channel: *Self = @ptrCast(@alignCast(ctx));
            channel.mutex.lock();
            defer channel.mutex.unlock();

            return channel.closed or !channel.ring.empty();
        }

        /// This will queue up a Task that will run whenever there is data available on the Channel.
        pub fn recv(
            self: *Self,
            then_ctx: anytype,
            comptime then: TaskFn(?*const T, @TypeOf(then_ctx)),
        ) !void {
            self.mutex.lock();
            defer self.mutex.unlock();

            _ = try self.runtime.scheduler.spawn(
                ?*const T,
                then_ctx,
                then,
                .{
                    .channel = .{
                        .check = channel_check,
                        .gen = channel_gen,
                        .ctx = self,
                    },
                },
                null,
            );
        }
    };
}
