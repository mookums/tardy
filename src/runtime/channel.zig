const std = @import("std");

const Ring = @import("../core/ring.zig").Ring;
const TaskFn = @import("task.zig").TaskFn;
const Runtime = @import("lib.zig").Runtime;

pub fn Channel(comptime T: type) type {
    return struct {
        const Self = @This();

        runtime: *Runtime,
        ring: Ring(T),

        pub fn init(allocator: std.mem.Allocator, runtime: *Runtime, size: usize) !Self {
            return .{
                .runtime = runtime,
                .ring = try Ring(T).init(allocator, size),
            };
        }

        pub fn deinit(self: Self) void {
            self.ring.deinit();
        }

        pub fn send(self: *Self, message: T) !void {
            try self.ring.send(message);
        }

        fn channel_gen(ctx: *anyopaque) *anyopaque {
            const channel: *Self = @ptrCast(@alignCast(ctx));
            return channel.ring.recv_ptr() catch unreachable;
        }

        fn channel_func(ctx: *anyopaque) bool {
            const channel: *Self = @ptrCast(@alignCast(ctx));
            return !channel.ring.empty();
        }

        /// This will queue up a Task that will run whenever there is data available on the Channel.
        pub fn recv(self: *Self, then_ctx: anytype, comptime then: TaskFn(*const T, @TypeOf(then_ctx))) !void {
            _ = try self.runtime.scheduler.spawn(
                *const T,
                then_ctx,
                then,
                .{ .predicate = .{
                    .func = channel_func,
                    .gen = channel_gen,
                    .ctx = self,
                } },
            );
        }
    };
}
