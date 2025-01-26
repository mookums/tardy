const std = @import("std");
const log = std.log.scoped(.@"tardy/channels/spsc");

const Frame = @import("../frame/lib.zig").Frame;
const Ring = @import("../core/ring.zig").Ring;
const Runtime = @import("../lib.zig").Runtime;

const State = enum(u8) {
    starting,
    running,
    closed,
};

// A single-producer single-consumer Channel.
pub fn Spsc(comptime T: type) type {
    return struct {
        const Self = @This();

        fn trigger_consumer(self: *Self) !void {
            if (self.consumer_index) |index| try self.consumer_rt.?.trigger(index);
        }

        fn trigger_producer(self: *Self) !void {
            if (self.producer_index) |index| try self.producer_rt.?.trigger(index);
        }

        pub const Producer = struct {
            inner: *Self,

            pub fn send(self: Producer, message: T) !void {
                std.log.debug("producer sending...", .{});
                while (true) switch (self.inner.state.load(.acquire)) {
                    // Both ends must be open.
                    .starting => return error.NotReady,
                    // Channel was cleaned up.
                    .closed => return error.ChannelClosed,
                    .running => {
                        self.inner.mutex.lock();
                        self.inner.producer_index = null;

                        self.inner.ring.push(message) catch |e| switch (e) {
                            error.RingFull => {
                                self.inner.producer_index = self.inner.producer_rt.?.current_task;
                                try self.inner.trigger_consumer();
                                self.inner.mutex.unlock();
                                try self.inner.producer_rt.?.scheduler.trigger_await();
                                continue;
                            },
                        };

                        try self.inner.trigger_consumer();
                        self.inner.mutex.unlock();

                        return;
                    },
                };
            }
        };

        pub const Consumer = struct {
            inner: *Self,

            pub fn recv(self: Consumer) !T {
                log.debug("consumer recving...", .{});
                while (true) switch (self.inner.state.load(.acquire)) {
                    // Both ends must be open.
                    .starting => return error.NotReady,
                    // Channel was cleaned up.
                    .closed => return error.ChannelClosed,
                    .running => {
                        self.inner.mutex.lock();
                        self.inner.consumer_index = null;

                        const data = self.inner.ring.pop() catch |e| switch (e) {
                            // If we are empty, trigger the producer to run.
                            error.RingEmpty => {
                                self.inner.consumer_index = self.inner.consumer_rt.?.current_task;
                                try self.inner.trigger_producer();
                                self.inner.mutex.unlock();
                                try self.inner.consumer_rt.?.scheduler.trigger_await();
                                continue;
                            },
                        };

                        try self.inner.trigger_producer();
                        self.inner.mutex.unlock();

                        return data;
                    },
                };
            }
        };

        mutex: std.Thread.Mutex = .{},
        ring: Ring(T),

        producer_rt: ?*Runtime = null,
        producer_index: ?usize = null,

        consumer_rt: ?*Runtime = null,
        consumer_index: ?usize = null,
        state: std.atomic.Value(State) = std.atomic.Value(State).init(.starting),

        pub fn init(allocator: std.mem.Allocator, size: usize) !Self {
            return .{
                .ring = try Ring(T).init(allocator, size),
                .state = std.atomic.Value(State).init(.starting),
            };
        }

        pub fn deinit(self: *Self) void {
            switch (self.state.load(.acquire)) {
                .closed => unreachable,
                .starting, .running => self.ring.deinit(),
            }
        }

        pub fn producer(self: *Self, runtime: *Runtime) Producer {
            self.mutex.lock();
            defer self.mutex.unlock();
            self.producer_rt = runtime;
            if (self.consumer_rt != null) self.state.store(.running, .release);
            return .{ .inner = self };
        }

        pub fn consumer(self: *Self, runtime: *Runtime) Consumer {
            self.mutex.lock();
            defer self.mutex.unlock();
            self.consumer_rt = runtime;
            if (self.producer_rt != null) self.state.store(.running, .release);
            return .{ .inner = self };
        }
    };
}
