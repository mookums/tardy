const std = @import("std");

const Runtime = @import("../runtime/lib.zig").Runtime;

const wrap = @import("../wrapping.zig").wrap;
const unwrap = @import("../wrapping.zig").unwrap;

pub const EventBus = struct {
    allocator: std.mem.Allocator,
    topics: std.StringHashMapUnmanaged(Topic),
    lock: std.Thread.Mutex = .{},

    pub fn init(allocator: std.mem.Allocator) !EventBus {
        const topics = std.StringHashMapUnmanaged(Topic){};
        return .{ .allocator = allocator, .topics = topics };
    }

    pub const Subscription = struct {
        rt: ?*Runtime,
        task: ?usize,
        queue: std.ArrayListUnmanaged(usize),

        pub fn init(allocator: std.mem.Allocator) !Subscription {
            const queue = try std.ArrayListUnmanaged(usize).initCapacity(allocator, 0);
            return .{ .rt = null, .task = null, .queue = queue };
        }

        pub fn send(self: *Subscription, allocator: std.mem.Allocator, comptime T: type, item: T) !void {
            try self.queue.append(allocator, wrap(usize, item));
            if (self.rt != null and self.task != null) try self.rt.?.trigger(self.task.?);
        }

        pub fn recv(self: *Subscription, rt: *Runtime, comptime T: type) !T {
            while (self.queue.items.len == 0) {
                self.rt = rt;
                self.task = rt.current_task.?;
                try self.rt.?.scheduler.trigger_await();
            }

            self.rt = null;
            self.task = null;
            return unwrap(T, self.queue.orderedRemove(0));
        }
    };

    pub const Topic = struct {
        subscribers: std.ArrayListUnmanaged(*Subscription),
        lock: std.Thread.Mutex = .{},

        pub fn init(allocator: std.mem.Allocator) !Topic {
            const subscribers = try std.ArrayListUnmanaged(*Subscription).initCapacity(allocator, 0);
            return .{ .subscribers = subscribers };
        }

        pub fn push(self: *Topic, allocator: std.mem.Allocator, comptime T: type, item: T) !void {
            self.lock.lock();
            defer self.lock.unlock();

            for (self.subscribers.items) |s| try s.send(allocator, T, item);
        }

        pub fn subscribe(self: *Topic, allocator: std.mem.Allocator) !*Subscription {
            self.lock.lock();
            defer self.lock.unlock();

            const subscription = try allocator.create(Subscription);
            subscription.* = try Subscription.init(allocator);
            try self.subscribers.append(allocator, subscription);
            return subscription;
        }
    };

    pub fn subscribe(self: *EventBus, topic_enum: anytype) !*Subscription {
        const info = @typeInfo(@TypeOf(topic_enum));
        if (info != .Enum and info != .EnumLiteral) @compileError("topic must be an enum type!");

        self.lock.lock();
        defer self.lock.unlock();

        var entry = try self.topics.getOrPut(self.allocator, @tagName(topic_enum));
        if (!entry.found_existing) entry.value_ptr.* = try Topic.init(self.allocator);
        return try entry.value_ptr.subscribe(self.allocator);
    }

    pub fn push(self: *EventBus, topic_enum: anytype, comptime T: type, item: T) !void {
        const info = @typeInfo(@TypeOf(topic_enum));
        if (info != .Enum and info != .EnumLiteral) @compileError("topic must be an enum type!");

        self.lock.lock();
        defer self.lock.unlock();

        const entry = try self.topics.getOrPut(self.allocator, @tagName(topic_enum));
        if (!entry.found_existing) entry.value_ptr.* = try Topic.init(self.allocator);
        try entry.value_ptr.push(self.allocator, T, item);
    }
};
