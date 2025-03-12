const std = @import("std");

const Runtime = @import("../runtime/lib.zig").Runtime;

const Pool = @import("../lib.zig").Pool;

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
        topic: *Topic,
        index: usize,
        rt: ?*Runtime,
        task: ?usize,
        queue: std.ArrayListUnmanaged(usize),
        lock: std.Thread.Mutex = .{},

        pub fn init(allocator: std.mem.Allocator, topic: *Topic, index: usize) !Subscription {
            const queue = try std.ArrayListUnmanaged(usize).initCapacity(allocator, 0);
            return .{ .topic = topic, .index = index, .rt = null, .task = null, .queue = queue };
        }

        pub fn unsubscribe(self: *Subscription) void {
            // TODO: clean up the topic if it has no subscribers, it shouldn't leak.
            self.topic.unsubscribe(self.index);
        }

        pub fn send(self: *Subscription, allocator: std.mem.Allocator, comptime T: type, item: T) !void {
            self.lock.lock();
            defer self.lock.unlock();

            try self.queue.append(allocator, wrap(usize, item));
            if (self.rt != null and self.task != null) try self.rt.?.trigger(self.task.?);
        }

        pub fn recv(self: *const Subscription, rt: *Runtime, comptime T: type) !T {
            var item_ptr = self.topic.subscribers.get_ptr(self.index);

            item_ptr.lock.lock();
            defer item_ptr.lock.unlock();

            while (item_ptr.queue.items.len == 0) {
                item_ptr.rt = rt;
                item_ptr.task = rt.current_task.?;
                item_ptr.lock.unlock();

                try item_ptr.rt.?.scheduler.trigger_await();
                // get a new pointer as  the old one might be invalid.
                item_ptr = self.topic.subscribers.get_ptr(self.index);
                item_ptr.lock.lock();
            }

            item_ptr.rt = null;
            item_ptr.task = null;
            return unwrap(T, item_ptr.queue.orderedRemove(0));
        }
    };

    pub const Topic = struct {
        subscribers: Pool(Subscription),
        lock: std.Thread.Mutex = .{},

        pub fn init(allocator: std.mem.Allocator) !Topic {
            const subscribers = try Pool(Subscription).init(allocator, 1, .grow);
            return .{ .subscribers = subscribers };
        }

        pub fn deinit(self: Topic) void {
            self.subscribers.deinit();
        }

        pub fn push(self: *Topic, allocator: std.mem.Allocator, comptime T: type, item: T) !void {
            self.lock.lock();
            defer self.lock.unlock();

            var iter = self.subscribers.iterator();
            while (iter.next_ptr()) |s| try s.send(allocator, T, item);
        }

        pub fn subscribe(self: *Topic, allocator: std.mem.Allocator) !Subscription {
            self.lock.lock();
            defer self.lock.unlock();

            const index = try self.subscribers.borrow();
            const item_ptr = self.subscribers.get_ptr(index);
            item_ptr.* = try Subscription.init(allocator, self, index);
            return item_ptr.*;
        }

        pub fn unsubscribe(self: *Topic, index: usize) void {
            self.subscribers.release(index);
        }
    };

    pub fn subscribe(self: *EventBus, topic: anytype) !Subscription {
        const topic_name: []const u8 = switch (@typeInfo(@TypeOf(topic))) {
            .@"enum", .enum_literal => @tagName(topic),
            else => topic,
        };

        self.lock.lock();
        defer self.lock.unlock();

        var entry = try self.topics.getOrPut(self.allocator, topic_name);
        if (!entry.found_existing) entry.value_ptr.* = try Topic.init(self.allocator);
        return try entry.value_ptr.subscribe(self.allocator);
    }

    pub fn push(self: *EventBus, topic: anytype, comptime T: type, item: T) !void {
        const topic_name: []const u8 = switch (@typeInfo(@TypeOf(topic))) {
            .@"enum", .enum_literal => @tagName(topic),
            else => topic,
        };

        self.lock.lock();
        defer self.lock.unlock();

        const entry = try self.topics.getOrPut(self.allocator, topic_name);
        if (!entry.found_existing) entry.value_ptr.* = try Topic.init(self.allocator);
        try entry.value_ptr.push(self.allocator, T, item);
    }
};
