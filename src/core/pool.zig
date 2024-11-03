const std = @import("std");
const assert = std.debug.assert;

fn Borrow(comptime T: type) type {
    return struct {
        index: usize,
        item: *T,
    };
}

pub fn Pool(comptime T: type) type {
    return struct {
        const Iterator = struct {
            items: []T,
            inner: std.DynamicBitSetUnmanaged.Iterator(.{}),

            pub fn next(self: *Iterator) ?T {
                const index = self.inner.next() orelse return null;
                return self.items[index];
            }

            pub fn next_ptr(self: *Iterator) ?*T {
                const index = self.inner.next() orelse return null;
                return &self.items[index];
            }
        };

        const Self = @This();
        allocator: std.mem.Allocator,
        // Buffer for the Pool.
        items: []T,
        dirty: std.DynamicBitSetUnmanaged,

        /// Initalizes our items buffer as undefined.
        pub fn init(
            allocator: std.mem.Allocator,
            size: usize,
            args: anytype,
            init_hook: ?*const fn (buffer: []T, args: @TypeOf(args)) void,
        ) !Self {
            const items: []T = try allocator.alloc(T, size);
            const self = Self{
                .allocator = allocator,
                .items = items,
                .dirty = try std.DynamicBitSetUnmanaged.initEmpty(allocator, size),
            };

            if (init_hook) |hook| {
                @call(.auto, hook, .{ self.items, args });
            }

            return self;
        }

        /// Deinitalizes our items buffer with a passed in hook.
        pub fn deinit(
            self: *Self,
            args: anytype,
            deinit_hook: ?*const fn (buffer: []T, args: @TypeOf(args)) void,
        ) void {
            if (deinit_hook) |hook| {
                @call(.auto, hook, .{ self.items, args });
            }

            self.allocator.free(self.items);
            self.dirty.deinit(self.allocator);
        }

        fn get(self: *const Self, index: usize) T {
            return self.items[index];
        }

        fn get_ptr(self: *Self, index: usize) *T {
            return &self.items[index];
        }

        /// Is this empty?
        pub fn empty(self: *const Self) bool {
            return self.dirty.count() == 0;
        }

        /// Is this full?
        pub fn full(self: *const Self) bool {
            return self.dirty.count() == self.items.len;
        }

        /// Returns the number of clean (or available) slots.
        pub fn clean(self: *const Self) usize {
            return self.items.len - self.dirty.count();
        }

        /// Linearly probes for an available slot in the pool.
        ///
        /// Returns a tuple of the index into the pool and a pointer to the item.
        pub fn borrow(self: *Self) !Borrow(T) {
            var iter = self.dirty.iterator(.{ .kind = .unset });
            const index = iter.next() orelse return error.Full;
            self.dirty.set(index);
            return .{ .index = index, .item = self.get_ptr(index) };
        }

        /// Linearly probes for an available slot in the pool.
        /// Uses a provided hint value as the starting index.
        ///
        /// Returns a tuple of the index into the pool and a pointer to the item.
        pub fn borrow_hint(self: *Self, hint: usize) !Borrow(T) {
            for (0..self.items.len) |i| {
                const index = @mod(hint + i, self.items.len);
                if (!self.dirty.isSet(index)) {
                    self.dirty.set(index);
                    return .{ .index = index, .item = self.get_ptr(index) };
                }
            }

            return error.Full;
        }

        /// Attempts to borrow at the given index.
        /// Asserts that it is an available slot.
        pub fn borrow_assume_unset(self: *Self, index: usize) Borrow(T) {
            assert(!self.dirty.isSet(index));

            self.dirty.set(index);
            return .{ .index = index, .item = self.get_ptr(index) };
        }

        /// Releases the item with the given index back to the Pool.
        /// Asserts that the given index was borrowed.
        pub fn release(self: *Self, index: usize) void {
            assert(self.dirty.isSet(index));
            self.dirty.unset(index);
        }

        /// Returns an iterator over the taken values in the Pool.
        pub fn iterator(self: *Self) Iterator {
            const iter = self.dirty.iterator(.{});
            const items = self.items;
            return .{ .inner = iter, .items = items };
        }
    };
}

const testing = std.testing;

test "Pool Initalization (integer)" {
    var byte_pool = try Pool(u8).init(testing.allocator, 1024, struct {
        fn init_hook(buffer: []u8, _: anytype) void {
            for (buffer) |*item| {
                item.* = 2;
            }
        }
    }.init_hook, .{});
    defer byte_pool.deinit(null, null);

    for (byte_pool.items) |item| {
        try testing.expectEqual(item, 2);
    }
}

test "Pool Initalization & Deinit (ArrayList)" {
    var list_pool = try Pool(std.ArrayList(u8)).init(testing.allocator, 256, struct {
        fn init_hook(buffer: []std.ArrayList(u8), allocator: anytype) void {
            for (buffer) |*item| {
                item.* = std.ArrayList(u8).init(allocator);
            }
        }
    }.init_hook, testing.allocator);

    defer list_pool.deinit(struct {
        fn deinit_hook(buffer: []std.ArrayList(u8), _: anytype) void {
            for (buffer) |*item| {
                item.deinit();
            }
        }
    }.deinit_hook, null);

    for (list_pool.items, 0..) |*item, i| {
        try item.appendNTimes(0, i);
    }

    for (list_pool.items, 0..) |item, i| {
        try testing.expectEqual(item.items.len, i);
    }
}

test "Pool BufferPool ([][]u8)" {
    var buffer_pool = try Pool([1024]u8).init(testing.allocator, 1024, null, .{});
    defer buffer_pool.deinit(null, null);

    for (buffer_pool.items) |*item| {
        std.mem.copyForwards(u8, item, "ABCDEF");
    }

    for (buffer_pool.items) |item| {
        try testing.expectEqualStrings("ABCDEF", item[0..6]);
    }
}

test "Pool Borrowing" {
    var byte_pool = try Pool(u8).init(testing.allocator, 1024, struct {
        fn init_hook(buffer: []u8, _: anytype) void {
            for (buffer) |*item| {
                item.* = 3;
            }
        }
    }.init_hook, .{});
    defer byte_pool.deinit(null, null);

    for (0..1024) |_| {
        const x = try byte_pool.borrow();
        try testing.expectEqual(3, x.item.*);
    }
}

test "Pool Borrowing Hint" {
    var byte_pool = try Pool(u8).init(testing.allocator, 1024, struct {
        fn init_hook(buffer: []u8, _: anytype) void {
            for (buffer) |*item| {
                item.* = 3;
            }
        }
    }.init_hook, .{});
    defer byte_pool.deinit(null, null);

    for (0..1024) |i| {
        const x = try byte_pool.borrow_hint(i);
        try testing.expectEqual(3, x.item.*);
    }
}

test "Pool Borrowing Unset" {
    var byte_pool = try Pool(u8).init(testing.allocator, 1024, struct {
        fn init_hook(buffer: []u8, _: anytype) void {
            for (buffer) |*item| {
                item.* = 3;
            }
        }
    }.init_hook, .{});
    defer byte_pool.deinit(null, null);

    for (0..1024) |i| {
        const x = byte_pool.borrow_assume_unset(i);
        try testing.expectEqual(3, x.item.*);
    }
}

test "Pool Iterator" {
    var int_pool = try Pool(usize).init(testing.allocator, 1024, null, null);
    defer int_pool.deinit(null, null);

    for (0..(1024 / 2)) |_| {
        const borrowed = try int_pool.borrow();
        borrowed.item.* = borrowed.index;
    }

    var iter = int_pool.iterator();
    while (iter.next()) |item| {
        try testing.expect(int_pool.dirty.isSet(item));
        int_pool.release(item);
    }

    try testing.expect(int_pool.empty());
}
