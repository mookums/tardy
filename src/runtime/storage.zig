const std = @import("std");

/// Storage is deleteless and clobberless.
pub const Storage = struct {
    arena: std.heap.ArenaAllocator,
    map: std.StringHashMapUnmanaged(*anyopaque),

    pub fn init(allocator: std.mem.Allocator) Storage {
        return .{
            .arena = std.heap.ArenaAllocator.init(allocator),
            .map = std.StringHashMapUnmanaged(*anyopaque){},
        };
    }

    pub fn deinit(self: Storage) void {
        self.arena.deinit();
    }

    /// Store a new item in the Storage.
    /// This will CLONE the item that you pass in and manage the clone.
    /// This asserts that no other item has the same name.
    pub fn store(self: *Storage, name: []const u8, item: anytype) !void {
        const allocator = self.arena.allocator();
        const clone = try allocator.create(@TypeOf(item));
        errdefer allocator.destroy(clone);
        clone.* = item;
        try self.map.putNoClobber(allocator, name, @ptrCast(clone));
    }

    /// Get an item that is within the Storage.
    /// This asserts that the item you are looking for exists.
    pub fn get(self: *Storage, name: []const u8, comptime T: type) T {
        const got = self.map.get(name) orelse unreachable;
        const value: *T = @ptrCast(@alignCast(got));
        return value.*;
    }

    /// Get a pointer to an item that is within the Storage.
    /// This asserts that the item you are looking for exists.
    pub fn get_ptr(self: *Storage, name: []const u8, comptime T: type) *T {
        const got = self.map.get(name) orelse unreachable;
        return @ptrCast(@alignCast(got));
    }
};

const testing = std.testing;

test "Storage Storing" {
    var storage = Storage.init(testing.allocator);
    defer storage.deinit();

    const byte: u8 = 100;
    try storage.store("byte", byte);

    const index: usize = 9447721;
    try storage.store("index", index);

    const value: u32 = 100;
    try storage.store("value", value);

    const value_ptr = storage.get_ptr("value", u32);
    try testing.expectEqual(value_ptr.*, 100);
    value_ptr.* += 100;

    try testing.expectEqual(byte, storage.get("byte", u8));
    try testing.expectEqual(index, storage.get("index", usize));
    try testing.expectEqual(value + 100, storage.get("value", u32));
}
