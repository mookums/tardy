const std = @import("std");

pub fn Queue(comptime T: type) type {
    const List = std.DoublyLinkedList(T);
    const Node = List.Node;

    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        items: List,

        pub fn init(allocator: std.mem.Allocator) Self {
            return .{ .allocator = allocator, .items = List{} };
        }

        pub fn deinit(self: *Self) void {
            while (self.items.pop()) |node| self.allocator.destroy(node);
        }

        pub fn append(self: *Self, item: T) !void {
            const node = try self.allocator.create(Node);
            node.* = .{ .data = item };
            self.items.append(node);
        }

        pub fn pop(self: *Self) !?T {
            const node = self.items.popFirst() orelse return null;
            defer self.allocator.destroy(node);
            return node.data;
        }

        pub fn pop_assert(self: *Self) !T {
            const node = self.items.popFirst().?;
            defer self.allocator.destroy(node);
            return node.data;
        }
    };
}
