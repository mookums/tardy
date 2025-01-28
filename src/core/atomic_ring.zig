const std = @import("std");
const assert = std.debug.assert;

const Atomic = std.atomic.Value;

pub fn AtomicRing(comptime T: type) type {
    return struct {
        const Self = @This();

        const Status = enum(u8) {
            empty,
            reading,
            writing,
            taken,
        };

        const Slot = struct {
            status: Atomic(Status),
            item: T,
        };

        allocator: std.mem.Allocator,
        items: []Slot,

        read_index: Atomic(usize),
        write_index: Atomic(usize),
        count: Atomic(usize),

        pub fn init(allocator: std.mem.Allocator, size: usize) !Self {
            assert(size >= 1);
            assert(std.math.isPowerOfTwo(size));

            const items = try allocator.alloc(Slot, size);
            for (items) |*item| item.status = .{ .raw = .empty };

            return .{
                .allocator = allocator,
                .items = items,
                .read_index = .{ .raw = 0 },
                .write_index = .{ .raw = 0 },
                .count = .{ .raw = 0 },
            };
        }

        pub fn deinit(self: Self) void {
            self.allocator.free(self.items);
        }

        pub fn push(self: *Self, item: T) !void {
            while (true) {
                const curr_count = self.count.load(.acquire);
                assert(curr_count <= self.items.len);
                if (curr_count == self.items.len) return error.RingFull;

                if (self.count.cmpxchgWeak(curr_count, curr_count + 1, .acq_rel, .acquire)) |_| {
                    continue;
                }

                const write_pos = self.write_index.fetchAdd(1, .acquire) % self.items.len;
                const slot = &self.items[write_pos];
                const status = slot.status.cmpxchgWeak(.empty, .writing, .acq_rel, .acquire);

                if (status) |_| {
                    _ = self.count.fetchSub(1, .release);
                    continue;
                } else {
                    slot.item = item;
                    slot.status.store(.taken, .release);
                    return;
                }
            }
        }

        pub fn pop(self: *Self) !T {
            while (true) {
                const curr_count = self.count.load(.acquire);
                if (curr_count == 0) return error.RingEmpty;

                if (self.count.cmpxchgWeak(curr_count, curr_count - 1, .acq_rel, .acquire)) |_| {
                    continue;
                }

                const read_pos = self.read_index.fetchAdd(1, .acquire) % self.items.len;
                const slot = &self.items[read_pos];
                const status = slot.status.cmpxchgWeak(.taken, .reading, .acq_rel, .acquire);

                if (status) |_| {
                    _ = self.count.fetchAdd(1, .release);
                    continue;
                } else {
                    const item = slot.item;
                    slot.status.store(.empty, .release);
                    return item;
                }
            }
        }
    };
}

const testing = std.testing;

test "AtomicRing: Fill and Empty" {
    const size: u32 = 128;
    var ring = try AtomicRing(usize).init(testing.allocator, size);
    defer ring.deinit();

    for (0..size) |i| try ring.push(i);
    try testing.expectError(error.RingFull, ring.push(1));
    for (0..size) |i| try testing.expectEqual(i, try ring.pop());
    try testing.expectError(error.RingEmpty, ring.pop());
}
