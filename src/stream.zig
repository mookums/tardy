const std = @import("std");
const Runtime = @import("runtime/lib.zig").Runtime;

pub const Stream = struct {
    const VTable = struct {
        read: *const fn (*anyopaque, *Runtime, []u8) anyerror!usize,
        write: *const fn (*anyopaque, *Runtime, []const u8) anyerror!usize,
    };

    inner: *anyopaque,
    vtable: VTable,

    pub fn read(self: Stream, rt: *Runtime, buffer: []u8) !usize {
        return try self.vtable.read(self.inner, rt, buffer);
    }

    pub fn write(self: Stream, rt: *Runtime, buffer: []const u8) !usize {
        return try self.vtable.write(self.inner, rt, buffer);
    }

    pub fn copy(rt: *Runtime, from: Stream, to: Stream, buffer: []u8) !void {
        while (true) {
            const read_count = from.read(rt, buffer) catch |e| switch (e) {
                error.EndOfFile, error.Closed => break,
                else => return e,
            };

            _ = to.write(rt, buffer[0..read_count]) catch |e| switch (e) {
                error.NoSpace, error.Closed => break,
                else => return e,
            };
        }
    }
};
