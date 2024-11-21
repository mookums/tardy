const std = @import("std");
const assert = std.debug.assert;

/// Wraps the given value into a specified integer type.
/// The value must fit within the size of the given I.
/// I is usually a usize.
pub fn wrap(comptime I: type, value: anytype) I {
    assert(@typeInfo(I) == .Int);
    assert(@typeInfo(I).Int.signedness == .unsigned);

    return context: {
        switch (comptime @typeInfo(@TypeOf(value))) {
            .Pointer => break :context @intFromPtr(value),
            .Void => break :context 1,
            .Int => |int_info| {
                comptime assert(int_info.bits <= @bitSizeOf(usize));
                const uint = @Type(std.builtin.Type{
                    .Int = .{
                        .signedness = .unsigned,
                        .bits = int_info.bits,
                    },
                });

                break :context @intCast(@as(uint, @bitCast(value)));
            },
            .Struct => |struct_info| {
                comptime assert(@bitSizeOf(struct_info.backing_integer.?) <= @bitSizeOf(usize));
                const uint = @Type(std.builtin.Type{
                    .Int = .{
                        .signedness = .unsigned,
                        .bits = @bitSizeOf(struct_info.backing_integer.?),
                    },
                });

                break :context @intCast(@as(uint, @bitCast(value)));
            },
            else => unreachable,
        }
    };
}

/// Unwraps a specified type from an underlying value.
/// The value must be an unsigned integer type, typically a usize.
pub fn unwrap(comptime T: type, value: anytype) T {
    const I = @TypeOf(value);
    assert(@typeInfo(I) == .Int);
    assert(@typeInfo(I).Int.signedness == .unsigned);

    return context: {
        switch (comptime @typeInfo(T)) {
            .Pointer => break :context @ptrFromInt(value),
            .Void => break :context {},
            .Int => |int_info| {
                const uint = @Type(std.builtin.Type{
                    .Int = .{
                        .signedness = .unsigned,
                        .bits = int_info.bits,
                    },
                });

                break :context @bitCast(@as(uint, @truncate(value)));
            },
            .Struct => |struct_info| {
                const uint = @Type(std.builtin.Type{
                    .Int = .{
                        .signedness = .unsigned,
                        .bits = @bitSizeOf(struct_info.backing_integer.?),
                    },
                });

                break :context @bitCast(@as(uint, @truncate(value)));
            },
            else => unreachable,
        }
    };
}
