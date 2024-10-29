const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.@"tardy/runtime/task");

const Runtime = @import("../runtime/lib.zig").Runtime;
const Result = @import("../aio/completion.zig").Result;

// This is what is internally passed around.
pub const InnerTaskFn = *const fn (*Runtime, *const Task, usize) anyerror!void;

pub fn TaskFn(comptime Context: type) type {
    return *const fn (*Runtime, *const Task, Context) anyerror!void;
}

pub fn TaskFnWrapper(comptime Context: type, comptime task_fn: TaskFn(Context)) InnerTaskFn {
    return struct {
        fn wrapper(rt: *Runtime, t: *const Task, ctx: usize) anyerror!void {
            const context: Context = context: {
                switch (comptime @typeInfo(Context)) {
                    .Pointer => break :context @ptrFromInt(ctx),
                    .Void => break :context {},
                    .Int => |int_info| {
                        const uint = @Type(std.builtin.Type{
                            .Int = .{
                                .signedness = .unsigned,
                                .bits = int_info.bits,
                            },
                        });

                        break :context @bitCast(@as(uint, @truncate(ctx)));
                    },
                    .Struct => |struct_info| {
                        const uint = @Type(std.builtin.Type{
                            .Int = .{
                                .signedness = .unsigned,
                                .bits = @bitSizeOf(struct_info.backing_integer.?),
                            },
                        });

                        break :context @bitCast(@as(uint, @truncate(ctx)));
                    },
                    else => unreachable,
                }
            };
            try @call(.auto, task_fn, .{ rt, t, context });
        }
    }.wrapper;
}

pub const Task = struct {
    pub const State = enum(u8) {
        waiting,
        runnable,
        dead,
    };
    // 1 byte
    state: State = .dead,
    // no idea on bytes.
    result: ?Result = null,
    // 8 bytes
    index: usize,
    // 8 bytes
    func: InnerTaskFn,
    context: usize,
};
