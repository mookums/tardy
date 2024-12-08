const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.@"tardy/runtime/task");

const Runtime = @import("../runtime/lib.zig").Runtime;
const Result = @import("../aio/completion.zig").Result;

const Stat = @import("../aio/completion.zig").Stat;

const unwrap = @import("../utils.zig").unwrap;

// This is what is internally passed around.
pub const InnerTaskFn = *const fn (*Runtime, *const Task) anyerror!void;

pub fn TaskFn(comptime R: type, comptime C: type) type {
    return *const fn (*Runtime, R, C) anyerror!void;
}

pub fn TaskFnWrapper(comptime R: type, comptime C: type, comptime task_fn: TaskFn(R, C)) InnerTaskFn {
    return struct {
        fn wrapper(rt: *Runtime, t: *const Task) anyerror!void {
            const context: C = unwrap(C, t.context);

            const result: R = result: {
                switch (t.result) {
                    .wake => unreachable,
                    .none => {
                        if (comptime R != void) unreachable;
                        break :result {};
                    },
                    .stat => |inner| {
                        if (comptime R != Stat) unreachable;
                        break :result inner;
                    },
                    .fd => |inner| {
                        if (comptime R != std.posix.fd_t) unreachable;
                        break :result inner;
                    },
                    .socket => |inner| {
                        if (comptime R != std.posix.socket_t) unreachable;
                        break :result inner;
                    },
                    .value => |inner| {
                        if (comptime R != i32) unreachable;
                        break :result inner;
                    },
                    .ptr => |inner| {
                        if (comptime @typeInfo(R) != .Optional) unreachable;
                        if (comptime @typeInfo(@typeInfo(R).Optional.child) != .Pointer) unreachable;

                        if (inner == null) break :result null;
                        break :result @ptrCast(@alignCast(inner.?));
                    },
                }
            };

            try @call(.always_inline, task_fn, .{ rt, result, context });
        }
    }.wrapper;
}

pub const Task = struct {
    pub const State = union(enum) {
        channel: struct {
            check: *const fn (*anyopaque) bool,
            gen: *const fn (*anyopaque) ?*anyopaque,
            ctx: *anyopaque,
        },
        waiting,
        runnable,
        dead,
    };
    // 1 byte
    state: State = .dead,
    // no idea on bytes.
    result: Result = .none,
    // 8 bytes
    index: usize,
    // 8 bytes
    func: InnerTaskFn,
    context: usize,
};
