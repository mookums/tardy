const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.@"tardy/runtime/task");

const Runtime = @import("../runtime/lib.zig").Runtime;
const Result = @import("../aio/completion.zig").Result;

const Stat = @import("../aio/completion.zig").Stat;

const unwrap = @import("../utils.zig").unwrap;

const StatResult = @import("../aio/completion.zig").StatResult;
const OpenResult = @import("../aio/completion.zig").OpenResult;
const ReadResult = @import("../aio/completion.zig").ReadResult;
const WriteResult = @import("../aio/completion.zig").WriteResult;
const AcceptResult = @import("../aio/completion.zig").AcceptResult;
const ConnectResult = @import("../aio/completion.zig").ConnectResult;
const RecvResult = @import("../aio/completion.zig").RecvResult;
const SendResult = @import("../aio/completion.zig").SendResult;

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
                    .none, .close => {
                        if (comptime R != void) unreachable;
                        break :result {};
                    },
                    .stat => |inner| {
                        if (comptime R != StatResult) unreachable;
                        break :result inner;
                    },
                    .accept => |inner| {
                        if (comptime R != AcceptResult) unreachable;
                        break :result inner;
                    },
                    .connect => |inner| {
                        if (comptime R != ConnectResult) unreachable;
                        break :result inner;
                    },
                    .recv => |inner| {
                        if (comptime R != RecvResult) unreachable;
                        break :result inner;
                    },
                    .send => |inner| {
                        if (comptime R != SendResult) unreachable;
                        break :result inner;
                    },
                    .open => |inner| {
                        if (comptime R != OpenResult) unreachable;
                        break :result inner;
                    },
                    .read => |inner| {
                        if (comptime R != ReadResult) unreachable;
                        break :result inner;
                    },
                    .write => |inner| {
                        if (comptime R != WriteResult) unreachable;
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
