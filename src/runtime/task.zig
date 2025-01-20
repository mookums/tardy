const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.@"tardy/runtime/task");

const Frame = @import("../frame/lib.zig").Frame;
const Runtime = @import("../runtime/lib.zig").Runtime;
const Result = @import("../aio/completion.zig").Result;

const Stat = @import("../aio/completion.zig").Stat;

const unwrap = @import("../utils.zig").unwrap;

const StatResult = @import("../aio/completion.zig").StatResult;

const InnerOpenResult = @import("../aio/completion.zig").InnerOpenResult;
const OpenFileResult = @import("../aio/completion.zig").OpenFileResult;
const OpenDirResult = @import("../aio/completion.zig").OpenDirResult;

const MkdirResult = @import("../aio/completion.zig").MkdirResult;
const DeleteResult = @import("../aio/completion.zig").DeleteResult;
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
            assert(t.runner == .callback);
            const task_callback = t.runner.callback;
            const context: C = unwrap(C, task_callback.context);

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
                        if (comptime R != OpenFileResult and R != OpenDirResult) unreachable;
                        switch (inner) {
                            .actual => |actual| switch (actual) {
                                .file => |f| if (comptime R == OpenFileResult) {
                                    break :result .{ .actual = f };
                                } else unreachable,
                                .dir => |d| if (comptime R == OpenDirResult) {
                                    break :result .{ .actual = d };
                                } else unreachable,
                            },
                            .err => |e| break :result .{ .err = e },
                        }
                    },
                    .mkdir => |inner| {
                        if (comptime R != MkdirResult) unreachable;
                        break :result inner;
                    },
                    .delete => |inner| {
                        if (comptime R != DeleteResult) unreachable;
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

            try @call(.auto, task_fn, .{ rt, result, context });
        }
    }.wrapper;
}

const TaskRunner = union(enum) {
    callback: struct {
        func: InnerTaskFn,
        context: usize,
    },
    frame: *Frame,
};

pub const Task = struct {
    pub const State = union(enum) {
        channel: struct {
            check: *const fn (*anyopaque) bool,
            gen: *const fn (*anyopaque) ?*anyopaque,
            ctx: *anyopaque,
        },
        /// Waiting for an Async I/O Event.
        wait_for_io,
        /// Immediately Runnable.
        runnable,
        /// Dead.
        dead,
    };
    // 1 byte
    state: State = .dead,
    // no idea on bytes.
    result: Result = .none,
    // 8 bytes
    index: usize,
    // 8 bytes
    runner: TaskRunner,
};
