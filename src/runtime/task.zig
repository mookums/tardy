const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.@"tardy/runtime/task");

const Runtime = @import("../runtime/lib.zig").Runtime;
const Result = @import("../aio/completion.zig").Result;

pub const Task = struct {
    pub const TaskFn = *const fn (*Runtime, *const Task, ?*anyopaque) anyerror!void;
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
    func: TaskFn,
    // 8 bytes
    context: ?*anyopaque,
};
