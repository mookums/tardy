const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.@"tardy/scheduler/task");

const Runtime = @import("../runtime/lib.zig").Runtime;
const Result = @import("../aio/completion.zig").Result;

pub const Task = struct {
    pub const PredicateFn = *const fn (*Runtime, *Task) bool;
    pub const TaskFn = *const fn (*Runtime, *Task, ?*anyopaque) void;
    pub const State = union(enum) {
        waiting,
        runnable,
        dead,
    };
    predicate: ?PredicateFn = null,
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
