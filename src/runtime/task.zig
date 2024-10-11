const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.@"tardy/scheduler/task");

const Result = @import("../aio/completion.zig").Result;

pub fn Task(comptime Runtime: type) type {
    return struct {
        const Self = @This();
        pub const PredicateFn = *const fn (*Runtime, *Self) bool;
        pub const TaskFn = *const fn (*Runtime, *Self, ?*anyopaque) void;
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
}
