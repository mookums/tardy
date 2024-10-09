const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.@"tardy/scheduler/task");

fn true_predicate(_: *Task) bool {
    return true;
}

const PredicateFn = *const fn (*Task) bool;
const TaskFn = *const fn (*anyopaque) void;

pub const Task = struct {
    // 8 bytes
    index: usize,
    // 8 bytes
    predicate: PredicateFn = true_predicate,
    // 8 bytes
    func: TaskFn,
    // 8 bytes
    context: *anyopaque,
};
