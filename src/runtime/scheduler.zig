const std = @import("std");
const assert = std.debug.assert;

const Task = @import("./task.zig").Task;
const Pool = @import("../core/pool.zig").Pool;

pub const Scheduler = struct {
    tasks: Pool(Task),
    runnable: std.DynamicBitSetUnmanaged,

    pub fn init(allocator: std.mem.Allocator, size: usize) !Scheduler {
        return .{
            .tasks = try Pool(Task).init(allocator, size, null, null),
            .runnable = try std.DynamicBitSetUnmanaged.initEmpty(allocator, size),
        };
    }

    pub fn deinit(self: *Scheduler, allocator: std.mem.Allocator) void {
        self.tasks.deinit(null, null);
        self.runnable.deinit(allocator);
    }

    pub fn set_runnable(self: *Scheduler, index: usize) void {
        assert(!self.runnable.isSet(index));
        const task: *Task = &self.tasks.items[index];
        task.state = .runnable;
        self.runnable.set(index);
    }

    /// Spawns a Task by adding it into the scheduler pool.
    /// This means that if the predicate is true that it might run.
    pub fn spawn(
        self: *Scheduler,
        task_fn: Task.TaskFn,
        task_ctx: ?*anyopaque,
        task_predicate: ?Task.PredicateFn,
        task_state: Task.State,
    ) !usize {
        const borrowed = try self.tasks.borrow();
        borrowed.item.* = .{
            .index = borrowed.index,
            .func = task_fn,
            .context = task_ctx,
            .predicate = task_predicate,
            .state = task_state,
        };

        if (task_state == .runnable) self.set_runnable(borrowed.index);

        return borrowed.index;
    }

    pub fn release(self: *Scheduler, index: usize) void {
        assert(self.runnable.isSet(index));
        self.runnable.unset(index);
        self.tasks.release(index);
    }
};
