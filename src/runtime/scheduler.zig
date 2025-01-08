const std = @import("std");
const assert = std.debug.assert;

const Task = @import("task.zig").Task;
const TaskFn = @import("task.zig").TaskFn;
const TaskFnWrapper = @import("task.zig").TaskFnWrapper;
const InnerTaskFn = @import("task.zig").InnerTaskFn;

const Pool = @import("../core/pool.zig").Pool;

const wrap = @import("../utils.zig").wrap;

pub const Scheduler = struct {
    tasks: Pool(Task),
    runnable: std.DynamicBitSetUnmanaged,
    released: std.ArrayList(usize),

    pub fn init(allocator: std.mem.Allocator, size: usize) !Scheduler {
        return .{
            .tasks = try Pool(Task).init(allocator, size, .fixed),
            .runnable = try std.DynamicBitSetUnmanaged.initEmpty(allocator, size),
            .released = try std.ArrayList(usize).initCapacity(allocator, @divFloor(size, 2)),
        };
    }

    pub fn deinit(self: *Scheduler, allocator: std.mem.Allocator) void {
        self.tasks.deinit();
        self.runnable.deinit(allocator);
        self.released.deinit();
    }

    pub fn set_runnable(self: *Scheduler, index: usize) void {
        assert(!self.runnable.isSet(index));
        const task: *Task = &self.tasks.items[index];
        task.state = .runnable;
        self.runnable.set(index);
    }

    /// Spawns a Task by adding it into the scheduler pool.
    pub fn spawn(
        self: *Scheduler,
        comptime R: type,
        task_ctx: anytype,
        comptime task_fn: TaskFn(R, @TypeOf(task_ctx)),
        task_state: Task.State,
    ) !usize {
        const borrowed = blk: {
            if (self.released.popOrNull()) |index| {
                break :blk self.tasks.borrow_assume_unset(index);
            } else {
                break :blk try self.tasks.borrow();
            }
        };

        const context: usize = wrap(usize, task_ctx);

        borrowed.item.* = .{
            .index = borrowed.index,
            .func = TaskFnWrapper(R, @TypeOf(task_ctx), task_fn),
            .context = context,
            .state = task_state,
        };

        if (task_state == .runnable) self.set_runnable(borrowed.index);
        return borrowed.index;
    }

    pub fn release(self: *Scheduler, index: usize) !void {
        assert(self.runnable.isSet(index));
        self.runnable.unset(index);
        self.tasks.release(index);
        try self.released.append(index);
    }
};
