const std = @import("std");
const assert = std.debug.assert;

const Task = @import("task.zig").Task;
const TaskFn = @import("task.zig").TaskFn;
const TaskFnWrapper = @import("task.zig").TaskFnWrapper;
const InnerTaskFn = @import("task.zig").InnerTaskFn;

const Runtime = @import("lib.zig").Runtime;
const Pool = @import("../core/pool.zig").Pool;

const wrap = @import("../utils.zig").wrap;
const unwrap = @import("../utils.zig").unwrap;

pub const AsyncSubmission = @import("../aio/lib.zig").AsyncSubmission;

const TaskWithJob = struct {
    task: Task,
    job: ?AsyncSubmission = null,
};

pub const Scheduler = struct {
    tasks: Pool(Task),
    runnable: std.DynamicBitSetUnmanaged,
    released: std.ArrayList(usize),
    // NOTE: the problem is that this spawned task never gets to run.
    // this is because the other task is taking up the task slot so this is in the
    // overflow and things cant currently run FROM the overflow.
    //
    // maybe we have two types of waiting? one on aio and one on another task?
    // then that one that is waiting on the other task is able to point at it?
    //
    // not sure but not solving rn.
    //
    // this was solved by using the task_fn directly BUT the issue still remains.
    // even with the overflow, we can get weird blocks if things are stacked up weirdly.
    //
    // this doesnt even use the overflow anymore.
    // maybe it should all be a big priority queue where aio tasks get prioritized to be ran before normal tasks?
    //
    // somehow we need to ensure that the Tasks that are running do not end up being dependent on a task
    // that is going to get put on the overflow.
    //
    // how can we solve this?
    // 1. smart programming
    // 2. dependency chaining, have a .wait_for_io and a .wait_for_task and a .wait_for_task
    //      -> if we encounter a .wait_for_task, we swap it and its linked tasks spots in the main queue and overflow queue.
    //      -> maybe we add a new spawn that automatically creates this implicit chaining?
    //
    //      this raises the question where if a task is dependent on another, why doesnt it just spawn it when it runs?
    //      still allowing for overflowing while ignoring issues?
    overflow: std.ArrayList(TaskWithJob),

    pub fn init(allocator: std.mem.Allocator, size: usize) !Scheduler {
        return .{
            .tasks = try Pool(Task).init(allocator, size),
            .runnable = try std.DynamicBitSetUnmanaged.initEmpty(allocator, size),
            .released = try std.ArrayList(usize).initCapacity(allocator, @divFloor(size, 2)),
            .overflow = std.ArrayList(TaskWithJob).init(allocator),
        };
    }

    pub fn deinit(self: *Scheduler, allocator: std.mem.Allocator) void {
        self.tasks.deinit();
        self.runnable.deinit(allocator);
        self.released.deinit();
        self.overflow.deinit();
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
        const index = blk: {
            if (self.released.popOrNull()) |index| {
                break :blk self.tasks.borrow_assume_unset(index);
            } else {
                break :blk try self.tasks.borrow();
            }
        };

        const context: usize = wrap(usize, task_ctx);

        const item = self.tasks.get_ptr(index);
        item.* = .{
            .index = index,
            .func = TaskFnWrapper(R, @TypeOf(task_ctx), task_fn),
            .context = context,
            .state = task_state,
        };

        if (task_state == .runnable) self.set_runnable(index);
        return index;
    }

    pub fn spawn2(
        self: *Scheduler,
        comptime R: type,
        task_ctx: anytype,
        comptime task_fn: TaskFn(R, @TypeOf(task_ctx)),
        task_state: Task.State,
        job: ?AsyncSubmission,
    ) !void {
        const context: usize = wrap(usize, task_ctx);
        const item: Task = .{
            .index = 0,
            .func = TaskFnWrapper(R, @TypeOf(task_ctx), task_fn),
            .context = context,
            .state = task_state,
        };

        const index = blk: {
            if (self.released.popOrNull()) |index| {
                break :blk self.tasks.borrow_assume_unset(index);
            } else {
                break :blk self.tasks.borrow() catch {
                    try self.overflow.append(.{ .task = item, .job = job });
                    return;
                };
            }
        };

        const item_ptr = self.tasks.get_ptr(index);
        item_ptr.* = item;
        item_ptr.index = index;

        switch (task_state) {
            .runnable => self.set_runnable(index),
            .waiting => if (job) |j| {
                const rt: *Runtime = @fieldParentPtr("scheduler", self);
                try rt.aio.queue_job(index, j);
            },
            else => {},
        }
    }

    pub fn release(self: *Scheduler, index: usize) !void {
        assert(self.runnable.isSet(index));
        self.runnable.unset(index);

        if (self.overflow.popOrNull()) |task_with| {
            const task = task_with.task;
            const task_ptr = self.tasks.get_ptr(index);
            assert(task_ptr.state == .dead);

            task_ptr.* = task;
            task_ptr.index = index;
            switch (task.state) {
                .runnable => self.set_runnable(index),
                .waiting => if (task_with.job) |j| {
                    const rt: *Runtime = @fieldParentPtr("scheduler", self);
                    try rt.aio.queue_job(index, j);
                },
                else => {},
            }
        } else {
            self.tasks.release(index);
            try self.released.append(index);
        }
    }
};
