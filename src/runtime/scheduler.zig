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
    const OverflowLinkedList = std.DoublyLinkedList(TaskWithJob);
    const OverflowNode = OverflowLinkedList.Node;

    allocator: std.mem.Allocator,
    tasks: Pool(Task),
    runnable: std.DynamicBitSetUnmanaged,
    released: std.ArrayListUnmanaged(usize),
    overflow: OverflowLinkedList,

    pub fn init(allocator: std.mem.Allocator, size: usize) !Scheduler {
        return .{
            .allocator = allocator,
            .tasks = try Pool(Task).init(allocator, size),
            .runnable = try std.DynamicBitSetUnmanaged.initEmpty(allocator, size),
            .released = try std.ArrayListUnmanaged(usize).initCapacity(allocator, size),
            .overflow = OverflowLinkedList{},
        };
    }

    pub fn deinit(self: *Scheduler) void {
        self.tasks.deinit();
        self.runnable.deinit(self.allocator);
        self.released.deinit(self.allocator);
        while (self.overflow.pop()) |node| self.allocator.destroy(node);
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
                    const node = try self.allocator.create(OverflowNode);
                    node.data = .{ .task = item, .job = job };
                    self.overflow.append(node);
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

        if (self.overflow.popFirst()) |node| {
            defer self.allocator.destroy(node);

            const task_with = node.data;
            const task_ptr = self.tasks.get_ptr(index);
            assert(task_ptr.state == .dead);

            task_ptr.* = task_with.task;
            task_ptr.index = index;
            switch (task_with.task.state) {
                .runnable => self.set_runnable(index),
                .waiting => if (task_with.job) |j| {
                    const rt: *Runtime = @fieldParentPtr("scheduler", self);
                    try rt.aio.queue_job(index, j);
                },
                else => {},
            }
        } else {
            self.tasks.release(index);
            try self.released.append(self.allocator, index);
        }
    }
};
