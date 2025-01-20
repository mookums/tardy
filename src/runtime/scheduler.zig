const std = @import("std");
const assert = std.debug.assert;

const Task = @import("task.zig").Task;
const TaskFn = @import("task.zig").TaskFn;
const TaskFnWrapper = @import("task.zig").TaskFnWrapper;
const InnerTaskFn = @import("task.zig").InnerTaskFn;

const Runtime = @import("lib.zig").Runtime;
const Frame = @import("../frame/lib.zig").Frame;

const Pool = @import("../core/pool.zig").Pool;
const PoolKind = @import("../core/pool.zig").PoolKind;

const wrap = @import("../utils.zig").wrap;
const unwrap = @import("../utils.zig").unwrap;

pub const AsyncSubmission = @import("../aio/lib.zig").AsyncSubmission;

const TaskWithJob = struct {
    task: Task,
    job: ?AsyncSubmission = null,
};

pub const Scheduler = struct {
    allocator: std.mem.Allocator,
    tasks: Pool(Task),
    runnable: std.DynamicBitSetUnmanaged,
    released: std.ArrayListUnmanaged(usize),

    pub fn init(allocator: std.mem.Allocator, size: usize, pooling: PoolKind) !Scheduler {
        return .{
            .allocator = allocator,
            .tasks = try Pool(Task).init(allocator, size, pooling),
            .runnable = try std.DynamicBitSetUnmanaged.initEmpty(allocator, size),
            .released = try std.ArrayListUnmanaged(usize).initCapacity(allocator, size),
        };
    }

    pub fn deinit(self: *Scheduler) void {
        self.tasks.deinit();
        self.runnable.deinit(self.allocator);
        self.released.deinit(self.allocator);
    }

    pub fn set_runnable(self: *Scheduler, index: usize) !void {
        // Resizes the runnable if the underlying task has changed size
        if (self.tasks.kind == .grow and self.runnable.bit_length != self.tasks.items.len) {
            const new_size = self.tasks.items.len;
            try self.runnable.resize(self.allocator, new_size, false);
        }

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
        job: ?AsyncSubmission,
    ) !void {
        const index = blk: {
            if (self.released.popOrNull()) |index| {
                break :blk self.tasks.borrow_assume_unset(index);
            } else {
                break :blk try self.tasks.borrow();
            }
        };

        const context: usize = wrap(usize, task_ctx);
        const item: Task = .{
            .index = index,
            .runner = .{
                .callback = .{
                    .func = TaskFnWrapper(R, @TypeOf(task_ctx), task_fn),
                    .context = context,
                },
            },
            .state = task_state,
        };

        const item_ptr = self.tasks.get_ptr(index);
        item_ptr.* = item;

        switch (task_state) {
            .runnable => try self.set_runnable(index),
            .wait_for_io => if (job) |j| {
                const rt: *Runtime = @fieldParentPtr("scheduler", self);
                try rt.aio.queue_job(index, j);
            },
            else => {},
        }
    }

    pub fn frame_await(self: *Scheduler, job: AsyncSubmission) !void {
        const rt: *Runtime = @fieldParentPtr("scheduler", self);
        const index = rt.current_task.?;

        const task = self.tasks.get_ptr(index);
        // Return to the waiting state.
        task.state = .wait_for_io;
        self.runnable.unset(index);
        // Queue the related I/O job.
        try rt.aio.queue_job(index, job);
        Frame.yield();
    }

    pub fn spawn_frame(
        self: *Scheduler,
        frame_ctx: anytype,
        comptime frame_fn: anytype,
        stack_size: usize,
    ) !void {
        const index = blk: {
            if (self.released.popOrNull()) |index| {
                break :blk self.tasks.borrow_assume_unset(index);
            } else {
                break :blk try self.tasks.borrow();
            }
        };

        const frame = try Frame.init(self.allocator, stack_size, frame_ctx, frame_fn);

        const item: Task = .{
            .index = index,
            .runner = .{ .frame = frame },
            .state = .runnable,
        };

        const item_ptr = self.tasks.get_ptr(index);
        item_ptr.* = item;
        try self.set_runnable(index);
    }

    pub fn release(self: *Scheduler, index: usize) !void {
        // must be runnable to set?
        assert(self.runnable.isSet(index));
        self.runnable.unset(index);

        self.tasks.release(index);
        try self.released.append(self.allocator, index);
    }
};
