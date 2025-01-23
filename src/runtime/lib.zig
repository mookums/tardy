const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.@"tardy/runtime");

const Frame = @import("../frame/lib.zig").Frame;
const AsyncIO = @import("../aio/lib.zig").AsyncIO;
const Scheduler = @import("./scheduler.zig").Scheduler;

const PoolKind = @import("../core/pool.zig").PoolKind;

const Task = @import("task.zig").Task;
const TaskFn = @import("task.zig").TaskFn;
const TaskFnWrapper = @import("task.zig").TaskFnWrapper;
const InnerTaskFn = @import("task.zig").InnerTaskFn;

const Storage = @import("storage.zig").Storage;

const Timespec = @import("../lib.zig").Timespec;

const RuntimeOptions = struct {
    id: usize,
    pooling: PoolKind,
    size_tasks_initial: usize,
    size_aio_reap_max: usize,
};

/// A runtime is what runs tasks and handles the Async I/O.
/// Every thread should have an independent Runtime.
pub const Runtime = struct {
    allocator: std.mem.Allocator,
    storage: Storage,
    scheduler: Scheduler,
    aio: AsyncIO,
    id: usize,
    running: bool = true,
    // The currently running Task.
    current_task: ?usize = null,

    pub fn init(allocator: std.mem.Allocator, aio: AsyncIO, options: RuntimeOptions) !Runtime {
        const scheduler = try Scheduler.init(
            allocator,
            options.size_tasks_initial,
            options.pooling,
        );
        const storage = Storage.init(allocator);

        return .{
            .allocator = allocator,
            .storage = storage,
            .scheduler = scheduler,
            .aio = aio,
            .id = options.id,
            .current_task = null,
        };
    }

    pub fn deinit(self: *Runtime) void {
        self.storage.deinit();
        self.scheduler.deinit();
        self.allocator.free(self.aio.completions);
        self.aio.deinit(self.allocator);
    }

    pub fn wake(self: *Runtime) !void {
        try self.aio.wake();
    }

    /// Spawns a new Task. This task is immediately runnable and will run as soon as possible.
    pub fn spawn_task(
        self: *Runtime,
        comptime R: type,
        task_ctx: anytype,
        comptime task_fn: TaskFn(R, @TypeOf(task_ctx)),
    ) !void {
        try self.scheduler.spawn(R, task_ctx, task_fn, .runnable, null);
    }

    /// Spawns a new Frame. This creates a new heap-allocated stack for the Frame to run.
    pub fn spawn_frame(
        self: *Runtime,
        frame_ctx: anytype,
        comptime frame_fn: anytype,
        stack_size: usize,
    ) !void {
        try self.scheduler.spawn_frame(frame_ctx, frame_fn, stack_size);
    }

    /// Is the runtime asleep?
    pub inline fn asleep(self: *Runtime) bool {
        return self.aio.asleep.load(.acquire);
    }

    pub fn stop(self: *Runtime) void {
        self.running = false;
    }

    fn run_task(self: *Runtime, task: *Task) !void {
        self.current_task = task.index;

        switch (task.runner) {
            .callback => |_| {
                const cloned_task: Task = task.*;
                task.state = .dead;
                try self.scheduler.release(task.index);

                @call(.auto, cloned_task.runner.callback.func, .{ self, &cloned_task }) catch |e| {
                    log.debug("task failed: {}", .{e});
                };
            },
            .frame => |inner| {
                inner.proceed();

                switch (inner.status) {
                    else => {},
                    .done => {
                        // remember: task is invalid IF it resizes.
                        // so we only hit that condition sometimes in here.
                        const index = self.current_task.?;
                        const inner_task = self.scheduler.tasks.get_ptr(index);

                        // If the frame is done, clean it up.
                        inner_task.state = .dead;

                        // task index is somehow invalid here?
                        try self.scheduler.release(inner_task.index);

                        // frees the heap-allocated stack.
                        //
                        // this should be evaluted as it does have a perf impact but
                        // if frames are long lived (as they should be) and most data is
                        // stack allocated within that context, i think it should be ok?
                        inner.deinit(self.allocator);
                    },
                    .errored => {
                        log.warn("cleaning up failed frame...", .{});
                        task.state = .dead;
                        try self.scheduler.release(task.index);
                        inner.deinit(self.allocator);
                    },
                }
            },
        }
    }

    pub fn run(self: *Runtime) !void {
        while (true) {
            var force_woken = false;
            var iter = self.scheduler.tasks.dirty.iterator(.{ .kind = .set });
            while (iter.next()) |index| {
                log.debug("running index={d}", .{index});
                const task: *Task = &self.scheduler.tasks.items[index];
                switch (task.state) {
                    .channel => unreachable,
                    .runnable => try self.run_task(task),
                    else => continue,
                }
                self.current_task = null;
            }

            if (!self.running) break;
            try self.aio.submit();

            // If we don't have any runnable tasks, we just want to wait for an Async I/O.
            // Otherwise, we want to just reap whatever completion we have and continue running.
            //
            // Also don't wait for I/O if we have no tasks ready.
            const wait_for_io = self.scheduler.runnable.count() == 0 and !self.scheduler.tasks.empty();
            log.debug("Wait for I/O: {}", .{wait_for_io});

            const completions = try self.aio.reap(wait_for_io);
            for (completions) |completion| {
                if (completion.result == .wake) {
                    force_woken = true;
                    continue;
                }

                const index = completion.task;
                const task = &self.scheduler.tasks.items[index];
                assert(task.state == .wait_for_io);
                task.result = completion.result;
                try self.scheduler.set_runnable(index);
            }

            if (self.scheduler.runnable.count() == 0 and !force_woken) {
                log.warn("no more runnable tasks", .{});
                break;
            }
        }
    }
};
