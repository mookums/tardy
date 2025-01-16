const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.@"tardy/runtime");

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
    running: bool = true,

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

    /// Spawns a new Task. This task is immediately runnable
    /// and will run as soon as possible.
    pub fn spawn(
        self: *Runtime,
        comptime R: type,
        task_ctx: anytype,
        comptime task_fn: TaskFn(R, @TypeOf(task_ctx)),
    ) !void {
        try self.scheduler.spawn(R, task_ctx, task_fn, .runnable, null);
    }

    /// Is the runtime asleep?
    pub inline fn asleep(self: *Runtime) bool {
        return self.aio.asleep.load(.acquire);
    }

    pub fn stop(self: *Runtime) void {
        self.running = false;
    }

    fn run_task(self: *Runtime, task: *Task) !void {
        const cloned_task: Task = task.*;
        task.state = .dead;
        try self.scheduler.release(task.index);

        @call(.auto, cloned_task.func, .{ self, &cloned_task }) catch |e| {
            log.debug("task failed: {}", .{e});
        };
    }

    pub fn run(self: *Runtime) !void {
        // what if we just tracked an index in the runtime?
        // and then we just used the iterator but starting from that index.
        // that way this run wouldn't need to break the while loop all the time?

        while (true) {
            var force_woken = false;
            var iter = self.scheduler.tasks.dirty.iterator(.{ .kind = .set });
            while (iter.next()) |index| {
                const task: *Task = &self.scheduler.tasks.items[index];
                switch (task.state) {
                    .channel => |inner| {
                        if (inner.check(inner.ctx)) {
                            task.result = .{ .ptr = inner.gen(inner.ctx) };
                            try self.scheduler.set_runnable(task.index);
                            try self.run_task(task);
                        }
                    },
                    .runnable => try self.run_task(task),
                    else => continue,
                }
            }

            if (!self.running) break;
            try self.aio.submit();

            if (self.scheduler.tasks.empty()) {
                log.info("no more tasks", .{});
                break;
            }

            // If we don't have any runnable tasks, we just want to wait for an Async I/O.
            // Otherwise, we want to just reap whatever completion we have and continue running.
            const wait_for_io = self.scheduler.runnable.count() == 0;
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
