const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.@"tardy/runtime");

const AsyncIO = @import("../aio/lib.zig").AsyncIO;
const Scheduler = @import("./scheduler.zig").Scheduler;

const Task = @import("task.zig").Task;
const TaskFn = @import("task.zig").TaskFn;
const TaskFnWrapper = @import("task.zig").TaskFnWrapper;
const InnerTaskFn = @import("task.zig").InnerTaskFn;

const Storage = @import("storage.zig").Storage;

const Timespec = @import("../aio/timespec.zig").Timespec;

const Net = @import("../net/lib.zig").Net;
const Filesystem = @import("../fs/lib.zig").Filesystem;

const RuntimeOptions = struct {
    allocator: std.mem.Allocator,
    size_tasks_max: u16,
    size_aio_jobs_max: u16,
    size_aio_reap_max: u16,
};

/// A runtime is what runs tasks and handles the Async I/O.
/// Every thread should have an independent Runtime.
pub const Runtime = struct {
    allocator: std.mem.Allocator,
    storage: Storage,
    scheduler: Scheduler,
    aio: AsyncIO,
    net: Net = .{},
    fs: Filesystem = .{},
    running: bool = true,

    pub fn init(aio: AsyncIO, options: RuntimeOptions) !Runtime {
        assert(options.size_aio_reap_max <= options.size_aio_jobs_max);

        const scheduler: Scheduler = try Scheduler.init(options.allocator, options.size_tasks_max);
        const storage = Storage.init(options.allocator);

        return .{
            .allocator = options.allocator,
            .storage = storage,
            .scheduler = scheduler,
            .aio = aio,
        };
    }

    pub fn deinit(self: *Runtime) void {
        self.storage.deinit();
        self.scheduler.deinit(self.allocator);
        self.allocator.free(self.aio.completions);
        self.aio.deinit(self.allocator);
    }

    /// Spawns a new Task. This task is immediately runnable
    /// and will run as soon as possible.
    pub fn spawn(
        self: *Runtime,
        comptime R: type,
        task_ctx: anytype,
        comptime task_fn: TaskFn(R, @TypeOf(task_ctx)),
    ) !void {
        _ = try self.scheduler.spawn(R, task_ctx, task_fn, .runnable);
    }

    /// Spawns a new Task. This task will be set as runnable
    /// after the `timespec` amount of time has elasped.
    pub fn spawn_delay(
        self: *Runtime,
        comptime R: type,
        task_ctx: anytype,
        comptime task_fn: TaskFn(R, @TypeOf(task_ctx)),
        timespec: Timespec,
    ) !void {
        const index = try self.scheduler.spawn(R, task_ctx, task_fn, .waiting);
        try self.aio.queue_timer(index, timespec);
    }

    pub fn stop(self: *Runtime) void {
        self.running = false;
    }

    inline fn run_task(self: *Runtime, task: *Task) !void {
        const cloned_task: Task = task.*;
        task.state = .dead;
        try self.scheduler.release(task.index);

        @call(.auto, cloned_task.func, .{ self, &cloned_task }) catch |e| {
            log.debug("task failed: {}", .{e});
        };
    }

    pub fn run(self: *Runtime) !void {
        while (self.running) {
            var iter = self.scheduler.tasks.dirty.iterator(.{ .kind = .set });
            while (iter.next()) |index| {
                const task: *Task = &self.scheduler.tasks.items[index];
                switch (task.state) {
                    .predicate => |inner| {
                        const predicate = inner.func(inner.ctx);
                        if (predicate) {
                            task.result = .{ .ptr = inner.gen(inner.ctx) };
                            self.scheduler.set_runnable(task.index);
                            try self.run_task(task);
                        }
                    },
                    .runnable => try self.run_task(task),
                    else => continue,
                }
            }

            if (!self.running) break;
            try self.aio.submit();

            // If we don't have any runnable tasks, we just want to wait for an Async I/O.
            // Otherwise, we want to just reap whatever completion we have and continue running.
            const wait_for_io = self.scheduler.runnable.count() == 0;
            log.debug("Wait for I/O: {}", .{wait_for_io});

            const completions = try self.aio.reap(wait_for_io);
            for (completions) |completion| {
                const index = completion.task;
                const task = &self.scheduler.tasks.items[index];
                assert(task.state == .waiting);
                task.result = completion.result;
                self.scheduler.set_runnable(index);
            }

            if (self.scheduler.runnable.count() == 0) {
                log.err("no more runnable tasks", .{});
                break;
            }
        }
    }
};
