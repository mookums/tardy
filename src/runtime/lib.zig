const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.@"tardy/scheduler/runtime");

const Scheduler = @import("./scheduler.zig").Scheduler;
const Task = @import("task.zig").Task;

const AsyncIO = @import("../aio/lib.zig").AsyncIO;

const Net = @import("../net/lib.zig").Net;

const RuntimeOptions = struct {
    /// The allocator that server will use.
    allocator: std.mem.Allocator,
    /// Number of Maximum Tasks.
    ///
    /// Default: 1024
    size_tasks_max: u16 = 1024,
    /// Number of Maximum Asynchronous I/O Jobs.
    ///
    /// Default: 1024
    size_aio_jobs_max: u16 = 1024,
    /// Maximum number of aio completions we can reap
    /// with a single call of reap().
    ///
    /// Default: 256
    size_aio_reap_max: u16 = 256,
};

/// A runtime is what runs tasks and handles the Async I/O.
/// Every thread should have an independent Runtime.
pub const Runtime = struct {
    allocator: std.mem.Allocator,
    storage: std.StringHashMap(*anyopaque),
    scheduler: Scheduler,
    aio: AsyncIO,
    net: Net = .{},
    running: bool = true,

    pub fn init(aio: AsyncIO, options: RuntimeOptions) !Runtime {
        assert(options.size_aio_reap_max <= options.size_aio_jobs_max);

        const scheduler: Scheduler = try Scheduler.init(options.allocator, options.size_tasks_max);
        const storage = std.StringHashMap(*anyopaque).init(options.allocator);

        return .{
            .allocator = options.allocator,
            .storage = storage,
            .scheduler = scheduler,
            .aio = aio,
        };
    }

    pub fn deinit(self: *Runtime) void {
        self.storage.deinit();
    }

    const SpawnParams = struct {
        func: Task.TaskFn,
        ctx: ?*anyopaque = null,
        predicate: ?Task.PredicateFn = null,
    };

    /// Spawns a new async Task. It will be immediately added to the
    /// runtime as `.runnable` and will run whenever it is encountered.
    pub fn spawn(self: *Runtime, params: SpawnParams) !void {
        _ = try self.scheduler.spawn(
            params.func,
            params.ctx,
            params.predicate,
            .runnable,
        );
    }

    pub fn stop(self: *Runtime) void {
        self.running = false;
    }

    pub fn run(self: *Runtime) !void {
        while (self.running) {
            var iter = self.scheduler.runnable.iterator(.{ .kind = .set });
            while (iter.next()) |index| {
                const task: *Task = &self.scheduler.tasks.items[index];
                assert(task.state == .runnable);

                // if we have a predicate,
                // we want to check it before running.
                if (task.predicate) |predicate| {
                    const state = @call(.auto, predicate, .{ self, task });
                    if (!state) continue;
                }

                // run task
                @call(.auto, task.func, .{ self, task, task.context });

                // release task from pool.
                task.state = .dead;
                self.scheduler.release(task.index);
            }

            try self.aio.submit();

            // If we don't have any runnable tasks, we just want to wait for an Async I/O.
            // Otherwise, we want to just reap whatever completion we have and continue running.
            const wait_for_io: usize = @intFromBool(self.scheduler.runnable.count() == 0);
            log.debug("Wait for I/O: {d}", .{wait_for_io});

            const completions = try self.aio.reap(wait_for_io);
            for (completions) |completion| {
                const index = completion.task;
                const task = &self.scheduler.tasks.items[index];
                assert(task.state == .waiting);
                task.result = completion.result;
                self.scheduler.set_runnable(index);
            }
        }
    }
};
