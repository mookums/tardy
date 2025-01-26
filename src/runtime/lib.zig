const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.@"tardy/runtime");

const Frame = @import("../frame/lib.zig").Frame;
const AsyncIO = @import("../aio/lib.zig").AsyncIO;
const Scheduler = @import("./scheduler.zig").Scheduler;

const PoolKind = @import("../core/pool.zig").PoolKind;
const Queue = @import("../core/queue.zig").Queue;

const Task = @import("task.zig").Task;
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

    // The currently running Task's index.
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

    /// Is the runtime asleep?
    pub inline fn asleep(self: *Runtime) bool {
        return self.aio.asleep.load(.acquire);
    }

    /// Trigger a waiting Task.
    pub fn trigger(self: *Runtime, index: usize) !void {
        try self.scheduler.trigger(index);
        if (self.asleep()) try self.wake();
    }

    /// Spawns a new Frame. This creates a new heap-allocated stack for the Frame to run.
    pub fn spawn(
        self: *Runtime,
        frame_ctx: anytype,
        comptime frame_fn: anytype,
        stack_size: usize,
    ) !void {
        try self.scheduler.spawn(frame_ctx, frame_fn, stack_size);
    }

    pub fn stop(self: *Runtime) void {
        self.running = false;
        self.wake() catch unreachable;
    }

    fn run_task(self: *Runtime, task: *Task) !void {
        self.current_task = task.index;

        const frame = task.frame;
        frame.proceed();

        switch (frame.status) {
            else => {},
            .done => {
                // remember: task is invalid IF it resizes.
                // so we only hit that condition sometimes in here.
                const index = self.current_task.?;
                const inner_task = self.scheduler.tasks.get_ptr(index);

                // If the frame is done, clean it up.
                try self.scheduler.release(inner_task.index);

                // frees the heap-allocated stack.
                //
                // this should be evaluted as it does have a perf impact but
                // if frames are long lived (as they should be) and most data is
                // stack allocated within that context, i think it should be ok?
                frame.deinit(self.allocator);
            },
            .errored => {
                log.warn("cleaning up failed frame...", .{});
                try self.scheduler.release(task.index);
                frame.deinit(self.allocator);
            },
        }
    }

    pub fn run(self: *Runtime) !void {
        while (true) {
            var force_woken = false;

            // Processing Section
            var iter = self.scheduler.tasks.dirty.iterator(.{ .kind = .set });
            while (iter.next()) |index| {
                log.debug("{d} - processing index={d}", .{ self.id, index });
                const task = self.scheduler.tasks.get_ptr(index);
                switch (task.state) {
                    .runnable => {
                        log.debug("{d} - running index={d}", .{ self.id, index });
                        try self.run_task(task);
                        self.current_task = null;
                    },
                    .wait_for_trigger => if (self.scheduler.triggers.is_set(index)) {
                        log.debug("{d} - trigger={d} | state={s}", .{
                            self.id,
                            index,
                            @tagName(task.state),
                        });
                        try self.scheduler.set_runnable(index);
                    },
                    else => continue,
                }
            }

            self.scheduler.triggers.unset_all();
            if (!self.running) break;

            // I/O Section
            try self.aio.submit();

            // If we don't have any runnable tasks, we just want to wait for an Async I/O.
            // Otherwise, we want to just reap whatever completion we have and continue running.
            //
            // Also don't wait for I/O if we have no tasks ready.
            const wait_for_io = self.scheduler.runnable == 0 and !self.scheduler.tasks.empty();
            log.debug("{d} - Wait for I/O: {}", .{ self.id, wait_for_io });

            const completions = try self.aio.reap(wait_for_io);
            for (completions) |completion| {
                if (completion.result == .wake) {
                    assert(force_woken == false);
                    force_woken = true;
                    if (!self.running) return;
                    continue;
                }

                const index = completion.task;
                log.debug("{d} - completion={d}", .{ self.id, index });
                const task = self.scheduler.tasks.get_ptr(index);
                assert(task.state == .wait_for_io);
                task.result = completion.result;
                try self.scheduler.set_runnable(index);
            }

            if (self.scheduler.runnable == 0 and !force_woken) {
                log.warn("no more runnable tasks", .{});
                break;
            }
        }
    }
};
