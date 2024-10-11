const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.@"tardy/scheduler/runtime");

const _Scheduler = @import("./scheduler.zig").Scheduler;
const _Task = @import("task.zig").Task;

const auto_async_match = @import("../aio/lib.zig").auto_async_match;
const AsyncIO = @import("../aio/lib.zig").AsyncIO;
const AsyncIOType = @import("../aio/lib.zig").AsyncIOType;
const AsyncIOOptions = @import("../aio/lib.zig").AsyncIOOptions;
const AsyncBusyLoop = @import("../aio/busy_loop.zig").AsyncBusyLoop;
const AsyncEpoll = @import("../aio/epoll.zig").AsyncEpoll;
const AsyncIoUring = @import("../aio/io_uring.zig").AsyncIoUring;
const Completion = @import("../aio/completion.zig").Completion;

const RuntimeThreadCount = union(enum) {
    auto,
    count: u32,
};

const RuntimeThreading = union(enum) {
    single_threaded,
    multi_threaded: RuntimeThreadCount,
};

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
    /// Parent Async (if there is one we want to link to).
    parent_async: ?*const AsyncIO = null,
};

/// A runtime is what runs tasks and handles the Async I/O.
/// Every thread should have an independent Runtime.
pub fn Runtime(comptime _aio_type: AsyncIOType) type {
    const aio_type: AsyncIOType = comptime if (_aio_type == .auto) auto_async_match() else _aio_type;
    return struct {
        const Self = @This();
        pub const Task = _Task(Self);
        const Scheduler = _Scheduler(Self);
        storage: std.StringHashMap(*anyopaque),
        scheduler: Scheduler,
        aio: AsyncIO,
        options: RuntimeOptions,

        pub fn init(options: RuntimeOptions) !Self {
            assert(options.size_aio_reap_max <= options.size_aio_jobs_max);

            const scheduler: Scheduler = try Scheduler.init(options.allocator, options.size_tasks_max);
            const storage = std.StringHashMap(*anyopaque).init(options.allocator);

            const aio_options: AsyncIOOptions = .{
                .size_aio_jobs_max = options.size_aio_jobs_max,
                .size_aio_reap_max = options.size_aio_reap_max,
                .parent_async = options.parent_async,
            };

            log.debug("aio backend: {s}", .{@tagName(aio_type)});
            var aio: AsyncIO = blk: {
                switch (comptime aio_type) {
                    .auto => unreachable,
                    .io_uring => {
                        var uring = try options.allocator.create(AsyncIoUring);
                        uring.* = try AsyncIoUring.init(options.allocator, aio_options);
                        break :blk uring.to_async();
                    },
                    .epoll => {
                        var epoll = try options.allocator.create(AsyncEpoll);
                        epoll.* = try AsyncEpoll.init(options.allocator, aio_options);
                        break :blk epoll.to_async();
                    },
                    .busy_loop => {
                        var busy = try options.allocator.create(AsyncBusyLoop);
                        busy.* = try AsyncBusyLoop.init(options.allocator, aio_options);
                        break :blk busy.to_async();
                    },
                    .custom => |AsyncCustom| {
                        var custom = try options.allocator.create(AsyncCustom);
                        custom.* = try AsyncCustom.init(options.allocator, aio_options);
                        break :blk custom.to_async();
                    },
                }
            };

            // attach the completions
            aio.attach(try options.allocator.alloc(Completion, options.size_aio_reap_max));

            return .{
                .storage = storage,
                .scheduler = scheduler,
                .aio = aio,
                .options = options,
            };
        }

        pub fn deinit(self: *const Self) void {
            _ = self;
        }

        const AcceptParams = struct {
            socket: std.posix.socket_t,
            func: Task.TaskFn,
            ctx: ?*anyopaque = null,
            predicate: ?Task.PredicateFn = null,
        };

        pub fn accept(self: *Self, params: AcceptParams) !void {
            const index = try self.scheduler.spawn(
                params.func,
                params.ctx,
                params.predicate,
                .waiting,
            );
            try self.aio.queue_accept(index, params.socket);
        }

        const RecvParams = struct {
            socket: std.posix.socket_t,
            buffer: []u8,
            func: Task.TaskFn,
            ctx: ?*anyopaque = null,
            predicate: ?Task.PredicateFn = null,
        };

        pub fn recv(self: *Self, params: RecvParams) !void {
            const index = try self.scheduler.spawn(
                params.func,
                params.ctx,
                params.predicate,
                .waiting,
            );
            try self.aio.queue_recv(index, params.socket, params.buffer);
        }

        const SendParams = struct {
            socket: std.posix.socket_t,
            buffer: []const u8,
            func: Task.TaskFn,
            ctx: ?*anyopaque = null,
            predicate: ?Task.PredicateFn = null,
        };

        pub fn send(self: *Self, params: SendParams) !void {
            const index = try self.scheduler.spawn(
                params.func,
                params.ctx,
                params.predicate,
                .waiting,
            );
            try self.aio.queue_send(index, params.socket, params.buffer);
        }

        const SpawnParams = struct {
            func: Task.TaskFn,
            ctx: ?*anyopaque = null,
            predicate: ?Task.PredicateFn = null,
        };

        /// Spawns a new async Task. It will be immediately added to the
        /// runtime as `.runnable` and will run whenever it is encountered.
        pub fn spawn(self: *Self, params: SpawnParams) !void {
            _ = try self.scheduler.spawn(
                params.func,
                params.ctx,
                params.predicate,
                .runnable,
            );
        }

        pub fn run(self: *Self) !noreturn {
            while (true) {
                var iter = self.scheduler.tasks.iterator();
                while (iter.next_ptr()) |task| {
                    switch (task.state) {
                        .runnable => {
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
                        },
                        .waiting => continue,
                        else => unreachable,
                    }
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
}
