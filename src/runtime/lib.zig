const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.@"tardy/scheduler/runtime");

const Scheduler = @import("./scheduler.zig").Scheduler;
const Task = @import("task.zig").Task;

const auto_async_match = @import("../aio/lib.zig").auto_async_match;
const AsyncIO = @import("../aio/lib.zig").AsyncIO;
const AsyncIOType = @import("../aio/lib.zig").AsyncIOType;
const AsyncIOOptions = @import("../aio/lib.zig").AsyncIOOptions;
const AsyncBusyLoop = @import("../aio/busy_loop.zig").AsyncBusyLoop;
const AsyncEpoll = @import("../aio/epoll.zig").AsyncEpoll;
const AsyncIoUring = @import("../aio/io_uring.zig").AsyncIoUring;
const Completion = @import("../aio/completion.zig").Completion;

pub fn Runtime(comptime _aio_type: AsyncIOType) type {
    const aio_type: AsyncIOType = comptime if (_aio_type == .auto) auto_async_match() else _aio_type;
    return struct {
        const Self = @This();
        pub const RuntimeTask = Task(Self);
        const RuntimeScheduler = Scheduler(Self);
        storage: std.StringHashMap(*anyopaque),
        scheduler: RuntimeScheduler,
        aio: AsyncIO,

        pub fn init(allocator: std.mem.Allocator, max_tasks: usize) !Self {
            const scheduler: RuntimeScheduler = try RuntimeScheduler.init(allocator, max_tasks);

            const storage = std.StringHashMap(*anyopaque).init(allocator);

            const options: AsyncIOOptions = .{
                .size_connections_max = @intCast(max_tasks),
                .size_completions_reap_max = @intCast(max_tasks),
            };

            log.debug("aio backend: {s}", .{@tagName(aio_type)});
            var aio: AsyncIO = blk: {
                switch (comptime aio_type) {
                    .auto => unreachable,
                    .io_uring => {
                        var uring = try allocator.create(AsyncIoUring);
                        uring.* = try AsyncIoUring.init(allocator, options);
                        break :blk uring.to_async();
                    },
                    .epoll => {
                        var epoll = try allocator.create(AsyncEpoll);
                        epoll.* = try AsyncEpoll.init(allocator, options);
                        break :blk epoll.to_async();
                    },
                    .busy_loop => {
                        var busy = try allocator.create(AsyncBusyLoop);
                        busy.* = try AsyncBusyLoop.init(allocator, options);
                        break :blk busy.to_async();
                    },
                    .custom => |AsyncCustom| {
                        var custom = try allocator.create(AsyncCustom);
                        custom.* = try AsyncCustom.init(allocator, options);
                        break :blk custom.to_async();
                    },
                }
            };

            // attach the completions
            aio.attach(try allocator.alloc(Completion, max_tasks));

            return .{ .storage = storage, .scheduler = scheduler, .aio = aio };
        }

        pub fn deinit(self: *const Self) void {
            _ = self;
        }

        pub fn accept(self: *Self, socket: std.posix.socket_t, task_fn: RuntimeTask.TaskFn, task_ctx: ?*anyopaque) !void {
            const index = try self.scheduler.spawn(task_fn, task_ctx, .waiting);
            try self.aio.queue_accept(index, socket);
        }

        pub fn recv(self: *Self, socket: std.posix.socket_t, buffer: []u8, task_fn: RuntimeTask.TaskFn, task_ctx: ?*anyopaque) !void {
            const index = try self.scheduler.spawn(task_fn, task_ctx, .waiting);
            try self.aio.queue_recv(index, socket, buffer);
        }

        pub fn send(self: *Self, socket: std.posix.socket_t, buffer: []const u8, task_fn: RuntimeTask.TaskFn, task_ctx: ?*anyopaque) !void {
            const index = try self.scheduler.spawn(task_fn, task_ctx, .waiting);
            try self.aio.queue_send(index, socket, buffer);
        }

        pub fn spawn(self: *Self, task_fn: RuntimeTask.TaskFn, task_ctx: ?*anyopaque) !void {
            _ = try self.scheduler.spawn(task_fn, task_ctx, .runnable);
        }

        pub fn run(self: *Self) !noreturn {
            const running = true;
            while (running) {
                var iter = self.scheduler.tasks.iterator();
                while (iter.next_ptr()) |task| {
                    if (task.state == .runnable) {
                        // run task
                        @call(.auto, task.func, .{ self, task, task.context });

                        // release task from pool.
                        task.state = .dead;
                        self.scheduler.release(task.index);
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
