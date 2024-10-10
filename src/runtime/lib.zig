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
    _ = _aio_type;
    //const aio_type: AsyncIOType = comptime if (_aio_type == .auto) auto_async_match() else _aio_type;
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
                .ms_operation_max = null,
            };

            // temporarily only use the busyloop...
            var aio: AsyncIO = blk: {
                // this is going out of scope.
                // needs to be allocated on heap.
                const busy = try allocator.create(AsyncBusyLoop);
                busy.* = try AsyncBusyLoop.init(allocator, options);
                break :blk busy.to_async();
            };

            // attach the completions
            aio.attach(try allocator.alloc(Completion, max_tasks));

            //const aio: AsyncIO = blk: {
            //    switch (comptime aio_type) {
            //        .auto => unreachable,
            //        .io_uring => {
            //            var uring = try AsyncIoUring(void).init(
            //                allocator,
            //                options,
            //            );

            //            break :blk uring.to_async();
            //        },
            //        .epoll => {
            //            var epoll = try AsyncEpoll.init(
            //                allocator,
            //                options,
            //            );

            //            break :blk epoll.to_async();
            //        },
            //        .busy_loop => {
            //            var busy = try AsyncBusyLoop.init(
            //                allocator,
            //                options,
            //            );

            //            break :blk busy.to_async();
            //        },
            //        .custom => |AsyncCustom| {
            //            var custom = try AsyncCustom.init(
            //                allocator,
            //                options,
            //            );

            //            break :blk custom.to_async();
            //        },
            //    }
            //};

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

                // if the task is an AIO one and it has completed,
                // it is now eligible to run.
                const completions = try self.aio.reap(0);
                for (completions) |completion| {
                    const index = completion.task;
                    const task = &self.scheduler.tasks.items[index];
                    assert(task.state == .waiting);
                    task.state = .runnable;
                    task.result = completion.result;
                }
            }
        }
    };
}
