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

fn Runtime(comptime _aio_type: AsyncIOType) type {
    const aio_type: AsyncIOType = comptime if (_aio_type == .auto) auto_async_match() else _aio_type;
    return struct {
        const Self = @This();
        scheduler: Scheduler,
        aio: AsyncIO,

        pub fn init(allocator: std.mem.Allocator, size: usize) !Runtime {
            const scheduler: Scheduler = Scheduler.init(allocator, size);

            const options: AsyncIOOptions = .{
                .size_connections_max = size,
                .size_completions_reap_max = size,
            };

            const aio: AsyncIO = blk: {
                switch (comptime aio_type) {
                    .auto => unreachable,
                    .io_uring => {
                        var uring = try AsyncIoUring(void).init(
                            allocator,
                            options,
                        );

                        break :blk uring.to_async();
                    },
                    .epoll => {
                        var epoll = try AsyncEpoll.init(
                            allocator,
                            options,
                        );

                        break :blk epoll.to_async();
                    },
                    .busy_loop => {
                        var busy = try AsyncBusyLoop.init(
                            allocator,
                            options,
                        );

                        break :blk busy.to_async();
                    },
                    .custom => |AsyncCustom| {
                        var custom = try AsyncCustom.init(
                            allocator,
                            options,
                        );

                        break :blk custom.to_async();
                    },
                }
            };

            return .{ .scheduler = scheduler, .aio = aio };
        }

        pub fn deinit(self: *const Self) void {
            _ = self;
        }

        pub fn spawn(self: *Self, task: Task) void {
            try self.scheduler.spawn(task);
        }

        pub fn run(self: *Self) noreturn {
            const running = true;
            while (running) {
                try self.aio.submit();

                var iter = self.scheduler.tasks.iterator();
                while (iter.next_ptr()) |task| {
                    if (task.predicate()) {
                        // run task
                        @call(.auto, task.func, .{task.context});

                        // release task from pool.
                        self.scheduler.release(task.index);
                    }
                }

                const completions = self.aio.reap();
            }
        }
    };
}
