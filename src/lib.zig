const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const log = std.log.scoped(.tardy);

pub const Pool = @import("core/pool.zig").Pool;
pub const Runtime = @import("runtime/lib.zig").Runtime;
pub const Task = @import("runtime/task.zig").Task;
pub const TaskFn = @import("runtime/task.zig").TaskFn;

pub const auto_async_match = @import("aio/lib.zig").auto_async_match;
const async_to_type = @import("aio/lib.zig").async_to_type;
const AsyncIO = @import("aio/lib.zig").AsyncIO;
pub const AsyncIOType = @import("aio/lib.zig").AsyncIOType;
const AsyncIOOptions = @import("aio/lib.zig").AsyncIOOptions;
const AsyncBusyLoop = @import("aio/busy_loop.zig").AsyncBusyLoop;
const AsyncEpoll = @import("aio/epoll.zig").AsyncEpoll;
const AsyncIoUring = @import("aio/io_uring.zig").AsyncIoUring;
const Completion = @import("aio/completion.zig").Completion;

pub const TardyThreading = union(enum) {
    single,
    multi: usize,
    /// Calculated by `@max((cpu_count / 2) - 1, 1)`
    auto,
};

const TardyOptions = struct {
    /// The allocator that server will use.
    allocator: std.mem.Allocator,
    /// Threading Mode that Tardy runtime will use.
    ///
    /// Default = .auto
    threading: TardyThreading = .auto,
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

pub fn Tardy(comptime _aio_type: AsyncIOType) type {
    const aio_type: AsyncIOType = comptime if (_aio_type == .auto) auto_async_match() else _aio_type;
    const AioInnerType = comptime async_to_type(aio_type);
    return struct {
        const Self = @This();
        aios: std.ArrayListUnmanaged(*AioInnerType),
        options: TardyOptions,

        pub fn init(options: TardyOptions) !Self {
            log.debug("aio backend: {s}", .{@tagName(aio_type)});

            return .{
                .options = options,
                .aios = try std.ArrayListUnmanaged(*AioInnerType).initCapacity(options.allocator, 0),
            };
        }

        pub fn deinit(self: *Self) void {
            for (self.aios.items) |aio| {
                self.options.allocator.destroy(aio);
            }

            self.aios.deinit(self.options.allocator);
        }

        /// This will spawn a new Runtime.
        fn spawn_runtime(self: *Self, options: AsyncIOOptions) !Runtime {
            var aio: AsyncIO = blk: {
                var io = try self.options.allocator.create(AioInnerType);
                io.* = try AioInnerType.init(self.options.allocator, options);
                try self.aios.append(self.options.allocator, io);
                break :blk io.to_async();
            };

            aio.attach(try self.options.allocator.alloc(Completion, self.options.size_aio_reap_max));

            const runtime = try Runtime.init(aio, .{
                .allocator = self.options.allocator,
                .size_tasks_max = self.options.size_tasks_max,
                .size_aio_jobs_max = self.options.size_aio_jobs_max,
                .size_aio_reap_max = self.options.size_aio_reap_max,
            });

            return runtime;
        }

        /// This is the entry into all of the runtimes.
        ///
        /// The provided func needs to have a signature of (*Runtime, std.mem.Allocator, anytype) !void;
        ///
        /// The provided allocator is meant to just initialize any structures that will exist throughout the lifetime
        /// of the runtime. It happens in an arena and is cleaned up after the runtime terminates.
        pub fn entry(
            self: *Self,
            init_func: anytype,
            init_params: anytype,
            deinit_func: anytype,
            deinit_params: anytype,
        ) !void {
            const runtime_count: usize = blk: {
                switch (self.options.threading) {
                    .single => break :blk 1,
                    .auto => break :blk @max(try std.Thread.getCpuCount() / 2 - 1, 1),
                    .multi => |count| break :blk count,
                }
            };

            assert(runtime_count > 0);
            log.info("thread count: {d}", .{runtime_count});

            var threads = try std.ArrayListUnmanaged(std.Thread).initCapacity(
                self.options.allocator,
                runtime_count -| 1,
            );
            defer {
                for (threads.items) |thread| {
                    thread.join();
                }

                threads.deinit(self.options.allocator);
            }

            var runtime = try self.spawn_runtime(.{
                .parent_async = null,
                .size_aio_jobs_max = self.options.size_aio_jobs_max,
                .size_aio_reap_max = self.options.size_aio_reap_max,
            });
            defer runtime.deinit();

            for (0..runtime_count - 1) |_| {
                const handle = try std.Thread.spawn(.{}, struct {
                    fn thread_init(
                        tardy: *Self,
                        options: TardyOptions,
                        parent: *AsyncIO,
                        init_parameters: anytype,
                        deinit_parameters: anytype,
                    ) void {
                        // Experimental: Should allow for avoiding contention
                        // over a shared fd table across threads.
                        if (comptime builtin.target.os.tag == .linux) {
                            const result = std.os.linux.unshare(std.os.linux.CLONE.FILES);
                            if (result < 0) unreachable;
                        }

                        var thread_rt = tardy.spawn_runtime(.{
                            .parent_async = parent,
                            .size_aio_jobs_max = options.size_aio_jobs_max,
                            .size_aio_reap_max = options.size_aio_reap_max,
                        }) catch return;
                        defer thread_rt.deinit();

                        @call(.auto, init_func, .{ &thread_rt, options.allocator, init_parameters }) catch return;
                        defer @call(.auto, deinit_func, .{ &thread_rt, options.allocator, deinit_parameters });

                        thread_rt.run() catch return;
                    }
                }.thread_init, .{ self, self.options, &runtime.aio, init_params, deinit_params });

                threads.appendAssumeCapacity(handle);
            }

            try @call(.auto, init_func, .{ &runtime, self.options.allocator, init_params });
            defer @call(.auto, deinit_func, .{ &runtime, self.options.allocator, deinit_params });

            try runtime.run();
        }
    };
}
