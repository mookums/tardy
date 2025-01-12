const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const log = std.log.scoped(.tardy);

const Atomic = std.atomic.Value;

pub const Pool = @import("core/pool.zig").Pool;
pub const Runtime = @import("runtime/lib.zig").Runtime;
pub const Task = @import("runtime/task.zig").Task;
pub const TaskFn = @import("runtime/task.zig").TaskFn;
pub const Broadcast = @import("runtime/broadcast.zig").Broadcast;
pub const Channel = @import("runtime/channel.zig").Channel;
pub const RxChannel = @import("runtime/channel.zig").RxChannel;
pub const TxChannel = @import("runtime/channel.zig").TxChannel;

pub const File = @import("fs/lib.zig").File;
pub const Dir = @import("fs/lib.zig").Dir;
pub const Path = @import("fs/lib.zig").Path;
pub const Stat = @import("fs/lib.zig").Stat;

pub const TcpSocket = @import("net/lib.zig").TcpSocket;
pub const TcpServer = @import("net/lib.zig").TcpServer;

// Results
pub const AcceptTcpResult = @import("aio/completion.zig").AcceptTcpResult;
pub const ConnectResult = @import("aio/completion.zig").ConnectResult;
pub const RecvResult = @import("aio/completion.zig").RecvResult;
pub const SendResult = @import("aio/completion.zig").SendResult;
pub const OpenFileResult = @import("aio/completion.zig").OpenFileResult;
pub const OpenDirResult = @import("aio/completion.zig").OpenDirResult;
pub const ReadResult = @import("aio/completion.zig").ReadResult;
pub const WriteResult = @import("aio/completion.zig").WriteResult;
pub const WriteAllResult = @import("aio/completion.zig").WriteAllResult;
pub const StatResult = @import("aio/completion.zig").StatResult;
// pub const MkdirResult = @import("aio/completion.zig").MkdirResult;
pub const CreateDirResult = @import("aio/completion.zig").CreateDirResult;
pub const DeleteResult = @import("aio/completion.zig").DeleteResult;
pub const DeleteTreeResult = @import("aio/completion.zig").DeleteTreeResult;

pub const ZeroCopy = @import("core/zero_copy.zig").ZeroCopy;
pub const wrap = @import("utils.zig").wrap;
pub const unwrap = @import("utils.zig").unwrap;

/// Cross-platform abstractions.
/// For the `std.posix` interface types.
pub const Cross = @import("cross/lib.zig");

pub const auto_async_match = @import("aio/lib.zig").auto_async_match;
const async_to_type = @import("aio/lib.zig").async_to_type;
const AsyncIO = @import("aio/lib.zig").AsyncIO;
pub const AsyncIOType = @import("aio/lib.zig").AsyncIOType;
const AsyncIOOptions = @import("aio/lib.zig").AsyncIOOptions;
const Completion = @import("aio/completion.zig").Completion;

pub const TardyThreading = union(enum) {
    single,
    multi: usize,
    all,
    /// Calculated by `@max((cpu_count / 2) - 1, 1)`
    auto,
};

const TardyOptions = struct {
    /// Threading Mode that Tardy runtime will use.
    ///
    /// Default = .auto
    threading: TardyThreading = .auto,
    /// Number of Maximum Tasks.
    ///
    /// Default: 1024
    size_tasks_max: usize = 1024,
    /// Number of Maximum Asynchronous I/O Jobs.
    ///
    /// Default: 1024
    size_aio_jobs_max: usize = 1024,
    /// Maximum number of aio completions we can reap
    /// with a single call of reap().
    ///
    /// Default: 256
    size_aio_reap_max: usize = 256,
};

pub fn Tardy(comptime _aio_type: AsyncIOType) type {
    const aio_type: AsyncIOType = comptime if (_aio_type == .auto) auto_async_match() else _aio_type;
    const AioInnerType = comptime async_to_type(aio_type);
    return struct {
        const Self = @This();
        aios: std.ArrayListUnmanaged(*AioInnerType),
        allocator: std.mem.Allocator,
        options: TardyOptions,
        mutex: std.Thread.Mutex = .{},

        pub fn init(allocator: std.mem.Allocator, options: TardyOptions) !Self {
            log.debug("aio backend: {s}", .{@tagName(aio_type)});

            return .{
                .allocator = allocator,
                .options = options,
                .aios = try std.ArrayListUnmanaged(*AioInnerType).initCapacity(allocator, 0),
            };
        }

        pub fn deinit(self: *Self) void {
            for (self.aios.items) |aio| {
                self.allocator.destroy(aio);
            }

            self.aios.deinit(self.allocator);
        }

        /// This will spawn a new Runtime.
        fn spawn_runtime(self: *Self, options: AsyncIOOptions) !Runtime {
            self.mutex.lock();
            defer self.mutex.unlock();

            const aio: AsyncIO = blk: {
                var io = try self.allocator.create(AioInnerType);
                io.* = try AioInnerType.init(self.allocator, options);
                try self.aios.append(self.allocator, io);
                var aio = io.to_async();
                aio.attach(try self.allocator.alloc(Completion, self.options.size_aio_reap_max));
                break :blk aio;
            };

            const runtime = try Runtime.init(aio, .{
                .allocator = self.allocator,
                .size_tasks_max = self.options.size_tasks_max,
                .size_aio_jobs_max = self.options.size_aio_jobs_max,
                .size_aio_reap_max = self.options.size_aio_reap_max,
            });

            return runtime;
        }

        /// This is the entry into all of the runtimes.
        ///
        /// The provided func needs to have a signature of (*Runtime, anytype) !void;
        ///
        /// The provided allocator is meant to just initialize any structures that will exist throughout the lifetime
        /// of the runtime. It happens in an arena and is cleaned up after the runtime terminates.
        pub fn entry(
            self: *Self,
            init_params: anytype,
            comptime init_func: *const fn (
                *Runtime,
                @TypeOf(init_params),
            ) anyerror!void,
            deinit_params: anytype,
            comptime deinit_func: *const fn (
                *Runtime,
                @TypeOf(deinit_params),
            ) anyerror!void,
        ) !void {
            const runtime_count: usize = blk: {
                switch (self.options.threading) {
                    .single => break :blk 1,
                    .multi => |count| break :blk count,
                    .auto => break :blk @max(try std.Thread.getCpuCount() / 2 - 1, 1),
                    .all => break :blk try std.Thread.getCpuCount(),
                }
            };

            assert(runtime_count > 0);
            log.info("thread count: {d}", .{runtime_count});

            var threads = try std.ArrayListUnmanaged(std.Thread).initCapacity(
                self.allocator,
                runtime_count -| 1,
            );
            defer {
                log.debug("waiting for the remaining threads to terminate", .{});
                for (threads.items) |thread| {
                    thread.join();
                }

                threads.deinit(self.allocator);
            }

            var runtime = try self.spawn_runtime(.{
                .parent_async = null,
                .size_aio_jobs_max = self.options.size_aio_jobs_max,
                .size_aio_reap_max = self.options.size_aio_reap_max,
            });
            defer runtime.deinit();

            var spawned_count = Atomic(usize).init(0);
            const spawning_count = runtime_count - 1;

            for (0..spawning_count) |_| {
                const handle = try std.Thread.spawn(.{}, struct {
                    fn thread_init(
                        tardy: *Self,
                        options: TardyOptions,
                        parent: *AsyncIO,
                        init_parameters: @TypeOf(init_params),
                        deinit_parameters: @TypeOf(deinit_params),
                        count: *Atomic(usize),
                        total_count: usize,
                    ) void {
                        var thread_rt = tardy.spawn_runtime(.{
                            .parent_async = parent,
                            .size_aio_jobs_max = options.size_aio_jobs_max,
                            .size_aio_reap_max = options.size_aio_reap_max,
                        }) catch return;
                        defer thread_rt.deinit();

                        _ = count.fetchAdd(1, .release);
                        while (count.load(.acquire) < total_count) {}

                        @call(.auto, init_func, .{
                            &thread_rt,
                            init_parameters,
                        }) catch return;

                        defer @call(.auto, deinit_func, .{
                            &thread_rt,
                            deinit_parameters,
                        }) catch unreachable;

                        thread_rt.run() catch return;
                    }
                }.thread_init, .{
                    self,
                    self.options,
                    &runtime.aio,
                    init_params,
                    deinit_params,
                    &spawned_count,
                    spawning_count,
                });

                threads.appendAssumeCapacity(handle);
            }

            while (spawned_count.load(.acquire) < spawning_count) {}
            log.debug("all runtimes spawned, initalizing...", .{});

            try @call(.auto, init_func, .{
                &runtime,
                init_params,
            });

            defer @call(.auto, deinit_func, .{
                &runtime,
                deinit_params,
            }) catch unreachable;

            try runtime.run();
        }

        /// This spawns in and enters into the runtime
        /// in a new Thread, allowing for more code to
        /// execute even after the runtime spawns.
        pub fn entry_in_new_thread(
            self: *Self,
            init_params: anytype,
            comptime init_func: *const fn (
                *Runtime,
                @TypeOf(init_params),
            ) anyerror!void,
            deinit_params: anytype,
            comptime deinit_func: *const fn (
                *Runtime,
                @TypeOf(deinit_params),
            ) anyerror!void,
        ) !void {
            const handle = try std.Thread.spawn(.{}, struct {
                fn entry_in_new_thread(
                    tardy: *Self,
                    ip: @TypeOf(init_params),
                    dp: @TypeOf(deinit_params),
                ) void {
                    tardy.entry(ip, init_func, dp, deinit_func) catch unreachable;
                }
            }.entry_in_new_thread, .{
                self,
                init_params,
                deinit_params,
            });
            handle.detach();
        }
    };
}

pub const Timespec = struct {
    seconds: u64 = 0,
    nanos: u64 = 0,
};
