const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const log = std.log.scoped(.tardy);

const Atomic = std.atomic.Value;

pub const Frame = @import("frame/lib.zig").Frame;

pub const Runtime = @import("runtime/lib.zig").Runtime;
pub const Task = @import("runtime/task.zig").Task;

pub const Spsc = @import("channel/spsc.zig").Spsc;

pub const Timer = @import("runtime/timer.zig").Timer;
pub const File = @import("fs/lib.zig").File;
pub const Dir = @import("fs/lib.zig").Dir;
pub const Path = @import("fs/lib.zig").Path;
pub const Stat = @import("fs/lib.zig").Stat;

pub const Socket = @import("net/lib.zig").Socket;
pub const Stream = @import("stream.zig").Stream;

// Results
pub const AcceptResult = @import("aio/completion.zig").AcceptResult;
pub const ConnectResult = @import("aio/completion.zig").ConnectResult;
pub const RecvResult = @import("aio/completion.zig").RecvResult;
pub const SendResult = @import("aio/completion.zig").SendResult;
pub const OpenFileResult = @import("aio/completion.zig").OpenFileResult;
pub const OpenDirResult = @import("aio/completion.zig").OpenDirResult;
pub const ReadResult = @import("aio/completion.zig").ReadResult;
pub const WriteResult = @import("aio/completion.zig").WriteResult;
pub const StatResult = @import("aio/completion.zig").StatResult;
pub const CreateDirResult = @import("aio/completion.zig").CreateDirResult;
pub const DeleteResult = @import("aio/completion.zig").DeleteResult;
pub const DeleteTreeResult = @import("aio/completion.zig").DeleteTreeResult;

pub const ZeroCopy = @import("core/zero_copy.zig").ZeroCopy;
pub const Pool = @import("core/pool.zig").Pool;
pub const PoolKind = @import("core/pool.zig").PoolKind;
pub const Queue = @import("core/queue.zig").Queue;

/// Cross-platform abstractions.
/// For the `std.posix` interface types.
pub const Cross = @import("cross/lib.zig");

pub const auto_async_match = @import("aio/lib.zig").auto_async_match;
const async_to_type = @import("aio/lib.zig").async_to_type;
const AsyncIO = @import("aio/lib.zig").Async;
pub const AsyncType = @import("aio/lib.zig").AsyncType;
const AsyncOptions = @import("aio/lib.zig").AsyncOptions;
const Completion = @import("aio/completion.zig").Completion;

pub const TardyThreading = union(enum) {
    single,
    multi: usize,
    all,
    /// Calculated by `@max((cpu_count / 2) - 1, 1)`
    auto,
};

const TardyOptions = struct {
    /// Threading that Tardy runtime will use.
    ///
    /// Default = .auto
    threading: TardyThreading = .auto,
    /// Pooling Style
    ///
    /// By default (`.grow`), this means the internal pools
    /// will grow to fit however many tasks/async jobs
    /// you feed it until an OOM condition
    ///
    /// You can also set it to `.static` to lock the
    /// maximum number of tasks and aio jobs.
    ///
    /// Default = .grow
    pooling: PoolKind = .grow,
    /// Number of initial Tasks.
    ///
    /// If our pooling is grow, this will be the upper-limit
    /// before any allocations happen.
    ///
    /// If our pooling is static, this will be the maximum limit.
    ///
    /// Default: 1024
    size_tasks_initial: usize = 1024,
    /// Maximum number of aio completions we can reap
    /// with a single call of reap().
    ///
    /// Default: 1024
    size_aio_reap_max: usize = 1024,
};

pub fn Tardy(comptime selected_aio_type: AsyncType) type {
    const aio_type: AsyncType = comptime if (selected_aio_type == .auto)
        auto_async_match()
    else
        selected_aio_type;

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
            for (self.aios.items) |aio| self.allocator.destroy(aio);
            self.aios.deinit(self.allocator);
        }

        /// This will spawn a new Runtime.
        fn spawn_runtime(self: *Self, id: usize, options: AsyncOptions) !Runtime {
            self.mutex.lock();
            defer self.mutex.unlock();

            var aio: AsyncIO = blk: {
                var io = try self.allocator.create(AioInnerType);
                errdefer self.allocator.destroy(io);

                io.* = try AioInnerType.init(self.allocator, options);
                errdefer io.inner_deinit(self.allocator);

                try self.aios.append(self.allocator, io);
                var aio = io.to_async();

                const completions = try self.allocator.alloc(Completion, self.options.size_aio_reap_max);
                errdefer self.allocator.free(completions);

                aio.attach(completions);
                break :blk aio;
            };
            errdefer aio.deinit(self.allocator);

            return try Runtime.init(self.allocator, aio, .{
                .id = id,
                .pooling = self.options.pooling,
                .size_tasks_initial = self.options.size_tasks_initial,
                .size_aio_reap_max = self.options.size_aio_reap_max,
            });
        }

        /// This is the entry into all of the runtimes.
        ///
        /// The provided func needs to have a signature of (*Runtime, anytype) !void;
        ///
        /// The provided allocator is meant to just initialize any structures that will exist throughout the lifetime
        /// of the runtime. It happens in an arena and is cleaned up after the runtime terminates.
        pub fn entry(
            self: *Self,
            entry_params: anytype,
            comptime entry_func: *const fn (*Runtime, @TypeOf(entry_params)) anyerror!void,
        ) !void {
            const runtime_count: usize = switch (self.options.threading) {
                .single => 1,
                .multi => |count| count,
                .auto => @max(try std.Thread.getCpuCount() / 2 - 1, 1),
                .all => try std.Thread.getCpuCount(),
            };

            // for post-spawn syncing
            var spawned_count = Atomic(usize).init(0);
            const spawning_count = runtime_count - 1;

            var runtime = try self.spawn_runtime(0, .{
                .parent_async = null,
                .pooling = self.options.pooling,
                .size_tasks_initial = self.options.size_tasks_initial,
                .size_aio_reap_max = self.options.size_aio_reap_max,
            });
            defer runtime.deinit();

            assert(runtime_count > 0);
            log.info("thread count: {d}", .{runtime_count});

            var threads = try std.ArrayListUnmanaged(std.Thread).initCapacity(
                self.allocator,
                runtime_count -| 1,
            );
            defer {
                log.debug("waiting for the remaining threads to terminate", .{});
                for (threads.items) |thread| thread.join();
                threads.deinit(self.allocator);
            }
            // for in-spawn id assignment
            var spawn_id = Atomic(usize).init(1);

            for (0..spawning_count) |_| {
                const current_index = spawn_id.fetchAdd(1, .monotonic);
                const handle = try std.Thread.spawn(.{}, struct {
                    fn thread_init(
                        tardy: *Self,
                        options: TardyOptions,
                        parent: *AsyncIO,
                        entry_parameters: @TypeOf(entry_params),
                        count: *Atomic(usize),
                        total_count: usize,
                        current_id: usize,
                    ) void {
                        var thread_rt = tardy.spawn_runtime(current_id, .{
                            .parent_async = parent,
                            .pooling = options.pooling,
                            .size_tasks_initial = options.size_tasks_initial,
                            .size_aio_reap_max = options.size_aio_reap_max,
                        }) catch return;
                        defer thread_rt.deinit();

                        _ = count.fetchAdd(1, .acquire);
                        while (count.load(.acquire) < total_count) {}

                        @call(.auto, entry_func, .{ &thread_rt, entry_parameters }) catch |e| {
                            log.err("{d} - entry error={}", .{ thread_rt.id, e });
                            thread_rt.stop();
                        };

                        thread_rt.run() catch |e| log.err("{d} - runtime error={}", .{ thread_rt.id, e });

                        // wait for the rest to stop before cleaning ourselves up.
                        // this is because the runtime is allocate on our stack and others might be checking
                        // our running status or attempting to wake us.
                        _ = count.fetchSub(1, .acquire);
                        while (count.load(.acquire) > 0) std.time.sleep(std.time.ns_per_s);
                    }
                }.thread_init, .{
                    self,
                    self.options,
                    &runtime.aio,
                    entry_params,
                    &spawned_count,
                    spawning_count,
                    current_index,
                });

                threads.appendAssumeCapacity(handle);
            }

            while (spawned_count.load(.acquire) < spawning_count) {}
            log.debug("all runtimes spawned, initalizing...", .{});

            @call(.auto, entry_func, .{ &runtime, entry_params }) catch |e| {
                log.err("0 - entry error={}", .{e});
                runtime.stop();
            };
            runtime.run() catch |e| log.err("0 - runtime error={}", .{e});
        }

        /// This spawns in and enters into the runtime
        /// in a new Thread, allowing for more code to
        /// execute even after the runtime spawns.
        pub fn entry_in_new_thread(
            self: *Self,
            entry_params: anytype,
            comptime entry_func: *const fn (
                *Runtime,
                @TypeOf(entry_params),
            ) anyerror!void,
        ) !void {
            const handle = try std.Thread.spawn(.{}, struct {
                fn entry_in_new_thread(tardy: *Self, ip: @TypeOf(entry_params)) void {
                    tardy.entry(ip, entry_func) catch unreachable;
                }
            }.entry_in_new_thread, .{ self, entry_params });
            handle.detach();
        }
    };
}

pub const Timespec = struct {
    seconds: u64 = 0,
    nanos: u64 = 0,
};
