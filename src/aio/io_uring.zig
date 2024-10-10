const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.@"tardy/aio/io_uring");

const Completion = @import("completion.zig").Completion;
const Result = @import("completion.zig").Result;

const AsyncIO = @import("lib.zig").AsyncIO;
const AsyncIOError = @import("lib.zig").AsyncIOError;
const AsyncIOOptions = @import("lib.zig").AsyncIOOptions;

const Job = @import("job.zig").Job;
const Pool = @import("../core/pool.zig").Pool;

pub const AsyncIoUring = struct {
    const Self = @This();
    const base_flags = blk: {
        var flags = std.os.linux.IORING_SETUP_COOP_TASKRUN;
        flags |= std.os.linux.IORING_SETUP_SINGLE_ISSUER;
        break :blk flags;
    };

    inner: *std.os.linux.IoUring,
    cqes: []std.os.linux.io_uring_cqe,
    jobs: Pool(Job),

    pub fn init(allocator: std.mem.Allocator, options: AsyncIOOptions) !Self {
        // with io_uring, our timeouts take up an additional slot in the ring.
        // this means if they are enabled, we need 2x the slots.
        const size = options.size_connections_max;

        const uring = blk: {
            if (options.in_thread) {
                assert(options.root_async != null);
                const parent_uring: *Self = @ptrCast(
                    @alignCast(options.root_async.?.runner),
                );
                assert(parent_uring.inner.fd >= 0);

                // Initialize using the WQ from the parent ring.
                const flags: u32 = base_flags | std.os.linux.IORING_SETUP_ATTACH_WQ;

                var params = std.mem.zeroInit(std.os.linux.io_uring_params, .{
                    .flags = flags,
                    .wq_fd = @as(u32, @intCast(parent_uring.inner.fd)),
                });

                const uring = try allocator.create(std.os.linux.IoUring);
                uring.* = try std.os.linux.IoUring.init_params(
                    // TODO: determine if this needs to be doubled with timeouts.
                    std.math.ceilPowerOfTwoAssert(u16, size),
                    &params,
                );

                break :blk uring;
            } else {
                // Initalize IO Uring
                const uring = try allocator.create(std.os.linux.IoUring);
                uring.* = try std.os.linux.IoUring.init(
                    std.math.ceilPowerOfTwoAssert(u16, size),
                    base_flags,
                );

                break :blk uring;
            }
        };

        return Self{
            .inner = uring,
            .jobs = try Pool(Job).init(allocator, size, null, null),
            .cqes = try allocator.alloc(std.os.linux.io_uring_cqe, options.size_completions_reap_max),
        };
    }

    pub fn deinit(self: *AsyncIO, allocator: std.mem.Allocator) void {
        const uring: *Self = @ptrCast(@alignCast(self.runner));
        uring.inner.deinit();
        uring.jobs.deinit(null, null);
        allocator.free(uring.cqes);
        allocator.destroy(uring.inner);
    }

    pub fn queue_accept(
        self: *AsyncIO,
        task: usize,
        socket: std.posix.socket_t,
    ) AsyncIOError!void {
        const uring: *Self = @ptrCast(@alignCast(self.runner));
        const borrowed = uring.jobs.borrow() catch return AsyncIOError.QueueFull;
        borrowed.item.* = .{
            .index = borrowed.index,
            .type = .accept,
            .task = task,
            .socket = socket,
        };

        _ = uring.inner.accept(@intFromPtr(borrowed.item), socket, null, null, 0) catch |e| switch (e) {
            error.SubmissionQueueFull => return AsyncIOError.QueueFull,
            else => unreachable,
        };
    }

    pub fn queue_recv(
        self: *AsyncIO,
        task: usize,
        socket: std.posix.socket_t,
        buffer: []u8,
    ) AsyncIOError!void {
        const uring: *Self = @ptrCast(@alignCast(self.runner));
        const borrowed = uring.jobs.borrow() catch return AsyncIOError.QueueFull;
        borrowed.item.* = .{
            .index = borrowed.index,
            .type = .{ .recv = buffer },
            .task = task,
            .socket = socket,
        };

        _ = uring.inner.recv(@intFromPtr(borrowed.item), socket, .{ .buffer = buffer }, 0) catch |e| switch (e) {
            error.SubmissionQueueFull => return AsyncIOError.QueueFull,
            else => unreachable,
        };
    }

    pub fn queue_send(
        self: *AsyncIO,
        task: usize,
        socket: std.posix.socket_t,
        buffer: []const u8,
    ) AsyncIOError!void {
        const uring: *Self = @ptrCast(@alignCast(self.runner));
        const borrowed = uring.jobs.borrow() catch return AsyncIOError.QueueFull;
        borrowed.item.* = .{
            .index = borrowed.index,
            .type = .{ .send = buffer },
            .task = task,
            .socket = socket,
        };

        _ = uring.inner.send(@intFromPtr(borrowed.item), socket, buffer, 0) catch |e| switch (e) {
            error.SubmissionQueueFull => return AsyncIOError.QueueFull,
            else => unreachable,
        };
    }

    pub fn queue_close(
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
    ) AsyncIOError!void {
        const uring: *Self = @ptrCast(@alignCast(self.runner));
        const borrowed = uring.jobs.borrow() catch return AsyncIOError.QueueFull;
        borrowed.item.* = .{
            .index = borrowed.index,
            .type = .close,
            .task = task,
            .socket = fd,
        };
        _ = uring.inner.close(@intFromPtr(borrowed.item), fd) catch |e| switch (e) {
            error.SubmissionQueueFull => return AsyncIOError.QueueFull,
            else => unreachable,
        };
    }

    pub fn submit(self: *AsyncIO) AsyncIOError!void {
        const uring: *Self = @ptrCast(@alignCast(self.runner));
        _ = uring.inner.submit() catch |e| switch (e) {
            // TODO: match error states.
            else => unreachable,
        };
    }

    pub fn reap(self: *AsyncIO, min: usize) AsyncIOError![]Completion {
        const uring: *Self = @ptrCast(@alignCast(self.runner));

        const min_length = @min(uring.cqes.len, self.completions.len);
        const count = uring.inner.copy_cqes(uring.cqes[0..min_length], @intCast(min)) catch |e| switch (e) {
            // TODO: match error states.
            else => unreachable,
        };

        for (uring.cqes[0..count], 0..) |cqe, i| {
            const job: *Job = @ptrFromInt(@as(usize, cqe.user_data));
            defer uring.jobs.release(job.index);

            const result: Result = blk: {
                if (cqe.res < 0) {
                    log.debug("{d} - other status on SQE: {s}", .{
                        job.index,
                        @tagName(@as(std.os.linux.E, @enumFromInt(-cqe.res))),
                    });
                }

                if (job.type == .accept) {
                    break :blk .{ .socket = cqe.res };
                } else {
                    break :blk .{ .value = cqe.res };
                }
            };

            self.completions[i] = Completion{
                .result = result,
                .task = job.task,
            };
        }

        return self.completions[0..count];
    }

    pub fn to_async(self: *Self) AsyncIO {
        return AsyncIO{
            .runner = self,
            ._deinit = deinit,
            ._queue_accept = queue_accept,
            ._queue_recv = queue_recv,
            ._queue_send = queue_send,
            ._queue_close = queue_close,
            ._submit = submit,
            ._reap = reap,
        };
    }
};
