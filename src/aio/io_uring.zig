const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.@"tardy/aio/io_uring");

const Completion = @import("completion.zig").Completion;
const Result = @import("completion.zig").Result;

const AsyncIO = @import("lib.zig").AsyncIO;
const AsyncIOOptions = @import("lib.zig").AsyncIOOptions;

const Job = @import("job.zig").Job;
const Pool = @import("../core/pool.zig").Pool;

pub const AsyncIoUring = struct {
    const base_flags = blk: {
        var flags = 0;
        //flags |= std.os.linux.IORING_SETUP_COOP_TASKRUN;
        flags |= std.os.linux.IORING_SETUP_DEFER_TASKRUN;
        flags |= std.os.linux.IORING_SETUP_SINGLE_ISSUER;
        break :blk flags;
    };

    inner: *std.os.linux.IoUring,
    cqes: []std.os.linux.io_uring_cqe,
    jobs: Pool(Job),

    pub fn init(allocator: std.mem.Allocator, options: AsyncIOOptions) !AsyncIoUring {
        // with io_uring, our timeouts take up an additional slot in the ring.
        // this means if they are enabled, we need 2x the slots.
        const size = options.size_aio_jobs_max;

        const uring = blk: {
            if (options.parent_async) |parent| {
                const parent_uring: *AsyncIoUring = @ptrCast(
                    @alignCast(parent.runner),
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

        return AsyncIoUring{
            .inner = uring,
            .jobs = try Pool(Job).init(allocator, size, null, null),
            .cqes = try allocator.alloc(std.os.linux.io_uring_cqe, options.size_aio_reap_max),
        };
    }

    pub fn deinit(self: *AsyncIO, allocator: std.mem.Allocator) void {
        const uring: *AsyncIoUring = @ptrCast(@alignCast(self.runner));
        uring.inner.deinit();
        uring.jobs.deinit(null, null);
        allocator.free(uring.cqes);
        allocator.destroy(uring.inner);
    }

    pub fn queue_open(
        self: *AsyncIO,
        task: usize,
        path: []const u8,
    ) !void {
        const uring: *AsyncIoUring = @ptrCast(@alignCast(self.runner));
        const borrowed = try uring.jobs.borrow_hint(task);
        borrowed.item.* = .{
            .index = borrowed.index,
            .type = .{ .open = path },
            .task = task,
            .fd = undefined,
        };

        _ = try uring.inner.openat(
            @intFromPtr(borrowed.item),
            std.posix.AT.FDCWD,
            @ptrCast(path.ptr),
            .{},
            0,
        );
    }

    pub fn queue_read(
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
        buffer: []u8,
        offset: usize,
    ) !void {
        const uring: *AsyncIoUring = @ptrCast(@alignCast(self.runner));
        const borrowed = try uring.jobs.borrow_hint(task);
        borrowed.item.* = .{
            .index = borrowed.index,
            .type = .{ .read = .{ .buffer = buffer, .offset = offset } },
            .task = task,
            .fd = fd,
        };

        _ = try uring.inner.read(
            @intFromPtr(borrowed.item),
            fd,
            .{ .buffer = buffer },
            offset,
        );
    }

    pub fn queue_write(
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
        buffer: []const u8,
        offset: usize,
    ) !void {
        const uring: *AsyncIoUring = @ptrCast(@alignCast(self.runner));
        const borrowed = try uring.jobs.borrow_hint(task);
        borrowed.item.* = .{
            .index = borrowed.index,
            .type = .{ .write = .{ .buffer = buffer, .offset = offset } },
            .task = task,
            .fd = fd,
        };

        _ = try uring.inner.write(@intFromPtr(borrowed.item), fd, buffer, offset);
    }

    pub fn queue_close(
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
    ) !void {
        const uring: *AsyncIoUring = @ptrCast(@alignCast(self.runner));
        const borrowed = try uring.jobs.borrow_hint(task);
        borrowed.item.* = .{
            .index = borrowed.index,
            .type = .close,
            .task = task,
            .fd = fd,
        };
        _ = try uring.inner.close(@intFromPtr(borrowed.item), fd);
    }

    pub fn queue_accept(
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
    ) !void {
        const uring: *AsyncIoUring = @ptrCast(@alignCast(self.runner));
        const borrowed = try uring.jobs.borrow_hint(task);
        borrowed.item.* = .{
            .index = borrowed.index,
            .type = .accept,
            .task = task,
            .fd = fd,
        };

        _ = try uring.inner.accept(
            @intFromPtr(borrowed.item),
            fd,
            null,
            null,
            0,
        );
    }

    pub fn queue_connect(
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
        host: []const u8,
        port: u16,
    ) !void {
        const uring: *AsyncIoUring = @ptrCast(@alignCast(self.runner));
        const borrowed = try uring.jobs.borrow_hint(task);
        const addr = try std.net.Address.parseIp(host, port);

        borrowed.item.* = .{
            .index = borrowed.index,
            .type = .{ .connect = addr.any },
            .task = task,
            .fd = fd,
        };

        _ = try uring.inner.connect(
            @intFromPtr(borrowed.item),
            fd,
            &borrowed.item.type.connect,
            addr.getOsSockLen(),
        );
    }

    pub fn queue_recv(
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
        buffer: []u8,
    ) !void {
        const uring: *AsyncIoUring = @ptrCast(@alignCast(self.runner));
        const borrowed = try uring.jobs.borrow_hint(task);
        borrowed.item.* = .{
            .index = borrowed.index,
            .type = .{ .recv = buffer },
            .task = task,
            .fd = fd,
        };

        _ = try uring.inner.recv(
            @intFromPtr(borrowed.item),
            fd,
            .{ .buffer = buffer },
            0,
        );
    }

    pub fn queue_send(
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
        buffer: []const u8,
    ) !void {
        const uring: *AsyncIoUring = @ptrCast(@alignCast(self.runner));
        const borrowed = try uring.jobs.borrow_hint(task);
        borrowed.item.* = .{
            .index = borrowed.index,
            .type = .{ .send = buffer },
            .task = task,
            .fd = fd,
        };

        _ = try uring.inner.send(@intFromPtr(borrowed.item), fd, buffer, 0);
    }

    pub fn submit(self: *AsyncIO) !void {
        const uring: *AsyncIoUring = @ptrCast(@alignCast(self.runner));
        _ = try uring.inner.submit();
    }

    pub fn reap(self: *AsyncIO, min: usize) ![]Completion {
        const uring: *AsyncIoUring = @ptrCast(@alignCast(self.runner));

        var first_run: bool = true;
        var reaped: usize = 0;

        while (reaped < min or first_run) {
            const copy_count: usize = self.completions.len - reaped;
            const uring_nr: u32 = @intCast(@min(copy_count, min));
            const count = try uring.inner.copy_cqes(uring.cqes[0..copy_count], uring_nr);

            for (uring.cqes[0..count]) |cqe| {
                const job: *Job = @ptrFromInt(@as(usize, cqe.user_data));
                defer uring.jobs.release(job.index);

                const result: Result = blk: {
                    if (cqe.res >= 0) {
                        switch (job.type) {
                            .accept, .connect => break :blk .{ .socket = cqe.res },
                            .open => break :blk .{ .fd = cqe.res },
                            else => break :blk .{ .value = cqe.res },
                        }
                    } else {
                        log.debug("{d} - other status on SQE: {s}", .{
                            job.index,
                            @tagName(@as(std.os.linux.E, @enumFromInt(-cqe.res))),
                        });
                        break :blk .{ .value = cqe.res };
                    }
                };

                self.completions[reaped] = Completion{
                    .result = result,
                    .task = job.task,
                };
                reaped += 1;
            }

            first_run = false;
        }

        return self.completions[0..reaped];
    }

    pub fn to_async(self: *AsyncIoUring) AsyncIO {
        return AsyncIO{
            .runner = self,
            ._deinit = deinit,
            ._queue_open = queue_open,
            ._queue_read = queue_read,
            ._queue_write = queue_write,
            ._queue_close = queue_close,
            ._queue_accept = queue_accept,
            ._queue_connect = queue_connect,
            ._queue_recv = queue_recv,
            ._queue_send = queue_send,
            ._submit = submit,
            ._reap = reap,
        };
    }
};
