const std = @import("std");
const assert = std.debug.assert;
const builtin = @import("builtin");
const log = std.log.scoped(.@"tardy/aio/io_uring");

const Completion = @import("../completion.zig").Completion;
const Result = @import("../completion.zig").Result;
const Stat = @import("../completion.zig").Stat;
const Timespec = @import("../timespec.zig").Timespec;

const AsyncIO = @import("../lib.zig").AsyncIO;
const AsyncIOOptions = @import("../lib.zig").AsyncIOOptions;

const Job = @import("../job.zig").Job;
const Pool = @import("../../core/pool.zig").Pool;

const LinuxError = std.os.linux.E;

const AcceptResult = @import("../completion.zig").AcceptResult;
const AcceptError = @import("../completion.zig").AcceptError;
const ConnectResult = @import("../completion.zig").ConnectResult;
const ConnectError = @import("../completion.zig").ConnectError;
const RecvResult = @import("../completion.zig").RecvResult;
const RecvError = @import("../completion.zig").RecvError;
const SendResult = @import("../completion.zig").SendResult;
const SendError = @import("../completion.zig").SendError;

const OpenResult = @import("../completion.zig").OpenResult;
const OpenError = @import("../completion.zig").OpenError;
const ReadResult = @import("../completion.zig").ReadResult;
const ReadError = @import("../completion.zig").ReadError;
const WriteResult = @import("../completion.zig").WriteResult;
const WriteError = @import("../completion.zig").WriteError;

const StatResult = @import("../completion.zig").StatResult;
const StatError = @import("../completion.zig").StatError;

pub const AsyncIoUring = struct {
    const base_flags = blk: {
        var flags = 0;
        const version = builtin.target.os.getVersionRange().linux;

        // If you are building for musl, you won't have access to these flags.
        // This means you will run with no flags for compatibility reasons.

        // SINGLE_ISSUER requires 6.0
        if (version.isAtLeast(.{ .major = 6, .minor = 0, .patch = 0 })) |is_atleast| {
            if (is_atleast) flags |= std.os.linux.IORING_SETUP_SINGLE_ISSUER;
        }

        // COOP_TASKRUN requires 5.19
        if (version.isAtLeast(.{ .major = 5, .minor = 19, .patch = 0 })) |is_atleast| {
            if (is_atleast) flags |= std.os.linux.IORING_SETUP_COOP_TASKRUN;
        }

        break :blk flags;
    };

    inner: *std.os.linux.IoUring,
    wake_event_fd: std.posix.fd_t,
    wake_event_buffer: []u8,
    cqes: []std.os.linux.io_uring_cqe,
    statx: []std.os.linux.Statx,
    timespec: []std.os.linux.kernel_timespec,
    jobs: Pool(Job),

    pub fn init(allocator: std.mem.Allocator, options: AsyncIOOptions) !AsyncIoUring {
        // Extra job for the wake event_fd.
        const size = options.size_aio_jobs_max + 1;
        const wake_event_fd: std.posix.fd_t = @intCast(std.os.linux.eventfd(0, std.os.linux.EFD.CLOEXEC));
        const wake_event_buffer = try allocator.alloc(u8, 8);

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
                    std.math.ceilPowerOfTwoAssert(u16, @truncate(size)),
                    &params,
                );

                break :blk uring;
            } else {
                // Initalize IO Uring
                const uring = try allocator.create(std.os.linux.IoUring);
                uring.* = try std.os.linux.IoUring.init(
                    std.math.ceilPowerOfTwoAssert(u16, @truncate(size)),
                    base_flags,
                );

                break :blk uring;
            }
        };

        var jobs = try Pool(Job).init(allocator, size, null, null);

        {
            const borrowed = jobs.borrow_assume_unset(0);
            borrowed.item.* = .{
                .index = borrowed.index,
                .type = .wake,
                .task = undefined,
            };

            _ = try uring.read(
                @intFromPtr(borrowed.item),
                wake_event_fd,
                .{ .buffer = wake_event_buffer },
                0,
            );
        }

        return AsyncIoUring{
            .inner = uring,
            .wake_event_fd = wake_event_fd,
            .wake_event_buffer = wake_event_buffer,
            .jobs = jobs,
            .cqes = try allocator.alloc(
                std.os.linux.io_uring_cqe,
                options.size_aio_reap_max,
            ),
            .statx = try allocator.alloc(
                std.os.linux.Statx,
                options.size_aio_jobs_max,
            ),
            .timespec = try allocator.alloc(
                std.os.linux.kernel_timespec,
                options.size_aio_jobs_max,
            ),
        };
    }

    pub fn deinit(self: *AsyncIO, allocator: std.mem.Allocator) void {
        const uring: *AsyncIoUring = @ptrCast(@alignCast(self.runner));
        uring.inner.deinit();
        uring.jobs.deinit(null, null);
        std.posix.close(uring.wake_event_fd);
        allocator.free(uring.wake_event_buffer);
        allocator.free(uring.cqes);
        allocator.free(uring.statx);
        allocator.free(uring.timespec);
        allocator.destroy(uring.inner);
    }

    pub fn queue_timer(
        self: *AsyncIO,
        task: usize,
        timespec: Timespec,
    ) !void {
        const uring: *AsyncIoUring = @ptrCast(@alignCast(self.runner));
        const borrowed = try uring.jobs.borrow_hint(task);
        borrowed.item.* = .{
            .index = borrowed.index,
            .task = task,
            .type = .{ .timer = .none },
        };

        const ktimespec = &uring.timespec[borrowed.index];
        ktimespec.* = .{
            .tv_sec = @intCast(timespec.seconds),
            .tv_nsec = @intCast(timespec.nanos),
        };

        _ = try uring.inner.timeout(@intFromPtr(borrowed.item), ktimespec, 0, 0);
    }

    pub fn queue_open(
        self: *AsyncIO,
        task: usize,
        path: [:0]const u8,
    ) !void {
        const uring: *AsyncIoUring = @ptrCast(@alignCast(self.runner));
        const borrowed = try uring.jobs.borrow_hint(task);
        borrowed.item.* = .{
            .index = borrowed.index,
            .type = .{ .open = path },
            .task = task,
        };

        _ = try uring.inner.openat(
            @intFromPtr(borrowed.item),
            std.posix.AT.FDCWD,
            @ptrCast(path.ptr),
            .{},
            0,
        );
    }

    pub fn queue_stat(
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
    ) !void {
        const uring: *AsyncIoUring = @ptrCast(@alignCast(self.runner));
        const borrowed = try uring.jobs.borrow_hint(task);
        borrowed.item.* = .{
            .index = borrowed.index,
            .type = .{ .stat = fd },
            .task = task,
        };

        const statx = &uring.statx[borrowed.index];

        _ = try uring.inner.statx(
            @intFromPtr(borrowed.item),
            fd,
            "",
            std.os.linux.AT.EMPTY_PATH,
            0,
            statx,
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
            .type = .{
                .read = .{
                    .fd = fd,
                    .buffer = buffer,
                    .offset = offset,
                },
            },
            .task = task,
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
            .type = .{
                .write = .{
                    .fd = fd,
                    .buffer = buffer,
                    .offset = offset,
                },
            },
            .task = task,
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
            .type = .{ .close = fd },
            .task = task,
        };
        _ = try uring.inner.close(@intFromPtr(borrowed.item), fd);
    }

    pub fn queue_accept(
        self: *AsyncIO,
        task: usize,
        socket: std.posix.socket_t,
    ) !void {
        const uring: *AsyncIoUring = @ptrCast(@alignCast(self.runner));
        const borrowed = try uring.jobs.borrow_hint(task);
        borrowed.item.* = .{
            .index = borrowed.index,
            .type = .{ .accept = socket },
            .task = task,
        };

        _ = try uring.inner.accept(
            @intFromPtr(borrowed.item),
            socket,
            null,
            null,
            0,
        );
    }

    pub fn queue_connect(
        self: *AsyncIO,
        task: usize,
        socket: std.posix.socket_t,
        host: []const u8,
        port: u16,
    ) !void {
        const uring: *AsyncIoUring = @ptrCast(@alignCast(self.runner));
        const borrowed = try uring.jobs.borrow_hint(task);
        const addr = try std.net.Address.parseIp(host, port);

        borrowed.item.* = .{
            .index = borrowed.index,
            .type = .{
                .connect = .{
                    .socket = socket,
                    .addr = addr,
                },
            },
            .task = task,
        };

        _ = try uring.inner.connect(
            @intFromPtr(borrowed.item),
            socket,
            &borrowed.item.type.connect.addr.any,
            addr.getOsSockLen(),
        );
    }

    pub fn queue_recv(
        self: *AsyncIO,
        task: usize,
        socket: std.posix.socket_t,
        buffer: []u8,
    ) !void {
        const uring: *AsyncIoUring = @ptrCast(@alignCast(self.runner));
        const borrowed = try uring.jobs.borrow_hint(task);
        borrowed.item.* = .{
            .index = borrowed.index,
            .type = .{
                .recv = .{
                    .socket = socket,
                    .buffer = buffer,
                },
            },
            .task = task,
        };

        _ = try uring.inner.recv(
            @intFromPtr(borrowed.item),
            socket,
            .{ .buffer = buffer },
            0,
        );
    }

    pub fn queue_send(
        self: *AsyncIO,
        task: usize,
        socket: std.posix.socket_t,
        buffer: []const u8,
    ) !void {
        const uring: *AsyncIoUring = @ptrCast(@alignCast(self.runner));
        const borrowed = try uring.jobs.borrow_hint(task);
        borrowed.item.* = .{
            .index = borrowed.index,
            .type = .{
                .send = .{
                    .socket = socket,
                    .buffer = buffer,
                },
            },
            .task = task,
        };

        _ = try uring.inner.send(@intFromPtr(borrowed.item), socket, buffer, 0);
    }

    inline fn queue_wake(self: *AsyncIoUring) !void {
        const borrowed = try self.jobs.borrow();

        borrowed.item.* = .{
            .index = borrowed.index,
            .type = .wake,
            .task = undefined,
        };

        _ = try self.inner.read(
            @intFromPtr(borrowed.item),
            self.wake_event_fd,
            .{ .buffer = self.wake_event_buffer },
            0,
        );
    }

    pub fn wake(self: *AsyncIO) !void {
        const uring: *AsyncIoUring = @ptrCast(@alignCast(self.runner));
        const bytes: []const u8 = "00000000";
        var i: usize = 0;
        while (i < bytes.len) {
            i += try std.posix.write(uring.wake_event_fd, bytes);
        }
    }

    pub fn submit(self: *AsyncIO) !void {
        const uring: *AsyncIoUring = @ptrCast(@alignCast(self.runner));
        _ = try uring.inner.submit();
    }

    pub fn reap(self: *AsyncIO, wait: bool) ![]Completion {
        const uring: *AsyncIoUring = @ptrCast(@alignCast(self.runner));
        // either wait for atleast 1 or just take whats there.
        const uring_nr: u32 = if (wait) 1 else 0;
        const count = try uring.inner.copy_cqes(uring.cqes[0..], uring_nr);

        for (uring.cqes[0..count], 0..) |cqe, i| {
            const job: *Job = @ptrFromInt(@as(usize, cqe.user_data));
            defer uring.jobs.release(job.index);

            const result: Result = blk: {
                if (cqe.res < 0) {
                    log.debug("{d} - other status on SQE: {s}", .{
                        job.index,
                        @tagName(@as(LinuxError, @enumFromInt(-cqe.res))),
                    });
                }
                switch (job.type) {
                    .wake => {
                        try uring.queue_wake();
                        break :blk .wake;
                    },
                    .timer => break :blk .none,
                    .accept => {
                        if (cqe.res >= 0) break :blk .{ .accept = .{ .actual = cqe.res } };
                        const result: AcceptResult = result: {
                            const e: LinuxError = @enumFromInt(-cqe.res);
                            switch (e) {
                                LinuxError.AGAIN => break :result .{ .err = AcceptError.WouldBlock },
                                LinuxError.BADF => break :result .{ .err = AcceptError.InvalidFd },
                                LinuxError.CONNABORTED => break :result .{ .err = AcceptError.ConnectionAborted },
                                LinuxError.FAULT => break :result .{ .err = AcceptError.InvalidAddress },
                                LinuxError.INTR => break :result .{ .err = AcceptError.Interrupted },
                                LinuxError.INVAL => break :result .{ .err = AcceptError.NotListening },
                                LinuxError.MFILE => break :result .{ .err = AcceptError.ProcessFdQuotaExceeded },
                                LinuxError.NFILE => break :result .{ .err = AcceptError.SystemFdQuotaExceeded },
                                LinuxError.NOBUFS, LinuxError.NOMEM => {
                                    break :result .{ .err = AcceptError.OutOfMemory };
                                },
                                LinuxError.NOTSOCK => break :result .{ .err = AcceptError.NotASocket },
                                LinuxError.OPNOTSUPP => break :result .{ .err = AcceptError.OperationNotSupported },
                                else => break :result .{ .err = AcceptError.Unexpected },
                            }
                        };

                        break :blk .{ .accept = result };
                    },
                    .connect => |inner| {
                        if (cqe.res >= 0) break :blk .{ .connect = .{ .actual = inner.socket } };
                        const result: ConnectResult = result: {
                            const e: LinuxError = @enumFromInt(-cqe.res);
                            switch (e) {
                                LinuxError.ACCES, LinuxError.PERM => break :result .{ .err = ConnectError.AccessDenied },
                                LinuxError.ADDRINUSE => break :result .{ .err = ConnectError.AddressInUse },
                                LinuxError.ADDRNOTAVAIL => break :result .{ .err = ConnectError.AddressNotAvailable },
                                LinuxError.AFNOSUPPORT => break :result .{ .err = ConnectError.AddressFamilyNotSupported },
                                LinuxError.AGAIN, LinuxError.ALREADY, LinuxError.INPROGRESS => {
                                    break :result .{ .err = ConnectError.WouldBlock };
                                },
                                LinuxError.BADF => break :result .{ .err = ConnectError.InvalidFd },
                                LinuxError.CONNREFUSED => break :result .{ .err = ConnectError.ConnectionRefused },
                                LinuxError.FAULT => break :result .{ .err = ConnectError.InvalidAddress },
                                LinuxError.INTR => break :result .{ .err = ConnectError.Interrupted },
                                LinuxError.ISCONN => break :result .{ .err = ConnectError.AlreadyConnected },
                                LinuxError.NETUNREACH => break :result .{ .err = ConnectError.NetworkUnreachable },
                                LinuxError.NOTSOCK => break :result .{ .err = ConnectError.NotASocket },
                                LinuxError.PROTOTYPE => break :result .{ .err = ConnectError.ProtocolFamilyNotSupported },
                                LinuxError.TIMEDOUT => break :result .{ .err = ConnectError.TimedOut },
                                else => break :result .{ .err = ConnectError.Unexpected },
                            }
                        };

                        break :blk .{ .connect = result };
                    },
                    .recv => {
                        if (cqe.res > 0) break :blk .{ .recv = .{ .actual = @intCast(cqe.res) } };
                        if (cqe.res == 0) break :blk .{ .recv = .{ .err = RecvError.Closed } };
                        const result: RecvResult = result: {
                            const e: LinuxError = @enumFromInt(-cqe.res);
                            switch (e) {
                                LinuxError.AGAIN => break :result .{ .err = RecvError.WouldBlock },
                                LinuxError.BADF => break :result .{ .err = RecvError.InvalidFd },
                                LinuxError.CONNREFUSED => break :result .{ .err = RecvError.ConnectionRefused },
                                LinuxError.FAULT => break :result .{ .err = RecvError.InvalidAddress },
                                LinuxError.INTR => break :result .{ .err = RecvError.Interrupted },
                                LinuxError.INVAL => break :result .{ .err = RecvError.InvalidArguments },
                                LinuxError.NOMEM => break :result .{ .err = RecvError.OutOfMemory },
                                LinuxError.NOTCONN => break :result .{ .err = RecvError.NotConnected },
                                LinuxError.NOTSOCK => break :result .{ .err = RecvError.NotASocket },
                                else => break :result .{ .err = RecvError.Unexpected },
                            }
                        };

                        break :blk .{ .recv = result };
                    },
                    .send => {
                        if (cqe.res >= 0) break :blk .{ .send = .{ .actual = @intCast(cqe.res) } };
                        const result: SendResult = result: {
                            const e: LinuxError = @enumFromInt(-cqe.res);
                            switch (e) {
                                LinuxError.ACCES => break :result .{ .err = SendError.AccessDenied },
                                LinuxError.AGAIN => break :result .{ .err = SendError.WouldBlock },
                                LinuxError.ALREADY => break :result .{ .err = SendError.OpenInProgress },
                                LinuxError.BADF => break :result .{ .err = SendError.InvalidFd },
                                LinuxError.CONNRESET => break :result .{ .err = SendError.ConnectionReset },
                                LinuxError.DESTADDRREQ => break :result .{ .err = SendError.NoDestinationAddress },
                                LinuxError.FAULT => break :result .{ .err = SendError.InvalidAddress },
                                LinuxError.INTR => break :result .{ .err = SendError.Interrupted },
                                LinuxError.INVAL => break :result .{ .err = SendError.InvalidArguments },
                                LinuxError.ISCONN => break :result .{ .err = SendError.AlreadyConnected },
                                LinuxError.MSGSIZE => break :result .{ .err = SendError.InvalidSize },
                                LinuxError.NOBUFS, LinuxError.NOMEM => {
                                    break :result .{ .err = SendError.OutOfMemory };
                                },
                                LinuxError.NOTCONN => break :result .{ .err = SendError.NotConnected },
                                LinuxError.OPNOTSUPP => break :result .{ .err = SendError.OperationNotSupported },
                                LinuxError.PIPE => break :result .{ .err = SendError.BrokenPipe },
                                else => break :result .{ .err = SendError.Unexpected },
                            }
                        };

                        break :blk .{ .send = result };
                    },
                    .open => {
                        if (cqe.res >= 0) break :blk .{ .open = .{ .actual = @intCast(cqe.res) } };
                        const result: OpenResult = result: {
                            const e: LinuxError = @enumFromInt(-cqe.res);
                            switch (e) {
                                LinuxError.ACCES, LinuxError.PERM => break :result .{ .err = OpenError.AccessDenied },
                                LinuxError.BADF => break :result .{ .err = OpenError.InvalidFd },
                                LinuxError.BUSY => break :result .{ .err = OpenError.FileBusy },
                                LinuxError.DQUOT => break :result .{ .err = OpenError.DiskQuotaExceeded },
                                LinuxError.EXIST => break :result .{ .err = OpenError.FileAlreadyExists },
                                LinuxError.FAULT => break :result .{ .err = OpenError.InvalidAddress },
                                LinuxError.FBIG, LinuxError.OVERFLOW => break :result .{ .err = OpenError.FileTooBig },
                                LinuxError.INTR => break :result .{ .err = OpenError.Interrupted },
                                LinuxError.INVAL => break :result .{ .err = OpenError.InvalidArguments },
                                LinuxError.ISDIR => break :result .{ .err = OpenError.IsDirectory },
                                LinuxError.LOOP => break :result .{ .err = OpenError.Loop },
                                LinuxError.MFILE => break :result .{ .err = OpenError.ProcessFdQuotaExceeded },
                                LinuxError.NAMETOOLONG => break :result .{ .err = OpenError.NameTooLong },
                                LinuxError.NFILE => break :result .{ .err = OpenError.SystemFdQuotaExceeded },
                                LinuxError.NODEV, LinuxError.NXIO => break :result .{ .err = OpenError.DeviceNotFound },
                                LinuxError.NOENT => break :result .{ .err = OpenError.FileNotFound },
                                LinuxError.NOMEM => break :result .{ .err = OpenError.OutOfMemory },
                                LinuxError.NOSPC => break :result .{ .err = OpenError.NoSpace },
                                LinuxError.NOTDIR => break :result .{ .err = OpenError.NotADirectory },
                                LinuxError.OPNOTSUPP => break :result .{ .err = OpenError.OperationNotSupported },
                                LinuxError.ROFS => break :result .{ .err = OpenError.ReadOnlyFileSystem },
                                LinuxError.TXTBSY => break :result .{ .err = OpenError.FileLocked },
                                LinuxError.AGAIN => break :result .{ .err = OpenError.WouldBlock },
                                else => break :result .{ .err = OpenError.Unexpected },
                            }
                        };

                        break :blk .{ .open = result };
                    },
                    .read => {
                        if (cqe.res > 0) break :blk .{ .read = .{ .actual = @intCast(cqe.res) } };
                        if (cqe.res == 0) break :blk .{ .read = .{ .err = ReadError.EndOfFile } };
                        const result: ReadResult = result: {
                            const e: LinuxError = @enumFromInt(-cqe.res);
                            switch (e) {
                                LinuxError.AGAIN => break :result .{ .err = ReadError.WouldBlock },
                                LinuxError.BADF => break :result .{ .err = ReadError.InvalidFd },
                                LinuxError.FAULT => break :result .{ .err = ReadError.InvalidAddress },
                                LinuxError.INTR => break :result .{ .err = ReadError.Interrupted },
                                LinuxError.INVAL => break :result .{ .err = ReadError.InvalidArguments },
                                LinuxError.IO => break :result .{ .err = ReadError.IoError },
                                LinuxError.ISDIR => break :result .{ .err = ReadError.IsDirectory },
                                else => break :result .{ .err = ReadError.Unexpected },
                            }
                        };

                        break :blk .{ .read = result };
                    },
                    .write => {
                        if (cqe.res > 0) break :blk .{ .write = .{ .actual = @intCast(cqe.res) } };
                        const result: WriteResult = result: {
                            const e: LinuxError = @enumFromInt(-cqe.res);
                            switch (e) {
                                LinuxError.AGAIN => break :result .{ .err = WriteError.WouldBlock },
                                LinuxError.BADF => break :result .{ .err = WriteError.InvalidFd },
                                LinuxError.DESTADDRREQ => break :result .{ .err = WriteError.NoDestinationAddress },
                                LinuxError.DQUOT => break :result .{ .err = WriteError.DiskQuotaExceeded },
                                LinuxError.FAULT => break :result .{ .err = WriteError.InvalidAddress },
                                LinuxError.FBIG => break :result .{ .err = WriteError.FileTooBig },
                                LinuxError.INTR => break :result .{ .err = WriteError.Interrupted },
                                LinuxError.INVAL => break :result .{ .err = WriteError.InvalidArguments },
                                LinuxError.IO => break :result .{ .err = WriteError.IoError },
                                LinuxError.NOSPC => break :result .{ .err = WriteError.NoSpace },
                                LinuxError.PERM => break :result .{ .err = WriteError.AccessDenied },
                                LinuxError.PIPE => break :result .{ .err = WriteError.BrokenPipe },
                                else => break :result .{ .err = WriteError.Unexpected },
                            }
                        };

                        break :blk .{ .write = result };
                    },
                    .close => break :blk .close,
                    .stat => {
                        if (cqe.res >= 0) {
                            const statx = &uring.statx[job.index];
                            const stat: Stat = .{
                                .size = @intCast(statx.size),
                                .mode = @intCast(statx.mode),
                                .accessed = .{
                                    .seconds = @intCast(statx.atime.tv_sec),
                                    .nanos = @intCast(statx.atime.tv_nsec),
                                },
                                .modified = .{
                                    .seconds = @intCast(statx.mtime.tv_sec),
                                    .nanos = @intCast(statx.mtime.tv_nsec),
                                },
                                .changed = .{
                                    .seconds = @intCast(statx.ctime.tv_sec),
                                    .nanos = @intCast(statx.ctime.tv_nsec),
                                },
                            };
                            break :blk .{ .stat = .{ .actual = stat } };
                        }

                        const result: StatResult = result: {
                            const e: LinuxError = @enumFromInt(-cqe.res);
                            switch (e) {
                                LinuxError.ACCES => break :result .{ .err = StatError.AccessDenied },
                                LinuxError.BADF => break :result .{ .err = StatError.InvalidFd },
                                LinuxError.FAULT => break :result .{ .err = StatError.InvalidAddress },
                                LinuxError.INVAL => break :result .{ .err = StatError.InvalidArguments },
                                LinuxError.LOOP => break :result .{ .err = StatError.Loop },
                                LinuxError.NAMETOOLONG => break :result .{ .err = StatError.NameTooLong },
                                LinuxError.NOENT => break :result .{ .err = StatError.FileNotFound },
                                LinuxError.NOMEM => break :result .{ .err = StatError.OutOfMemory },
                                LinuxError.NOTDIR => break :result .{ .err = StatError.NotADirectory },
                                else => break :result .{ .err = StatError.Unexpected },
                            }
                        };

                        break :blk .{ .stat = result };
                    },
                }
            };

            self.completions[i] = Completion{
                .result = result,
                .task = job.task,
            };
        }

        return self.completions[0..count];
    }

    pub fn to_async(self: *AsyncIoUring) AsyncIO {
        return AsyncIO{
            .runner = self,
            ._deinit = deinit,
            ._queue_timer = queue_timer,
            ._queue_open = queue_open,
            ._queue_stat = queue_stat,
            ._queue_read = queue_read,
            ._queue_write = queue_write,
            ._queue_close = queue_close,
            ._queue_accept = queue_accept,
            ._queue_connect = queue_connect,
            ._queue_recv = queue_recv,
            ._queue_send = queue_send,
            ._wake = wake,
            ._submit = submit,
            ._reap = reap,
        };
    }
};
