const std = @import("std");
const assert = std.debug.assert;
const builtin = @import("builtin");
const log = std.log.scoped(.@"tardy/aio/io_uring");

const Completion = @import("../completion.zig").Completion;
const Result = @import("../completion.zig").Result;
const Stat = @import("../../fs/lib.zig").Stat;

const Timespec = @import("../../lib.zig").Timespec;
const Path = @import("../../fs/lib.zig").Path;

const AsyncIO = @import("../lib.zig").AsyncIO;
const AsyncIOOptions = @import("../lib.zig").AsyncIOOptions;

const Job = @import("../job.zig").Job;
const Pool = @import("../../core/pool.zig").Pool;

const LinuxError = std.os.linux.E;

const InnerAcceptResult = @import("../completion.zig").InnerAcceptResult;
const AcceptError = @import("../completion.zig").AcceptError;
const AcceptKind = @import("../job.zig").AcceptKind;

const ConnectResult = @import("../completion.zig").ConnectResult;
const ConnectError = @import("../completion.zig").ConnectError;
const RecvResult = @import("../completion.zig").RecvResult;
const RecvError = @import("../completion.zig").RecvError;
const SendResult = @import("../completion.zig").SendResult;
const SendError = @import("../completion.zig").SendError;

const OpenError = @import("../completion.zig").OpenError;
const AioOpenFlags = @import("../lib.zig").AioOpenFlags;

const InnerOpenResult = @import("../completion.zig").InnerOpenResult;
const DeleteResult = @import("../completion.zig").DeleteResult;
const DeleteError = @import("../completion.zig").DeleteError;
const ReadResult = @import("../completion.zig").ReadResult;
const ReadError = @import("../completion.zig").ReadError;
const WriteResult = @import("../completion.zig").WriteResult;
const WriteError = @import("../completion.zig").WriteError;

const StatResult = @import("../completion.zig").StatResult;
const StatError = @import("../completion.zig").StatError;

const AsyncSubmission = @import("../lib.zig").AsyncSubmission;

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

        var jobs = try Pool(Job).init(allocator, size);

        const index = jobs.borrow_assume_unset(0);
        const item = jobs.get_ptr(index);
        item.* = .{ .index = index, .type = .wake, .task = undefined };
        _ = try uring.read(index, wake_event_fd, .{ .buffer = wake_event_buffer }, 0);

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
        uring.jobs.deinit();
        std.posix.close(uring.wake_event_fd);
        allocator.free(uring.wake_event_buffer);
        allocator.free(uring.cqes);
        allocator.free(uring.statx);
        allocator.free(uring.timespec);
        allocator.destroy(uring.inner);
    }

    pub fn queue_job(
        self: *AsyncIO,
        task: usize,
        job: AsyncSubmission,
    ) !void {
        const uring: *AsyncIoUring = @ptrCast(@alignCast(self.runner));
        log.debug("queuing up job: {s}", .{@tagName(job)});
        try switch (job) {
            .timer => |inner| queue_timer(uring, task, inner),
            .open => |inner| queue_open(uring, task, inner.path, inner.flags),
            .delete => |inner| queue_delete(uring, task, inner.path, inner.is_dir),
            .mkdir => |inner| queue_mkdir(uring, task, inner.path, inner.mode),
            .stat => |inner| queue_stat(uring, task, inner.fd),
            .read => |inner| queue_read(uring, task, inner.fd, inner.buffer, inner.offset),
            .write => |inner| queue_write(uring, task, inner.fd, inner.buffer, inner.offset),
            .close => |inner| queue_close(uring, task, inner),
            .accept => |inner| queue_accept(uring, task, inner.socket, inner.kind),
            .connect => |inner| queue_connect(uring, task, inner.socket, inner.host, inner.port),
            .recv => |inner| queue_recv(uring, task, inner.socket, inner.buffer),
            .send => |inner| queue_send(uring, task, inner.socket, inner.buffer),
        };
    }

    fn queue_timer(self: *AsyncIoUring, task: usize, timespec: Timespec) !void {
        const index = try self.jobs.borrow_hint(task);

        const item = self.jobs.get_ptr(index);
        item.* = .{
            .index = index,
            .task = task,
            .type = .{ .timer = .none },
        };

        const ktimespec = &self.timespec[index];
        ktimespec.* = .{
            .tv_sec = @intCast(timespec.seconds),
            .tv_nsec = @intCast(timespec.nanos),
        };

        _ = try self.inner.timeout(index, ktimespec, 0, 0);
    }

    fn queue_open(self: *AsyncIoUring, task: usize, path: Path, flags: AioOpenFlags) !void {
        const index = try self.jobs.borrow_hint(task);

        const item = self.jobs.get_ptr(index);
        item.* = .{
            .index = index,
            .type = .{
                .open = .{
                    .path = path,
                    .kind = if (flags.directory) .dir else .file,
                    .perms = flags.perms,
                },
            },
            .task = task,
        };

        const o_flags: std.os.linux.O = blk: {
            var o: std.os.linux.O = .{};

            switch (flags.mode) {
                .read => o.ACCMODE = .RDONLY,
                .write => o.ACCMODE = .WRONLY,
                .read_write => o.ACCMODE = .RDWR,
            }

            o.APPEND = flags.append;
            o.CREAT = flags.create;
            o.TRUNC = flags.truncate;
            o.EXCL = flags.exclusive;
            o.NONBLOCK = flags.non_block;
            o.SYNC = flags.sync;
            o.DIRECTORY = flags.directory;

            break :blk o;
        };

        const perms = flags.perms orelse 0;

        switch (path) {
            .rel => |inner| _ = try self.inner.openat(index, inner.dir, inner.path.ptr, o_flags, perms),
            .abs => |inner| _ = try self.inner.openat(index, std.posix.AT.FDCWD, inner.ptr, o_flags, perms),
        }
    }

    fn queue_delete(self: *AsyncIoUring, task: usize, path: Path, is_dir: bool) !void {
        const index = try self.jobs.borrow_hint(task);

        const item = self.jobs.get_ptr(index);
        item.* = .{ .index = index, .type = .{ .delete = path }, .task = task };

        const mode: u32 = if (is_dir) std.posix.AT.REMOVEDIR else 0;

        switch (path) {
            .rel => |inner| _ = try self.inner.unlinkat(index, inner.dir, inner.path.ptr, mode),
            .abs => |inner| _ = try self.inner.unlinkat(index, std.posix.AT.FDCWD, inner.ptr, mode),
        }
    }

    fn queue_mkdir(self: *AsyncIoUring, task: usize, path: Path, mode: std.posix.mode_t) !void {
        const index = try self.jobs.borrow_hint(task);

        const item = self.jobs.get_ptr(index);
        item.* = .{ .index = index, .type = .{ .mkdir = .{ .path = path, .mode = mode } }, .task = task };

        switch (path) {
            .rel => |inner| _ = try self.inner.mkdirat(index, inner.dir, inner.path.ptr, mode),
            .abs => |inner| _ = try self.inner.mkdirat(index, std.posix.AT.FDCWD, inner.ptr, mode),
        }
    }

    fn queue_stat(self: *AsyncIoUring, task: usize, fd: std.posix.fd_t) !void {
        const index = try self.jobs.borrow_hint(task);

        const item = self.jobs.get_ptr(index);
        item.* = .{ .index = index, .type = .{ .stat = fd }, .task = task };

        const statx = &self.statx[index];
        _ = try self.inner.statx(index, fd, "", std.os.linux.AT.EMPTY_PATH, 0, statx);
    }

    fn queue_read(self: *AsyncIoUring, task: usize, fd: std.posix.fd_t, buffer: []u8, offset: ?usize) !void {
        // If we don't have an offset, set it as -1.
        const real_offset: usize = if (offset) |o| o else @bitCast(@as(isize, -1));

        const index = try self.jobs.borrow_hint(task);
        const item = self.jobs.get_ptr(index);
        item.* = .{
            .index = index,
            .type = .{
                .read = .{
                    .fd = fd,
                    .buffer = buffer,
                    .offset = real_offset,
                },
            },
            .task = task,
        };

        _ = try self.inner.read(index, fd, .{ .buffer = buffer }, real_offset);
    }

    fn queue_write(self: *AsyncIoUring, task: usize, fd: std.posix.fd_t, buffer: []const u8, offset: ?usize) !void {
        // If we don't have an offset, set it as -1.
        const real_offset: usize = if (offset) |o| o else @bitCast(@as(isize, -1));

        const index = try self.jobs.borrow_hint(task);
        const item = self.jobs.get_ptr(index);
        item.* = .{
            .index = index,
            .type = .{
                .write = .{
                    .fd = fd,
                    .buffer = buffer,
                    .offset = real_offset,
                },
            },
            .task = task,
        };

        _ = try self.inner.write(index, fd, buffer, real_offset);
    }

    fn queue_close(self: *AsyncIoUring, task: usize, fd: std.posix.fd_t) !void {
        const index = try self.jobs.borrow_hint(task);

        const item = self.jobs.get_ptr(index);
        item.* = .{ .index = index, .type = .{ .close = fd }, .task = task };

        _ = try self.inner.close(index, fd);
    }

    fn queue_accept(self: *AsyncIoUring, task: usize, socket: std.posix.socket_t, kind: AcceptKind) !void {
        const index = try self.jobs.borrow_hint(task);

        const item = self.jobs.get_ptr(index);
        item.* = .{
            .index = index,
            .type = .{ .accept = .{ .socket = socket, .kind = kind } },
            .task = task,
        };

        _ = try self.inner.accept(index, socket, null, null, 0);
    }

    fn queue_connect(
        self: *AsyncIoUring,
        task: usize,
        socket: std.posix.socket_t,
        host: []const u8,
        port: u16,
    ) !void {
        const addr = try std.net.Address.parseIp(host, port);

        const index = try self.jobs.borrow_hint(task);
        const item = self.jobs.get_ptr(index);
        item.* = .{
            .index = index,
            .type = .{
                .connect = .{
                    .socket = socket,
                    .addr = addr,
                },
            },
            .task = task,
        };

        _ = try self.inner.connect(
            index,
            socket,
            &item.type.connect.addr.any,
            addr.getOsSockLen(),
        );
    }

    fn queue_recv(
        self: *AsyncIoUring,
        task: usize,
        socket: std.posix.socket_t,
        buffer: []u8,
    ) !void {
        const index = try self.jobs.borrow_hint(task);
        const item = self.jobs.get_ptr(index);
        item.* = .{
            .index = index,
            .type = .{
                .recv = .{
                    .socket = socket,
                    .buffer = buffer,
                },
            },
            .task = task,
        };

        _ = try self.inner.recv(index, socket, .{ .buffer = buffer }, 0);
    }

    fn queue_send(self: *AsyncIoUring, task: usize, socket: std.posix.socket_t, buffer: []const u8) !void {
        const index = try self.jobs.borrow_hint(task);

        const item = self.jobs.get_ptr(index);
        item.* = .{
            .index = index,
            .type = .{
                .send = .{
                    .socket = socket,
                    .buffer = buffer,
                },
            },
            .task = task,
        };

        _ = try self.inner.send(index, socket, buffer, 0);
    }

    inline fn queue_wake(self: *AsyncIoUring) !void {
        const index = try self.jobs.borrow();

        const item = self.jobs.get_ptr(index);
        item.* = .{
            .index = index,
            .type = .wake,
            .task = undefined,
        };

        _ = try self.inner.read(
            index,
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
            const job: *Job = uring.jobs.get_ptr(cqe.user_data);
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
                    .timer, .mkdir => break :blk .none,
                    .close => break :blk .close,
                    .accept => |inner| {
                        if (cqe.res >= 0) switch (inner.kind) {
                            .tcp => break :blk .{ .accept = .{ .actual = .{ .tcp = .{ .socket = cqe.res } } } },
                        };
                        const result: InnerAcceptResult = result: {
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
                                LinuxError.ACCES, LinuxError.PERM => break :result .{
                                    .err = ConnectError.AccessDenied,
                                },
                                LinuxError.ADDRINUSE => break :result .{ .err = ConnectError.AddressInUse },
                                LinuxError.ADDRNOTAVAIL => break :result .{ .err = ConnectError.AddressNotAvailable },
                                LinuxError.AFNOSUPPORT => break :result .{
                                    .err = ConnectError.AddressFamilyNotSupported,
                                },
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
                                LinuxError.PROTOTYPE => break :result .{
                                    .err = ConnectError.ProtocolFamilyNotSupported,
                                },
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
                    .open => |inner| {
                        if (cqe.res >= 0) switch (inner.kind) {
                            .file => break :blk .{ .open = .{ .actual = .{ .file = .{ .handle = @intCast(cqe.res) } } } },
                            .dir => break :blk .{ .open = .{ .actual = .{ .dir = .{ .handle = @intCast(cqe.res) } } } },
                        };

                        const result: InnerOpenResult = result: {
                            const e: LinuxError = @enumFromInt(-cqe.res);
                            switch (e) {
                                LinuxError.ACCES, LinuxError.PERM => break :result .{ .err = OpenError.AccessDenied },
                                LinuxError.BADF => break :result .{ .err = OpenError.InvalidFd },
                                LinuxError.BUSY => break :result .{ .err = OpenError.FileBusy },
                                LinuxError.DQUOT => break :result .{ .err = OpenError.DiskQuotaExceeded },
                                LinuxError.EXIST => break :result .{ .err = OpenError.FileAlreadyExists },
                                LinuxError.FAULT => break :result .{ .err = OpenError.InvalidAddress },
                                LinuxError.FBIG, LinuxError.OVERFLOW => break :result .{
                                    .err = OpenError.FileTooBig,
                                },
                                LinuxError.INTR => break :result .{ .err = OpenError.Interrupted },
                                LinuxError.INVAL => break :result .{ .err = OpenError.InvalidArguments },
                                LinuxError.ISDIR => break :result .{ .err = OpenError.IsDirectory },
                                LinuxError.LOOP => break :result .{ .err = OpenError.Loop },
                                LinuxError.MFILE => break :result .{ .err = OpenError.ProcessFdQuotaExceeded },
                                LinuxError.NAMETOOLONG => break :result .{ .err = OpenError.NameTooLong },
                                LinuxError.NFILE => break :result .{ .err = OpenError.SystemFdQuotaExceeded },
                                LinuxError.NODEV, LinuxError.NXIO => break :result .{
                                    .err = OpenError.DeviceNotFound,
                                },
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
                    .delete => {
                        if (cqe.res > 0) break :blk .{ .delete = .{ .actual = {} } };

                        const result: DeleteResult = result: {
                            const e: LinuxError = @enumFromInt(-cqe.res);
                            switch (e) {
                                // unlink
                                LinuxError.ACCES => break :result .{ .err = error.AccessDenied },
                                LinuxError.BUSY => break :result .{ .err = error.Busy },
                                LinuxError.FAULT => break :result .{ .err = error.InvalidAddress },
                                LinuxError.IO => break :result .{ .err = error.IoError },
                                LinuxError.ISDIR, LinuxError.PERM => break :result .{ .err = error.IsDirectory },
                                LinuxError.LOOP => break :result .{ .err = error.Loop },
                                LinuxError.NAMETOOLONG => break :result .{ .err = error.NameTooLong },
                                LinuxError.NOENT => break :result .{ .err = error.NotFound },
                                LinuxError.NOMEM => break :result .{ .err = error.OutOfMemory },
                                LinuxError.NOTDIR => break :result .{ .err = error.IsNotDirectory },
                                LinuxError.ROFS => break :result .{ .err = error.ReadOnlyFileSystem },
                                // rmdir
                                LinuxError.INVAL => break :result .{ .err = error.InvalidArguments },
                                LinuxError.NOTEMPTY => break :result .{ .err = error.NotEmpty },
                                else => break :result .{ .err = error.Unexpected },
                            }
                        };

                        break :blk .{ .delete = result };
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
            ._queue_job = queue_job,
            ._wake = wake,
            ._submit = submit,
            ._reap = reap,
        };
    }
};
