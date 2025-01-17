const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.@"tardy/aio/epoll");

const Completion = @import("../completion.zig").Completion;
const Result = @import("../completion.zig").Result;
const Stat = @import("../../fs/lib.zig").Stat;
const Timespec = @import("../../lib.zig").Timespec;

const AsyncIO = @import("../lib.zig").AsyncIO;
const AsyncIOOptions = @import("../lib.zig").AsyncIOOptions;
const Job = @import("../job.zig").Job;
const Pool = @import("../../core/pool.zig").Pool;

const Path = @import("../../fs/lib.zig").Path;
const Socket = @import("../../net/lib.zig").Socket;

const Cross = @import("../../cross/lib.zig");
const AsyncSubmission = @import("../lib.zig").AsyncSubmission;

const LinuxError = std.os.linux.E;

const InnerOpenResult = @import("../completion.zig").InnerOpenResult;
const OpenError = @import("../completion.zig").OpenError;
const AioOpenFlags = @import("../lib.zig").AioOpenFlags;

const AcceptResult = @import("../completion.zig").AcceptResult;
const AcceptError = @import("../completion.zig").AcceptError;
const ConnectResult = @import("../completion.zig").ConnectResult;
const ConnectError = @import("../completion.zig").ConnectError;
const RecvResult = @import("../completion.zig").RecvResult;
const RecvError = @import("../completion.zig").RecvError;
const SendResult = @import("../completion.zig").SendResult;
const SendError = @import("../completion.zig").SendError;

const MkdirResult = @import("../completion.zig").MkdirResult;
const MkdirError = @import("../completion.zig").MkdirError;
const DeleteResult = @import("../completion.zig").DeleteResult;
const DeleteError = @import("../completion.zig").DeleteError;
const ReadResult = @import("../completion.zig").ReadResult;
const ReadError = @import("../completion.zig").ReadError;
const WriteResult = @import("../completion.zig").WriteResult;
const WriteError = @import("../completion.zig").WriteError;
const StatResult = @import("../completion.zig").StatResult;
const StatError = @import("../completion.zig").StatError;

pub const AsyncEpoll = struct {
    epoll_fd: std.posix.fd_t,
    wake_event_fd: std.posix.fd_t,
    events: []std.os.linux.epoll_event,

    jobs: Pool(Job),
    // This is for jobs that are not supported and need to be blocking.
    blocking: std.ArrayList(usize),

    pub fn init(allocator: std.mem.Allocator, options: AsyncIOOptions) !AsyncEpoll {
        const size = options.size_tasks_initial + 1;
        const epoll_fd = try std.posix.epoll_create1(0);
        assert(epoll_fd > -1);

        const wake_event_fd: std.posix.fd_t = @intCast(std.os.linux.eventfd(0, std.os.linux.EFD.CLOEXEC));

        const events = try allocator.alloc(std.os.linux.epoll_event, options.size_aio_reap_max);
        var jobs = try Pool(Job).init(allocator, size, options.pooling);
        const blocking = std.ArrayList(usize).init(allocator);

        // Queue the wake task.
        {
            const index = jobs.borrow_assume_unset(0);
            const item = jobs.get_ptr(index);
            item.* = .{
                .index = index,
                .type = .wake,
                .task = @bitCast(@as(isize, -1)),
            };

            var event: std.os.linux.epoll_event = .{
                .events = std.os.linux.EPOLL.IN,
                .data = .{ .u64 = index },
            };

            try std.posix.epoll_ctl(epoll_fd, std.os.linux.EPOLL.CTL_ADD, wake_event_fd, &event);
        }

        return AsyncEpoll{
            .epoll_fd = epoll_fd,
            .wake_event_fd = wake_event_fd,
            .events = events,
            .jobs = jobs,
            .blocking = blocking,
        };
    }

    pub fn inner_deinit(self: *AsyncEpoll, allocator: std.mem.Allocator) void {
        std.posix.close(self.epoll_fd);
        allocator.free(self.events);
        self.jobs.deinit();
        self.blocking.deinit();
        std.posix.close(self.wake_event_fd);
    }

    fn deinit(self: *AsyncIO, allocator: std.mem.Allocator) void {
        const epoll: *AsyncEpoll = @ptrCast(@alignCast(self.runner));
        epoll.inner_deinit(allocator);
    }

    pub fn queue_job(self: *AsyncIO, task: usize, job: AsyncSubmission) !void {
        const epoll: *AsyncEpoll = @ptrCast(@alignCast(self.runner));

        try switch (job) {
            .timer => |inner| queue_timer(epoll, task, inner),
            .open => |inner| queue_open(epoll, task, inner.path, inner.flags),
            .delete => |inner| queue_delete(epoll, task, inner.path, inner.is_dir),
            .mkdir => |inner| queue_mkdir(epoll, task, inner.path, inner.mode),
            .stat => |inner| queue_stat(epoll, task, inner),
            .read => |inner| queue_read(epoll, task, inner.fd, inner.buffer, inner.offset),
            .write => |inner| queue_write(epoll, task, inner.fd, inner.buffer, inner.offset),
            .close => |inner| queue_close(epoll, task, inner),
            .accept => |inner| queue_accept(epoll, task, inner.socket, inner.kind),
            .connect => |inner| queue_connect(epoll, task, inner.socket, inner.addr, inner.kind),
            .recv => |inner| queue_recv(epoll, task, inner.socket, inner.buffer),
            .send => |inner| queue_send(epoll, task, inner.socket, inner.buffer),
        };
    }

    fn queue_timer(self: *AsyncEpoll, task: usize, timespec: Timespec) !void {
        const index = try self.jobs.borrow_hint(task);
        const item = self.jobs.get_ptr(index);

        const timer_fd_usize = std.os.linux.timerfd_create(std.os.linux.CLOCK.MONOTONIC, .{ .NONBLOCK = true });
        const timer_fd: i32 = @intCast(timer_fd_usize);
        const ktimerspec: std.os.linux.itimerspec = .{
            .it_value = .{
                .tv_sec = @intCast(timespec.seconds),
                .tv_nsec = @intCast(timespec.nanos),
            },
            .it_interval = .{
                .tv_sec = 0,
                .tv_nsec = 0,
            },
        };

        const rc = std.os.linux.timerfd_settime(timer_fd, .{}, &ktimerspec, null);
        const e: LinuxError = std.posix.errno(rc);
        if (e != .SUCCESS) return error.SetTimerFailed;

        item.* = .{
            .index = index,
            .type = .{ .timer = .{ .fd = timer_fd } },
            .task = task,
        };

        var event: std.os.linux.epoll_event = .{
            .events = std.os.linux.EPOLL.IN,
            .data = .{ .u64 = index },
        };

        try self.add_fd(timer_fd, &event);
    }

    fn queue_open(self: *AsyncEpoll, task: usize, path: Path, flags: AioOpenFlags) !void {
        const index = try self.jobs.borrow_hint(task);
        const item = self.jobs.get_ptr(index);
        item.* = .{
            .index = index,
            .type = .{
                .open = .{
                    .path = path,
                    .kind = if (flags.directory) .dir else .file,
                    .flags = flags,
                },
            },
            .task = task,
        };

        try self.blocking.append(index);
    }

    fn queue_delete(self: *AsyncEpoll, task: usize, path: Path, is_dir: bool) !void {
        const index = try self.jobs.borrow_hint(task);
        const item = self.jobs.get_ptr(index);
        item.* = .{
            .index = index,
            .type = .{ .delete = .{ .path = path, .is_dir = is_dir } },
            .task = task,
        };

        try self.blocking.append(index);
    }

    fn queue_mkdir(self: *AsyncEpoll, task: usize, path: Path, mode: std.posix.mode_t) !void {
        const index = try self.jobs.borrow_hint(task);
        const item = self.jobs.get_ptr(index);
        item.* = .{
            .index = index,
            .type = .{ .mkdir = .{ .path = path, .mode = mode } },
            .task = task,
        };

        try self.blocking.append(index);
    }

    fn queue_stat(self: *AsyncEpoll, task: usize, fd: std.posix.fd_t) !void {
        const index = try self.jobs.borrow_hint(task);
        const item = self.jobs.get_ptr(index);
        item.* = .{
            .index = index,
            .type = .{ .stat = fd },
            .task = task,
        };

        try self.blocking.append(index);
    }

    fn queue_read(
        self: *AsyncEpoll,
        task: usize,
        fd: std.posix.fd_t,
        buffer: []u8,
        offset: ?usize,
    ) !void {
        const index = try self.jobs.borrow_hint(task);
        const item = self.jobs.get_ptr(index);
        item.* = .{
            .index = index,
            .type = .{
                .read = .{
                    .fd = fd,
                    .buffer = buffer,
                    .offset = offset,
                },
            },
            .task = task,
        };

        try self.blocking.append(index);
    }

    fn queue_write(
        self: *AsyncEpoll,
        task: usize,
        fd: std.posix.fd_t,
        buffer: []const u8,
        offset: ?usize,
    ) !void {
        const index = try self.jobs.borrow_hint(task);
        const item = self.jobs.get_ptr(index);
        item.* = .{
            .index = index,
            .type = .{
                .write = .{
                    .fd = fd,
                    .buffer = buffer,
                    .offset = offset,
                },
            },
            .task = task,
        };

        try self.blocking.append(index);
    }

    fn queue_close(
        self: *AsyncEpoll,
        task: usize,
        fd: std.posix.fd_t,
    ) !void {
        const index = try self.jobs.borrow_hint(task);
        const item = self.jobs.get_ptr(index);
        item.* = .{
            .index = index,
            .type = .{ .close = fd },
            .task = task,
        };

        self.remove_fd(fd) catch {};
        try self.blocking.append(index);
    }

    fn queue_accept(
        self: *AsyncEpoll,
        task: usize,
        socket: std.posix.socket_t,
        kind: Socket.Kind,
    ) !void {
        const index = try self.jobs.borrow_hint(task);
        const item = self.jobs.get_ptr(index);
        item.* = .{
            .index = index,
            .type = .{
                .accept = .{
                    .socket = socket,
                    .kind = kind,
                    .addr = undefined,
                    .addr_len = @sizeOf(std.net.Address),
                },
            },
            .task = task,
        };

        var event: std.os.linux.epoll_event = .{
            .events = std.os.linux.EPOLL.IN,
            .data = .{ .u64 = index },
        };

        try self.add_or_mod_fd(socket, &event);
    }

    fn queue_connect(
        self: *AsyncEpoll,
        task: usize,
        socket: std.posix.socket_t,
        addr: std.net.Address,
        kind: Socket.Kind,
    ) !void {
        const index = try self.jobs.borrow_hint(task);
        const item = self.jobs.get_ptr(index);
        item.* = .{
            .index = index,
            .type = .{
                .connect = .{
                    .socket = socket,
                    .addr = addr,
                    .kind = kind,
                },
            },
            .task = task,
        };

        var event: std.os.linux.epoll_event = .{
            .events = std.os.linux.EPOLL.OUT,
            .data = .{ .u64 = index },
        };

        try self.add_or_mod_fd(socket, &event);
    }

    fn queue_recv(self: *AsyncEpoll, task: usize, socket: std.posix.socket_t, buffer: []u8) !void {
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

        var event: std.os.linux.epoll_event = .{
            .events = std.os.linux.EPOLL.IN,
            .data = .{ .u64 = index },
        };

        try self.add_or_mod_fd(socket, &event);
    }

    fn queue_send(self: *AsyncEpoll, task: usize, socket: std.posix.socket_t, buffer: []const u8) !void {
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

        var event: std.os.linux.epoll_event = .{
            .events = std.os.linux.EPOLL.OUT,
            .data = .{ .u64 = index },
        };

        try self.add_or_mod_fd(socket, &event);
    }

    fn add_or_mod_fd(self: *AsyncEpoll, fd: std.posix.fd_t, event: *std.os.linux.epoll_event) !void {
        self.add_fd(fd, event) catch |e| {
            if (e == error.FileDescriptorAlreadyPresentInSet) {
                try self.mod_fd(fd, event);
            } else return e;
        };
    }

    inline fn add_fd(self: *AsyncEpoll, fd: std.posix.fd_t, event: *std.os.linux.epoll_event) !void {
        try std.posix.epoll_ctl(self.epoll_fd, std.os.linux.EPOLL.CTL_ADD, fd, event);
    }

    inline fn mod_fd(self: *AsyncEpoll, fd: std.posix.fd_t, event: *std.os.linux.epoll_event) !void {
        try std.posix.epoll_ctl(self.epoll_fd, std.os.linux.EPOLL.CTL_MOD, fd, event);
    }

    inline fn remove_fd(self: *AsyncEpoll, fd: std.posix.fd_t) !void {
        try std.posix.epoll_ctl(self.epoll_fd, std.os.linux.EPOLL.CTL_DEL, fd, null);
    }

    pub fn wake(self: *AsyncIO) !void {
        const epoll: *AsyncEpoll = @ptrCast(@alignCast(self.runner));

        const bytes: []const u8 = "00000000";
        var i: usize = 0;
        while (i < bytes.len) {
            i += try std.posix.write(epoll.wake_event_fd, bytes);
        }
    }

    pub fn submit(_: *AsyncIO) !void {}

    pub fn reap(self: *AsyncIO, wait: bool) ![]Completion {
        const epoll: *AsyncEpoll = @ptrCast(@alignCast(self.runner));
        var reaped: usize = 0;
        var first_run: bool = true;

        const busy_wait: bool = !wait or epoll.blocking.items.len > 0;
        log.debug("busy wait? {}", .{busy_wait});
        log.debug("blocking item count={d}", .{epoll.blocking.items.len});
        log.debug("dirty jobs count={d}", .{epoll.jobs.dirty.count()});

        while (reaped == 0 and wait) {
            defer first_run = false;

            blocking_loop: for (0..epoll.blocking.items.len) |_| {
                // Prevent overflow on completions.
                if (self.completions.len - reaped == 0) break;

                const index = epoll.blocking.popOrNull() orelse break;
                const job = epoll.jobs.get_ptr(index);
                assert(epoll.jobs.dirty.isSet(index));

                var job_complete = true;
                defer if (job_complete) {
                    epoll.jobs.release(index);
                } else {
                    epoll.blocking.append(index) catch unreachable;
                };

                const result: Result = blk: {
                    switch (job.type) {
                        else => unreachable,
                        .mkdir => |inner| {
                            const rc = switch (inner.path) {
                                .rel => |path| std.os.linux.mkdirat(path.dir, path.path, @intCast(inner.mode)),
                                .abs => |path| std.os.linux.mkdir(path, @intCast(inner.mode)),
                            };

                            if (rc == 0) break :blk .{ .mkdir = .{ .actual = {} } };

                            const result: MkdirResult = result: {
                                const e: LinuxError = std.posix.errno(rc);
                                break :result switch (e) {
                                    LinuxError.AGAIN => {
                                        job_complete = false;
                                        continue :blocking_loop;
                                    },
                                    LinuxError.ACCES => .{ .err = MkdirError.AccessDenied },
                                    LinuxError.EXIST => .{ .err = MkdirError.AlreadyExists },
                                    LinuxError.LOOP, LinuxError.MLINK => .{ .err = MkdirError.Loop },
                                    LinuxError.NAMETOOLONG => .{ .err = MkdirError.NameTooLong },
                                    LinuxError.NOENT => .{ .err = MkdirError.NotFound },
                                    LinuxError.NOSPC => .{ .err = MkdirError.NoSpace },
                                    LinuxError.NOTDIR => .{ .err = MkdirError.NotADirectory },
                                    LinuxError.ROFS => .{ .err = MkdirError.ReadOnlyFileSystem },
                                    else => .{ .err = MkdirError.Unexpected },
                                };
                            };

                            break :blk .{ .mkdir = result };
                        },
                        .open => |inner| {
                            const o_flags: std.os.linux.O = flag: {
                                var o: std.os.linux.O = .{};
                                switch (inner.flags.mode) {
                                    .read => o.ACCMODE = .RDONLY,
                                    .write => o.ACCMODE = .WRONLY,
                                    .read_write => o.ACCMODE = .RDWR,
                                }

                                o.APPEND = inner.flags.append;
                                o.CREAT = inner.flags.create;
                                o.TRUNC = inner.flags.truncate;
                                o.EXCL = inner.flags.exclusive;
                                o.NONBLOCK = inner.flags.non_block;
                                o.SYNC = inner.flags.sync;
                                o.DIRECTORY = inner.flags.directory;

                                break :flag o;
                            };
                            const perms = inner.flags.perms orelse 0;

                            const rc = switch (inner.path) {
                                .rel => |path| std.os.linux.openat(path.dir, path.path, o_flags, perms),
                                .abs => |path| std.os.linux.open(path, o_flags, perms),
                            };

                            if (rc >= 0) switch (inner.kind) {
                                .file => break :blk .{
                                    .open = .{ .actual = .{ .file = .{ .handle = @intCast(rc) } } },
                                },
                                .dir => break :blk .{
                                    .open = .{ .actual = .{ .dir = .{ .handle = @intCast(rc) } } },
                                },
                            };

                            const result: InnerOpenResult = result: {
                                const e: LinuxError = std.posix.errno(rc);
                                break :result switch (e) {
                                    LinuxError.AGAIN => {
                                        job_complete = false;
                                        continue :blocking_loop;
                                    },
                                    LinuxError.ACCES, LinuxError.PERM => .{ .err = OpenError.AccessDenied },
                                    LinuxError.BADF => .{ .err = OpenError.InvalidFd },
                                    LinuxError.BUSY => .{ .err = OpenError.Busy },
                                    LinuxError.DQUOT => .{ .err = OpenError.DiskQuotaExceeded },
                                    LinuxError.EXIST => .{ .err = OpenError.FileAlreadyExists },
                                    LinuxError.FAULT => .{ .err = OpenError.InvalidAddress },
                                    LinuxError.FBIG, LinuxError.OVERFLOW => .{ .err = OpenError.FileTooBig },
                                    LinuxError.INTR => .{ .err = OpenError.Interrupted },
                                    LinuxError.INVAL => .{ .err = OpenError.InvalidArguments },
                                    LinuxError.ISDIR => .{ .err = OpenError.IsDirectory },
                                    LinuxError.LOOP => .{ .err = OpenError.Loop },
                                    LinuxError.MFILE => .{ .err = OpenError.ProcessFdQuotaExceeded },
                                    LinuxError.NAMETOOLONG => .{ .err = OpenError.NameTooLong },
                                    LinuxError.NFILE => .{ .err = OpenError.SystemFdQuotaExceeded },
                                    LinuxError.NODEV, LinuxError.NXIO => .{ .err = OpenError.DeviceNotFound },
                                    LinuxError.NOENT => .{ .err = OpenError.NotFound },
                                    LinuxError.NOMEM => .{ .err = OpenError.OutOfMemory },
                                    LinuxError.NOSPC => .{ .err = OpenError.NoSpace },
                                    LinuxError.NOTDIR => .{ .err = OpenError.NotADirectory },
                                    LinuxError.OPNOTSUPP => .{ .err = OpenError.OperationNotSupported },
                                    LinuxError.ROFS => .{ .err = OpenError.ReadOnlyFileSystem },
                                    LinuxError.TXTBSY => .{ .err = OpenError.FileLocked },
                                    else => .{ .err = OpenError.Unexpected },
                                };
                            };

                            break :blk .{ .open = result };
                        },
                        .delete => |inner| {
                            const mode: u32 = if (inner.is_dir) std.posix.AT.REMOVEDIR else 0;
                            const rc = switch (inner.path) {
                                .rel => |path| std.os.linux.unlinkat(path.dir, path.path, mode),
                                .abs => |path| std.os.linux.unlinkat(std.posix.AT.FDCWD, path, mode),
                            };

                            if (rc == 0) break :blk .{ .delete = .{ .actual = {} } };

                            const result: DeleteResult = result: {
                                const e: LinuxError = std.posix.errno(rc);
                                break :result switch (e) {
                                    // unlink
                                    LinuxError.AGAIN => {
                                        job_complete = false;
                                        continue :blocking_loop;
                                    },
                                    LinuxError.ACCES => .{ .err = DeleteError.AccessDenied },
                                    LinuxError.BUSY => .{ .err = DeleteError.Busy },
                                    LinuxError.FAULT => .{ .err = DeleteError.InvalidAddress },
                                    LinuxError.IO => .{ .err = DeleteError.IoError },
                                    LinuxError.ISDIR, LinuxError.PERM => .{ .err = DeleteError.IsDirectory },
                                    LinuxError.LOOP => .{ .err = DeleteError.Loop },
                                    LinuxError.NAMETOOLONG => .{ .err = DeleteError.NameTooLong },
                                    LinuxError.NOENT => .{ .err = DeleteError.NotFound },
                                    LinuxError.NOMEM => .{ .err = DeleteError.OutOfMemory },
                                    LinuxError.NOTDIR => .{ .err = DeleteError.IsNotDirectory },
                                    LinuxError.ROFS => .{ .err = DeleteError.ReadOnlyFileSystem },
                                    LinuxError.BADF => .{ .err = DeleteError.InvalidFd },
                                    // rmdir
                                    LinuxError.INVAL => .{ .err = DeleteError.InvalidArguments },
                                    LinuxError.NOTEMPTY => .{ .err = DeleteError.NotEmpty },
                                    else => .{ .err = DeleteError.Unexpected },
                                };
                            };

                            break :blk .{ .delete = result };
                        },
                        .stat => |fd| {
                            var fstat: std.os.linux.Stat = undefined;
                            const rc = std.os.linux.fstat(fd, &fstat);

                            if (rc == 0) {
                                const stat = Stat{
                                    .size = @intCast(fstat.size),
                                    .mode = @intCast(fstat.mode),
                                    .accessed = .{
                                        .seconds = @intCast(fstat.atim.tv_sec),
                                        .nanos = @intCast(fstat.atim.tv_nsec),
                                    },
                                    .modified = .{
                                        .seconds = @intCast(fstat.mtim.tv_sec),
                                        .nanos = @intCast(fstat.mtim.tv_nsec),
                                    },
                                    .changed = .{
                                        .seconds = @intCast(fstat.ctim.tv_sec),
                                        .nanos = @intCast(fstat.ctim.tv_nsec),
                                    },
                                };

                                break :blk .{ .stat = .{ .actual = stat } };
                            }

                            const result: StatResult = result: {
                                const e: LinuxError = std.posix.errno(rc);
                                break :result switch (e) {
                                    LinuxError.ACCES => .{ .err = StatError.AccessDenied },
                                    LinuxError.BADF => .{ .err = StatError.InvalidFd },
                                    LinuxError.FAULT => .{ .err = StatError.InvalidAddress },
                                    LinuxError.INVAL => .{ .err = StatError.InvalidArguments },
                                    LinuxError.LOOP => .{ .err = StatError.Loop },
                                    LinuxError.NAMETOOLONG => .{ .err = StatError.NameTooLong },
                                    LinuxError.NOENT => .{ .err = StatError.NotFound },
                                    LinuxError.NOMEM => .{ .err = StatError.OutOfMemory },
                                    LinuxError.NOTDIR => .{ .err = StatError.NotADirectory },
                                    else => .{ .err = StatError.Unexpected },
                                };
                            };

                            break :blk .{ .stat = result };
                        },
                        .read => |inner| {
                            const rc = if (inner.offset) |offset|
                                std.os.linux.pread(inner.fd, inner.buffer.ptr, inner.buffer.len, @intCast(offset))
                            else
                                std.os.linux.read(inner.fd, inner.buffer.ptr, inner.buffer.len);

                            if (rc == 0) {
                                break :blk .{ .read = .{ .err = ReadError.EndOfFile } };
                            } else {
                                break :blk .{ .read = .{ .actual = @intCast(rc) } };
                            }

                            const result: ReadResult = result: {
                                const e: LinuxError = std.posix.errno(rc);
                                break :result switch (e) {
                                    LinuxError.AGAIN => {
                                        job_complete = false;
                                        continue :blocking_loop;
                                    },
                                    // If it is unseekable...
                                    LinuxError.NXIO, LinuxError.SPIPE, LinuxError.OVERFLOW => .{ .err = ReadError.Unexpected },
                                    LinuxError.BADF => .{ .err = ReadError.InvalidFd },
                                    LinuxError.FAULT => .{ .err = ReadError.InvalidAddress },
                                    LinuxError.INTR => .{ .err = ReadError.Interrupted },
                                    LinuxError.INVAL => .{ .err = ReadError.InvalidArguments },
                                    LinuxError.IO => .{ .err = ReadError.IoError },
                                    LinuxError.ISDIR => .{ .err = ReadError.IsDirectory },
                                    else => .{ .err = ReadError.Unexpected },
                                };
                            };

                            break :blk .{ .read = result };
                        },
                        .write => |inner| {
                            const rc = if (inner.offset) |offset|
                                std.os.linux.pwrite(inner.fd, inner.buffer.ptr, inner.buffer.len, @intCast(offset))
                            else
                                std.os.linux.write(inner.fd, inner.buffer.ptr, inner.buffer.len);

                            if (rc > 0) break :blk .{ .write = .{ .actual = @intCast(rc) } };

                            const result: WriteResult = result: {
                                const e: LinuxError = std.posix.errno(rc);
                                break :result switch (e) {
                                    LinuxError.AGAIN => {
                                        job_complete = false;
                                        continue :blocking_loop;
                                    },
                                    // If it is unseekable...
                                    LinuxError.NXIO, LinuxError.SPIPE, LinuxError.OVERFLOW => .{ .err = WriteError.Unexpected },
                                    LinuxError.BADF => .{ .err = WriteError.InvalidFd },
                                    LinuxError.DESTADDRREQ => .{ .err = WriteError.NoDestinationAddress },
                                    LinuxError.DQUOT => .{ .err = WriteError.DiskQuotaExceeded },
                                    LinuxError.FAULT => .{ .err = WriteError.InvalidAddress },
                                    LinuxError.FBIG => .{ .err = WriteError.FileTooBig },
                                    LinuxError.INTR => .{ .err = WriteError.Interrupted },
                                    LinuxError.INVAL => .{ .err = WriteError.InvalidArguments },
                                    LinuxError.IO => .{ .err = WriteError.IoError },
                                    LinuxError.NOSPC => .{ .err = WriteError.NoSpace },
                                    LinuxError.PERM => .{ .err = WriteError.AccessDenied },
                                    LinuxError.PIPE => .{ .err = WriteError.BrokenPipe },
                                    else => .{ .err = WriteError.Unexpected },
                                };
                            };

                            break :blk .{ .write = result };
                        },
                        .close => |handle| {
                            std.posix.close(handle);
                            break :blk .none;
                        },
                    }
                };

                self.completions[reaped] = .{
                    .result = result,
                    .task = job.task,
                };

                reaped += 1;
            }

            const timeout: i32 = if (busy_wait or reaped > 0) 0 else -1;
            // Handle all of the epoll I/O
            const epoll_events = std.posix.epoll_wait(epoll.epoll_fd, epoll.events[0..], timeout);
            epoll_loop: for (epoll.events[0..epoll_events]) |event| {
                const job_index = event.data.u64;
                assert(epoll.jobs.dirty.isSet(job_index));

                var job_complete = true;
                defer if (job_complete) epoll.jobs.release(job_index);
                const job = epoll.jobs.get_ptr(job_index);

                const result: Result = blk: {
                    switch (job.type) {
                        else => unreachable,
                        .wake => {
                            // this keeps it in the job queue and we pretty
                            // much never want to remove this fd.
                            job_complete = false;
                            var buffer: [8]u8 = undefined;

                            // Should NEVER fail.
                            _ = std.posix.read(epoll.wake_event_fd, buffer[0..]) catch |e| {
                                switch (e) {
                                    error.WouldBlock => unreachable,
                                    else => {
                                        log.err("wake failed: {}", .{e});
                                        unreachable;
                                    },
                                }
                            };

                            break :blk .wake;
                        },
                        .timer => |inner| {
                            const timer_fd = inner.fd;
                            defer epoll.remove_fd(timer_fd) catch unreachable;
                            assert(event.events & std.os.linux.EPOLL.IN != 0);

                            var buffer: [8]u8 = undefined;
                            // Should NEVER fail.
                            _ = std.posix.read(timer_fd, buffer[0..]) catch |e| {
                                switch (e) {
                                    error.WouldBlock => unreachable,
                                    else => {
                                        log.debug("timer failed: {}", .{e});
                                        unreachable;
                                    },
                                }
                            };

                            break :blk .none;
                        },
                        .accept => |*inner| {
                            assert(event.events & std.os.linux.EPOLL.IN != 0);

                            const res = std.os.linux.accept4(inner.socket, &inner.addr.any, @ptrCast(&inner.addr_len), 0);

                            const result: AcceptResult = result: {
                                const e: LinuxError = std.posix.errno(res);
                                break :result switch (e) {
                                    LinuxError.SUCCESS => .{ .actual = .{ .handle = @intCast(res), .addr = inner.addr, .kind = inner.kind } },
                                    LinuxError.AGAIN => {
                                        job_complete = false;
                                        continue :epoll_loop;
                                    },
                                    LinuxError.BADF => .{ .err = AcceptError.InvalidFd },
                                    LinuxError.CONNABORTED => .{ .err = AcceptError.ConnectionAborted },
                                    LinuxError.FAULT => .{ .err = AcceptError.InvalidAddress },
                                    LinuxError.INTR => .{ .err = AcceptError.Interrupted },
                                    LinuxError.INVAL => .{ .err = AcceptError.NotListening },
                                    LinuxError.MFILE => .{ .err = AcceptError.ProcessFdQuotaExceeded },
                                    LinuxError.NFILE => .{ .err = AcceptError.SystemFdQuotaExceeded },
                                    LinuxError.NOBUFS, LinuxError.NOMEM => .{ .err = AcceptError.OutOfMemory },
                                    LinuxError.NOTSOCK => .{ .err = AcceptError.NotASocket },
                                    LinuxError.OPNOTSUPP => .{ .err = AcceptError.OperationNotSupported },
                                    else => .{ .err = AcceptError.Unexpected },
                                };
                            };

                            try epoll.remove_fd(inner.socket);
                            break :blk .{ .accept = result };
                        },
                        .connect => |inner| {
                            assert(event.events & std.os.linux.EPOLL.OUT != 0);
                            const res = std.os.linux.connect(inner.socket, &inner.addr.any, @sizeOf(std.net.Address));

                            const result: ConnectResult = result: {
                                const e: LinuxError = std.posix.errno(res);
                                switch (e) {
                                    LinuxError.SUCCESS => break :result .{ .actual = .{ .handle = inner.socket, .addr = inner.addr, .kind = inner.kind } },
                                    LinuxError.AGAIN, LinuxError.ALREADY, LinuxError.INPROGRESS => {
                                        job_complete = false;
                                        continue :epoll_loop;
                                    },
                                    LinuxError.ACCES, LinuxError.PERM => break :result .{ .err = ConnectError.AccessDenied },
                                    LinuxError.ADDRINUSE => break :result .{ .err = ConnectError.AddressInUse },
                                    LinuxError.ADDRNOTAVAIL => break :result .{ .err = ConnectError.AddressNotAvailable },
                                    LinuxError.AFNOSUPPORT => break :result .{ .err = ConnectError.AddressFamilyNotSupported },
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

                            try epoll.remove_fd(inner.socket);
                            break :blk .{ .connect = result };
                        },
                        .recv => |inner| {
                            assert(event.events & std.os.linux.EPOLL.IN != 0);
                            const rc = std.os.linux.recvfrom(inner.socket, inner.buffer.ptr, inner.buffer.len, 0, null, null);

                            if (rc == 0)
                                break :blk .{ .recv = .{ .err = RecvError.Closed } }
                            else
                                break :blk .{ .recv = .{ .actual = @intCast(rc) } };

                            const result: RecvResult = result: {
                                const e: LinuxError = std.posix.errno(rc);
                                switch (e) {
                                    LinuxError.AGAIN => {
                                        job_complete = false;
                                        continue :epoll_loop;
                                    },
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

                            try epoll.remove_fd(inner.socket);
                            break :blk .{ .recv = result };
                        },
                        .send => |inner| {
                            assert(event.events & std.os.linux.EPOLL.OUT != 0);
                            const res = std.os.linux.sendto(inner.socket, inner.buffer.ptr, inner.buffer.len, 0, null, 0);

                            const result: SendResult = result: {
                                const e: LinuxError = std.posix.errno(res);
                                switch (e) {
                                    LinuxError.SUCCESS => break :result .{ .actual = @intCast(res) },
                                    LinuxError.AGAIN => {
                                        job_complete = false;
                                        continue :epoll_loop;
                                    },
                                    LinuxError.ACCES => break :result .{ .err = SendError.AccessDenied },
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
                    }
                };

                self.completions[reaped] = .{
                    .result = result,
                    .task = job.task,
                };
                reaped += 1;
            }
        }

        return self.completions[0..reaped];
    }

    pub fn to_async(self: *AsyncEpoll) AsyncIO {
        return AsyncIO{
            .runner = self,
            ._queue_job = queue_job,
            ._deinit = deinit,
            ._wake = wake,
            ._submit = submit,
            ._reap = reap,
        };
    }
};
