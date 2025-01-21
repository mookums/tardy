const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.@"tardy/aio/kqueue");

const Completion = @import("../completion.zig").Completion;
const Result = @import("../completion.zig").Result;
const Stat = @import("../../fs/lib.zig").Stat;
const Timespec = @import("../../lib.zig").Timespec;

const AsyncIO = @import("../lib.zig").AsyncIO;
const AsyncIOOptions = @import("../lib.zig").AsyncIOOptions;
const Job = @import("../job.zig").Job;
const Pool = @import("../../core/pool.zig").Pool;
const Queue = @import("../../core/queue.zig").Queue;

const Socket = @import("../../net/lib.zig").Socket;
const Path = @import("../../fs/lib.zig").Path;

const AioOpenFlags = @import("../lib.zig").AioOpenFlags;
const AsyncSubmission = @import("../lib.zig").AsyncSubmission;

const PosixError = std.posix.E;

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

const InnerOpenResult = @import("../completion.zig").InnerOpenResult;
const OpenError = @import("../completion.zig").OpenError;
const ReadResult = @import("../completion.zig").ReadResult;
const ReadError = @import("../completion.zig").ReadError;
const WriteResult = @import("../completion.zig").WriteResult;
const WriteError = @import("../completion.zig").WriteError;

const StatResult = @import("../completion.zig").StatResult;
const StatError = @import("../completion.zig").StatError;

const Atomic = std.atomic.Value;

const WAKE_IDENT = 1;

pub const AsyncKqueue = struct {
    kqueue_fd: std.posix.fd_t,

    changes: []std.posix.Kevent,
    change_count: usize = 0,
    events: []std.posix.Kevent,

    jobs: Pool(Job),
    blocking: Queue(usize),

    pub fn init(allocator: std.mem.Allocator, options: AsyncIOOptions) !AsyncKqueue {
        const kqueue_fd = try std.posix.kqueue();
        assert(kqueue_fd > -1);
        errdefer std.posix.close(kqueue_fd);

        const events = try allocator.alloc(std.posix.Kevent, options.size_aio_reap_max);
        const changes = try allocator.alloc(std.posix.Kevent, options.size_aio_reap_max);
        var jobs = try Pool(Job).init(allocator, options.size_tasks_initial, options.pooling);
        const blocking = Queue(usize).init(allocator);

        {
            const index = jobs.borrow_assume_unset(0);
            const item = jobs.get_ptr(index);
            item.* = .{
                .index = 0,
                .type = .wake,
                .task = undefined,
            };

            const event: std.posix.Kevent = .{
                .ident = WAKE_IDENT,
                .filter = std.posix.system.EVFILT_USER,
                .flags = std.posix.system.EV_ADD | std.posix.system.EV_CLEAR,
                .fflags = 0,
                .data = 0,
                .udata = 0,
            };

            _ = try std.posix.kevent(kqueue_fd, &.{event}, &.{}, null);
        }

        return AsyncKqueue{
            .kqueue_fd = kqueue_fd,
            .events = events,
            .changes = changes,
            .change_count = 0,
            .jobs = jobs,
            .blocking = blocking,
        };
    }

    pub fn inner_deinit(self: *AsyncKqueue, allocator: std.mem.Allocator) void {
        std.posix.close(self.kqueue_fd);
        allocator.free(self.events);
        allocator.free(self.changes);
        self.jobs.deinit();
        self.blocking.deinit();
    }

    pub fn deinit(self: *AsyncIO, allocator: std.mem.Allocator) void {
        const kqueue: *AsyncKqueue = @ptrCast(@alignCast(self.runner));
        kqueue.inner_deinit(allocator);
    }

    pub fn queue_job(self: *AsyncIO, task: usize, job: AsyncSubmission) !void {
        const kqueue: *AsyncKqueue = @ptrCast(@alignCast(self.runner));

        (switch (job) {
            .timer => |inner| queue_timer(kqueue, task, inner),
            .open => |inner| queue_open(kqueue, task, inner.path, inner.flags),
            .delete => |inner| queue_delete(kqueue, task, inner.path, inner.is_dir),
            .mkdir => |inner| queue_mkdir(kqueue, task, inner.path, inner.mode),
            .stat => |inner| queue_stat(kqueue, task, inner),
            .read => |inner| queue_read(kqueue, task, inner.fd, inner.buffer, inner.offset),
            .write => |inner| queue_write(kqueue, task, inner.fd, inner.buffer, inner.offset),
            .close => |inner| queue_close(kqueue, task, inner),
            .accept => |inner| queue_accept(kqueue, task, inner.socket, inner.kind),
            .connect => |inner| queue_connect(kqueue, task, inner.socket, inner.addr, inner.kind),
            .recv => |inner| queue_recv(kqueue, task, inner.socket, inner.buffer),
            .send => |inner| queue_send(kqueue, task, inner.socket, inner.buffer),
        }) catch |e| if (e == error.ChangeQueueFull) {
            try self.submit();
            try queue_job(self, task, job);
        } else return e;
    }

    fn queue_timer(self: *AsyncKqueue, task: usize, timespec: Timespec) !void {
        const index = try self.jobs.borrow_hint(task);
        errdefer self.jobs.release(index);
        const item = self.jobs.get_ptr(index);

        item.* = .{
            .index = index,
            .type = .{ .timer = .none },
            .task = task,
        };

        // kqueue uses milliseconds.
        const milliseconds: isize = @intCast(
            timespec.seconds * 1000 + @divFloor(timespec.nanos, std.time.ns_per_ms),
        );

        if (self.change_count < self.changes.len) {
            const event = &self.changes[self.change_count];
            self.change_count += 1;

            event.* = .{
                .ident = index,
                .filter = std.posix.system.EVFILT_TIMER,
                .flags = std.posix.system.EV_ADD | std.posix.system.EV_ONESHOT,
                .fflags = 0,
                .data = milliseconds,
                .udata = index,
            };
        } else return error.ChangeQueueFull;
    }

    fn queue_open(self: *AsyncKqueue, task: usize, path: Path, flags: AioOpenFlags) !void {
        const index = try self.jobs.borrow_hint(task);
        errdefer self.jobs.release(index);
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

    fn queue_mkdir(self: *AsyncKqueue, task: usize, path: Path, mode: std.posix.mode_t) !void {
        const index = try self.jobs.borrow_hint(task);
        errdefer self.jobs.release(index);
        const item = self.jobs.get_ptr(index);
        item.* = .{
            .index = index,
            .type = .{ .mkdir = .{ .path = path, .mode = mode } },
            .task = task,
        };

        try self.blocking.append(index);
    }

    fn queue_delete(self: *AsyncKqueue, task: usize, path: Path, is_dir: bool) !void {
        const index = try self.jobs.borrow_hint(task);
        errdefer self.jobs.release(index);
        const item = self.jobs.get_ptr(index);
        item.* = .{
            .index = index,
            .type = .{ .delete = .{ .path = path, .is_dir = is_dir } },
            .task = task,
        };

        try self.blocking.append(index);
    }

    fn queue_stat(self: *AsyncKqueue, task: usize, fd: std.posix.fd_t) !void {
        const index = try self.jobs.borrow_hint(task);
        errdefer self.jobs.release(index);
        const item = self.jobs.get_ptr(index);
        item.* = .{
            .index = index,
            .type = .{ .stat = fd },
            .task = task,
        };

        try self.blocking.append(index);
    }

    fn queue_read(
        self: *AsyncKqueue,
        task: usize,
        fd: std.posix.fd_t,
        buffer: []u8,
        offset: ?usize,
    ) !void {
        const index = try self.jobs.borrow_hint(task);
        errdefer self.jobs.release(index);
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
        self: *AsyncKqueue,
        task: usize,
        fd: std.posix.fd_t,
        buffer: []const u8,
        offset: ?usize,
    ) !void {
        const index = try self.jobs.borrow_hint(task);
        errdefer self.jobs.release(index);
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

    fn queue_close(self: *AsyncKqueue, task: usize, fd: std.posix.fd_t) !void {
        const index = try self.jobs.borrow_hint(task);
        errdefer self.jobs.release(index);
        const item = self.jobs.get_ptr(index);
        item.* = .{
            .index = index,
            .type = .{ .close = fd },
            .task = task,
        };

        try self.blocking.append(index);
    }

    fn queue_accept(self: *AsyncKqueue, task: usize, socket: std.posix.socket_t, kind: Socket.Kind) !void {
        const index = try self.jobs.borrow_hint(task);
        errdefer self.jobs.release(index);
        const item = self.jobs.get_ptr(index);
        item.* = .{
            .index = index,
            .type = .{
                .accept = .{
                    .socket = socket,
                    .addr = undefined,
                    .addr_len = @sizeOf(std.net.Address),
                    .kind = kind,
                },
            },
            .task = task,
        };

        if (self.change_count < self.changes.len) {
            const event = &self.changes[self.change_count];
            self.change_count += 1;

            event.* = .{
                .ident = @intCast(socket),
                .filter = std.posix.system.EVFILT_READ,
                .flags = std.posix.system.EV_ADD | std.posix.system.EV_ONESHOT,
                .fflags = 0,
                .data = 0,
                .udata = index,
            };
        } else return error.ChangeQueueFull;
    }

    fn queue_connect(
        self: *AsyncKqueue,
        task: usize,
        socket: std.posix.socket_t,
        addr: std.net.Address,
        kind: Socket.Kind,
    ) !void {
        const index = try self.jobs.borrow_hint(task);
        errdefer self.jobs.release(index);
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

        if (self.change_count < self.changes.len) {
            const event = &self.changes[self.change_count];
            self.change_count += 1;

            event.* = .{
                .ident = @intCast(socket),
                .filter = std.posix.system.EVFILT_WRITE,
                .flags = std.posix.system.EV_ADD | std.posix.system.EV_ONESHOT,
                .fflags = 0,
                .data = 0,
                .udata = index,
            };
        } else return error.ChangeQueueFull;
    }

    fn queue_recv(
        self: *AsyncKqueue,
        task: usize,
        socket: std.posix.socket_t,
        buffer: []u8,
    ) !void {
        const index = try self.jobs.borrow_hint(task);
        errdefer self.jobs.release(index);
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

        if (self.change_count < self.changes.len) {
            const event = &self.changes[self.change_count];
            self.change_count += 1;

            event.* = .{
                .ident = @intCast(socket),
                .filter = std.posix.system.EVFILT_READ,
                .flags = std.posix.system.EV_ADD | std.posix.system.EV_ONESHOT,
                .fflags = 0,
                .data = 0,
                .udata = index,
            };
        } else return error.ChangeQueueFull;
    }

    fn queue_send(
        self: *AsyncKqueue,
        task: usize,
        socket: std.posix.socket_t,
        buffer: []const u8,
    ) !void {
        const index = try self.jobs.borrow_hint(task);
        errdefer self.jobs.release(index);
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

        if (self.change_count < self.changes.len) {
            const event = &self.changes[self.change_count];
            self.change_count += 1;

            event.* = .{
                .ident = @intCast(socket),
                .filter = std.posix.system.EVFILT_WRITE,
                .flags = std.posix.system.EV_ADD | std.posix.system.EV_ONESHOT,
                .fflags = 0,
                .data = 0,
                .udata = index,
            };
        } else return error.ChangeQueueFull;
    }

    pub fn wake(self: *AsyncIO) !void {
        const kqueue: *AsyncKqueue = @ptrCast(@alignCast(self.runner));

        const event: std.posix.Kevent = .{
            .ident = WAKE_IDENT,
            .filter = std.posix.system.EVFILT_USER,
            .flags = std.posix.system.EV_ADD | std.posix.system.EV_ONESHOT,
            .fflags = std.posix.system.NOTE_TRIGGER,
            .data = 0,
            .udata = 0,
        };

        // add a new event to the change list.
        _ = try std.posix.kevent(kqueue.kqueue_fd, &.{event}, &.{}, null);
    }

    pub fn submit(self: *AsyncIO) !void {
        const kqueue: *AsyncKqueue = @ptrCast(@alignCast(self.runner));
        _ = try std.posix.kevent(kqueue.kqueue_fd, kqueue.changes[0..kqueue.change_count], &.{}, null);
        kqueue.change_count = 0;
    }

    pub fn reap(self: *AsyncIO, wait: bool) ![]Completion {
        const kqueue: *AsyncKqueue = @ptrCast(@alignCast(self.runner));
        var reaped: usize = 0;
        var first_run: bool = true;

        const busy_wait: bool = !wait or kqueue.blocking.items.len > 0;
        log.debug("busy wait? {}", .{busy_wait});

        while ((reaped == 0 and wait) or first_run) {
            defer first_run = false;

            blocking_loop: for (0..kqueue.blocking.items.len) |_| {
                if (self.completions.len - reaped == 0) break;

                const index = (try kqueue.blocking.pop()) orelse break;
                const job = kqueue.jobs.get_ptr(index);
                assert(kqueue.jobs.dirty.isSet(job.index));

                var job_complete = true;
                defer if (job_complete) {
                    kqueue.jobs.release(job.index);
                } else {
                    // if not done, readd to blocking list.
                    kqueue.blocking.append(job.index) catch unreachable;
                };

                const result: Result = blk: {
                    switch (job.type) {
                        else => unreachable,
                        .mkdir => |inner| {
                            const rc = switch (inner.path) {
                                .rel => |path| std.posix.system.mkdirat(path.dir, path.path, @intCast(inner.mode)),
                                .abs => |path| std.posix.system.mkdir(path, @intCast(inner.mode)),
                            };

                            const result: MkdirResult = result: {
                                const e: PosixError = std.posix.errno(rc);
                                break :result switch (e) {
                                    PosixError.SUCCESS => .{ .actual = {} },
                                    PosixError.AGAIN => {
                                        job_complete = false;
                                        continue :blocking_loop;
                                    },
                                    PosixError.ACCES => .{ .err = MkdirError.AccessDenied },
                                    PosixError.EXIST => .{ .err = MkdirError.AlreadyExists },
                                    PosixError.LOOP, PosixError.MLINK => .{ .err = MkdirError.Loop },
                                    PosixError.NAMETOOLONG => .{ .err = MkdirError.NameTooLong },
                                    PosixError.NOENT => .{ .err = MkdirError.NotFound },
                                    PosixError.NOSPC => .{ .err = MkdirError.NoSpace },
                                    PosixError.NOTDIR => .{ .err = MkdirError.NotADirectory },
                                    PosixError.ROFS => .{ .err = MkdirError.ReadOnlyFileSystem },
                                    else => .{ .err = MkdirError.Unexpected },
                                };
                            };

                            break :blk .{ .mkdir = result };
                        },
                        .open => |inner| {
                            const o_flags: std.posix.O = flag: {
                                var o: std.posix.O = .{};
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
                                .rel => |path| std.posix.system.openat(path.dir, path.path, o_flags, perms),
                                .abs => |path| std.posix.system.open(path, o_flags, perms),
                            };

                            const result: InnerOpenResult = result: {
                                const e: PosixError = std.posix.errno(rc);
                                break :result switch (e) {
                                    PosixError.SUCCESS => switch (inner.kind) {
                                        .file => .{ .actual = .{ .file = .{ .handle = @intCast(rc) } } },
                                        .dir => .{ .actual = .{ .dir = .{ .handle = @intCast(rc) } } },
                                    },
                                    PosixError.AGAIN => {
                                        job_complete = false;
                                        continue :blocking_loop;
                                    },
                                    PosixError.ACCES, PosixError.PERM => .{ .err = OpenError.AccessDenied },
                                    PosixError.BADF => .{ .err = OpenError.InvalidFd },
                                    PosixError.BUSY => .{ .err = OpenError.Busy },
                                    PosixError.DQUOT => .{ .err = OpenError.DiskQuotaExceeded },
                                    PosixError.EXIST => .{ .err = OpenError.AlreadyExists },
                                    PosixError.FAULT => .{ .err = OpenError.InvalidAddress },
                                    PosixError.FBIG, PosixError.OVERFLOW => .{ .err = OpenError.FileTooBig },
                                    PosixError.INTR => .{ .err = OpenError.Interrupted },
                                    PosixError.INVAL => .{ .err = OpenError.InvalidArguments },
                                    PosixError.ISDIR => .{ .err = OpenError.IsDirectory },
                                    PosixError.LOOP => .{ .err = OpenError.Loop },
                                    PosixError.MFILE => .{ .err = OpenError.ProcessFdQuotaExceeded },
                                    PosixError.NAMETOOLONG => .{ .err = OpenError.NameTooLong },
                                    PosixError.NFILE => .{ .err = OpenError.SystemFdQuotaExceeded },
                                    PosixError.NODEV, PosixError.NXIO => .{ .err = OpenError.DeviceNotFound },
                                    PosixError.NOENT => .{ .err = OpenError.NotFound },
                                    PosixError.NOMEM => .{ .err = OpenError.OutOfMemory },
                                    PosixError.NOSPC => .{ .err = OpenError.NoSpace },
                                    PosixError.NOTDIR => .{ .err = OpenError.NotADirectory },
                                    PosixError.OPNOTSUPP => .{ .err = OpenError.OperationNotSupported },
                                    PosixError.ROFS => .{ .err = OpenError.ReadOnlyFileSystem },
                                    PosixError.TXTBSY => .{ .err = OpenError.FileLocked },
                                    else => .{ .err = OpenError.Unexpected },
                                };
                            };

                            break :blk .{ .open = result };
                        },
                        .delete => |inner| {
                            const mode: u32 = if (inner.is_dir) std.posix.AT.REMOVEDIR else 0;
                            const rc = switch (inner.path) {
                                .rel => |path| std.posix.system.unlinkat(path.dir, path.path, mode),
                                .abs => |path| std.posix.system.unlinkat(std.posix.AT.FDCWD, path, mode),
                            };

                            const result: DeleteResult = result: {
                                const e: PosixError = std.posix.errno(rc);
                                break :result switch (e) {
                                    PosixError.SUCCESS => .{ .actual = {} },
                                    // unlink
                                    PosixError.AGAIN => {
                                        job_complete = false;
                                        continue :blocking_loop;
                                    },
                                    PosixError.ACCES => .{ .err = DeleteError.AccessDenied },
                                    PosixError.BUSY => .{ .err = DeleteError.Busy },
                                    PosixError.FAULT => .{ .err = DeleteError.InvalidAddress },
                                    PosixError.IO => .{ .err = DeleteError.IoError },
                                    PosixError.ISDIR, PosixError.PERM => .{ .err = DeleteError.IsDirectory },
                                    PosixError.LOOP => .{ .err = DeleteError.Loop },
                                    PosixError.NAMETOOLONG => .{ .err = DeleteError.NameTooLong },
                                    PosixError.NOENT => .{ .err = DeleteError.NotFound },
                                    PosixError.NOMEM => .{ .err = DeleteError.OutOfMemory },
                                    PosixError.NOTDIR => .{ .err = DeleteError.IsNotDirectory },
                                    PosixError.ROFS => .{ .err = DeleteError.ReadOnlyFileSystem },
                                    PosixError.BADF => .{ .err = DeleteError.InvalidFd },
                                    // rmdir
                                    PosixError.INVAL => .{ .err = DeleteError.InvalidArguments },
                                    PosixError.NOTEMPTY => .{ .err = DeleteError.NotEmpty },
                                    else => .{ .err = DeleteError.Unexpected },
                                };
                            };

                            break :blk .{ .delete = result };
                        },
                        .stat => |fd| {
                            var fstat: std.posix.system.Stat = undefined;
                            const rc = std.posix.system.fstat(fd, &fstat);

                            const result: StatResult = result: {
                                const e: PosixError = std.posix.errno(rc);
                                break :result switch (e) {
                                    PosixError.SUCCESS => {
                                        const atim = fstat.atime();
                                        const mtim = fstat.mtime();
                                        const ctim = fstat.ctime();

                                        const stat = Stat{
                                            .size = @intCast(fstat.size),
                                            .mode = @intCast(fstat.mode),
                                            .accessed = .{
                                                .seconds = @intCast(atim.tv_sec),
                                                .nanos = @intCast(atim.tv_nsec),
                                            },
                                            .modified = .{
                                                .seconds = @intCast(mtim.tv_sec),
                                                .nanos = @intCast(mtim.tv_nsec),
                                            },
                                            .changed = .{
                                                .seconds = @intCast(ctim.tv_sec),
                                                .nanos = @intCast(ctim.tv_nsec),
                                            },
                                        };

                                        break :result .{ .actual = stat };
                                    },
                                    PosixError.ACCES => .{ .err = StatError.AccessDenied },
                                    PosixError.BADF => .{ .err = StatError.InvalidFd },
                                    PosixError.FAULT => .{ .err = StatError.InvalidAddress },
                                    PosixError.INVAL => .{ .err = StatError.InvalidArguments },
                                    PosixError.LOOP => .{ .err = StatError.Loop },
                                    PosixError.NAMETOOLONG => .{ .err = StatError.NameTooLong },
                                    PosixError.NOENT => .{ .err = StatError.NotFound },
                                    PosixError.NOMEM => .{ .err = StatError.OutOfMemory },
                                    PosixError.NOTDIR => .{ .err = StatError.NotADirectory },
                                    else => .{ .err = StatError.Unexpected },
                                };
                            };

                            break :blk .{ .stat = result };
                        },
                        .read => |inner| {
                            const rc = if (inner.offset) |offset|
                                std.posix.system.pread(inner.fd, inner.buffer.ptr, inner.buffer.len, @intCast(offset))
                            else
                                std.posix.system.read(inner.fd, inner.buffer.ptr, inner.buffer.len);

                            const result: ReadResult = result: {
                                const e: PosixError = std.posix.errno(rc);
                                break :result switch (e) {
                                    PosixError.SUCCESS => switch (rc) {
                                        0 => .{ .err = ReadError.EndOfFile },
                                        else => .{ .actual = @intCast(rc) },
                                    },
                                    PosixError.AGAIN => {
                                        job_complete = false;
                                        continue :blocking_loop;
                                    },
                                    // If it is unseekable...
                                    PosixError.NXIO, PosixError.SPIPE, PosixError.OVERFLOW => .{
                                        .err = ReadError.Unexpected,
                                    },
                                    PosixError.BADF => .{ .err = ReadError.InvalidFd },
                                    PosixError.FAULT => .{ .err = ReadError.InvalidAddress },
                                    PosixError.INTR => .{ .err = ReadError.Interrupted },
                                    PosixError.INVAL => .{ .err = ReadError.InvalidArguments },
                                    PosixError.IO => .{ .err = ReadError.IoError },
                                    PosixError.ISDIR => .{ .err = ReadError.IsDirectory },
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

                            const result: WriteResult = result: {
                                const e: PosixError = std.posix.errno(rc);
                                break :result switch (e) {
                                    PosixError.SUCCESS => .{ .actual = @intCast(rc) },
                                    PosixError.AGAIN => {
                                        job_complete = false;
                                        continue :blocking_loop;
                                    },
                                    // If it is unseekable...
                                    PosixError.NXIO, PosixError.SPIPE, PosixError.OVERFLOW => .{
                                        .err = WriteError.Unexpected,
                                    },
                                    PosixError.BADF => .{ .err = WriteError.InvalidFd },
                                    PosixError.DESTADDRREQ => .{ .err = WriteError.NoDestinationAddress },
                                    PosixError.DQUOT => .{ .err = WriteError.DiskQuotaExceeded },
                                    PosixError.FAULT => .{ .err = WriteError.InvalidAddress },
                                    PosixError.FBIG => .{ .err = WriteError.FileTooBig },
                                    PosixError.INTR => .{ .err = WriteError.Interrupted },
                                    PosixError.INVAL => .{ .err = WriteError.InvalidArguments },
                                    PosixError.IO => .{ .err = WriteError.IoError },
                                    PosixError.NOSPC => .{ .err = WriteError.NoSpace },
                                    PosixError.PERM => .{ .err = WriteError.AccessDenied },
                                    PosixError.PIPE => .{ .err = WriteError.BrokenPipe },
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

            const remaining = self.completions.len - reaped;
            if (remaining == 0) break;

            const timeout_spec: std.posix.timespec = .{ .tv_sec = 0, .tv_nsec = 0 };
            const timeout: ?*const std.posix.timespec = if (busy_wait or reaped > 0) &timeout_spec else null;
            log.debug("remaining count={d}", .{remaining});

            // Handle all of the kqueue I/O
            const kqueue_events = try std.posix.kevent(
                kqueue.kqueue_fd,
                &.{},
                kqueue.events[0..remaining],
                timeout,
            );

            for (kqueue.events[0..kqueue_events]) |event| {
                const job_index = event.udata;
                assert(kqueue.jobs.dirty.isSet(job_index));

                var job_complete = true;
                defer if (job_complete) kqueue.jobs.release(job_index);

                const job = kqueue.jobs.get_ptr(job_index);

                const result: Result = blk: {
                    switch (job.type) {
                        else => unreachable,
                        .wake => {
                            assert(event.filter == std.posix.system.EVFILT_USER);
                            assert(event.ident == WAKE_IDENT);
                            job_complete = false;
                            break :blk .wake;
                        },
                        .timer => |inner| {
                            assert(event.filter == std.posix.system.EVFILT_TIMER);
                            assert(inner == .none);
                            break :blk .none;
                        },
                        .accept => |*inner| {
                            assert(event.filter == std.posix.system.EVFILT_READ);
                            const rc = std.posix.system.accept(
                                inner.socket,
                                &inner.addr.any,
                                @ptrCast(&inner.addr_len),
                            );

                            const result: AcceptResult = result: {
                                const e: PosixError = std.posix.errno(rc);
                                break :result switch (e) {
                                    PosixError.SUCCESS => .{
                                        .actual = .{
                                            .handle = @intCast(rc),
                                            .addr = inner.addr,
                                            .kind = inner.kind,
                                        },
                                    },
                                    PosixError.AGAIN => .{ .err = AcceptError.WouldBlock },
                                    PosixError.BADF => .{ .err = AcceptError.InvalidFd },
                                    PosixError.CONNABORTED => .{ .err = AcceptError.ConnectionAborted },
                                    PosixError.FAULT => .{ .err = AcceptError.InvalidAddress },
                                    PosixError.INTR => .{ .err = AcceptError.Interrupted },
                                    PosixError.INVAL => .{ .err = AcceptError.NotListening },
                                    PosixError.MFILE => .{ .err = AcceptError.ProcessFdQuotaExceeded },
                                    PosixError.NFILE => .{ .err = AcceptError.SystemFdQuotaExceeded },
                                    PosixError.NOBUFS, PosixError.NOMEM => .{ .err = AcceptError.OutOfMemory },
                                    PosixError.NOTSOCK => .{ .err = AcceptError.NotASocket },
                                    PosixError.OPNOTSUPP => .{ .err = AcceptError.OperationNotSupported },
                                    else => .{ .err = AcceptError.Unexpected },
                                };
                            };

                            break :blk .{ .accept = result };
                        },
                        .connect => |inner| {
                            assert(event.filter == std.posix.system.EVFILT_WRITE);
                            const rc = std.os.linux.connect(inner.socket, &inner.addr.any, @sizeOf(std.net.Address));

                            const result: ConnectResult = result: {
                                const e: PosixError = std.posix.errno(rc);
                                break :result switch (e) {
                                    PosixError.SUCCESS => .{
                                        .actual = .{
                                            .handle = inner.socket,
                                            .addr = inner.addr,
                                            .kind = inner.kind,
                                        },
                                    },
                                    PosixError.AGAIN, PosixError.ALREADY, PosixError.INPROGRESS => .{
                                        .err = ConnectError.WouldBlock,
                                    },
                                    PosixError.ACCES, PosixError.PERM => .{ .err = ConnectError.AccessDenied },
                                    PosixError.ADDRINUSE => .{ .err = ConnectError.AddressInUse },
                                    PosixError.ADDRNOTAVAIL => .{ .err = ConnectError.AddressNotAvailable },
                                    PosixError.AFNOSUPPORT => .{ .err = ConnectError.AddressFamilyNotSupported },
                                    PosixError.BADF => .{ .err = ConnectError.InvalidFd },
                                    PosixError.CONNREFUSED => .{ .err = ConnectError.ConnectionRefused },
                                    PosixError.FAULT => .{ .err = ConnectError.InvalidAddress },
                                    PosixError.INTR => .{ .err = ConnectError.Interrupted },
                                    PosixError.ISCONN => .{ .err = ConnectError.AlreadyConnected },
                                    PosixError.NETUNREACH => .{ .err = ConnectError.NetworkUnreachable },
                                    PosixError.NOTSOCK => .{ .err = ConnectError.NotASocket },
                                    PosixError.PROTOTYPE => .{ .err = ConnectError.ProtocolFamilyNotSupported },
                                    PosixError.TIMEDOUT => .{ .err = ConnectError.TimedOut },
                                    else => .{ .err = ConnectError.Unexpected },
                                };
                            };

                            break :blk .{ .connect = result };
                        },
                        .recv => |inner| {
                            assert(event.filter == std.posix.system.EVFILT_READ);
                            const rc = std.posix.system.recvfrom(
                                inner.socket,
                                inner.buffer.ptr,
                                inner.buffer.len,
                                0,
                                null,
                                null,
                            );

                            const result: RecvResult = result: {
                                const e: PosixError = std.posix.errno(rc);
                                break :result switch (e) {
                                    PosixError.SUCCESS => switch (rc) {
                                        0 => .{ .err = RecvError.Closed },
                                        else => .{ .actual = @intCast(rc) },
                                    },
                                    PosixError.AGAIN => .{ .err = RecvError.WouldBlock },
                                    PosixError.BADF => .{ .err = RecvError.InvalidFd },
                                    PosixError.CONNREFUSED => .{ .err = RecvError.ConnectionRefused },
                                    PosixError.FAULT => .{ .err = RecvError.InvalidAddress },
                                    PosixError.INTR => .{ .err = RecvError.Interrupted },
                                    PosixError.INVAL => .{ .err = RecvError.InvalidArguments },
                                    PosixError.NOMEM => .{ .err = RecvError.OutOfMemory },
                                    PosixError.NOTCONN => .{ .err = RecvError.NotConnected },
                                    PosixError.NOTSOCK => .{ .err = RecvError.NotASocket },
                                    else => .{ .err = RecvError.Unexpected },
                                };
                            };

                            break :blk .{ .recv = result };
                        },
                        .send => |inner| {
                            assert(event.filter == std.posix.system.EVFILT_WRITE);
                            const rc = std.posix.system.send(inner.socket, inner.buffer.ptr, inner.buffer.len, 0);

                            const result: SendResult = result: {
                                const e: PosixError = std.posix.errno(rc);
                                break :result switch (e) {
                                    PosixError.SUCCESS => .{ .actual = @intCast(rc) },
                                    PosixError.AGAIN => .{ .err = SendError.WouldBlock },
                                    PosixError.ACCES => .{ .err = SendError.AccessDenied },
                                    PosixError.ALREADY => .{ .err = SendError.OpenInProgress },
                                    PosixError.BADF => .{ .err = SendError.InvalidFd },
                                    PosixError.CONNRESET => .{ .err = SendError.ConnectionReset },
                                    PosixError.DESTADDRREQ => .{ .err = SendError.NoDestinationAddress },
                                    PosixError.FAULT => .{ .err = SendError.InvalidAddress },
                                    PosixError.INTR => .{ .err = SendError.Interrupted },
                                    PosixError.INVAL => .{ .err = SendError.InvalidArguments },
                                    PosixError.ISCONN => .{ .err = SendError.AlreadyConnected },
                                    PosixError.MSGSIZE => .{ .err = SendError.InvalidSize },
                                    PosixError.NOBUFS, PosixError.NOMEM => .{ .err = SendError.OutOfMemory },
                                    PosixError.NOTCONN => .{ .err = SendError.NotConnected },
                                    PosixError.OPNOTSUPP => .{ .err = SendError.OperationNotSupported },
                                    PosixError.PIPE => .{ .err = SendError.BrokenPipe },
                                    else => .{ .err = SendError.Unexpected },
                                };
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

    pub fn to_async(self: *AsyncKqueue) AsyncIO {
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
