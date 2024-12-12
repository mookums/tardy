const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.@"tardy/aio/kqueue");

const Completion = @import("../completion.zig").Completion;
const Result = @import("../completion.zig").Result;
const Stat = @import("../completion.zig").Stat;
const Timespec = @import("../timespec.zig").Timespec;

const AsyncIO = @import("../lib.zig").AsyncIO;
const AsyncIOOptions = @import("../lib.zig").AsyncIOOptions;
const Job = @import("../job.zig").Job;
const Pool = @import("../../core/pool.zig").Pool;

const PosixError = std.posix.E;

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

const Atomic = std.atomic.Value;

const WAKE_IDENT = 1;

pub const AsyncKQueue = struct {
    kqueue_fd: std.posix.fd_t,
    changes: []std.posix.Kevent,
    change_count: usize = 0,
    events: []std.posix.Kevent,
    jobs: Pool(Job),
    blocking: std.ArrayList(*Job),

    pub fn init(allocator: std.mem.Allocator, options: AsyncIOOptions) !AsyncKQueue {
        const kqueue_fd = try std.posix.kqueue();
        assert(kqueue_fd > -1);

        const events = try allocator.alloc(std.posix.Kevent, options.size_aio_reap_max);
        const changes = try allocator.alloc(std.posix.Kevent, options.size_aio_jobs_max);
        var jobs = try Pool(Job).init(allocator, options.size_aio_jobs_max + 1, null, null);
        const blocking = std.ArrayList(*Job).init(allocator);

        {
            const borrowed = jobs.borrow_assume_unset(0);
            borrowed.item.* = .{
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

        return AsyncKQueue{
            .kqueue_fd = kqueue_fd,
            .events = events,
            .changes = changes,
            .change_count = 0,
            .jobs = jobs,
            .blocking = blocking,
        };
    }

    pub fn deinit(self: *AsyncIO, allocator: std.mem.Allocator) void {
        const kqueue: *AsyncKQueue = @ptrCast(@alignCast(self.runner));
        std.posix.close(kqueue.kqueue_fd);
        allocator.free(kqueue.events);
        allocator.free(kqueue.changes);
        kqueue.jobs.deinit(null, null);
        kqueue.blocking.deinit();
    }

    pub fn queue_timer(
        self: *AsyncIO,
        task: usize,
        timespec: Timespec,
    ) !void {
        const kqueue: *AsyncKQueue = @ptrCast(@alignCast(self.runner));
        const borrowed = try kqueue.jobs.borrow_hint(task);

        borrowed.item.* = .{
            .index = borrowed.index,
            .type = .{ .timer = .none },
            .task = task,
        };

        // kqueue uses milliseconds.
        const milliseconds: isize = @intCast(
            timespec.seconds * 1000 + @divFloor(timespec.nanos, std.time.ns_per_ms),
        );

        const event = &kqueue.changes[kqueue.change_count];
        assert(kqueue.change_count < kqueue.changes.len);
        kqueue.change_count += 1;

        event.* = .{
            .ident = borrowed.index,
            .filter = std.posix.system.EVFILT_TIMER,
            .flags = std.posix.system.EV_ADD | std.posix.system.EV_ONESHOT,
            .fflags = 0,
            .data = milliseconds,
            .udata = borrowed.index,
        };
    }

    pub fn queue_open(
        self: *AsyncIO,
        task: usize,
        path: [:0]const u8,
    ) !void {
        const kqueue: *AsyncKQueue = @ptrCast(@alignCast(self.runner));
        const borrowed = try kqueue.jobs.borrow_hint(task);
        borrowed.item.* = .{
            .index = borrowed.index,
            .type = .{ .open = path },
            .task = task,
        };

        try kqueue.blocking.append(borrowed.item);
    }

    pub fn queue_stat(
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
    ) !void {
        const kqueue: *AsyncKQueue = @ptrCast(@alignCast(self.runner));
        const borrowed = try kqueue.jobs.borrow_hint(task);
        borrowed.item.* = .{
            .index = borrowed.index,
            .type = .{ .stat = fd },
            .task = task,
        };

        try kqueue.blocking.append(borrowed.item);
    }

    pub fn queue_read(
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
        buffer: []u8,
        offset: usize,
    ) !void {
        const kqueue: *AsyncKQueue = @ptrCast(@alignCast(self.runner));
        const borrowed = try kqueue.jobs.borrow_hint(task);
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

        const event = &kqueue.changes[kqueue.change_count];
        defer kqueue.change_count += 1;
        event.* = std.posix.Kevent{
            .ident = @intCast(fd),
            .filter = std.posix.system.EVFILT_READ,
            .flags = std.posix.system.EV_ADD | std.posix.system.EV_ONESHOT,
            .fflags = 0,
            .data = 0,
            .udata = borrowed.index,
        };
    }

    pub fn queue_write(
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
        buffer: []const u8,
        offset: usize,
    ) !void {
        const kqueue: *AsyncKQueue = @ptrCast(@alignCast(self.runner));
        const borrowed = try kqueue.jobs.borrow_hint(task);
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

        const event = &kqueue.changes[kqueue.change_count];
        defer kqueue.change_count += 1;
        event.* = .{
            .ident = @intCast(fd),
            .filter = std.posix.system.EVFILT_WRITE,
            .flags = std.posix.system.EV_ADD | std.posix.system.EV_ONESHOT,
            .fflags = 0,
            .data = 0,
            .udata = borrowed.index,
        };
    }

    pub fn queue_close(
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
    ) !void {
        const kqueue: *AsyncKQueue = @ptrCast(@alignCast(self.runner));
        const borrowed = try kqueue.jobs.borrow_hint(task);
        borrowed.item.* = .{
            .index = borrowed.index,
            .type = .{ .close = fd },
            .task = task,
        };

        try kqueue.blocking.append(borrowed.item);
    }

    pub fn queue_accept(
        self: *AsyncIO,
        task: usize,
        socket: std.posix.socket_t,
    ) !void {
        const kqueue: *AsyncKQueue = @ptrCast(@alignCast(self.runner));
        const borrowed = try kqueue.jobs.borrow_hint(task);
        borrowed.item.* = .{
            .index = borrowed.index,
            .type = .{ .accept = socket },
            .task = task,
        };

        const event = &kqueue.changes[kqueue.change_count];
        assert(kqueue.change_count < kqueue.changes.len);
        kqueue.change_count += 1;
        event.* = .{
            .ident = @intCast(socket),
            .filter = std.posix.system.EVFILT_READ,
            .flags = std.posix.system.EV_ADD | std.posix.system.EV_ONESHOT,
            .fflags = 0,
            .data = 0,
            .udata = borrowed.index,
        };
    }

    pub fn queue_connect(
        self: *AsyncIO,
        task: usize,
        socket: std.posix.socket_t,
        host: []const u8,
        port: u16,
    ) !void {
        const kqueue: *AsyncKQueue = @ptrCast(@alignCast(self.runner));
        const borrowed = try kqueue.jobs.borrow_hint(task);
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

        const event = &kqueue.changes[kqueue.change_count];
        defer kqueue.change_count += 1;
        event.* = .{
            .ident = @intCast(socket),
            .filter = std.posix.system.EVFILT_WRITE,
            .flags = std.posix.system.EV_ADD | std.posix.system.EV_ONESHOT,
            .fflags = 0,
            .data = 0,
            .udata = borrowed.index,
        };
    }

    pub fn queue_recv(
        self: *AsyncIO,
        task: usize,
        socket: std.posix.socket_t,
        buffer: []u8,
    ) !void {
        const kqueue: *AsyncKQueue = @ptrCast(@alignCast(self.runner));
        const borrowed = try kqueue.jobs.borrow_hint(task);
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

        const event = &kqueue.changes[kqueue.change_count];
        assert(kqueue.change_count < kqueue.changes.len);
        kqueue.change_count += 1;
        event.* = .{
            .ident = @intCast(socket),
            .filter = std.posix.system.EVFILT_READ,
            .flags = std.posix.system.EV_ADD | std.posix.system.EV_ONESHOT,
            .fflags = 0,
            .data = 0,
            .udata = borrowed.index,
        };
    }

    pub fn queue_send(
        self: *AsyncIO,
        task: usize,
        socket: std.posix.socket_t,
        buffer: []const u8,
    ) !void {
        const kqueue: *AsyncKQueue = @ptrCast(@alignCast(self.runner));
        const borrowed = try kqueue.jobs.borrow_hint(task);
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

        const event = &kqueue.changes[kqueue.change_count];
        assert(kqueue.change_count < kqueue.changes.len);
        kqueue.change_count += 1;

        event.* = .{
            .ident = @intCast(socket),
            .filter = std.posix.system.EVFILT_WRITE,
            .flags = std.posix.system.EV_ADD | std.posix.system.EV_ONESHOT,
            .fflags = 0,
            .data = 0,
            .udata = borrowed.index,
        };
    }

    pub fn wake(self: *AsyncIO) !void {
        const kqueue: *AsyncKQueue = @ptrCast(@alignCast(self.runner));

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
        const kqueue: *AsyncKQueue = @ptrCast(@alignCast(self.runner));
        _ = kqueue;
    }

    pub fn reap(self: *AsyncIO, wait: bool) ![]Completion {
        const kqueue: *AsyncKQueue = @ptrCast(@alignCast(self.runner));
        var reaped: usize = 0;
        var first_run: bool = true;

        const busy_wait: bool = !wait or kqueue.blocking.items.len > 0;
        log.debug("busy wait? {}", .{busy_wait});

        while ((reaped < 1 and wait) or first_run) {
            blocking_loop: for (0..kqueue.blocking.items.len) |_| {
                const job = kqueue.blocking.popOrNull() orelse break;
                assert(kqueue.jobs.dirty.isSet(job.index));

                if (self.completions.len - reaped == 0) break;

                var job_complete = true;
                defer if (job_complete) {
                    kqueue.jobs.release(job.index);
                } else {
                    // if not done, readd to blocking list.
                    kqueue.blocking.appendAssumeCapacity(job);
                };

                const result: Result = blk: {
                    switch (job.type) {
                        else => unreachable,
                        .open => |path| {
                            const res = std.posix.system.openat(std.posix.AT.FDCWD, path, .{}, @as(std.posix.mode_t, @intCast(0)));

                            const result: OpenResult = result: {
                                const e: PosixError = std.posix.errno(res);
                                switch (e) {
                                    PosixError.SUCCESS => break :result .{ .actual = res },
                                    PosixError.AGAIN => {
                                        job_complete = false;
                                        continue :blocking_loop;
                                    },
                                    PosixError.ACCES, PosixError.PERM => break :result .{ .err = OpenError.AccessDenied },
                                    PosixError.BADF => break :result .{ .err = OpenError.InvalidFd },
                                    PosixError.BUSY => break :result .{ .err = OpenError.FileBusy },
                                    PosixError.DQUOT => break :result .{ .err = OpenError.DiskQuotaExceeded },
                                    PosixError.EXIST => break :result .{ .err = OpenError.FileAlreadyExists },
                                    PosixError.FAULT => break :result .{ .err = OpenError.InvalidAddress },
                                    PosixError.FBIG, PosixError.OVERFLOW => break :result .{ .err = OpenError.FileTooBig },
                                    PosixError.INTR => break :result .{ .err = OpenError.Interrupted },
                                    PosixError.INVAL => break :result .{ .err = OpenError.InvalidArguments },
                                    PosixError.ISDIR => break :result .{ .err = OpenError.IsDirectory },
                                    PosixError.LOOP => break :result .{ .err = OpenError.Loop },
                                    PosixError.MFILE => break :result .{ .err = OpenError.ProcessFdQuotaExceeded },
                                    PosixError.NAMETOOLONG => break :result .{ .err = OpenError.NameTooLong },
                                    PosixError.NFILE => break :result .{ .err = OpenError.SystemFdQuotaExceeded },
                                    PosixError.NODEV, PosixError.NXIO => break :result .{ .err = OpenError.DeviceNotFound },
                                    PosixError.NOENT => break :result .{ .err = OpenError.FileNotFound },
                                    PosixError.NOMEM => break :result .{ .err = OpenError.OutOfMemory },
                                    PosixError.NOSPC => break :result .{ .err = OpenError.NoSpace },
                                    PosixError.NOTDIR => break :result .{ .err = OpenError.NotADirectory },
                                    PosixError.OPNOTSUPP => break :result .{ .err = OpenError.OperationNotSupported },
                                    PosixError.ROFS => break :result .{ .err = OpenError.ReadOnlyFileSystem },
                                    PosixError.TXTBSY => break :result .{ .err = OpenError.FileLocked },
                                    else => break :result .{ .err = OpenError.Unexpected },
                                }
                            };

                            break :blk .{ .open = result };
                        },
                        .stat => |fd| {
                            var fstat: std.posix.system.Stat = undefined;
                            const res = std.posix.system.fstat(fd, &fstat);

                            const result: StatResult = result: {
                                const e: PosixError = std.posix.errno(res);
                                switch (e) {
                                    PosixError.SUCCESS => {
                                        const atim = fstat.atime();
                                        const mtim = fstat.mtime();
                                        const ctim = fstat.ctime();

                                        const stat: Stat = .{
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
                                    PosixError.ACCES => break :result .{ .err = StatError.AccessDenied },
                                    PosixError.BADF => break :result .{ .err = StatError.InvalidFd },
                                    PosixError.FAULT => break :result .{ .err = StatError.InvalidAddress },
                                    PosixError.INVAL => break :result .{ .err = StatError.InvalidArguments },
                                    PosixError.LOOP => break :result .{ .err = StatError.Loop },
                                    PosixError.NAMETOOLONG => break :result .{ .err = StatError.NameTooLong },
                                    PosixError.NOENT => break :result .{ .err = StatError.FileNotFound },
                                    PosixError.NOMEM => break :result .{ .err = StatError.OutOfMemory },
                                    PosixError.NOTDIR => break :result .{ .err = StatError.NotADirectory },
                                    else => break :result .{ .err = StatError.Unexpected },
                                }
                            };

                            break :blk .{ .stat = result };
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

            const timeout_spec = std.mem.zeroInit(std.posix.timespec, .{});

            // Handle all of the kqueue I/O
            const kqueue_events = try std.posix.kevent(
                kqueue.kqueue_fd,
                kqueue.changes[0..kqueue.change_count],
                kqueue.events,
                if (busy_wait or reaped > 0) &timeout_spec else null,
            );
            kqueue.change_count = 0;

            for (kqueue.events[0..kqueue_events]) |event| {
                const job_index = event.udata;
                assert(kqueue.jobs.dirty.isSet(job_index));

                var job_complete = true;
                defer if (job_complete) kqueue.jobs.release(job_index);

                const job = kqueue.jobs.items[job_index];

                const result: Result = blk: {
                    switch (job.type) {
                        else => unreachable,
                        .wake => {
                            assert(event.filter == std.posix.system.EVFILT_USER);
                            job_complete = false;
                            break :blk .wake;
                        },
                        .timer => |inner| {
                            assert(inner == .none);
                            break :blk .none;
                        },
                        .accept => |socket| {
                            assert(event.filter == std.posix.system.EVFILT_READ);
                            const res = std.posix.system.accept(socket, null, null);

                            const result: AcceptResult = result: {
                                const e: PosixError = std.posix.errno(res);
                                switch (e) {
                                    PosixError.SUCCESS => break :result .{ .actual = @intCast(res) },
                                    PosixError.AGAIN => break :result .{ .err = AcceptError.WouldBlock },
                                    PosixError.BADF => break :result .{ .err = AcceptError.InvalidFd },
                                    PosixError.CONNABORTED => break :result .{ .err = AcceptError.ConnectionAborted },
                                    PosixError.FAULT => break :result .{ .err = AcceptError.InvalidAddress },
                                    PosixError.INTR => break :result .{ .err = AcceptError.Interrupted },
                                    PosixError.INVAL => break :result .{ .err = AcceptError.NotListening },
                                    PosixError.MFILE => break :result .{ .err = AcceptError.ProcessFdQuotaExceeded },
                                    PosixError.NFILE => break :result .{ .err = AcceptError.SystemFdQuotaExceeded },
                                    PosixError.NOBUFS, PosixError.NOMEM => {
                                        break :result .{ .err = AcceptError.OutOfMemory };
                                    },
                                    PosixError.NOTSOCK => break :result .{ .err = AcceptError.NotASocket },
                                    PosixError.OPNOTSUPP => break :result .{ .err = AcceptError.OperationNotSupported },
                                    else => break :result .{ .err = AcceptError.Unexpected },
                                }
                            };

                            break :blk .{ .accept = result };
                        },
                        .connect => |inner| {
                            assert(event.filter == std.posix.system.EVFILT_WRITE);
                            const addr_len: std.posix.socklen_t = switch (inner.addr.any.family) {
                                std.posix.AF.INET => @sizeOf(std.posix.sockaddr.in),
                                std.posix.AF.INET6 => @sizeOf(std.posix.sockaddr.in6),
                                std.posix.AF.UNIX => @sizeOf(std.posix.sockaddr.un),
                                else => @panic("Unsupported!"),
                            };

                            const res = std.os.linux.connect(inner.socket, &inner.addr.any, addr_len);

                            const result: ConnectResult = result: {
                                const e: PosixError = std.posix.errno(res);
                                switch (e) {
                                    PosixError.SUCCESS => break :result .{ .actual = inner.socket },
                                    PosixError.AGAIN, PosixError.ALREADY, PosixError.INPROGRESS => {
                                        break :result .{ .err = ConnectError.WouldBlock };
                                    },
                                    PosixError.ACCES, PosixError.PERM => break :result .{ .err = ConnectError.AccessDenied },
                                    PosixError.ADDRINUSE => break :result .{ .err = ConnectError.AddressInUse },
                                    PosixError.ADDRNOTAVAIL => break :result .{ .err = ConnectError.AddressNotAvailable },
                                    PosixError.AFNOSUPPORT => break :result .{ .err = ConnectError.AddressFamilyNotSupported },
                                    PosixError.BADF => break :result .{ .err = ConnectError.InvalidFd },
                                    PosixError.CONNREFUSED => break :result .{ .err = ConnectError.ConnectionRefused },
                                    PosixError.FAULT => break :result .{ .err = ConnectError.InvalidAddress },
                                    PosixError.INTR => break :result .{ .err = ConnectError.Interrupted },
                                    PosixError.ISCONN => break :result .{ .err = ConnectError.AlreadyConnected },
                                    PosixError.NETUNREACH => break :result .{ .err = ConnectError.NetworkUnreachable },
                                    PosixError.NOTSOCK => break :result .{ .err = ConnectError.NotASocket },
                                    PosixError.PROTOTYPE => break :result .{ .err = ConnectError.ProtocolFamilyNotSupported },
                                    PosixError.TIMEDOUT => break :result .{ .err = ConnectError.TimedOut },
                                    else => break :result .{ .err = ConnectError.Unexpected },
                                }
                            };

                            break :blk .{ .connect = result };
                        },
                        .recv => |inner| {
                            assert(event.filter == std.posix.system.EVFILT_READ);
                            const res = std.posix.system.recvfrom(inner.socket, inner.buffer.ptr, inner.buffer.len, 0, null, null);

                            const result: RecvResult = result: {
                                const e: PosixError = std.posix.errno(res);
                                switch (e) {
                                    PosixError.SUCCESS => if (res == 0) {
                                        break :result .{ .err = RecvError.Closed };
                                    } else {
                                        break :result .{ .actual = @intCast(res) };
                                    },
                                    PosixError.AGAIN => break :result .{ .err = RecvError.WouldBlock },
                                    PosixError.BADF => break :result .{ .err = RecvError.InvalidFd },
                                    PosixError.CONNREFUSED => break :result .{ .err = RecvError.ConnectionRefused },
                                    PosixError.FAULT => break :result .{ .err = RecvError.InvalidAddress },
                                    PosixError.INTR => break :result .{ .err = RecvError.Interrupted },
                                    PosixError.INVAL => break :result .{ .err = RecvError.InvalidArguments },
                                    PosixError.NOMEM => break :result .{ .err = RecvError.OutOfMemory },
                                    PosixError.NOTCONN => break :result .{ .err = RecvError.NotConnected },
                                    PosixError.NOTSOCK => break :result .{ .err = RecvError.NotASocket },
                                    else => break :result .{ .err = RecvError.Unexpected },
                                }
                            };

                            break :blk .{ .recv = result };
                        },
                        .send => |inner| {
                            assert(event.filter == std.posix.system.EVFILT_WRITE);
                            const res = std.posix.system.send(inner.socket, inner.buffer.ptr, inner.buffer.len, 0);

                            const result: SendResult = result: {
                                const e: PosixError = std.posix.errno(res);
                                switch (e) {
                                    PosixError.SUCCESS => break :result .{ .actual = @intCast(res) },
                                    PosixError.AGAIN => break :result .{ .err = SendError.WouldBlock },
                                    PosixError.ACCES => break :result .{ .err = SendError.AccessDenied },
                                    PosixError.ALREADY => break :result .{ .err = SendError.OpenInProgress },
                                    PosixError.BADF => break :result .{ .err = SendError.InvalidFd },
                                    PosixError.CONNRESET => break :result .{ .err = SendError.ConnectionReset },
                                    PosixError.DESTADDRREQ => break :result .{ .err = SendError.NoDestinationAddress },
                                    PosixError.FAULT => break :result .{ .err = SendError.InvalidAddress },
                                    PosixError.INTR => break :result .{ .err = SendError.Interrupted },
                                    PosixError.INVAL => break :result .{ .err = SendError.InvalidArguments },
                                    PosixError.ISCONN => break :result .{ .err = SendError.AlreadyConnected },
                                    PosixError.MSGSIZE => break :result .{ .err = SendError.InvalidSize },
                                    PosixError.NOBUFS, PosixError.NOMEM => {
                                        break :result .{ .err = SendError.OutOfMemory };
                                    },
                                    PosixError.NOTCONN => break :result .{ .err = SendError.NotConnected },
                                    PosixError.OPNOTSUPP => break :result .{ .err = SendError.OperationNotSupported },
                                    PosixError.PIPE => break :result .{ .err = SendError.BrokenPipe },
                                    else => break :result .{ .err = SendError.Unexpected },
                                }
                            };

                            break :blk .{ .send = result };
                        },
                        .read => |inner| {
                            assert(event.filter == std.posix.system.EVFILT_READ);
                            const res = std.posix.system.pread(inner.fd, inner.buffer.ptr, inner.buffer.len, @intCast(inner.offset));

                            const result: ReadResult = result: {
                                const e: PosixError = std.posix.errno(res);
                                switch (e) {
                                    PosixError.SUCCESS => if (res == 0) {
                                        break :result .{ .err = ReadError.EndOfFile };
                                    } else {
                                        break :result .{ .actual = @intCast(res) };
                                    },
                                    // If it is unseekable...
                                    PosixError.NXIO, PosixError.SPIPE, PosixError.OVERFLOW => {
                                        // try normal read.
                                        const read_res = std.os.linux.read(inner.fd, inner.buffer.ptr, inner.buffer.len);
                                        const read_e: PosixError = std.posix.errno(read_res);
                                        switch (read_e) {
                                            PosixError.SUCCESS => if (read_res == 0) {
                                                break :result .{ .err = ReadError.EndOfFile };
                                            } else {
                                                break :result .{ .actual = @intCast(read_res) };
                                            },
                                            PosixError.AGAIN => break :result .{ .err = ReadError.WouldBlock },
                                            PosixError.BADF => break :result .{ .err = ReadError.InvalidFd },
                                            PosixError.FAULT => break :result .{ .err = ReadError.InvalidAddress },
                                            PosixError.INTR => break :result .{ .err = ReadError.Interrupted },
                                            PosixError.INVAL => break :result .{ .err = ReadError.InvalidArguments },
                                            PosixError.IO => break :result .{ .err = ReadError.IoError },
                                            PosixError.ISDIR => break :result .{ .err = ReadError.IsDirectory },
                                            else => break :result .{ .err = ReadError.Unexpected },
                                        }
                                    },
                                    PosixError.AGAIN => break :result .{ .err = ReadError.WouldBlock },
                                    PosixError.BADF => break :result .{ .err = ReadError.InvalidFd },
                                    PosixError.FAULT => break :result .{ .err = ReadError.InvalidAddress },
                                    PosixError.INTR => break :result .{ .err = ReadError.Interrupted },
                                    PosixError.INVAL => break :result .{ .err = ReadError.InvalidArguments },
                                    PosixError.IO => break :result .{ .err = ReadError.IoError },
                                    PosixError.ISDIR => break :result .{ .err = ReadError.IsDirectory },
                                    else => break :result .{ .err = ReadError.Unexpected },
                                }
                            };

                            break :blk .{ .read = result };
                        },

                        .write => |inner| {
                            assert(event.filter == std.posix.system.EVFILT_WRITE);
                            const res = std.posix.system.pwrite(inner.fd, inner.buffer.ptr, inner.buffer.len, @intCast(inner.offset));

                            const result: WriteResult = result: {
                                const e: PosixError = std.posix.errno(res);
                                switch (e) {
                                    PosixError.SUCCESS => break :result .{ .actual = @intCast(res) },
                                    // If it is unseekable...
                                    PosixError.NXIO, PosixError.SPIPE, PosixError.OVERFLOW => {
                                        // try normal write.
                                        const write_res = std.os.linux.write(inner.fd, inner.buffer.ptr, inner.buffer.len);
                                        if (write_res >= 0) break :blk .{ .write = .{ .actual = @intCast(write_res) } };
                                        const write_e: PosixError = std.posix.errno(write_res);
                                        switch (write_e) {
                                            PosixError.SUCCESS => break :result .{ .actual = @intCast(write_res) },
                                            PosixError.AGAIN => break :result .{ .err = WriteError.WouldBlock },
                                            PosixError.BADF => break :result .{ .err = WriteError.InvalidFd },
                                            PosixError.DESTADDRREQ => break :result .{ .err = WriteError.NoDestinationAddress },
                                            PosixError.DQUOT => break :result .{ .err = WriteError.DiskQuotaExceeded },
                                            PosixError.FAULT => break :result .{ .err = WriteError.InvalidAddress },
                                            PosixError.FBIG => break :result .{ .err = WriteError.FileTooBig },
                                            PosixError.INTR => break :result .{ .err = WriteError.Interrupted },
                                            PosixError.INVAL => break :result .{ .err = WriteError.InvalidArguments },
                                            PosixError.IO => break :result .{ .err = WriteError.IoError },
                                            PosixError.NOSPC => break :result .{ .err = WriteError.NoSpace },
                                            PosixError.PERM => break :result .{ .err = WriteError.AccessDenied },
                                            PosixError.PIPE => break :result .{ .err = WriteError.BrokenPipe },
                                            else => break :result .{ .err = WriteError.Unexpected },
                                        }
                                    },
                                    PosixError.AGAIN => break :result .{ .err = WriteError.WouldBlock },
                                    PosixError.BADF => break :result .{ .err = WriteError.InvalidFd },
                                    PosixError.DESTADDRREQ => break :result .{ .err = WriteError.NoDestinationAddress },
                                    PosixError.DQUOT => break :result .{ .err = WriteError.DiskQuotaExceeded },
                                    PosixError.FAULT => break :result .{ .err = WriteError.InvalidAddress },
                                    PosixError.FBIG => break :result .{ .err = WriteError.FileTooBig },
                                    PosixError.INTR => break :result .{ .err = WriteError.Interrupted },
                                    PosixError.INVAL => break :result .{ .err = WriteError.InvalidArguments },
                                    PosixError.IO => break :result .{ .err = WriteError.IoError },
                                    PosixError.NOSPC => break :result .{ .err = WriteError.NoSpace },
                                    PosixError.PERM => break :result .{ .err = WriteError.AccessDenied },
                                    PosixError.PIPE => break :result .{ .err = WriteError.BrokenPipe },
                                    else => break :result .{ .err = WriteError.Unexpected },
                                }
                            };

                            break :blk .{ .write = result };
                        },
                    }
                };

                self.completions[reaped] = .{
                    .result = result,
                    .task = job.task,
                };

                reaped += 1;
            }

            first_run = false;
        }

        return self.completions[0..reaped];
    }

    pub fn to_async(self: *AsyncKQueue) AsyncIO {
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
