const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.@"tardy/aio/epoll");

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

pub const AsyncEpoll = struct {
    epoll_fd: std.posix.fd_t,
    wake_event_fd: std.posix.fd_t,
    events: []std.os.linux.epoll_event,
    jobs: Pool(Job),
    // This is for jobs that are not supported and need
    // to be blocking.
    blocking: std.ArrayList(*Job),

    pub fn init(allocator: std.mem.Allocator, options: AsyncIOOptions) !AsyncEpoll {
        const size = options.size_aio_jobs_max + 1;
        const epoll_fd = try std.posix.epoll_create1(0);
        assert(epoll_fd > -1);

        const wake_event_fd: std.posix.fd_t = @intCast(std.os.linux.eventfd(0, std.os.linux.EFD.CLOEXEC));

        const events = try allocator.alloc(std.os.linux.epoll_event, options.size_aio_reap_max);
        var jobs = try Pool(Job).init(allocator, size, null, null);
        const blocking = std.ArrayList(*Job).init(allocator);

        // Queue the wake task.

        {
            const borrowed = jobs.borrow_assume_unset(0);
            borrowed.item.* = .{
                .index = borrowed.index,
                .type = .wake,
                .task = undefined,
            };

            var event: std.os.linux.epoll_event = .{
                .events = std.os.linux.EPOLL.IN,
                .data = .{ .u64 = borrowed.index },
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

    pub fn deinit(self: *AsyncIO, allocator: std.mem.Allocator) void {
        const epoll: *AsyncEpoll = @ptrCast(@alignCast(self.runner));
        std.posix.close(epoll.epoll_fd);
        allocator.free(epoll.events);
        epoll.jobs.deinit(null, null);
        epoll.blocking.deinit();
        std.posix.close(epoll.wake_event_fd);
    }

    pub fn queue_timer(
        self: *AsyncIO,
        task: usize,
        timespec: Timespec,
    ) !void {
        const epoll: *AsyncEpoll = @ptrCast(@alignCast(self.runner));
        const borrowed = try epoll.jobs.borrow_hint(task);

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

        _ = std.os.linux.timerfd_settime(timer_fd, .{}, &ktimerspec, null);

        borrowed.item.* = .{
            .index = borrowed.index,
            .type = .{ .timer = .{ .fd = timer_fd } },
            .task = task,
        };

        var event: std.os.linux.epoll_event = .{
            .events = std.os.linux.EPOLL.IN,
            .data = .{ .u64 = borrowed.index },
        };

        try epoll.add_fd(timer_fd, &event);
    }

    pub fn queue_open(
        self: *AsyncIO,
        task: usize,
        path: [:0]const u8,
    ) !void {
        const epoll: *AsyncEpoll = @ptrCast(@alignCast(self.runner));
        const borrowed = try epoll.jobs.borrow_hint(task);
        borrowed.item.* = .{
            .index = borrowed.index,
            .type = .{ .open = path },
            .task = task,
        };

        try epoll.blocking.append(borrowed.item);
    }

    pub fn queue_stat(
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
    ) !void {
        const epoll: *AsyncEpoll = @ptrCast(@alignCast(self.runner));
        const borrowed = try epoll.jobs.borrow_hint(task);
        borrowed.item.* = .{
            .index = borrowed.index,
            .type = .{ .stat = fd },
            .task = task,
        };

        try epoll.blocking.append(borrowed.item);
    }

    pub fn queue_read(
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
        buffer: []u8,
        offset: usize,
    ) !void {
        const epoll: *AsyncEpoll = @ptrCast(@alignCast(self.runner));
        const borrowed = try epoll.jobs.borrow_hint(task);
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

        try epoll.blocking.append(borrowed.item);
    }

    pub fn queue_write(
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
        buffer: []const u8,
        offset: usize,
    ) !void {
        const epoll: *AsyncEpoll = @ptrCast(@alignCast(self.runner));
        const borrowed = try epoll.jobs.borrow_hint(task);
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

        try epoll.blocking.append(borrowed.item);
    }

    pub fn queue_close(
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
    ) !void {
        const epoll: *AsyncEpoll = @ptrCast(@alignCast(self.runner));
        const borrowed = try epoll.jobs.borrow_hint(task);
        borrowed.item.* = .{
            .index = borrowed.index,
            .type = .{ .close = fd },
            .task = task,
        };

        epoll.remove_fd(fd) catch {};
        try epoll.blocking.append(borrowed.item);
    }

    pub fn queue_accept(
        self: *AsyncIO,
        task: usize,
        socket: std.posix.socket_t,
    ) !void {
        const epoll: *AsyncEpoll = @ptrCast(@alignCast(self.runner));
        const borrowed = try epoll.jobs.borrow_hint(task);
        borrowed.item.* = .{
            .index = borrowed.index,
            .type = .{ .accept = socket },
            .task = task,
        };

        var event: std.os.linux.epoll_event = .{
            .events = std.os.linux.EPOLL.IN,
            .data = .{ .u64 = borrowed.index },
        };

        try epoll.add_or_mod_fd(socket, &event);
    }

    pub fn queue_connect(
        self: *AsyncIO,
        task: usize,
        socket: std.posix.socket_t,
        host: []const u8,
        port: u16,
    ) !void {
        const epoll: *AsyncEpoll = @ptrCast(@alignCast(self.runner));
        const borrowed = try epoll.jobs.borrow_hint(task);
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

        var event: std.os.linux.epoll_event = .{
            .events = std.os.linux.EPOLL.OUT,
            .data = .{ .u64 = borrowed.index },
        };

        try epoll.add_or_mod_fd(socket, &event);
    }

    pub fn queue_recv(
        self: *AsyncIO,
        task: usize,
        socket: std.posix.socket_t,
        buffer: []u8,
    ) !void {
        const epoll: *AsyncEpoll = @ptrCast(@alignCast(self.runner));
        const borrowed = try epoll.jobs.borrow_hint(task);
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

        var event: std.os.linux.epoll_event = .{
            .events = std.os.linux.EPOLL.IN,
            .data = .{ .u64 = borrowed.index },
        };

        try epoll.add_or_mod_fd(socket, &event);
    }

    pub fn queue_send(
        self: *AsyncIO,
        task: usize,
        socket: std.posix.socket_t,
        buffer: []const u8,
    ) !void {
        const epoll: *AsyncEpoll = @ptrCast(@alignCast(self.runner));
        const borrowed = try epoll.jobs.borrow_hint(task);
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

        var event: std.os.linux.epoll_event = .{
            .events = std.os.linux.EPOLL.OUT,
            .data = .{ .u64 = borrowed.index },
        };

        try epoll.add_or_mod_fd(socket, &event);
    }

    fn add_or_mod_fd(self: *AsyncEpoll, fd: std.posix.fd_t, event: *std.os.linux.epoll_event) !void {
        self.add_fd(fd, event) catch |e| {
            if (e == error.FileDescriptorAlreadyPresentInSet) {
                try self.mod_fd(fd, event);
            } else return e;
        };
    }

    fn add_fd(self: *AsyncEpoll, fd: std.posix.fd_t, event: *std.os.linux.epoll_event) !void {
        try std.posix.epoll_ctl(self.epoll_fd, std.os.linux.EPOLL.CTL_ADD, fd, event);
    }

    fn mod_fd(self: *AsyncEpoll, fd: std.posix.fd_t, event: *std.os.linux.epoll_event) !void {
        try std.posix.epoll_ctl(self.epoll_fd, std.os.linux.EPOLL.CTL_MOD, fd, event);
    }

    fn remove_fd(self: *AsyncEpoll, fd: std.posix.fd_t) !void {
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

    pub fn submit(self: *AsyncIO) !void {
        const epoll: *AsyncEpoll = @ptrCast(@alignCast(self.runner));
        _ = epoll;
    }

    pub fn reap(self: *AsyncIO, wait: bool) ![]Completion {
        const epoll: *AsyncEpoll = @ptrCast(@alignCast(self.runner));
        var reaped: usize = 0;
        var first_run: bool = true;

        const busy_wait: bool = !wait or epoll.blocking.items.len > 0;
        log.debug("busy wait? {}", .{busy_wait});

        while ((reaped < 1 and wait) or first_run) {
            blocking_loop: for (0..epoll.blocking.items.len) |_| {
                const job = epoll.blocking.popOrNull() orelse break;
                assert(epoll.jobs.dirty.isSet(job.index));

                if (self.completions.len - reaped == 0) break;

                var job_complete = true;
                defer if (job_complete) {
                    epoll.jobs.release(job.index);
                } else {
                    // if not done, readd to blocking list.
                    epoll.blocking.appendAssumeCapacity(job);
                };

                const result: Result = blk: {
                    switch (job.type) {
                        else => unreachable,
                        .open => |path| {
                            const res = std.os.linux.openat(std.posix.AT.FDCWD, path, .{}, 0);
                            if (res >= 0) break :blk .{ .open = .{ .actual = @intCast(res) } };

                            const result: OpenResult = result: {
                                const e: LinuxError = @enumFromInt(-res);
                                switch (e) {
                                    LinuxError.AGAIN => {
                                        job_complete = false;
                                        continue :blocking_loop;
                                    },

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
                                    else => break :result .{ .err = OpenError.Unexpected },
                                }
                            };

                            break :blk .{ .open = result };
                        },
                        .stat => |fd| {
                            var fstat: std.os.linux.Stat = undefined;
                            const res = std.os.linux.fstat(fd, &fstat);

                            const result: StatResult = result: {
                                const e: LinuxError = std.posix.errno(res);
                                switch (e) {
                                    LinuxError.SUCCESS => {
                                        const stat: Stat = .{
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

                                        break :result .{ .actual = stat };
                                    },
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
                        .read => |inner| {
                            const res = std.os.linux.pread(
                                inner.fd,
                                inner.buffer.ptr,
                                inner.buffer.len,
                                @intCast(inner.offset),
                            );

                            const result: ReadResult = result: {
                                const e: LinuxError = std.posix.errno(res);
                                switch (e) {
                                    LinuxError.SUCCESS => if (res == 0) {
                                        break :result .{ .err = ReadError.EndOfFile };
                                    } else {
                                        break :result .{ .actual = @intCast(res) };
                                    },
                                    // If it is unseekable...
                                    LinuxError.NXIO, LinuxError.SPIPE, LinuxError.OVERFLOW => {
                                        // try normal read.
                                        const read_res = std.os.linux.read(
                                            inner.fd,
                                            inner.buffer.ptr,
                                            inner.buffer.len,
                                        );
                                        const read_e: LinuxError = std.posix.errno(read_res);
                                        switch (read_e) {
                                            LinuxError.SUCCESS => if (read_res == 0) {
                                                break :result .{ .err = ReadError.EndOfFile };
                                            } else {
                                                break :result .{ .actual = @intCast(read_res) };
                                            },
                                            LinuxError.AGAIN => {
                                                job_complete = false;
                                                continue :blocking_loop;
                                            },
                                            LinuxError.BADF => break :result .{ .err = ReadError.InvalidFd },
                                            LinuxError.FAULT => break :result .{ .err = ReadError.InvalidAddress },
                                            LinuxError.INTR => break :result .{ .err = ReadError.Interrupted },
                                            LinuxError.INVAL => break :result .{ .err = ReadError.InvalidArguments },
                                            LinuxError.IO => break :result .{ .err = ReadError.IoError },
                                            LinuxError.ISDIR => break :result .{ .err = ReadError.IsDirectory },
                                            else => break :result .{ .err = ReadError.Unexpected },
                                        }
                                    },
                                    LinuxError.AGAIN => {
                                        job_complete = false;
                                        continue :blocking_loop;
                                    },
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
                        .write => |inner| {
                            const res = std.os.linux.pwrite(inner.fd, inner.buffer.ptr, inner.buffer.len, @intCast(inner.offset));

                            const result: WriteResult = result: {
                                const e: LinuxError = std.posix.errno(res);
                                switch (e) {
                                    LinuxError.SUCCESS => break :result .{ .actual = @intCast(res) },
                                    // If it is unseekable...
                                    LinuxError.NXIO, LinuxError.SPIPE, LinuxError.OVERFLOW => {
                                        // try normal write.
                                        const write_res = std.os.linux.write(inner.fd, inner.buffer.ptr, inner.buffer.len);
                                        if (write_res >= 0) break :blk .{ .write = .{ .actual = @intCast(write_res) } };
                                        const write_e: LinuxError = std.posix.errno(write_res);
                                        switch (write_e) {
                                            LinuxError.SUCCESS => break :result .{ .actual = @intCast(write_res) },
                                            LinuxError.AGAIN => {
                                                job_complete = false;
                                                continue :blocking_loop;
                                            },
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
                                    },
                                    LinuxError.AGAIN => {
                                        job_complete = false;
                                        continue :blocking_loop;
                                    },
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
                const job = epoll.jobs.items[job_index];

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
                        .accept => |socket| {
                            assert(event.events & std.os.linux.EPOLL.IN != 0);
                            const res = std.os.linux.accept4(socket, null, null, 0);

                            const result: AcceptResult = result: {
                                const e: LinuxError = std.posix.errno(res);
                                switch (e) {
                                    LinuxError.SUCCESS => break :result .{ .actual = @intCast(res) },
                                    LinuxError.AGAIN => {
                                        job_complete = false;
                                        continue :epoll_loop;
                                    },
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

                            try epoll.remove_fd(socket);
                            break :blk .{ .accept = result };
                        },
                        .connect => |inner| {
                            assert(event.events & std.os.linux.EPOLL.OUT != 0);
                            const addr_len: std.posix.socklen_t = switch (inner.addr.any.family) {
                                std.posix.AF.INET => @sizeOf(std.posix.sockaddr.in),
                                std.posix.AF.INET6 => @sizeOf(std.posix.sockaddr.in6),
                                std.posix.AF.UNIX => @sizeOf(std.posix.sockaddr.un),
                                else => @panic("Unsupported!"),
                            };

                            const res = std.os.linux.connect(inner.socket, &inner.addr.any, addr_len);

                            const result: ConnectResult = result: {
                                const e: LinuxError = std.posix.errno(res);
                                switch (e) {
                                    LinuxError.SUCCESS => break :result .{ .actual = inner.socket },
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
                            const res = std.os.linux.recvfrom(inner.socket, inner.buffer.ptr, inner.buffer.len, 0, null, null);

                            const result: RecvResult = result: {
                                const e: LinuxError = std.posix.errno(res);
                                switch (e) {
                                    LinuxError.SUCCESS => if (res == 0) {
                                        break :result .{ .err = RecvError.Closed };
                                    } else {
                                        break :result .{ .actual = @intCast(res) };
                                    },
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

            first_run = false;
        }

        return self.completions[0..reaped];
    }

    pub fn to_async(self: *AsyncEpoll) AsyncIO {
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
