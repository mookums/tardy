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
const Queue = @import("../../core/queue.zig").Queue;

const Path = @import("../../fs/lib.zig").Path;
const Socket = @import("../../net/lib.zig").Socket;

const Cross = @import("../../cross/lib.zig");
const AsyncFeatures = @import("../lib.zig").AsyncFeatures;
const AsyncSubmission = @import("../lib.zig").AsyncSubmission;

const LinuxError = std.os.linux.E;

const AcceptResult = @import("../completion.zig").AcceptResult;
const AcceptError = @import("../completion.zig").AcceptError;
const ConnectResult = @import("../completion.zig").ConnectResult;
const ConnectError = @import("../completion.zig").ConnectError;
const RecvResult = @import("../completion.zig").RecvResult;
const RecvError = @import("../completion.zig").RecvError;
const SendResult = @import("../completion.zig").SendResult;
const SendError = @import("../completion.zig").SendError;

pub const AsyncEpoll = struct {
    epoll_fd: std.posix.fd_t,
    wake_event_fd: std.posix.fd_t,
    events: []std.os.linux.epoll_event,

    jobs: Pool(Job),

    pub fn init(allocator: std.mem.Allocator, options: AsyncIOOptions) !AsyncEpoll {
        const size = options.size_tasks_initial + 1;
        const epoll_fd = try std.posix.epoll_create1(0);
        assert(epoll_fd > -1);
        errdefer std.posix.close(epoll_fd);

        const wake_event_fd: std.posix.fd_t = @intCast(std.os.linux.eventfd(0, std.os.linux.EFD.CLOEXEC));
        errdefer std.posix.close(wake_event_fd);

        const events = try allocator.alloc(std.os.linux.epoll_event, options.size_aio_reap_max);
        errdefer allocator.free(events);

        var jobs = try Pool(Job).init(allocator, size, options.pooling);
        errdefer jobs.deinit();

        // Queue the wake task.
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

        return AsyncEpoll{
            .epoll_fd = epoll_fd,
            .wake_event_fd = wake_event_fd,
            .events = events,
            .jobs = jobs,
        };
    }

    pub fn inner_deinit(self: *AsyncEpoll, allocator: std.mem.Allocator) void {
        std.posix.close(self.epoll_fd);
        allocator.free(self.events);
        self.jobs.deinit();
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
            .accept => |inner| queue_accept(epoll, task, inner.socket, inner.kind),
            .connect => |inner| queue_connect(epoll, task, inner.socket, inner.addr, inner.kind),
            .recv => |inner| queue_recv(epoll, task, inner.socket, inner.buffer),
            .send => |inner| queue_send(epoll, task, inner.socket, inner.buffer),
            .open, .delete, .mkdir, .stat, .read, .write, .close => unreachable,
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
            i += try std.posix.write(epoll.wake_event_fd, bytes[i..]);
        }
    }

    pub fn submit(_: *AsyncIO) !void {}

    pub fn reap(self: *AsyncIO, wait: bool) ![]Completion {
        const epoll: *AsyncEpoll = @ptrCast(@alignCast(self.runner));
        var reaped: usize = 0;

        while (reaped == 0 and wait) {
            const remaining = self.completions.len - reaped;
            if (remaining == 0) break;

            const timeout: i32 = if (!wait) 0 else -1;
            // Handle all of the epoll I/O
            const epoll_events = std.posix.epoll_wait(epoll.epoll_fd, epoll.events[0..remaining], timeout);
            for (epoll.events[0..epoll_events]) |event| {
                const job_index = event.data.u64;
                assert(epoll.jobs.dirty.isSet(job_index));

                var job_complete = true;
                defer if (job_complete) epoll.jobs.release(job_index);
                const job = epoll.jobs.get_ptr(job_index);

                const result: Result = blk: {
                    switch (job.type) {
                        .wake => {
                            // this keeps it in the job queue and we pretty
                            // much never want to remove this fd.
                            job_complete = false;
                            var buffer: [8]u8 = undefined;

                            // Should NEVER fail.
                            _ = std.posix.read(epoll.wake_event_fd, buffer[0..]) catch |e| {
                                log.err("wake failed: {}", .{e});
                                unreachable;
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
                                log.debug("timer failed: {}", .{e});
                                unreachable;
                            };

                            break :blk .none;
                        },
                        .accept => |*inner| {
                            assert(event.events & std.os.linux.EPOLL.IN != 0);

                            const rc = std.os.linux.accept4(
                                inner.socket,
                                &inner.addr.any,
                                @ptrCast(&inner.addr_len),
                                0,
                            );

                            const result: AcceptResult = result: {
                                const e: LinuxError = std.posix.errno(rc);
                                break :result switch (e) {
                                    LinuxError.SUCCESS => .{
                                        .actual = .{
                                            .handle = @intCast(rc),
                                            .addr = inner.addr,
                                            .kind = inner.kind,
                                        },
                                    },
                                    LinuxError.AGAIN => {
                                        job_complete = false;
                                        continue;
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
                            const rc = std.os.linux.connect(
                                inner.socket,
                                &inner.addr.any,
                                inner.addr.getOsSockLen(),
                            );

                            const result: ConnectResult = result: {
                                const e: LinuxError = std.posix.errno(rc);
                                break :result switch (e) {
                                    LinuxError.SUCCESS => .{
                                        .actual = .{
                                            .handle = inner.socket,
                                            .addr = inner.addr,
                                            .kind = inner.kind,
                                        },
                                    },
                                    LinuxError.AGAIN, LinuxError.ALREADY, LinuxError.INPROGRESS => {
                                        job_complete = false;
                                        continue;
                                    },
                                    LinuxError.ACCES, LinuxError.PERM => .{ .err = ConnectError.AccessDenied },
                                    LinuxError.ADDRINUSE => .{ .err = ConnectError.AddressInUse },
                                    LinuxError.ADDRNOTAVAIL => .{ .err = ConnectError.AddressNotAvailable },
                                    LinuxError.AFNOSUPPORT => .{ .err = ConnectError.AddressFamilyNotSupported },
                                    LinuxError.BADF => .{ .err = ConnectError.InvalidFd },
                                    LinuxError.CONNREFUSED => .{ .err = ConnectError.ConnectionRefused },
                                    LinuxError.FAULT => .{ .err = ConnectError.InvalidAddress },
                                    LinuxError.INTR => .{ .err = ConnectError.Interrupted },
                                    LinuxError.ISCONN => .{ .err = ConnectError.AlreadyConnected },
                                    LinuxError.NETUNREACH => .{ .err = ConnectError.NetworkUnreachable },
                                    LinuxError.NOTSOCK => .{ .err = ConnectError.NotASocket },
                                    LinuxError.PROTOTYPE => .{ .err = ConnectError.ProtocolFamilyNotSupported },
                                    LinuxError.TIMEDOUT => .{ .err = ConnectError.TimedOut },
                                    else => .{ .err = ConnectError.Unexpected },
                                };
                            };

                            try epoll.remove_fd(inner.socket);
                            break :blk .{ .connect = result };
                        },
                        .recv => |inner| {
                            assert(event.events & std.os.linux.EPOLL.IN != 0);
                            const rc = std.os.linux.recvfrom(
                                inner.socket,
                                inner.buffer.ptr,
                                inner.buffer.len,
                                0,
                                null,
                                null,
                            );

                            const result: RecvResult = result: {
                                const e: LinuxError = std.posix.errno(rc);
                                break :result switch (e) {
                                    LinuxError.SUCCESS => switch (rc) {
                                        0 => .{ .err = RecvError.Closed },
                                        else => .{ .actual = @intCast(rc) },
                                    },
                                    LinuxError.AGAIN => {
                                        job_complete = false;
                                        continue;
                                    },
                                    LinuxError.BADF => .{ .err = RecvError.InvalidFd },
                                    LinuxError.CONNREFUSED => .{ .err = RecvError.ConnectionRefused },
                                    LinuxError.FAULT => .{ .err = RecvError.InvalidAddress },
                                    LinuxError.INTR => .{ .err = RecvError.Interrupted },
                                    LinuxError.INVAL => .{ .err = RecvError.InvalidArguments },
                                    LinuxError.NOMEM => .{ .err = RecvError.OutOfMemory },
                                    LinuxError.NOTCONN => .{ .err = RecvError.NotConnected },
                                    LinuxError.NOTSOCK => .{ .err = RecvError.NotASocket },
                                    else => .{ .err = RecvError.Unexpected },
                                };
                            };

                            try epoll.remove_fd(inner.socket);
                            break :blk .{ .recv = result };
                        },
                        .send => |inner| {
                            assert(event.events & std.os.linux.EPOLL.OUT != 0);
                            const rc = std.os.linux.sendto(
                                inner.socket,
                                inner.buffer.ptr,
                                inner.buffer.len,
                                0,
                                null,
                                0,
                            );

                            const result: SendResult = result: {
                                const e: LinuxError = std.posix.errno(rc);
                                break :result switch (e) {
                                    LinuxError.SUCCESS => .{ .actual = @intCast(rc) },
                                    LinuxError.AGAIN => {
                                        job_complete = false;
                                        continue;
                                    },
                                    LinuxError.ACCES => .{ .err = SendError.AccessDenied },
                                    LinuxError.ALREADY => .{ .err = SendError.OpenInProgress },
                                    LinuxError.BADF => .{ .err = SendError.InvalidFd },
                                    LinuxError.CONNRESET => .{ .err = SendError.ConnectionReset },
                                    LinuxError.DESTADDRREQ => .{ .err = SendError.NoDestinationAddress },
                                    LinuxError.FAULT => .{ .err = SendError.InvalidAddress },
                                    LinuxError.INTR => .{ .err = SendError.Interrupted },
                                    LinuxError.INVAL => .{ .err = SendError.InvalidArguments },
                                    LinuxError.ISCONN => .{ .err = SendError.AlreadyConnected },
                                    LinuxError.MSGSIZE => .{ .err = SendError.InvalidSize },
                                    LinuxError.NOBUFS, LinuxError.NOMEM => .{ .err = SendError.OutOfMemory },
                                    LinuxError.NOTCONN => .{ .err = SendError.NotConnected },
                                    LinuxError.OPNOTSUPP => .{ .err = SendError.OperationNotSupported },
                                    LinuxError.PIPE => .{ .err = SendError.BrokenPipe },
                                    else => .{ .err = SendError.Unexpected },
                                };
                            };

                            break :blk .{ .send = result };
                        },
                        .open,
                        .delete,
                        .mkdir,
                        .stat,
                        .read,
                        .write,
                        .close,
                        => unreachable,
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
            .features = AsyncFeatures.init(&.{
                .timer,
                .accept,
                .connect,
                .recv,
                .send,
            }),
        };
    }
};
