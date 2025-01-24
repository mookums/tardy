const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.@"tardy/aio/kqueue");

const Completion = @import("../completion.zig").Completion;
const Result = @import("../completion.zig").Result;
const Timespec = @import("../../lib.zig").Timespec;

const AsyncIO = @import("../lib.zig").AsyncIO;
const AsyncIOOptions = @import("../lib.zig").AsyncIOOptions;
const Job = @import("../job.zig").Job;
const Pool = @import("../../core/pool.zig").Pool;

const Socket = @import("../../net/lib.zig").Socket;
const AsyncFeatures = @import("../lib.zig").AsyncFeatures;
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

const Atomic = std.atomic.Value;

const WAKE_IDENT = 1;

pub const AsyncKqueue = struct {
    kqueue_fd: std.posix.fd_t,

    changes: []std.posix.Kevent,
    change_count: usize = 0,
    events: []std.posix.Kevent,

    jobs: Pool(Job),

    pub fn init(allocator: std.mem.Allocator, options: AsyncIOOptions) !AsyncKqueue {
        const kqueue_fd = try std.posix.kqueue();
        assert(kqueue_fd > -1);
        errdefer std.posix.close(kqueue_fd);

        const events = try allocator.alloc(std.posix.Kevent, options.size_aio_reap_max);
        const changes = try allocator.alloc(std.posix.Kevent, options.size_aio_reap_max);
        var jobs = try Pool(Job).init(allocator, options.size_tasks_initial + 1, options.pooling);

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

        return AsyncKqueue{
            .kqueue_fd = kqueue_fd,
            .events = events,
            .changes = changes,
            .change_count = 0,
            .jobs = jobs,
        };
    }

    pub fn inner_deinit(self: *AsyncKqueue, allocator: std.mem.Allocator) void {
        std.posix.close(self.kqueue_fd);
        allocator.free(self.events);
        allocator.free(self.changes);
        self.jobs.deinit();
    }

    pub fn deinit(self: *AsyncIO, allocator: std.mem.Allocator) void {
        const kqueue: *AsyncKqueue = @ptrCast(@alignCast(self.runner));
        kqueue.inner_deinit(allocator);
    }

    pub fn queue_job(self: *AsyncIO, task: usize, job: AsyncSubmission) !void {
        const kqueue: *AsyncKqueue = @ptrCast(@alignCast(self.runner));

        (switch (job) {
            .timer => |inner| queue_timer(kqueue, task, inner),
            .accept => |inner| queue_accept(kqueue, task, inner.socket, inner.kind),
            .connect => |inner| queue_connect(kqueue, task, inner.socket, inner.addr, inner.kind),
            .recv => |inner| queue_recv(kqueue, task, inner.socket, inner.buffer),
            .send => |inner| queue_send(kqueue, task, inner.socket, inner.buffer),
            .open, .delete, .mkdir, .stat, .read, .write, .close => unreachable,
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

        while (reaped == 0 and wait) {
            const remaining = self.completions.len - reaped;
            if (remaining == 0) break;

            const timeout_spec: std.posix.timespec = .{ .tv_sec = 0, .tv_nsec = 0 };
            const timeout: ?*const std.posix.timespec = if (!wait or reaped > 0) &timeout_spec else null;
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
                            const rc = std.posix.system.connect(
                                inner.socket,
                                &inner.addr.any,
                                inner.addr.getOsSockLen(),
                            );

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

    pub fn to_async(self: *AsyncKqueue) AsyncIO {
        return AsyncIO{
            .runner = self,
            ._deinit = deinit,
            ._queue_job = queue_job,
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
