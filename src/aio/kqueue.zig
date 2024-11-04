const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.@"tardy/aio/kqueue");

const Completion = @import("completion.zig").Completion;
const Result = @import("completion.zig").Result;
const Stat = @import("completion.zig").Stat;
const Timespec = @import("timespec.zig").Timespec;

const AsyncIO = @import("lib.zig").AsyncIO;
const AsyncIOOptions = @import("lib.zig").AsyncIOOptions;
const Job = @import("job.zig").Job;
const Pool = @import("../core/pool.zig").Pool;

const Atomic = std.atomic.Value;

const WAKE_IDENT = 1;

pub const AsyncKQueue = struct {
    kqueue_fd: std.posix.fd_t,
    changes: []std.posix.Kevent,
    change_count: Atomic(usize) = Atomic(usize).init(0),
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
            .change_count = Atomic(usize).init(0),
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

        const event = &kqueue.changes[kqueue.change_count.fetchAdd(1, .acq_rel)];
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

        const event = &kqueue.changes[kqueue.change_count.fetchAdd(1, .acq_rel)];
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

        const event = &kqueue.changes[kqueue.change_count.fetchAdd(1, .acq_rel)];
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

        const event = &kqueue.changes[kqueue.change_count.fetchAdd(1, .acq_rel)];
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
                    .addr = addr.any,
                },
            },
            .task = task,
        };

        const event = &kqueue.changes[kqueue.change_count.fetchAdd(1, .acq_rel)];
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

        const event = &kqueue.changes[kqueue.change_count.fetchAdd(1, .acq_rel)];
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

        const event = &kqueue.changes[kqueue.change_count.fetchAdd(1, .acq_rel)];
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
        const change_slice = kqueue.changes[0..kqueue.change_count.swap(0, .acq_rel)];
        _ = try std.posix.kevent(kqueue.kqueue_fd, change_slice, &.{}, null);
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

                const result: Result = result: {
                    switch (job.type) {
                        else => unreachable,
                        .open => |path| {
                            const opened = std.posix.openatZ(std.posix.AT.FDCWD, path, .{}, 0) catch |e| {
                                switch (e) {
                                    error.WouldBlock => {
                                        job_complete = false;
                                        continue :blocking_loop;
                                    },
                                    else => {
                                        log.debug("open failed: {}", .{e});
                                        break :result .{ .fd = -1 };
                                    },
                                }
                            };

                            break :result .{ .fd = opened };
                        },
                        .stat => |fd| {
                            const fstat = std.posix.fstat(fd) catch |e| {
                                log.err("stat failed: {}", .{e});
                                unreachable;
                            };

                            const stat: Stat = .{
                                .size = @intCast(fstat.size),
                                .mode = @intCast(fstat.mode),
                                .accessed = .{
                                    .seconds = @intCast(fstat.atim.tv_sec),
                                    .nanos = @intCast(fstat.atim.tv_nsec),
                                },
                                .modified = .{
                                    .seconds = @intCast(fstat.atim.tv_sec),
                                    .nanos = @intCast(fstat.atim.tv_nsec),
                                },
                                .changed = .{
                                    .seconds = @intCast(fstat.atim.tv_sec),
                                    .nanos = @intCast(fstat.atim.tv_nsec),
                                },
                            };

                            break :result .{ .stat = stat };
                        },
                        .close => |handle| {
                            std.posix.close(handle);
                            break :result .none;
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
                &.{},
                kqueue.events,
                if (busy_wait or reaped > 0) &timeout_spec else null,
            );
            for (kqueue.events[0..kqueue_events]) |event| {
                const job_index = event.udata;
                assert(kqueue.jobs.dirty.isSet(job_index));

                var job_complete = true;
                defer if (job_complete) kqueue.jobs.release(job_index);

                const job = kqueue.jobs.items[job_index];

                const result: Result = result: {
                    switch (job.type) {
                        else => unreachable,
                        .wake => {
                            assert(event.filter == std.posix.system.EVFILT_USER);
                            job_complete = false;
                            break :result .wake;
                        },
                        .timer => |inner| {
                            assert(inner == .none);
                            break :result .none;
                        },
                        .accept => |socket| {
                            assert(event.filter == std.posix.system.EVFILT_READ);
                            const accepted = std.posix.accept(socket, null, null, 0) catch |e| {
                                switch (e) {
                                    error.WouldBlock => unreachable,
                                    else => {
                                        log.debug("accept failed: {}", .{e});
                                        break :result .{ .socket = -1 };
                                    },
                                }
                            };

                            break :result .{ .socket = accepted };
                        },
                        .connect => |inner| {
                            assert(event.filter == std.posix.system.EVFILT_WRITE);
                            const addr_len: std.posix.socklen_t = switch (inner.addr.family) {
                                std.posix.AF.INET => @sizeOf(std.posix.sockaddr.in),
                                std.posix.AF.INET6 => @sizeOf(std.posix.sockaddr.in6),
                                std.posix.AF.UNIX => @sizeOf(std.posix.sockaddr.un),
                                else => @panic("Unsupported!"),
                            };

                            std.posix.connect(inner.socket, &inner.addr, addr_len) catch |e| {
                                switch (e) {
                                    error.WouldBlock => unreachable,
                                    else => {
                                        log.debug("connect failed: {}", .{e});
                                        break :result .{ .value = -1 };
                                    },
                                }
                            };

                            break :result .{ .value = 1 };
                        },
                        .recv => |inner| {
                            assert(event.filter == std.posix.system.EVFILT_READ);

                            const count = @min(@as(usize, @intCast(event.data)), inner.buffer.len);
                            const bytes_read = std.posix.recv(inner.socket, inner.buffer[0..count], 0) catch |e| {
                                switch (e) {
                                    error.WouldBlock => unreachable,
                                    error.ConnectionResetByPeer => {
                                        break :result .{ .value = 0 };
                                    },
                                    else => {
                                        log.debug("recv failed: {}", .{e});
                                        break :result .{ .value = -1 };
                                    },
                                }
                            };

                            break :result .{ .value = @intCast(bytes_read) };
                        },
                        .send => |inner| {
                            assert(event.filter == std.posix.system.EVFILT_WRITE);
                            const count = @min(@as(usize, @intCast(event.data)), inner.buffer.len);
                            const bytes_sent = std.posix.send(inner.socket, inner.buffer[0..count], 0) catch |e| {
                                switch (e) {
                                    error.WouldBlock => unreachable,
                                    error.ConnectionResetByPeer => {
                                        break :result .{ .value = 0 };
                                    },
                                    else => {
                                        log.debug("send failed: {}", .{e});
                                        break :result .{ .value = -1 };
                                    },
                                }
                            };

                            break :result .{ .value = @intCast(bytes_sent) };
                        },
                        .read => |inner| {
                            assert(event.filter == std.posix.system.EVFILT_READ);

                            const count = @min(@as(usize, @intCast(event.data)), inner.buffer.len);
                            const read_buffer = inner.buffer[0..count];
                            const bytes_read = read: {
                                break :read std.posix.pread(inner.fd, read_buffer, inner.offset) catch |e| {
                                    switch (e) {
                                        error.WouldBlock => unreachable,
                                        error.Unseekable => {
                                            break :read std.posix.read(inner.fd, read_buffer) catch |re| {
                                                switch (re) {
                                                    error.WouldBlock => unreachable,
                                                    else => {
                                                        log.debug("read failed: {}", .{e});
                                                        break :result .{ .value = -1 };
                                                    },
                                                }
                                            };
                                        },
                                        else => {
                                            log.debug("pread failed: {}", .{e});
                                            break :result .{ .value = -1 };
                                        },
                                    }
                                };
                            };

                            break :result .{ .value = @intCast(bytes_read) };
                        },

                        .write => |inner| {
                            assert(event.filter == std.posix.system.EVFILT_WRITE);

                            const count = @min(@as(usize, @intCast(event.data)), inner.buffer.len);
                            const write_buffer = inner.buffer[0..count];

                            const bytes_written = write: {
                                break :write std.posix.pwrite(inner.fd, write_buffer, inner.offset) catch |e| {
                                    switch (e) {
                                        error.WouldBlock => unreachable,
                                        error.Unseekable => {
                                            break :write std.posix.write(inner.fd, write_buffer) catch |we| {
                                                switch (we) {
                                                    error.WouldBlock => unreachable,
                                                    else => {
                                                        log.debug("write failed: {}", .{e});
                                                        break :result .{ .value = -1 };
                                                    },
                                                }
                                            };
                                        },
                                        else => {
                                            log.debug("pwrite failed: {}", .{e});
                                            break :result .{ .value = -1 };
                                        },
                                    }
                                };
                            };

                            break :result .{ .value = @intCast(bytes_written) };
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
