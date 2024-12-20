const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.@"tardy/aio/busy_loop");

const builtin = @import("builtin");
const Atomic = std.atomic.Value;
const Completion = @import("../completion.zig").Completion;
const Stat = @import("../completion.zig").Stat;
const Timespec = @import("../timespec.zig").Timespec;

const AsyncIO = @import("../lib.zig").AsyncIO;
const AsyncIOOptions = @import("../lib.zig").AsyncIOOptions;
const Job = @import("../job.zig").Job;

const File = std.fs.File;

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

pub const AsyncBusyLoop = struct {
    inner: std.ArrayListUnmanaged(Job),
    wake_signal: Atomic(bool),

    pub fn init(allocator: std.mem.Allocator, options: AsyncIOOptions) !AsyncBusyLoop {
        const list = try std.ArrayListUnmanaged(Job).initCapacity(allocator, options.size_aio_jobs_max);
        return AsyncBusyLoop{
            .inner = list,
            .wake_signal = Atomic(bool).init(false),
        };
    }

    pub fn deinit(self: *AsyncIO, allocator: std.mem.Allocator) void {
        const loop: *AsyncBusyLoop = @ptrCast(@alignCast(self.runner));
        loop.inner.deinit(allocator);
    }

    pub fn queue_timer(
        self: *AsyncIO,
        task: usize,
        timespec: Timespec,
    ) !void {
        const loop: *AsyncBusyLoop = @ptrCast(@alignCast(self.runner));

        var time = std.time.nanoTimestamp();
        time += timespec.seconds * std.time.ns_per_s;
        time += timespec.nanos;

        loop.inner.appendAssumeCapacity(.{
            .type = .{ .timer = .{ .ns = time } },
            .task = task,
        });
    }

    pub fn queue_open(
        self: *AsyncIO,
        task: usize,
        path: [:0]const u8,
    ) !void {
        const loop: *AsyncBusyLoop = @ptrCast(@alignCast(self.runner));
        loop.inner.appendAssumeCapacity(.{
            .type = .{ .open = path },
            .task = task,
        });
    }

    pub fn queue_stat(
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
    ) !void {
        const loop: *AsyncBusyLoop = @ptrCast(@alignCast(self.runner));
        loop.inner.appendAssumeCapacity(.{
            .type = .{ .stat = fd },
            .task = task,
        });
    }

    pub fn queue_read(
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
        buffer: []u8,
        offset: usize,
    ) !void {
        const loop: *AsyncBusyLoop = @ptrCast(@alignCast(self.runner));
        loop.inner.appendAssumeCapacity(.{
            .type = .{
                .read = .{
                    .fd = fd,
                    .buffer = buffer,
                    .offset = offset,
                },
            },
            .task = task,
        });
    }

    pub fn queue_write(
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
        buffer: []const u8,
        offset: usize,
    ) !void {
        const loop: *AsyncBusyLoop = @ptrCast(@alignCast(self.runner));
        loop.inner.appendAssumeCapacity(.{
            .type = .{
                .write = .{
                    .fd = fd,
                    .buffer = buffer,
                    .offset = offset,
                },
            },
            .task = task,
        });
    }

    pub fn queue_close(
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
    ) !void {
        const loop: *AsyncBusyLoop = @ptrCast(@alignCast(self.runner));
        loop.inner.appendAssumeCapacity(.{
            .type = .{ .close = fd },
            .task = task,
        });
    }

    pub fn queue_accept(
        self: *AsyncIO,
        task: usize,
        socket: std.posix.socket_t,
    ) !void {
        const loop: *AsyncBusyLoop = @ptrCast(@alignCast(self.runner));
        loop.inner.appendAssumeCapacity(.{
            .type = .{ .accept = socket },
            .task = task,
        });
    }

    pub fn queue_connect(
        self: *AsyncIO,
        task: usize,
        socket: std.posix.socket_t,
        host: []const u8,
        port: u16,
    ) !void {
        const loop: *AsyncBusyLoop = @ptrCast(@alignCast(self.runner));

        const addr = try std.net.Address.parseIp(host, port);

        loop.inner.appendAssumeCapacity(.{
            .type = .{
                .connect = .{
                    .socket = socket,
                    .addr = addr,
                },
            },
            .task = task,
        });
    }

    pub fn queue_recv(
        self: *AsyncIO,
        task: usize,
        socket: std.posix.socket_t,
        buffer: []u8,
    ) !void {
        const loop: *AsyncBusyLoop = @ptrCast(@alignCast(self.runner));
        loop.inner.appendAssumeCapacity(.{
            .type = .{
                .recv = .{
                    .socket = socket,
                    .buffer = buffer,
                },
            },
            .task = task,
        });
    }

    pub fn queue_send(
        self: *AsyncIO,
        task: usize,
        socket: std.posix.socket_t,
        buffer: []const u8,
    ) !void {
        const loop: *AsyncBusyLoop = @ptrCast(@alignCast(self.runner));
        loop.inner.appendAssumeCapacity(.{
            .type = .{
                .send = .{
                    .socket = socket,
                    .buffer = buffer,
                },
            },
            .task = task,
        });
    }

    pub fn wake(self: *AsyncIO) !void {
        const loop: *AsyncBusyLoop = @ptrCast(@alignCast(self.runner));
        loop.wake_signal.store(true, .release);
    }

    pub fn submit(self: *AsyncIO) !void {
        const loop: *AsyncBusyLoop = @ptrCast(@alignCast(self.runner));
        _ = loop;
    }

    pub fn reap(self: *AsyncIO, wait: bool) ![]Completion {
        const loop: *AsyncBusyLoop = @ptrCast(@alignCast(self.runner));
        var reaped: usize = 0;
        var first_run: bool = true;

        while ((reaped < 1 and wait) or first_run) {
            var i: usize = 0;

            if (loop.wake_signal.swap(false, .acquire)) {
                const com_ptr = &self.completions[reaped];
                com_ptr.result = .wake;
                com_ptr.task = undefined;
                reaped += 1;
            }

            while (i < loop.inner.items.len and reaped < self.completions.len) : (i += 1) {
                const job = loop.inner.items[i];

                switch (job.type) {
                    // handled above with a wake_signal.
                    .wake => unreachable,
                    .timer => |inner| {
                        const com_ptr = &self.completions[reaped];
                        const target_time = inner.ns;
                        const current = std.time.nanoTimestamp();
                        if (current < target_time) continue;

                        com_ptr.result = .none;
                        com_ptr.task = job.task;
                        _ = loop.inner.swapRemove(i);
                        i -|= 1;
                        reaped += 1;
                    },
                    .open => |path| {
                        const com_ptr = &self.completions[reaped];

                        const result: OpenResult = blk: {
                            const open_result = std.fs.cwd().openFileZ(path.ptr, .{}) catch |e| {
                                const open_err: OpenError = switch (e) {
                                    File.OpenError.WouldBlock => continue,
                                    File.OpenError.AccessDenied => OpenError.AccessDenied,
                                    File.OpenError.BadPathName => OpenError.InvalidArguments,
                                    File.OpenError.DeviceBusy => OpenError.FileBusy,
                                    File.OpenError.SystemFdQuotaExceeded => OpenError.SystemFdQuotaExceeded,
                                    File.OpenError.ProcessFdQuotaExceeded => OpenError.ProcessFdQuotaExceeded,
                                    File.OpenError.FileNotFound => OpenError.FileNotFound,
                                    File.OpenError.PipeBusy => OpenError.FileBusy,
                                    File.OpenError.FileTooBig => OpenError.FileTooBig,
                                    File.OpenError.SharingViolation => OpenError.FileLocked,
                                    File.OpenError.IsDir => OpenError.IsDirectory,
                                    File.OpenError.NameTooLong => OpenError.NameTooLong,
                                    File.OpenError.NoDevice => OpenError.DeviceNotFound,
                                    File.OpenError.NoSpaceLeft => OpenError.NoSpace,
                                    File.OpenError.NotDir => OpenError.NotADirectory,
                                    File.OpenError.PathAlreadyExists => OpenError.FileAlreadyExists,
                                    File.OpenError.SymLinkLoop => OpenError.Loop,
                                    File.OpenError.SystemResources => OpenError.OutOfMemory,
                                    else => OpenError.Unexpected,
                                };
                                log.debug("open failed: {}", .{e});
                                break :blk .{ .err = open_err };
                            };

                            break :blk .{ .actual = open_result.handle };
                        };

                        com_ptr.result = .{ .open = result };
                        com_ptr.task = job.task;
                        _ = loop.inner.swapRemove(i);
                        i -|= 1;
                        reaped += 1;
                    },
                    .stat => |fd| {
                        const com_ptr = &self.completions[reaped];

                        const result: StatResult = blk: {
                            const file: File = .{ .handle = fd };
                            const stat_result = file.stat() catch |e| {
                                const stat_err: StatError = switch (e) {
                                    File.StatError.AccessDenied => StatError.AccessDenied,
                                    File.StatError.SystemResources => StatError.OutOfMemory,
                                    File.StatError.Unexpected => StatError.Unexpected,
                                };

                                log.debug("stat failed: {}", .{e});
                                break :blk .{ .err = stat_err };
                            };

                            const stat: Stat = .{
                                .size = stat_result.size,
                                .mode = stat_result.mode,
                                .changed = .{
                                    .seconds = @intCast(@divTrunc(stat_result.ctime, std.time.ns_per_s)),
                                    .nanos = @intCast(@mod(stat_result.ctime, std.time.ns_per_s)),
                                },
                                .modified = .{
                                    .seconds = @intCast(@divTrunc(stat_result.mtime, std.time.ns_per_s)),
                                    .nanos = @intCast(@mod(stat_result.mtime, std.time.ns_per_s)),
                                },
                                .accessed = .{
                                    .seconds = @intCast(@divTrunc(stat_result.atime, std.time.ns_per_s)),
                                    .nanos = @intCast(@mod(stat_result.atime, std.time.ns_per_s)),
                                },
                            };

                            break :blk .{ .actual = stat };
                        };

                        com_ptr.result = .{ .stat = result };
                        com_ptr.task = job.task;
                        _ = loop.inner.swapRemove(i);
                        i -|= 1;
                        reaped += 1;
                    },
                    .read => |inner| {
                        const com_ptr = &self.completions[reaped];

                        const result: ReadResult = blk: {
                            const read_result = std.posix.pread(inner.fd, inner.buffer, inner.offset) catch |e| {
                                const read_err: ReadError = err: {
                                    switch (e) {
                                        std.posix.PReadError.WouldBlock => continue,
                                        std.posix.PReadError.Unseekable => {
                                            const other_read_result = std.posix.read(inner.fd, inner.buffer) catch |e2| {
                                                switch (e2) {
                                                    std.posix.ReadError.WouldBlock => continue,
                                                    std.posix.ReadError.AccessDenied => break :err ReadError.AccessDenied,
                                                    std.posix.ReadError.IsDir => break :err ReadError.IsDirectory,
                                                    std.posix.ReadError.NotOpenForReading => break :err ReadError.InvalidFd,
                                                    std.posix.ReadError.InputOutput => break :err ReadError.IoError,
                                                    else => {
                                                        log.err("read unexpected error: {}", .{e2});
                                                        break :err ReadError.Unexpected;
                                                    },
                                                }
                                            };

                                            break :blk .{ .actual = other_read_result };
                                        },
                                        std.posix.PReadError.AccessDenied => break :err ReadError.AccessDenied,
                                        std.posix.PReadError.IsDir => break :err ReadError.IsDirectory,
                                        std.posix.PReadError.NotOpenForReading => break :err ReadError.InvalidFd,
                                        std.posix.PReadError.InputOutput => break :err ReadError.IoError,
                                        else => {
                                            log.err("pread unexpected error: {}", .{e});
                                            break :err ReadError.Unexpected;
                                        },
                                    }
                                };

                                break :blk .{ .err = read_err };
                            };

                            break :blk .{ .actual = read_result };
                        };

                        com_ptr.result = .{ .read = result };
                        com_ptr.task = job.task;
                        _ = loop.inner.swapRemove(i);
                        i -|= 1;
                        reaped += 1;
                    },
                    .write => |inner| {
                        const com_ptr = &self.completions[reaped];

                        const result: WriteResult = blk: {
                            const write_result = std.posix.pwrite(inner.fd, inner.buffer, inner.offset) catch |e| {
                                const write_err: WriteError = err: {
                                    switch (e) {
                                        std.posix.PWriteError.WouldBlock => continue,
                                        std.posix.PWriteError.Unseekable => {
                                            const other_write_result = std.posix.write(inner.fd, inner.buffer) catch |e2| {
                                                switch (e2) {
                                                    std.posix.WriteError.WouldBlock => continue,
                                                    std.posix.WriteError.AccessDenied => break :err WriteError.AccessDenied,
                                                    std.posix.WriteError.DiskQuota => break :err WriteError.DiskQuotaExceeded,
                                                    std.posix.WriteError.FileTooBig => break :err WriteError.FileTooBig,
                                                    std.posix.WriteError.NoSpaceLeft => break :err WriteError.NoSpace,
                                                    std.posix.WriteError.BrokenPipe => break :err WriteError.BrokenPipe,
                                                    std.posix.WriteError.InvalidArgument => break :err WriteError.InvalidArguments,
                                                    std.posix.WriteError.InputOutput => break :err WriteError.IoError,
                                                    std.posix.WriteError.NotOpenForWriting => break :err WriteError.InvalidFd,
                                                    else => {
                                                        log.err("write unexpected error: {}", .{e2});
                                                        break :err WriteError.Unexpected;
                                                    },
                                                }
                                            };

                                            break :blk .{ .actual = other_write_result };
                                        },
                                        std.posix.PWriteError.AccessDenied => break :err WriteError.AccessDenied,
                                        std.posix.PWriteError.DiskQuota => break :err WriteError.DiskQuotaExceeded,
                                        std.posix.PWriteError.FileTooBig => break :err WriteError.FileTooBig,
                                        std.posix.PWriteError.NoSpaceLeft => break :err WriteError.NoSpace,
                                        std.posix.PWriteError.BrokenPipe => break :err WriteError.BrokenPipe,
                                        std.posix.PWriteError.InvalidArgument => break :err WriteError.InvalidArguments,
                                        std.posix.PWriteError.InputOutput => break :err WriteError.IoError,
                                        std.posix.PWriteError.NotOpenForWriting => break :err WriteError.InvalidFd,
                                        else => {
                                            log.err("pwrite unexpected error: {}", .{e});
                                            break :err WriteError.Unexpected;
                                        },
                                    }
                                };

                                break :blk .{ .err = write_err };
                            };

                            break :blk .{ .actual = write_result };
                        };

                        com_ptr.result = .{ .write = result };
                        com_ptr.task = job.task;
                        _ = loop.inner.swapRemove(i);
                        i -|= 1;
                        reaped += 1;
                    },
                    .close => |handle| {
                        const com_ptr = &self.completions[reaped];
                        std.posix.close(handle);
                        com_ptr.result = .close;
                        com_ptr.task = job.task;
                        _ = loop.inner.swapRemove(i);
                        i -|= 1;
                        reaped += 1;
                    },
                    .accept => |socket| {
                        const com_ptr = &self.completions[reaped];

                        const result: AcceptResult = blk: {
                            const accept_result = std.posix.accept(socket, null, null, 0) catch |e| {
                                const accept_err: AcceptError = err: {
                                    switch (e) {
                                        std.posix.AcceptError.WouldBlock => continue,
                                        std.posix.AcceptError.FileDescriptorNotASocket => break :err AcceptError.NotASocket,
                                        std.posix.AcceptError.ConnectionResetByPeer => break :err AcceptError.ConnectionAborted,
                                        std.posix.AcceptError.ConnectionAborted => break :err AcceptError.ConnectionAborted,
                                        std.posix.AcceptError.SystemFdQuotaExceeded => break :err AcceptError.SystemFdQuotaExceeded,
                                        std.posix.AcceptError.ProcessFdQuotaExceeded => break :err AcceptError.ProcessFdQuotaExceeded,
                                        std.posix.AcceptError.SystemResources => break :err AcceptError.OutOfMemory,
                                        std.posix.AcceptError.SocketNotListening => break :err AcceptError.NotListening,
                                        std.posix.AcceptError.OperationNotSupported => break :err AcceptError.OperationNotSupported,
                                        else => {
                                            log.err("accept unexpected error: {}", .{e});
                                            break :err AcceptError.Unexpected;
                                        },
                                    }
                                };

                                break :blk .{ .err = accept_err };
                            };

                            break :blk .{ .actual = accept_result };
                        };

                        com_ptr.result = .{ .accept = result };
                        com_ptr.task = job.task;
                        _ = loop.inner.swapRemove(i);
                        i -|= 1;
                        reaped += 1;
                    },
                    .connect => |inner| {
                        const com_ptr = &self.completions[reaped];

                        const addr_len: std.posix.socklen_t = switch (inner.addr.any.family) {
                            std.posix.AF.INET => @sizeOf(std.posix.sockaddr.in),
                            std.posix.AF.INET6 => @sizeOf(std.posix.sockaddr.in6),
                            std.posix.AF.UNIX => @sizeOf(std.posix.sockaddr.un),
                            else => @panic("Unsupported!"),
                        };

                        const result: ConnectResult = blk: {
                            std.posix.connect(inner.socket, &inner.addr.any, addr_len) catch |e| {
                                const connect_err: ConnectError = err: {
                                    switch (e) {
                                        std.posix.ConnectError.WouldBlock => continue,
                                        std.posix.ConnectError.PermissionDenied => break :err ConnectError.AccessDenied,
                                        std.posix.ConnectError.ConnectionResetByPeer => break :err ConnectError.ConnectionRefused,
                                        std.posix.ConnectError.ConnectionPending => break :err ConnectError.AlreadyConnected,
                                        std.posix.ConnectError.AddressInUse => break :err ConnectError.AddressInUse,
                                        std.posix.ConnectError.AddressNotAvailable => break :err ConnectError.AddressNotAvailable,
                                        std.posix.ConnectError.AddressFamilyNotSupported => break :err ConnectError.AddressFamilyNotSupported,
                                        std.posix.ConnectError.NetworkUnreachable => break :err ConnectError.NetworkUnreachable,
                                        std.posix.ConnectError.ConnectionTimedOut => break :err ConnectError.TimedOut,
                                        std.posix.ConnectError.FileNotFound => break :err ConnectError.NotASocket,
                                        std.posix.ConnectError.ConnectionRefused => break :err ConnectError.ConnectionRefused,
                                        else => {
                                            log.err("connect unexpected error: {}", .{e});
                                            break :err ConnectError.Unexpected;
                                        },
                                    }
                                };

                                break :blk .{ .err = connect_err };
                            };

                            break :blk .{ .actual = inner.socket };
                        };

                        com_ptr.result = .{ .connect = result };
                        com_ptr.task = job.task;
                        _ = loop.inner.swapRemove(i);
                        i -|= 1;
                        reaped += 1;
                    },
                    .recv => |inner| {
                        const com_ptr = &self.completions[reaped];

                        const result: RecvResult = blk: {
                            const recv_result = std.posix.recv(inner.socket, inner.buffer, 0) catch |e| {
                                const recv_err: RecvError = err: {
                                    switch (e) {
                                        std.posix.RecvFromError.WouldBlock => continue,
                                        std.posix.RecvFromError.SystemResources => break :err RecvError.OutOfMemory,
                                        std.posix.RecvFromError.SocketNotConnected => break :err RecvError.NotConnected,
                                        std.posix.RecvFromError.ConnectionResetByPeer => break :err RecvError.Closed,
                                        std.posix.RecvFromError.ConnectionRefused => break :err RecvError.ConnectionRefused,
                                        else => {
                                            log.err("recv unexpected error: {}", .{e});
                                            break :err RecvError.Unexpected;
                                        },
                                    }
                                };
                                break :blk .{ .err = recv_err };
                            };

                            break :blk .{ .actual = recv_result };
                        };

                        com_ptr.result = .{ .recv = result };
                        com_ptr.task = job.task;
                        _ = loop.inner.swapRemove(i);
                        i -|= 1;
                        reaped += 1;
                    },
                    .send => |inner| {
                        const com_ptr = &self.completions[reaped];
                        const result: SendResult = blk: {
                            const send_result = std.posix.send(inner.socket, inner.buffer, 0) catch |e| {
                                const send_err: SendError = err: {
                                    switch (e) {
                                        std.posix.SendError.WouldBlock => continue,
                                        std.posix.SendError.SystemResources => break :err SendError.OutOfMemory,
                                        std.posix.SendError.AccessDenied => break :err SendError.AccessDenied,
                                        std.posix.SendError.ConnectionResetByPeer => break :err SendError.ConnectionReset,
                                        std.posix.SendError.BrokenPipe => break :err SendError.BrokenPipe,
                                        std.posix.SendError.FileDescriptorNotASocket => break :err SendError.InvalidFd,
                                        std.posix.SendError.FastOpenAlreadyInProgress => break :err SendError.AlreadyConnected,
                                        else => {
                                            log.err("send unexpected error: {}", .{e});
                                            break :err SendError.Unexpected;
                                        },
                                    }
                                };
                                break :blk .{ .err = send_err };
                            };

                            break :blk .{ .actual = send_result };
                        };

                        com_ptr.result = .{ .send = result };
                        com_ptr.task = job.task;
                        _ = loop.inner.swapRemove(i);
                        i -|= 1;
                        reaped += 1;
                    },
                }
            }

            first_run = false;
        }

        return self.completions[0..reaped];
    }

    pub fn to_async(self: *AsyncBusyLoop) AsyncIO {
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
