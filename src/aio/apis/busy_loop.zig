const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.@"tardy/aio/busy_loop");

const builtin = @import("builtin");
const Atomic = std.atomic.Value;
const Completion = @import("../completion.zig").Completion;
const Stat = @import("../../fs/lib.zig").Stat;
const Timespec = @import("../../lib.zig").Timespec;

const AsyncIO = @import("../lib.zig").AsyncIO;
const AsyncIOOptions = @import("../lib.zig").AsyncIOOptions;
const Job = @import("../job.zig").Job;

const StdFile = std.fs.File;
const StdDir = std.fs.Dir;

const Socket = @import("../../net/socket.zig").Socket;
const Path = @import("../../fs/lib.zig").Path;
const AioOpenFlags = @import("../lib.zig").AioOpenFlags;

const AsyncSubmission = @import("../lib.zig").AsyncSubmission;

const AcceptResult = @import("../completion.zig").AcceptResult;
const AcceptError = @import("../completion.zig").AcceptError;
const ConnectResult = @import("../completion.zig").ConnectResult;
const ConnectError = @import("../completion.zig").ConnectError;
const RecvResult = @import("../completion.zig").RecvResult;
const RecvError = @import("../completion.zig").RecvError;
const SendResult = @import("../completion.zig").SendResult;
const SendError = @import("../completion.zig").SendError;

const InnerOpenResult = @import("../completion.zig").InnerOpenResult;
const OpenError = @import("../completion.zig").OpenError;
const MkdirResult = @import("../completion.zig").MkdirResult;
const MkdirError = @import("../completion.zig").MkdirError;
const ReadResult = @import("../completion.zig").ReadResult;
const ReadError = @import("../completion.zig").ReadError;
const WriteResult = @import("../completion.zig").WriteResult;
const WriteError = @import("../completion.zig").WriteError;
const DeleteResult = @import("../completion.zig").DeleteResult;
const DeleteError = @import("../completion.zig").DeleteError;

const StatResult = @import("../completion.zig").StatResult;
const StatError = @import("../completion.zig").StatError;

pub const AsyncBusyLoop = struct {
    inner: std.ArrayList(Job),
    wake_signal: Atomic(bool),

    pub fn init(allocator: std.mem.Allocator, options: AsyncIOOptions) !AsyncBusyLoop {
        const list = try std.ArrayList(Job).initCapacity(allocator, options.size_tasks_initial);
        return AsyncBusyLoop{
            .inner = list,
            .wake_signal = Atomic(bool).init(false),
        };
    }

    pub fn inner_deinit(self: *AsyncBusyLoop, _: std.mem.Allocator) void {
        self.inner.deinit();
    }

    fn deinit(self: *AsyncIO, allocator: std.mem.Allocator) void {
        const loop: *AsyncBusyLoop = @ptrCast(@alignCast(self.runner));
        loop.inner_deinit(allocator);
    }

    pub fn queue_job(self: *AsyncIO, task: usize, job: AsyncSubmission) !void {
        const loop: *AsyncBusyLoop = @ptrCast(@alignCast(self.runner));

        try switch (job) {
            .timer => |inner| queue_timer(loop, task, inner),
            .open => |inner| queue_open(loop, task, inner.path, inner.flags),
            .delete => |inner| queue_delete(loop, task, inner.path, inner.is_dir),
            .mkdir => |inner| queue_mkdir(loop, task, inner.path, inner.mode),
            .stat => |inner| queue_stat(loop, task, inner),
            .read => |inner| queue_read(loop, task, inner.fd, inner.buffer, inner.offset),
            .write => |inner| queue_write(loop, task, inner.fd, inner.buffer, inner.offset),
            .close => |inner| queue_close(loop, task, inner),
            .accept => |inner| queue_accept(loop, task, inner.socket, inner.kind),
            .connect => |inner| queue_connect(loop, task, inner.socket, inner.addr, inner.kind),
            .recv => |inner| queue_recv(loop, task, inner.socket, inner.buffer),
            .send => |inner| queue_send(loop, task, inner.socket, inner.buffer),
        };
    }

    pub fn queue_timer(self: *AsyncBusyLoop, task: usize, timespec: Timespec) !void {
        var time = std.time.nanoTimestamp();
        time += timespec.seconds * std.time.ns_per_s;
        time += timespec.nanos;

        try self.inner.append(.{ .type = .{ .timer = .{ .ns = time } }, .task = task });
    }

    pub fn queue_open(self: *AsyncBusyLoop, task: usize, path: Path, flags: AioOpenFlags) !void {
        try self.inner.append(.{
            .type = .{
                .open = .{
                    .path = path,
                    .flags = flags,
                    .kind = if (flags.directory) .dir else .file,
                },
            },
            .task = task,
        });
    }

    pub fn queue_delete(self: *AsyncBusyLoop, task: usize, path: Path, is_dir: bool) !void {
        try self.inner.append(.{ .type = .{ .delete = .{ .path = path, .is_dir = is_dir } }, .task = task });
    }

    pub fn queue_mkdir(self: *AsyncBusyLoop, task: usize, path: Path, mode: std.posix.mode_t) !void {
        try self.inner.append(.{
            .type = .{ .mkdir = .{ .path = path, .mode = mode } },
            .task = task,
        });
    }

    pub fn queue_stat(self: *AsyncBusyLoop, task: usize, fd: std.posix.fd_t) !void {
        try self.inner.append(.{ .type = .{ .stat = fd }, .task = task });
    }

    pub fn queue_read(
        self: *AsyncBusyLoop,
        task: usize,
        fd: std.posix.fd_t,
        buffer: []u8,
        offset: ?usize,
    ) !void {
        try self.inner.append(.{
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
        self: *AsyncBusyLoop,
        task: usize,
        fd: std.posix.fd_t,
        buffer: []const u8,
        offset: ?usize,
    ) !void {
        try self.inner.append(.{
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

    pub fn queue_close(self: *AsyncBusyLoop, task: usize, fd: std.posix.fd_t) !void {
        try self.inner.append(.{
            .type = .{ .close = fd },
            .task = task,
        });
    }

    pub fn queue_accept(
        self: *AsyncBusyLoop,
        task: usize,
        socket: std.posix.socket_t,
        kind: Socket.Kind,
    ) !void {
        try self.inner.append(.{
            .type = .{ .accept = .{
                .socket = socket,
                .addr = undefined,
                .addr_len = @sizeOf(std.net.Address),
                .kind = kind,
            } },
            .task = task,
        });
    }

    pub fn queue_connect(
        self: *AsyncBusyLoop,
        task: usize,
        socket: std.posix.socket_t,
        addr: std.net.Address,
        kind: Socket.Kind,
    ) !void {
        try self.inner.append(.{
            .type = .{
                .connect = .{
                    .socket = socket,
                    .addr = addr,
                    .kind = kind,
                },
            },
            .task = task,
        });
    }

    pub fn queue_recv(
        self: *AsyncBusyLoop,
        task: usize,
        socket: std.posix.socket_t,
        buffer: []u8,
    ) !void {
        try self.inner.append(.{
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
        self: *AsyncBusyLoop,
        task: usize,
        socket: std.posix.socket_t,
        buffer: []const u8,
    ) !void {
        try self.inner.append(.{
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

    pub fn submit(_: *AsyncIO) !void {}

    fn open_file(path: Path, flags: AioOpenFlags) ?InnerOpenResult {
        const std_flags: StdFile.OpenFlags = .{
            .mode = switch (flags.mode) {
                .read => .read_only,
                .write => .write_only,
                .read_write => .read_write,
            },
        };

        switch (path) {
            .rel => |inner| {
                const dir: StdDir = .{ .fd = inner.dir };
                const opened = dir.openFileZ(inner.path, std_flags) catch |e| {
                    const err: OpenError = switch (e) {
                        StdFile.OpenError.WouldBlock => return null,
                        StdFile.OpenError.AccessDenied => OpenError.AccessDenied,
                        StdFile.OpenError.BadPathName => OpenError.InvalidArguments,
                        StdFile.OpenError.DeviceBusy => OpenError.Busy,
                        StdFile.OpenError.SystemFdQuotaExceeded => OpenError.SystemFdQuotaExceeded,
                        StdFile.OpenError.ProcessFdQuotaExceeded => OpenError.ProcessFdQuotaExceeded,
                        StdFile.OpenError.FileNotFound => OpenError.NotFound,
                        StdFile.OpenError.PipeBusy => OpenError.Busy,
                        StdFile.OpenError.FileTooBig => OpenError.FileTooBig,
                        StdFile.OpenError.SharingViolation => OpenError.FileLocked,
                        StdFile.OpenError.IsDir => OpenError.IsDirectory,
                        StdFile.OpenError.NameTooLong => OpenError.NameTooLong,
                        StdFile.OpenError.NoDevice => OpenError.DeviceNotFound,
                        StdFile.OpenError.NoSpaceLeft => OpenError.NoSpace,
                        StdFile.OpenError.NotDir => OpenError.NotADirectory,
                        StdFile.OpenError.PathAlreadyExists => OpenError.AlreadyExists,
                        StdFile.OpenError.SymLinkLoop => OpenError.Loop,
                        StdFile.OpenError.SystemResources => OpenError.OutOfMemory,
                        else => OpenError.Unexpected,
                    };

                    return .{ .err = err };
                };

                return .{ .actual = .{ .file = .{ .handle = opened.handle } } };
            },
            .abs => |inner| {
                const opened = std.fs.cwd().openFileZ(inner, std_flags) catch |e| {
                    const err: OpenError = switch (e) {
                        StdFile.OpenError.WouldBlock => return null,
                        StdFile.OpenError.AccessDenied => OpenError.AccessDenied,
                        StdFile.OpenError.BadPathName => OpenError.InvalidArguments,
                        StdFile.OpenError.DeviceBusy, StdFile.OpenError.PipeBusy => OpenError.Busy,
                        StdFile.OpenError.SystemFdQuotaExceeded => OpenError.SystemFdQuotaExceeded,
                        StdFile.OpenError.ProcessFdQuotaExceeded => OpenError.ProcessFdQuotaExceeded,
                        StdFile.OpenError.FileNotFound => OpenError.NotFound,
                        StdFile.OpenError.FileTooBig => OpenError.FileTooBig,
                        StdFile.OpenError.SharingViolation => OpenError.FileLocked,
                        StdFile.OpenError.IsDir => OpenError.IsDirectory,
                        StdFile.OpenError.NameTooLong => OpenError.NameTooLong,
                        StdFile.OpenError.NoDevice => OpenError.DeviceNotFound,
                        StdFile.OpenError.NoSpaceLeft => OpenError.NoSpace,
                        StdFile.OpenError.NotDir => OpenError.NotADirectory,
                        StdFile.OpenError.PathAlreadyExists => OpenError.AlreadyExists,
                        StdFile.OpenError.SymLinkLoop => OpenError.Loop,
                        StdFile.OpenError.SystemResources => OpenError.OutOfMemory,
                        else => OpenError.Unexpected,
                    };

                    return .{ .err = err };
                };

                return .{ .actual = .{ .file = .{ .handle = opened.handle } } };
            },
        }
    }

    fn create_file(path: Path, flags: AioOpenFlags) ?InnerOpenResult {
        const std_flags: StdFile.CreateFlags = .{
            .read = (flags.mode == .read or flags.mode == .read_write),
            .truncate = flags.truncate,
            .exclusive = flags.exclusive,
        };

        switch (path) {
            .rel => |inner| {
                const dir: StdDir = .{ .fd = inner.dir };
                const opened = dir.createFileZ(inner.path, std_flags) catch |e| {
                    const err: OpenError = switch (e) {
                        StdFile.OpenError.WouldBlock => return null,
                        StdFile.OpenError.AccessDenied => OpenError.AccessDenied,
                        StdFile.OpenError.BadPathName => OpenError.InvalidArguments,
                        StdFile.OpenError.DeviceBusy => OpenError.Busy,
                        StdFile.OpenError.SystemFdQuotaExceeded => OpenError.SystemFdQuotaExceeded,
                        StdFile.OpenError.ProcessFdQuotaExceeded => OpenError.ProcessFdQuotaExceeded,
                        StdFile.OpenError.FileNotFound => OpenError.NotFound,
                        StdFile.OpenError.PipeBusy => OpenError.Busy,
                        StdFile.OpenError.FileTooBig => OpenError.FileTooBig,
                        StdFile.OpenError.SharingViolation => OpenError.FileLocked,
                        StdFile.OpenError.IsDir => OpenError.IsDirectory,
                        StdFile.OpenError.NameTooLong => OpenError.NameTooLong,
                        StdFile.OpenError.NoDevice => OpenError.DeviceNotFound,
                        StdFile.OpenError.NoSpaceLeft => OpenError.NoSpace,
                        StdFile.OpenError.NotDir => OpenError.NotADirectory,
                        StdFile.OpenError.PathAlreadyExists => OpenError.AlreadyExists,
                        StdFile.OpenError.SymLinkLoop => OpenError.Loop,
                        StdFile.OpenError.SystemResources => OpenError.OutOfMemory,
                        else => OpenError.Unexpected,
                    };

                    return .{ .err = err };
                };

                return .{ .actual = .{ .file = .{ .handle = opened.handle } } };
            },
            .abs => |inner| {
                const opened = std.fs.cwd().createFileZ(inner, std_flags) catch |e| {
                    const err: OpenError = switch (e) {
                        StdFile.OpenError.WouldBlock => return null,
                        StdFile.OpenError.AccessDenied => OpenError.AccessDenied,
                        StdFile.OpenError.BadPathName => OpenError.InvalidArguments,
                        StdFile.OpenError.DeviceBusy, StdFile.OpenError.PipeBusy => OpenError.Busy,
                        StdFile.OpenError.SystemFdQuotaExceeded => OpenError.SystemFdQuotaExceeded,
                        StdFile.OpenError.ProcessFdQuotaExceeded => OpenError.ProcessFdQuotaExceeded,
                        StdFile.OpenError.FileNotFound => OpenError.NotFound,
                        StdFile.OpenError.FileTooBig => OpenError.FileTooBig,
                        StdFile.OpenError.SharingViolation => OpenError.FileLocked,
                        StdFile.OpenError.IsDir => OpenError.IsDirectory,
                        StdFile.OpenError.NameTooLong => OpenError.NameTooLong,
                        StdFile.OpenError.NoDevice => OpenError.DeviceNotFound,
                        StdFile.OpenError.NoSpaceLeft => OpenError.NoSpace,
                        StdFile.OpenError.NotDir => OpenError.NotADirectory,
                        StdFile.OpenError.PathAlreadyExists => OpenError.AlreadyExists,
                        StdFile.OpenError.SymLinkLoop => OpenError.Loop,
                        StdFile.OpenError.SystemResources => OpenError.OutOfMemory,
                        else => OpenError.Unexpected,
                    };

                    return .{ .err = err };
                };

                return .{ .actual = .{ .file = .{ .handle = opened.handle } } };
            },
        }
    }

    fn open_dir(path: Path) ?InnerOpenResult {
        switch (path) {
            .rel => |inner| {
                const dir: StdDir = .{ .fd = inner.dir };
                const opened = dir.openDirZ(inner.path, .{ .iterate = true }) catch |e| {
                    const err: OpenError = switch (e) {
                        StdDir.OpenError.AccessDenied => OpenError.AccessDenied,
                        else => OpenError.Unexpected,
                    };

                    return .{ .err = err };
                };

                return .{ .actual = .{ .dir = .{ .handle = opened.fd } } };
            },
            .abs => |inner| {
                const opened = std.fs.cwd().openDirZ(inner, .{ .iterate = true }) catch |e| {
                    const err: OpenError = switch (e) {
                        StdDir.OpenError.AccessDenied => OpenError.AccessDenied,
                        else => OpenError.Unexpected,
                    };

                    return .{ .err = err };
                };

                return .{ .actual = .{ .dir = .{ .handle = opened.fd } } };
            },
        }
    }

    fn read_file(fd: std.posix.fd_t, buffer: []u8, offset: ?usize) ReadResult {
        switch (comptime builtin.os.tag) {
            .windows => {
                const read = std.os.windows.ReadFile(fd, buffer, offset) catch |e| {
                    const err: ReadError = switch (e) {
                        else => ReadError.Unexpected,
                        //std.os.windows.ReadFileError.BrokenPipe => .{},
                    };

                    return .{ .err = err };
                };

                if (read == 0) return .{ .err = ReadError.EndOfFile };
                return .{ .actual = read };
            },
            else => if (offset) |o| {
                const read = std.posix.pread(fd, buffer, o) catch |e| {
                    const err: ReadError = switch (e) {
                        else => ReadError.Unexpected,
                    };

                    return .{ .err = err };
                };

                if (read == 0) return .{ .err = ReadError.EndOfFile };
                return .{ .actual = read };
            } else {
                const read = std.posix.read(fd, buffer) catch |e| {
                    const err: ReadError = switch (e) {
                        else => ReadError.Unexpected,
                    };

                    return .{ .err = err };
                };

                if (read == 0) return .{ .err = ReadError.EndOfFile };
                return .{ .actual = read };
            },
        }
    }

    fn write_file(fd: std.posix.fd_t, buffer: []const u8, offset: ?usize) WriteResult {
        switch (comptime builtin.os.tag) {
            .windows => {
                const write = std.os.windows.WriteFile(fd, buffer, offset) catch |e| {
                    switch (e) {
                        else => WriteError.Unexpected,
                        //std.os.windows.WriteFileError.BrokenPipe => .{},
                    }
                };

                return .{ .actual = write };
            },
            else => if (offset) |o| {
                const write = std.posix.pwrite(fd, buffer, o) catch |e| {
                    const err: WriteError = switch (e) {
                        else => WriteError.Unexpected,
                    };

                    return .{ .err = err };
                };

                return .{ .actual = write };
            } else {
                const write = std.posix.write(fd, buffer) catch |e| {
                    const err = switch (e) {
                        else => WriteError.Unexpected,
                    };

                    return .{ .err = err };
                };

                return .{ .actual = write };
            },
        }
    }

    fn delete_file(path: Path) DeleteResult {
        switch (path) {
            .rel => |inner| {
                const dir: StdDir = .{ .fd = inner.dir };
                dir.deleteFileZ(inner.path) catch |e| {
                    const err: DeleteError = switch (e) {
                        StdDir.DeleteFileError.AccessDenied => DeleteError.AccessDenied,
                        else => DeleteError.Unexpected,
                    };

                    return .{ .err = err };
                };

                return .{ .actual = {} };
            },
            .abs => |inner| {
                std.fs.cwd().deleteFileZ(inner) catch |e| {
                    const err: DeleteError = switch (e) {
                        StdDir.DeleteFileError.AccessDenied => DeleteError.AccessDenied,
                        else => DeleteError.Unexpected,
                    };

                    return .{ .err = err };
                };

                return .{ .actual = {} };
            },
        }
    }

    fn delete_dir(path: Path) DeleteResult {
        switch (path) {
            .rel => |inner| {
                const dir: StdDir = .{ .fd = inner.dir };
                dir.deleteDirZ(inner.path) catch |e| {
                    const err: DeleteError = switch (e) {
                        StdDir.DeleteDirError.AccessDenied => DeleteError.AccessDenied,
                        else => DeleteError.Unexpected,
                    };

                    return .{ .err = err };
                };

                return .{ .actual = {} };
            },
            .abs => |inner| {
                std.fs.cwd().deleteDirZ(inner) catch |e| {
                    const err: DeleteError = switch (e) {
                        StdDir.DeleteDirError.AccessDenied => DeleteError.AccessDenied,
                        else => DeleteError.Unexpected,
                    };

                    return .{ .err = err };
                };

                return .{ .actual = {} };
            },
        }
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
                const job = &loop.inner.items[i];

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
                    .open => |inner| {
                        const com_ptr = &self.completions[reaped];

                        const result: InnerOpenResult = if (inner.flags.create) switch (inner.kind) {
                            .file => create_file(inner.path, inner.flags) orelse continue,
                            // Handled in mkdir.
                            .dir => unreachable,
                        } else switch (inner.kind) {
                            .file => open_file(inner.path, inner.flags) orelse continue,
                            .dir => open_dir(inner.path) orelse continue,
                        };

                        com_ptr.result = .{ .open = result };
                        com_ptr.task = job.task;
                        _ = loop.inner.swapRemove(i);
                        i -|= 1;
                        reaped += 1;
                    },
                    .delete => |inner| {
                        const com_ptr = &self.completions[reaped];

                        const result: DeleteResult = if (inner.is_dir)
                            delete_dir(inner.path)
                        else
                            delete_file(inner.path);

                        com_ptr.result = .{ .delete = result };
                        com_ptr.task = job.task;
                        _ = loop.inner.swapRemove(i);
                        i -|= 1;
                        reaped += 1;
                    },
                    .mkdir => |inner| {
                        const com_ptr = &self.completions[reaped];

                        const result: MkdirResult = blk: {
                            switch (inner.path) {
                                .rel => |path| {
                                    const dir: StdDir = .{ .fd = path.dir };
                                    dir.makeDirZ(path.path) catch |e| {
                                        const err: MkdirError = switch (e) {
                                            else => MkdirError.Unexpected,
                                        };

                                        break :blk .{ .err = err };
                                    };

                                    break :blk .{ .actual = {} };
                                },
                                .abs => |path| {
                                    std.fs.makeDirAbsoluteZ(path) catch |e| {
                                        const err: MkdirError = switch (e) {
                                            else => MkdirError.Unexpected,
                                        };

                                        break :blk .{ .err = err };
                                    };

                                    break :blk .{ .actual = {} };
                                },
                            }
                        };

                        com_ptr.result = .{ .mkdir = result };
                        com_ptr.task = job.task;
                        _ = loop.inner.swapRemove(i);
                        i -|= 1;
                        reaped += 1;
                    },
                    .stat => |fd| {
                        const com_ptr = &self.completions[reaped];

                        const result: StatResult = blk: {
                            const file: StdFile = .{ .handle = fd };
                            const stat_result = file.stat() catch |e| {
                                const stat_err: StatError = switch (e) {
                                    StdFile.StatError.AccessDenied => StatError.AccessDenied,
                                    StdFile.StatError.SystemResources => StatError.OutOfMemory,
                                    StdFile.StatError.Unexpected => StatError.Unexpected,
                                };
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

                        const result: ReadResult = read_file(inner.fd, inner.buffer, inner.offset);

                        com_ptr.result = .{ .read = result };
                        com_ptr.task = job.task;
                        _ = loop.inner.swapRemove(i);
                        i -|= 1;
                        reaped += 1;
                    },
                    .write => |inner| {
                        const com_ptr = &self.completions[reaped];

                        const result: WriteResult = write_file(inner.fd, inner.buffer, inner.offset);

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
                    .accept => |*inner| {
                        const com_ptr = &self.completions[reaped];

                        const result: AcceptResult = blk: {
                            const accept_result = std.posix.accept(
                                inner.socket,
                                &inner.addr.any,
                                @ptrCast(&inner.addr_len),
                                0,
                            ) catch |e| {
                                const accept_err: AcceptError = err: {
                                    break :err switch (e) {
                                        std.posix.AcceptError.WouldBlock => continue,
                                        std.posix.AcceptError.FileDescriptorNotASocket => AcceptError.NotASocket,
                                        std.posix.AcceptError.ConnectionResetByPeer => AcceptError.ConnectionAborted,
                                        std.posix.AcceptError.ConnectionAborted => AcceptError.ConnectionAborted,
                                        std.posix.AcceptError.SystemFdQuotaExceeded => AcceptError.SystemFdQuotaExceeded,
                                        std.posix.AcceptError.ProcessFdQuotaExceeded => AcceptError.ProcessFdQuotaExceeded,
                                        std.posix.AcceptError.SystemResources => AcceptError.OutOfMemory,
                                        std.posix.AcceptError.SocketNotListening => AcceptError.NotListening,
                                        std.posix.AcceptError.OperationNotSupported => AcceptError.OperationNotSupported,
                                        else => AcceptError.Unexpected,
                                    };
                                };

                                break :blk .{ .err = accept_err };
                            };

                            break :blk .{ .actual = .{
                                .handle = accept_result,
                                .addr = inner.addr,
                                .kind = inner.kind,
                            } };
                        };

                        com_ptr.result = .{ .accept = result };
                        com_ptr.task = job.task;
                        _ = loop.inner.swapRemove(i);
                        i -|= 1;
                        reaped += 1;
                    },
                    .connect => |inner| {
                        const com_ptr = &self.completions[reaped];

                        const result: ConnectResult = blk: {
                            std.posix.connect(inner.socket, &inner.addr.any, @sizeOf(std.net.Address)) catch |e| {
                                const connect_err: ConnectError = err: {
                                    break :err switch (e) {
                                        std.posix.ConnectError.WouldBlock => continue,
                                        std.posix.ConnectError.PermissionDenied => ConnectError.AccessDenied,
                                        std.posix.ConnectError.ConnectionResetByPeer => ConnectError.ConnectionRefused,
                                        std.posix.ConnectError.ConnectionPending => ConnectError.AlreadyConnected,
                                        std.posix.ConnectError.AddressInUse => ConnectError.AddressInUse,
                                        std.posix.ConnectError.AddressNotAvailable => ConnectError.AddressNotAvailable,
                                        std.posix.ConnectError.AddressFamilyNotSupported => ConnectError.AddressFamilyNotSupported,
                                        std.posix.ConnectError.NetworkUnreachable => ConnectError.NetworkUnreachable,
                                        std.posix.ConnectError.ConnectionTimedOut => ConnectError.TimedOut,
                                        std.posix.ConnectError.FileNotFound => ConnectError.NotASocket,
                                        std.posix.ConnectError.ConnectionRefused => ConnectError.ConnectionRefused,
                                        else => ConnectError.Unexpected,
                                    };
                                };

                                break :blk .{ .err = connect_err };
                            };

                            break :blk .{ .actual = .{
                                .handle = inner.socket,
                                .addr = inner.addr,
                                .kind = inner.kind,
                            } };
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
                                    break :err switch (e) {
                                        std.posix.RecvFromError.WouldBlock => continue,
                                        std.posix.RecvFromError.SystemResources => RecvError.OutOfMemory,
                                        std.posix.RecvFromError.SocketNotConnected => RecvError.NotConnected,
                                        std.posix.RecvFromError.ConnectionResetByPeer => RecvError.Closed,
                                        std.posix.RecvFromError.ConnectionRefused => RecvError.ConnectionRefused,
                                        else => break :err RecvError.Unexpected,
                                    };
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
                                    break :err switch (e) {
                                        std.posix.SendError.WouldBlock => continue,
                                        std.posix.SendError.SystemResources => SendError.OutOfMemory,
                                        std.posix.SendError.AccessDenied => SendError.AccessDenied,
                                        std.posix.SendError.ConnectionResetByPeer => SendError.ConnectionReset,
                                        std.posix.SendError.BrokenPipe => SendError.BrokenPipe,
                                        std.posix.SendError.FileDescriptorNotASocket => SendError.InvalidFd,
                                        std.posix.SendError.FastOpenAlreadyInProgress => SendError.AlreadyConnected,
                                        else => SendError.Unexpected,
                                    };
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
            ._queue_job = queue_job,
            ._deinit = deinit,
            ._wake = wake,
            ._submit = submit,
            ._reap = reap,
        };
    }
};
