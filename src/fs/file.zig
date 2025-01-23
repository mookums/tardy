const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const log = std.log.scoped(.@"tardy/fs/file");

const Runtime = @import("../runtime/lib.zig").Runtime;
const TaskFn = @import("../runtime/task.zig").TaskFn;

const Path = @import("lib.zig").Path;
const Stat = @import("lib.zig").Stat;

const FileMode = @import("../aio/lib.zig").FileMode;
const AioOpenFlags = @import("../aio/lib.zig").AioOpenFlags;

const Resulted = @import("../aio/completion.zig").Resulted;
const OpenFileResult = @import("../aio/completion.zig").OpenFileResult;
const OpenError = @import("../aio/completion.zig").OpenError;
const StatResult = @import("../aio/completion.zig").StatResult;
const StatError = @import("../aio/completion.zig").StatError;
const ReadResult = @import("../aio/completion.zig").ReadResult;
const ReadError = @import("../aio/completion.zig").ReadError;
const WriteResult = @import("../aio/completion.zig").WriteResult;
const WriteError = @import("../aio/completion.zig").WriteError;

const Cross = @import("../cross/lib.zig");
const wrap = @import("../utils.zig").wrap;

const StdFile = std.fs.File;
const StdDir = std.fs.Dir;

pub const File = struct {
    handle: std.posix.fd_t,

    pub const CreateFlags = struct {
        mode: FileMode = .write,
        perms: isize = 0o644,
        truncate: bool = true,
        overwrite: bool = true,
    };

    pub const OpenFlags = struct {
        mode: FileMode = .read,
    };

    pub fn to_std(self: File) std.fs.File {
        return std.fs.File{ .handle = self.handle };
    }

    pub fn from_std(self: std.fs.File) File {
        return .{ .handle = self.handle };
    }

    /// Get `stdout` as a File.
    pub fn std_out() File {
        return .{ .handle = Cross.get_std_out() };
    }

    /// Get `stdin` as a File.
    pub fn std_in() File {
        return .{ .handle = Cross.get_std_in() };
    }

    /// Get `stderr` as a File.
    pub fn std_err() File {
        return .{ .handle = Cross.get_std_err() };
    }

    const CreateAction = struct {
        path: Path,
        flags: AioOpenFlags,

        pub fn resolve(self: *const CreateAction, rt: *Runtime) !File {
            if (rt.aio.features.has_capability(.open)) {
                try rt.scheduler.frame_await(.{ .open = .{ .path = self.path, .flags = self.flags } });

                const index = rt.current_task.?;
                const task = rt.scheduler.tasks.get(index);

                const result: OpenFileResult = switch (task.result.open) {
                    .actual => |actual| .{ .actual = actual.file },
                    .err => |err| .{ .err = err },
                };

                return try result.unwrap();
            } else {
                const std_flags: StdFile.CreateFlags = .{
                    .read = (self.flags.mode == .read or self.flags.mode == .read_write),
                    .truncate = self.flags.truncate,
                    .exclusive = self.flags.exclusive,
                };

                switch (self.path) {
                    .rel => |inner| {
                        const dir: StdDir = .{ .fd = inner.dir };
                        const opened = dir.createFileZ(inner.path, std_flags) catch |e| return switch (e) {
                            StdFile.OpenError.WouldBlock => @panic("WouldBlock"),
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

                        return .{ .handle = opened.handle };
                    },
                    .abs => |inner| {
                        const opened = std.fs.cwd().createFileZ(inner, std_flags) catch |e| return switch (e) {
                            StdFile.OpenError.WouldBlock => @panic("WouldBlock"),
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

                        return .{ .handle = opened.handle };
                    },
                }
            }
        }

        pub fn callback(
            self: *const OpenAction,
            rt: *Runtime,
            task_ctx: anytype,
            comptime task_fn: TaskFn(OpenFileResult, @TypeOf(task_ctx)),
        ) !void {
            try rt.scheduler.spawn(
                OpenFileResult,
                task_ctx,
                task_fn,
                .wait_for_io,
                .{ .open = .{
                    .path = self.path,
                    .flags = self.flags,
                } },
            );
        }
    };

    pub fn create(path: Path, flags: CreateFlags) CreateAction {
        const aio_flags: AioOpenFlags = .{
            .mode = flags.mode,
            .perms = flags.perms,
            .create = true,
            .truncate = flags.truncate,
            .exclusive = !flags.overwrite,
            .directory = false,
        };

        return .{ .path = path, .flags = aio_flags };
    }

    const OpenAction = struct {
        path: Path,
        flags: OpenFlags,

        pub fn resolve(self: *const OpenAction, rt: *Runtime) !File {
            const flags: AioOpenFlags = .{
                .mode = self.flags.mode,
                .create = false,
                .directory = false,
            };

            if (rt.aio.features.has_capability(.open)) {
                try rt.scheduler.frame_await(.{ .open = .{ .path = self.path, .flags = flags } });

                const index = rt.current_task.?;
                const task = rt.scheduler.tasks.get(index);
                const result: OpenFileResult = switch (task.result.open) {
                    .actual => |actual| .{ .actual = actual.file },
                    .err => |err| .{ .err = err },
                };

                return try result.unwrap();
            } else {
                log.debug("rt aio doesnt have open...", .{});
                const std_flags: StdFile.OpenFlags = .{
                    .mode = switch (flags.mode) {
                        .read => .read_only,
                        .write => .write_only,
                        .read_write => .read_write,
                    },
                };

                switch (self.path) {
                    .rel => |inner| {
                        const dir: StdDir = .{ .fd = inner.dir };
                        const opened = dir.openFileZ(inner.path, std_flags) catch |e| return switch (e) {
                            StdFile.OpenError.WouldBlock => @panic("WouldBlock"),
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

                        return .{ .handle = opened.handle };
                    },
                    .abs => |inner| {
                        const opened = std.fs.cwd().openFileZ(inner, std_flags) catch |e| return switch (e) {
                            StdFile.OpenError.WouldBlock => @panic("WouldBlock"),
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

                        return .{ .handle = opened.handle };
                    },
                }
            }
        }

        pub fn callback(
            self: *const OpenAction,
            rt: *Runtime,
            task_ctx: anytype,
            comptime task_fn: TaskFn(OpenFileResult, @TypeOf(task_ctx)),
        ) !void {
            const aio_flags: AioOpenFlags = .{
                .mode = self.flags.mode,
                .create = false,
                .directory = false,
            };

            try rt.scheduler.spawn(
                OpenFileResult,
                task_ctx,
                task_fn,
                .wait_for_io,
                .{ .open = .{
                    .path = self.path,
                    .flags = aio_flags,
                } },
            );
        }
    };

    pub fn open(path: Path, flags: OpenFlags) OpenAction {
        return .{ .path = path, .flags = flags };
    }

    const ReadAction = struct {
        file: File,
        buffer: []u8,
        offset: ?usize,

        pub fn resolve(self: *const ReadAction, rt: *Runtime) !usize {
            if (rt.aio.features.has_capability(.read)) {
                try rt.scheduler.frame_await(.{
                    .read = .{
                        .fd = self.file.handle,
                        .buffer = self.buffer,
                        .offset = self.offset,
                    },
                });

                const index = rt.current_task.?;
                const task = rt.scheduler.tasks.get(index);
                return try task.result.read.unwrap();
            } else {
                const std_file = self.file.to_std();

                // TODO: Proper error handling.
                const count = blk: {
                    if (self.offset) |o| {
                        break :blk std.fs.File.pread(std_file, self.buffer, o) catch |e| return switch (e) {
                            else => ReadError.Unexpected,
                        };
                    } else {
                        break :blk std.fs.File.read(std_file, self.buffer) catch |e| return switch (e) {
                            else => ReadError.Unexpected,
                        };
                    }
                };

                if (count == 0) return ReadError.EndOfFile;
                return count;
            }
        }

        pub fn callback(
            self: *const ReadAction,
            rt: *Runtime,
            task_ctx: anytype,
            comptime task_fn: TaskFn(ReadResult, @TypeOf(task_ctx)),
        ) !void {
            try rt.scheduler.spawn(
                ReadResult,
                task_ctx,
                task_fn,
                .wait_for_io,
                .{ .read = .{
                    .fd = self.file.handle,
                    .buffer = self.buffer,
                    .offset = self.offset,
                } },
            );
        }
    };

    pub fn read(self: File, buffer: []u8, offset: ?usize) ReadAction {
        return .{ .file = self, .buffer = buffer, .offset = offset };
    }

    const ReadAllAction = struct {
        file: File,
        buffer: []u8,
        offset: ?usize,

        pub fn resolve(self: *const ReadAllAction, rt: *Runtime) !usize {
            var length: usize = 0;

            while (length < self.buffer.len) {
                const real_offset: ?usize = if (self.offset) |offset| offset + length else null;

                const result = self.file.read(self.buffer[length..], real_offset).resolve(rt) catch |e| switch (e) {
                    error.EndOfFile => return length,
                    else => return e,
                };

                length += result;
            }

            return length;
        }

        pub fn callback(
            self: *const ReadAllAction,
            rt: *Runtime,
            task_ctx: anytype,
            comptime task_fn: TaskFn(ReadResult, @TypeOf(task_ctx)),
        ) !void {
            const Provision = struct {
                const Self = @This();
                buffer: []u8,
                read: usize,
                offset: ?usize = null,
                file: File,
                task_ctx: @TypeOf(task_ctx),

                fn read_all_task(runtime: *Runtime, res: ReadResult, p: *Self) !void {
                    var run_task = false;

                    scope: {
                        errdefer runtime.allocator.destroy(p);
                        const length = res.unwrap() catch |e| {
                            switch (e) {
                                error.EndOfFile => {
                                    run_task = true;
                                    break :scope;
                                },
                                else => {
                                    try task_fn(runtime, .{ .err = @errorCast(e) }, p.task_ctx);
                                    return;
                                },
                            }
                        };

                        p.read += length;
                        if (p.offset) |*off| off.* = off.* + length;

                        // if we have read more than the buffer len,
                        // something is very wrong.
                        assert(p.read <= p.buffer.len);
                        if (p.read == p.buffer.len) {
                            run_task = true;
                        } else {
                            try p.file.read(p.buffer[p.read..], p.offset).callback(runtime, p, read_all_task);
                        }
                    }

                    if (run_task) {
                        defer runtime.allocator.destroy(p);
                        try task_fn(runtime, .{ .actual = p.read }, p.task_ctx);
                    }
                }
            };

            const p = try rt.allocator.create(Provision);
            errdefer rt.allocator.destroy(p);
            p.* = Provision{
                .buffer = self.buffer,
                .read = 0,
                .file = self.file.*,
                .offset = self.offset,
                .task_ctx = task_ctx,
            };

            try self.file.read(self.buffer, self.offset).callback(rt, p, Provision.read_all_task);
        }
    };

    pub fn read_all(self: File, buffer: []u8, offset: ?usize) ReadAllAction {
        return .{ .file = self, .buffer = buffer, .offset = offset };
    }

    const WriteAction = struct {
        file: File,
        buffer: []const u8,
        offset: ?usize,

        pub fn resolve(self: *const WriteAction, rt: *Runtime) !usize {
            if (rt.aio.features.has_capability(.write)) {
                try rt.scheduler.frame_await(.{
                    .write = .{ .fd = self.file.handle, .buffer = self.buffer, .offset = self.offset },
                });

                const index = rt.current_task.?;
                const task = rt.scheduler.tasks.get(index);
                return try task.result.write.unwrap();
            } else {
                const std_file = self.file.to_std();

                // TODO: Proper error handling.
                if (self.offset) |o| {
                    return std.fs.File.pwrite(std_file, self.buffer, o) catch |e| switch (e) {
                        StdFile.WriteError.NoSpaceLeft => WriteError.NoSpace,
                        else => WriteError.Unexpected,
                    };
                } else {
                    return std.fs.File.write(std_file, self.buffer) catch |e| switch (e) {
                        StdFile.WriteError.NoSpaceLeft => WriteError.NoSpace,
                        else => WriteError.Unexpected,
                    };
                }
            }
        }

        pub fn callback(
            self: *const WriteAction,
            rt: *Runtime,
            task_ctx: anytype,
            comptime task_fn: TaskFn(WriteResult, @TypeOf(task_ctx)),
        ) !void {
            try rt.scheduler.spawn(
                WriteResult,
                task_ctx,
                task_fn,
                .wait_for_io,
                .{ .write = .{
                    .fd = self.file.handle,
                    .buffer = self.buffer,
                    .offset = self.offset,
                } },
            );
        }
    };

    pub fn write(self: File, buffer: []const u8, offset: ?usize) WriteAction {
        return .{ .file = self, .buffer = buffer, .offset = offset };
    }

    const WriteAllAction = struct {
        file: File,
        buffer: []const u8,
        offset: ?usize,

        pub fn resolve(self: *const WriteAllAction, rt: *Runtime) !usize {
            var length: usize = 0;

            while (length < self.buffer.len) {
                const real_offset: ?usize = if (self.offset) |offset| offset + length else null;

                const result = self.file.write(
                    self.buffer[length..],
                    real_offset,
                ).resolve(rt) catch |e| switch (e) {
                    error.NoSpace => return length,
                    else => return e,
                };

                length += result;
            }

            return length;
        }

        pub fn callback(
            self: *const WriteAllAction,
            rt: *Runtime,
            task_ctx: anytype,
            comptime task_fn: TaskFn(WriteResult, @TypeOf(task_ctx)),
        ) !void {
            const Provision = struct {
                const Self = @This();
                buffer: []const u8,
                wrote: usize,
                offset: ?usize = null,
                file: File,
                task_ctx: @TypeOf(task_ctx),

                fn write_all_task(runtime: *Runtime, res: WriteResult, p: *Self) !void {
                    var run_task = false;
                    scope: {
                        errdefer runtime.allocator.destroy(p);
                        const length = res.unwrap() catch |e| {
                            switch (e) {
                                error.NoSpace => {
                                    run_task = true;
                                    break :scope;
                                },
                                else => {
                                    try task_fn(runtime, .{ .err = @errorCast(e) }, p.task_ctx);
                                    return e;
                                },
                            }
                        };

                        p.wrote += length;
                        if (p.offset) |*off| off.* = off.* + length;

                        if (p.wrote >= p.buffer.len) {
                            run_task = true;
                        } else {
                            try p.file.write(p.buffer[p.wrote..], p.offset).callback(runtime, p, write_all_task);
                        }
                    }

                    if (run_task) {
                        defer runtime.allocator.destroy(p);
                        try task_fn(runtime, .{ .actual = p.wrote }, p.task_ctx);
                    }
                }
            };

            const p = try rt.allocator.create(Provision);
            errdefer rt.allocator.destroy(p);
            p.* = Provision{
                .buffer = self.buffer,
                .wrote = 0,
                .file = self.file.*,
                .offset = self.offset,
                .task_ctx = task_ctx,
            };

            try self.file.write(self.buffer, self.offset).callback(rt, p, Provision.write_all_task);
        }
    };

    pub fn write_all(self: File, buffer: []const u8, offset: ?usize) WriteAllAction {
        return .{ .file = self, .buffer = buffer, .offset = offset };
    }

    const StatAction = struct {
        file: File,

        pub fn resolve(self: *const StatAction, rt: *Runtime) !Stat {
            if (rt.aio.features.has_capability(.stat)) {
                try rt.scheduler.frame_await(.{ .stat = self.file.handle });

                const index = rt.current_task.?;
                const task = rt.scheduler.tasks.get(index);
                return try task.result.stat.unwrap();
            } else {
                const std_file = self.file.to_std();

                const file_stat = std_file.stat() catch |e| {
                    return switch (e) {
                        StdFile.StatError.AccessDenied => StatError.AccessDenied,
                        StdFile.StatError.SystemResources => StatError.OutOfMemory,
                        StdFile.StatError.Unexpected => StatError.Unexpected,
                    };
                };

                return Stat{
                    .size = file_stat.size,
                    .mode = file_stat.mode,
                    .changed = .{
                        .seconds = @intCast(@divTrunc(file_stat.ctime, std.time.ns_per_s)),
                        .nanos = @intCast(@mod(file_stat.ctime, std.time.ns_per_s)),
                    },
                    .modified = .{
                        .seconds = @intCast(@divTrunc(file_stat.mtime, std.time.ns_per_s)),
                        .nanos = @intCast(@mod(file_stat.mtime, std.time.ns_per_s)),
                    },
                    .accessed = .{
                        .seconds = @intCast(@divTrunc(file_stat.atime, std.time.ns_per_s)),
                        .nanos = @intCast(@mod(file_stat.atime, std.time.ns_per_s)),
                    },
                };
            }
        }

        pub fn callback(
            self: *const StatAction,
            rt: *Runtime,
            task_ctx: anytype,
            comptime task_fn: TaskFn(StatResult, @TypeOf(task_ctx)),
        ) !void {
            try rt.scheduler.spawn(StatResult, task_ctx, task_fn, .wait_for_io, .{ .stat = self.file.handle });
        }
    };

    pub fn stat(self: File) StatAction {
        return .{ .file = self };
    }

    const CloseAction = struct {
        file: File,

        pub fn resolve(self: *const CloseAction, rt: *Runtime) !void {
            if (rt.aio.features.has_capability(.close))
                try rt.scheduler.frame_await(.{ .close = self.file.handle })
            else
                std.posix.close(self.file.handle);
        }

        pub fn callback(
            self: *const CloseAction,
            rt: *Runtime,
            task_ctx: anytype,
            comptime task_fn: TaskFn(void, @TypeOf(task_ctx)),
        ) !void {
            try rt.scheduler.spawn(void, task_ctx, task_fn, .wait_for_io, .{ .close = self.handle });
        }
    };

    pub fn close(self: File) CloseAction {
        return .{ .file = self };
    }

    pub fn close_blocking(self: File) void {
        std.posix.close(self.handle);
    }
};
