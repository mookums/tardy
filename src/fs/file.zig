const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const log = std.log.scoped(.@"tardy/fs/file");

const Runtime = @import("../runtime/lib.zig").Runtime;
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

pub const File = packed struct {
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

    pub fn create(rt: *Runtime, path: Path, flags: CreateFlags) !File {
        const aio_flags: AioOpenFlags = .{
            .mode = flags.mode,
            .perms = flags.perms,
            .create = true,
            .truncate = flags.truncate,
            .exclusive = !flags.overwrite,
            .directory = false,
        };

        if (rt.aio.features.has_capability(.open)) {
            try rt.scheduler.io_await(.{ .open = .{ .path = path, .flags = aio_flags } });

            const index = rt.current_task.?;
            const task = rt.scheduler.tasks.get(index);

            const result: OpenFileResult = switch (task.result.open) {
                .actual => |actual| .{ .actual = actual.file },
                .err => |err| .{ .err = err },
            };

            return try result.unwrap();
        } else {
            const std_flags: StdFile.CreateFlags = .{
                .read = (aio_flags.mode == .read or aio_flags.mode == .read_write),
                .truncate = aio_flags.truncate,
                .exclusive = aio_flags.exclusive,
            };

            switch (path) {
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
                    const opened = std.fs.createFileAbsoluteZ(inner, std_flags) catch |e| return switch (e) {
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

    pub fn open(rt: *Runtime, path: Path, flags: OpenFlags) !File {
        if (rt.aio.features.has_capability(.open)) {
            const aio_flags: AioOpenFlags = .{
                .mode = flags.mode,
                .create = false,
                .directory = false,
            };

            try rt.scheduler.io_await(.{ .open = .{ .path = path, .flags = aio_flags } });

            const index = rt.current_task.?;
            const task = rt.scheduler.tasks.get(index);
            const result: OpenFileResult = switch (task.result.open) {
                .actual => |actual| .{ .actual = actual.file },
                .err => |err| .{ .err = err },
            };

            return try result.unwrap();
        } else {
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
                    const opened = std.fs.openFileAbsoluteZ(inner, std_flags) catch |e| return switch (e) {
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

    pub fn read(self: File, rt: *Runtime, buffer: []u8, offset: ?usize) !usize {
        if (rt.aio.features.has_capability(.read)) {
            try rt.scheduler.io_await(.{
                .read = .{
                    .fd = self.handle,
                    .buffer = buffer,
                    .offset = offset,
                },
            });

            const index = rt.current_task.?;
            const task = rt.scheduler.tasks.get(index);
            return try task.result.read.unwrap();
        } else {
            const std_file = self.to_std();

            // TODO: Proper error handling.
            const count = blk: {
                if (offset) |o| {
                    break :blk std.fs.File.pread(std_file, buffer, o) catch |e| return switch (e) {
                        else => ReadError.Unexpected,
                    };
                } else {
                    break :blk std.fs.File.read(std_file, buffer) catch |e| return switch (e) {
                        else => ReadError.Unexpected,
                    };
                }
            };

            if (count == 0) return ReadError.EndOfFile;
            return count;
        }
        return .{ .file = self, .buffer = buffer, .offset = offset };
    }

    pub fn read_all(self: File, rt: *Runtime, buffer: []u8, offset: ?usize) !usize {
        var length: usize = 0;

        while (length < buffer.len) {
            const real_offset: ?usize = if (offset) |o| o + length else null;

            const result = self.read(rt, buffer[length..], real_offset) catch |e| switch (e) {
                error.EndOfFile => return length,
                else => return e,
            };

            length += result;
        }

        return length;
    }

    pub fn write(self: File, rt: *Runtime, buffer: []const u8, offset: ?usize) !usize {
        if (rt.aio.features.has_capability(.write)) {
            try rt.scheduler.io_await(.{
                .write = .{ .fd = self.handle, .buffer = buffer, .offset = offset },
            });

            const index = rt.current_task.?;
            const task = rt.scheduler.tasks.get(index);
            return try task.result.write.unwrap();
        } else {
            const std_file = self.to_std();

            // TODO: Proper error handling.
            if (offset) |o| {
                return std.fs.File.pwrite(std_file, buffer, o) catch |e| switch (e) {
                    StdFile.WriteError.NoSpaceLeft => WriteError.NoSpace,
                    else => WriteError.Unexpected,
                };
            } else {
                return std.fs.File.write(std_file, buffer) catch |e| switch (e) {
                    StdFile.WriteError.NoSpaceLeft => WriteError.NoSpace,
                    else => WriteError.Unexpected,
                };
            }
        }
    }

    pub fn write_all(self: File, rt: *Runtime, buffer: []const u8, offset: ?usize) !usize {
        var length: usize = 0;

        while (length < buffer.len) {
            const real_offset: ?usize = if (offset) |o| o + length else null;

            const result = self.write(rt, buffer[length..], real_offset) catch |e| switch (e) {
                error.NoSpace => return length,
                else => return e,
            };

            length += result;
        }

        return length;
    }

    pub fn stat(self: File, rt: *Runtime) !Stat {
        if (rt.aio.features.has_capability(.stat)) {
            try rt.scheduler.io_await(.{ .stat = self.handle });

            const index = rt.current_task.?;
            const task = rt.scheduler.tasks.get(index);
            return try task.result.stat.unwrap();
        } else {
            const std_file = self.to_std();

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

    pub fn close(self: File, rt: *Runtime) !void {
        if (rt.aio.features.has_capability(.close))
            try rt.scheduler.io_await(.{ .close = self.handle })
        else
            std.posix.close(self.handle);
    }

    pub fn close_blocking(self: File) void {
        std.posix.close(self.handle);
    }
};
