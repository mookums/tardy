const std = @import("std");
const assert = std.debug.assert;

const Runtime = @import("../runtime/lib.zig").Runtime;
const TaskFn = @import("../runtime/task.zig").TaskFn;

const Path = @import("lib.zig").Path;

const FileMode = @import("../aio/lib.zig").FileMode;
const AioOpenFlags = @import("../aio/lib.zig").AioOpenFlags;

const OpenFileResult = @import("../aio/completion.zig").OpenFileResult;

const Resulted = @import("../aio/completion.zig").Resulted;
const StatResult = @import("../aio/completion.zig").StatResult;
const ReadResult = @import("../aio/completion.zig").ReadResult;

const WriteAllResult = @import("../aio/completion.zig").WriteAllResult;
const WriteResult = @import("../aio/completion.zig").WriteResult;
const WriteError = @import("../aio/completion.zig").WriteError;

const Cross = @import("../cross/lib.zig");
const ZeroCopy = @import("../core/zero_copy.zig").ZeroCopy;
const wrap = @import("../utils.zig").wrap;

pub const File = struct {
    handle: std.posix.fd_t,

    pub const CreateFlags = struct {
        mode: FileMode = .write,
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

    pub fn create(
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(OpenFileResult, @TypeOf(task_ctx)),
        path: Path,
        flags: CreateFlags,
    ) !void {
        const aio_flags: AioOpenFlags = .{
            .mode = flags.mode,
            .perms = 0o644,
            .create = true,
            .truncate = flags.truncate,
            .exclusive = !flags.overwrite,
            .directory = false,
        };

        try rt.scheduler.spawn(
            OpenFileResult,
            task_ctx,
            task_fn,
            .wait_for_io,
            .{ .open = .{ .path = path, .flags = aio_flags } },
        );
    }

    pub fn open(
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(OpenFileResult, @TypeOf(task_ctx)),
        path: Path,
        flags: OpenFlags,
    ) !void {
        const aio_flags: AioOpenFlags = .{
            .mode = flags.mode,
            .create = false,
            .directory = false,
        };

        try rt.scheduler.spawn(
            OpenFileResult,
            task_ctx,
            task_fn,
            .wait_for_io,
            .{ .open = .{
                .path = path,
                .flags = aio_flags,
            } },
        );
    }

    pub fn read(
        self: *const File,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(ReadResult, @TypeOf(task_ctx)),
        buffer: []u8,
    ) !void {
        try rt.scheduler.spawn(
            ReadResult,
            task_ctx,
            task_fn,
            .wait_for_io,
            .{ .read = .{
                .fd = self.handle,
                .buffer = buffer,
                .offset = null,
            } },
        );
    }

    pub fn read_offset(
        self: *const File,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(ReadResult, @TypeOf(task_ctx)),
        buffer: []u8,
        offset: usize,
    ) !void {
        try rt.scheduler.spawn(
            ReadResult,
            task_ctx,
            task_fn,
            .wait_for_io,
            .{ .read = .{
                .fd = self.handle,
                .buffer = buffer,
                .offset = offset,
            } },
        );
    }

    pub fn read_all(
        self: *const File,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(ReadResult, @TypeOf(task_ctx)),
        buffer: []u8,
        offset: ?usize,
    ) !void {
        const Provision = struct {
            const Self = @This();
            buffer: []u8,
            read: usize,
            offset: ?usize = null,
            file: *const File,
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
                        if (p.offset) |off| {
                            try p.file.read_offset(runtime, p, read_all_task, p.buffer[p.read..], off);
                        } else {
                            try p.file.read(runtime, p, read_all_task, p.buffer[p.read..]);
                        }
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
            .buffer = buffer,
            .read = 0,
            .file = self,
            .offset = offset,
            .task_ctx = task_ctx,
        };

        if (offset) |off|
            try self.read_offset(rt, p, Provision.read_all_task, buffer, off)
        else
            try self.read(rt, p, Provision.read_all_task, buffer);
    }

    pub fn read_all_alloc(
        self: *const File,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(ReadResult, @TypeOf(task_ctx)),
        list: *ZeroCopy(u8),
        offset: ?usize,
    ) !void {
        const READ_CHUNK_SIZE: usize = 4096 * 2;

        const Provision = struct {
            const Self = @This();
            buffer: []u8,
            list: *ZeroCopy(u8),
            offset: ?usize = null,
            file: *const File,
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

                    p.list.mark_written(length);
                    if (p.offset) |*off| off.* = off.* + length;
                    p.buffer = try p.list.get_write_area(READ_CHUNK_SIZE);

                    // only ends on the EOF.
                    if (p.offset) |off| {
                        try p.file.read_offset(runtime, p, read_all_task, p.buffer, off);
                    } else {
                        try p.file.read(runtime, p, read_all_task, p.buffer);
                    }
                }

                if (run_task) {
                    defer runtime.allocator.destroy(p);
                    try task_fn(runtime, .{ .actual = p.list.len }, p.task_ctx);
                }
            }
        };

        const p = try rt.allocator.create(Provision);
        errdefer rt.allocator.destroy(p);
        p.* = Provision{
            .buffer = try list.get_write_area(READ_CHUNK_SIZE),
            .list = list,
            .file = self,
            .offset = offset,
            .task_ctx = task_ctx,
        };

        if (offset) |off|
            try self.read_offset(rt, p, Provision.read_all_task, p.buffer, off)
        else
            try self.read(rt, p, Provision.read_all_task, p.buffer);
    }

    pub fn write(
        self: *const File,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(WriteResult, @TypeOf(task_ctx)),
        buffer: []const u8,
    ) !void {
        try rt.scheduler.spawn(
            WriteResult,
            task_ctx,
            task_fn,
            .wait_for_io,
            .{ .write = .{
                .fd = self.handle,
                .buffer = buffer,
                .offset = null,
            } },
        );
    }

    pub fn write_offset(
        self: *const File,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(WriteResult, @TypeOf(task_ctx)),
        buffer: []const u8,
        offset: usize,
    ) !void {
        try rt.scheduler.spawn(
            WriteResult,
            task_ctx,
            task_fn,
            .wait_for_io,
            .{ .write = .{
                .fd = self.handle,
                .buffer = buffer,
                .offset = offset,
            } },
        );
    }

    pub fn write_all(
        self: *const File,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(WriteResult, @TypeOf(task_ctx)),
        buffer: []const u8,
        offset: ?usize,
    ) !void {
        const Provision = struct {
            const Self = @This();
            buffer: []const u8,
            wrote: usize,
            offset: ?usize = null,
            file: *const File,
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
                        if (p.offset) |off|
                            try p.file.write_offset(runtime, p, write_all_task, p.buffer[p.wrote..], off)
                        else
                            try p.file.write(runtime, p, write_all_task, p.buffer[p.wrote..]);
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
            .buffer = buffer,
            .wrote = 0,
            .file = self,
            .offset = offset,
            .task_ctx = task_ctx,
        };

        if (offset) |off|
            try self.write_offset(rt, p, Provision.write_all_task, buffer, off)
        else
            try self.write(rt, p, Provision.write_all_task, buffer);
    }

    pub fn stat(
        self: *const File,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(StatResult, @TypeOf(task_ctx)),
    ) !void {
        try rt.scheduler.spawn(StatResult, task_ctx, task_fn, .wait_for_io, .{ .stat = self.handle });
    }

    pub fn close(
        self: *const File,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(void, @TypeOf(task_ctx)),
    ) !void {
        try rt.scheduler.spawn(void, task_ctx, task_fn, .wait_for_io, .{ .close = self.handle });
    }

    pub fn close_blocking(self: *const File) void {
        std.posix.close(self.handle);
    }
};
