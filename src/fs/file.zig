const std = @import("std");

const Runtime = @import("../runtime/lib.zig").Runtime;
const TaskFn = @import("../runtime/task.zig").TaskFn;

const Path = @import("lib.zig").Path;

const FileMode = @import("../aio/lib.zig").FileMode;
const AioOpenFlags = @import("../aio/lib.zig").AioOpenFlags;

const OpenFileResult = @import("../aio/completion.zig").OpenFileResult;

const StatResult = @import("../aio/completion.zig").StatResult;
const ReadResult = @import("../aio/completion.zig").ReadResult;
const WriteResult = @import("../aio/completion.zig").WriteResult;

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

        try rt.scheduler.spawn2(
            OpenFileResult,
            task_ctx,
            task_fn,
            .waiting,
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

        try rt.scheduler.spawn2(
            OpenFileResult,
            task_ctx,
            task_fn,
            .waiting,
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
        try rt.scheduler.spawn2(
            ReadResult,
            task_ctx,
            task_fn,
            .waiting,
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
        try rt.scheduler.spawn2(
            ReadResult,
            task_ctx,
            task_fn,
            .waiting,
            .{ .read = .{
                .fd = self.handle,
                .buffer = buffer,
                .offset = offset,
            } },
        );
    }

    pub fn write(
        self: *const File,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(WriteResult, @TypeOf(task_ctx)),
        buffer: []const u8,
    ) !void {
        try rt.scheduler.spawn2(
            WriteResult,
            task_ctx,
            task_fn,
            .waiting,
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
        try rt.scheduler.spawn2(
            WriteResult,
            task_ctx,
            task_fn,
            .waiting,
            .{ .write = .{
                .fd = self.handle,
                .buffer = buffer,
                .offset = offset,
            } },
        );
    }

    /// Writes all of the buffer into the File then runs the passed in Callback.
    /// There is an internal allocation that happens within this method using the
    /// runtime allocator.
    pub fn write_all(
        self: *const File,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(void, @TypeOf(task_ctx)),
        buffer: []const u8,
    ) !void {
        // Certain operations require a heap-allocated Context.
        // These provisions serve as an object for managing that and any
        // other recursive tasks.
        const Provision = struct {
            const Self = @This();
            buffer: []const u8,
            sent: usize,
            file: *const File,

            fn write_all_task(runtime: *Runtime, res: WriteResult, p: *Self) !void {
                const length = try res.unwrap();
                std.log.debug("written: {d}", .{length});
                p.sent += length;

                if (p.sent >= p.buffer.len) {
                    defer runtime.allocator.destroy(p);
                    try runtime.scheduler.spawn2(void, task_ctx, task_fn, .runnable, null);
                } else {
                    try p.file.write(runtime, p, write_all_task, p.buffer[p.sent..]);
                }
            }
        };

        const p = try rt.allocator.create(Provision);
        p.* = Provision{
            .buffer = buffer,
            .sent = 0,
            .file = self,
        };

        try self.write(rt, p, Provision.write_all_task, buffer);
    }

    pub fn write_all_offset(
        self: *const File,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(void, @TypeOf(task_ctx)),
        buffer: []const u8,
        offset: usize,
    ) !void {
        const Provision = struct {
            const Self = @This();
            file: *const File,
            sent: usize,
            buffer: []const u8,
            offset: usize,

            fn write_all_offset_task(runtime: *Runtime, res: WriteResult, p: *Self) !void {
                const length = try res.unwrap();
                std.log.debug("written: {d}", .{length});
                p.sent += length;
                p.offset += length;

                if (p.sent >= p.buffer.len) {
                    defer runtime.allocator.destroy(p);
                    try runtime.scheduler.spawn2(void, task_ctx, task_fn, .runnable, null);
                } else {
                    try p.file.write_offset(runtime, p, write_all_offset_task, p.buffer[p.sent..], p.offset);
                }
            }
        };

        const p = try rt.allocator.create(Provision);
        p.* = Provision{
            .file = self,
            .sent = 0,
            .buffer = buffer,
            .offset = offset,
        };

        try self.write_offset(rt, p, Provision.write_all_offset_task, buffer, offset);
    }

    pub fn stat(
        self: *const File,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(StatResult, @TypeOf(task_ctx)),
    ) !void {
        try rt.scheduler.spawn2(void, task_ctx, task_fn, .waiting, .{ .stat = self.handle });
    }

    pub fn close(
        self: *const File,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(void, @TypeOf(task_ctx)),
    ) !void {
        try rt.scheduler.spawn2(void, task_ctx, task_fn, .waiting, .{ .close = self.handle });
    }
};
