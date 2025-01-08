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
        const index = try rt.scheduler.spawn(OpenFileResult, task_ctx, task_fn, .waiting);

        const aio_flags: AioOpenFlags = .{
            .mode = flags.mode,
            .create = true,
            .truncate = flags.truncate,
            .exclusive = !flags.overwrite,
            .directory = false,
        };

        try rt.aio.queue_open(index, path, aio_flags);
    }

    pub fn open(
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(OpenFileResult, @TypeOf(task_ctx)),
        path: Path,
        flags: OpenFlags,
    ) !void {
        const index = try rt.scheduler.spawn(OpenFileResult, task_ctx, task_fn, .waiting);

        const aio_flags: AioOpenFlags = .{
            .mode = flags.mode,
            .create = false,
            .directory = false,
        };

        try rt.aio.queue_open(index, path, aio_flags);
    }

    pub fn read(
        self: *const File,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(ReadResult, @TypeOf(task_ctx)),
        buffer: []u8,
    ) !void {
        const index = try rt.scheduler.spawn(ReadResult, task_ctx, task_fn, .waiting);
        try rt.aio.queue_read(index, self.handle, buffer, null);
    }

    pub fn read_offset(
        self: *const File,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(ReadResult, @TypeOf(task_ctx)),
        buffer: []u8,
        offset: usize,
    ) !void {
        const index = try rt.scheduler.spawn(ReadResult, task_ctx, task_fn, .waiting);
        try rt.aio.queue_read(index, self.handle, buffer, offset);
    }

    pub fn write(
        self: *const File,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(WriteResult, @TypeOf(task_ctx)),
        buffer: []const u8,
    ) !void {
        const index = try rt.scheduler.spawn(WriteResult, task_ctx, task_fn, .waiting);
        try rt.aio.queue_write(index, self.handle, buffer, null);
    }

    pub fn write_offset(
        self: *const File,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(WriteResult, @TypeOf(task_ctx)),
        buffer: []const u8,
        offset: usize,
    ) !void {
        const index = try rt.scheduler.spawn(WriteResult, task_ctx, task_fn, .waiting);
        try rt.aio.queue_write(index, self.handle, buffer, offset);
    }

    pub fn stat(
        self: *const File,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(StatResult, @TypeOf(task_ctx)),
    ) !void {
        const index = try rt.scheduler.spawn(StatResult, task_ctx, task_fn, .waiting);
        try rt.aio.queue_stat(index, self.handle);
    }

    pub fn close(
        self: *const File,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(void, @TypeOf(task_ctx)),
    ) !void {
        const index = try rt.scheduler.spawn(void, task_ctx, task_fn, .waiting);
        try rt.aio.queue_close(index, self.handle);
    }
};
