const std = @import("std");
const builtin = @import("builtin");

const Runtime = @import("../runtime/lib.zig").Runtime;
const TaskFn = @import("../runtime/task.zig").TaskFn;

const Path = @import("lib.zig").Path;
const File = @import("lib.zig").File;

const FileMode = @import("../aio/lib.zig").FileMode;
const AioOpenFlags = @import("../aio/lib.zig").AioOpenFlags;

const OpenFileResult = @import("../aio/completion.zig").OpenFileResult;
const OpenDirResult = @import("../aio/completion.zig").OpenDirResult;

const DeleteResult = @import("../aio/completion.zig").DeleteResult;
const StatResult = @import("../aio/completion.zig").StatResult;
const ReadResult = @import("../aio/completion.zig").ReadResult;
const WriteResult = @import("../aio/completion.zig").WriteResult;

pub const Dir = struct {
    handle: std.posix.fd_t,

    /// Create a std.fs.Dir from a Dir.
    pub fn to_std(self: Dir) std.fs.Dir {
        return std.fs.Dir{ .fd = self.handle };
    }

    /// Create a Dir from the std.fs.Dir
    pub fn from_std(self: std.fs.Dir) Dir {
        return .{ .handle = self.fd };
    }

    /// Create a Directory.
    pub fn create(
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(OpenDirResult, @TypeOf(task_ctx)),
        path: Path,
    ) !void {
        const index = try rt.scheduler.spawn(OpenDirResult, task_ctx, task_fn, .waiting);
        try rt.aio.queue_mkdir(index, path);
    }

    /// Open a Directory.
    pub fn open(
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(OpenDirResult, @TypeOf(task_ctx)),
        path: Path,
    ) !void {
        const index = try rt.scheduler.spawn(OpenDirResult, task_ctx, task_fn, .waiting);

        const aio_flags: AioOpenFlags = .{
            .mode = .read,
            .create = false,
            .directory = true,
        };

        try rt.aio.queue_open(index, path, aio_flags);
    }

    /// Create a File relative to this Dir.
    pub fn create_file(
        self: *const Dir,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(OpenFileResult, @TypeOf(task_ctx)),
        subpath: [:0]const u8,
        flags: File.CreateFlags,
    ) !void {
        try File.create(rt, task_ctx, task_fn, .{ .rel = .{ .dir = self.handle, .path = subpath } }, flags);
    }

    /// Open a File relative to this Dir.
    pub fn open_file(
        self: *const Dir,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(OpenFileResult, @TypeOf(task_ctx)),
        subpath: [:0]const u8,
        flags: File.OpenFlags,
    ) !void {
        try File.open(rt, task_ctx, task_fn, .{ .rel = .{ .dir = self.handle, .path = subpath } }, flags);
    }

    /// Create a Dir relative to this Dir.
    pub fn create_dir(
        self: *const Dir,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(OpenDirResult, @TypeOf(task_ctx)),
        subpath: [:0]const u8,
    ) !void {
        try Dir.create(rt, task_ctx, task_fn, .{ .rel = .{ .dir = self.handle, .path = subpath } });
    }

    /// Open a Dir relative to this Dir.
    pub fn open_dir(
        self: *const Dir,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(OpenDirResult, @TypeOf(task_ctx)),
        subpath: [:0]const u8,
    ) !void {
        try Dir.open(rt, task_ctx, task_fn, .{ .rel = .{ .dir = self.handle, .path = subpath } });
    }

    /// Get Stat information of this Dir.
    pub fn stat(
        self: *const Dir,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(StatResult, @TypeOf(task_ctx)),
    ) !void {
        const index = try rt.scheduler.spawn(StatResult, task_ctx, task_fn, .waiting);
        try rt.aio.queue_stat(index, self.handle);
    }

    /// TODO: This needs to basically walk through the directory. This will end up
    /// having to be blocking?
    ///
    /// Might as well just use the std lib walker then and queue ops from there?
    pub fn walk(self: *const Dir, allocator: std.mem.Allocator) !void {
        _ = self;
        _ = allocator;
        @panic("Not implemented yet");
    }

    /// Delete a File within this Dir.
    pub fn delete_file(
        self: *const Dir,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(DeleteResult, @TypeOf(task_ctx)),
        sub_path: [:0]const u8,
    ) !void {
        const index = try rt.scheduler.spawn(DeleteResult, task_ctx, task_fn, .waiting);
        try rt.aio.queue_delete(index, .{ .rel = .{ .dir = self.handle, .path = sub_path } }, false);
    }

    /// Delete a Dir within this Dir.
    pub fn delete_dir(
        self: *const Dir,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(DeleteResult, @TypeOf(task_ctx)),
        sub_path: [:0]const u8,
    ) !void {
        const index = try rt.scheduler.spawn(DeleteResult, task_ctx, task_fn, .waiting);
        try rt.aio.queue_delete(index, .{ .rel = .{ .dir = self.handle, .path = sub_path } }, true);
    }

    /// Close the underlying Handle of this Dir.
    pub fn close(
        self: *const Dir,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(void, @TypeOf(task_ctx)),
    ) !void {
        const index = try rt.scheduler.spawn(void, task_ctx, task_fn, .waiting);
        try rt.aio.queue_close(index, self.handle);
    }
};
