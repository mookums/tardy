const std = @import("std");

const Runtime = @import("../runtime/lib.zig").Runtime;
const TaskFn = @import("../runtime/task.zig").TaskFn;

const Stat = @import("../aio/completion.zig").Stat;

const OpenResult = @import("../aio/completion.zig").OpenResult;
const StatResult = @import("../aio/completion.zig").StatResult;
const ReadResult = @import("../aio/completion.zig").ReadResult;
const WriteResult = @import("../aio/completion.zig").WriteResult;

pub const Filesystem = struct {
    pub fn open(
        self: *Filesystem,
        task_ctx: anytype,
        comptime task_fn: TaskFn(OpenResult, @TypeOf(task_ctx)),
        path: [:0]const u8,
    ) !void {
        const rt: *Runtime = @alignCast(@fieldParentPtr("fs", self));
        const index = try rt.scheduler.spawn(OpenResult, task_ctx, task_fn, .waiting);
        try rt.aio.queue_open(index, path);
    }

    pub fn stat(
        self: *Filesystem,
        task_ctx: anytype,
        comptime task_fn: TaskFn(StatResult, @TypeOf(task_ctx)),
        fd: std.posix.fd_t,
    ) !void {
        const rt: *Runtime = @alignCast(@fieldParentPtr("fs", self));
        const index = try rt.scheduler.spawn(StatResult, task_ctx, task_fn, .waiting);
        try rt.aio.queue_stat(index, fd);
    }

    pub fn read(
        self: *Filesystem,
        task_ctx: anytype,
        comptime task_fn: TaskFn(ReadResult, @TypeOf(task_ctx)),
        fd: std.posix.fd_t,
        buffer: []u8,
        offset: usize,
    ) !void {
        const rt: *Runtime = @alignCast(@fieldParentPtr("fs", self));
        const index = try rt.scheduler.spawn(ReadResult, task_ctx, task_fn, .waiting);
        try rt.aio.queue_read(index, fd, buffer, offset);
    }

    pub fn write(
        self: *Filesystem,
        task_ctx: anytype,
        comptime task_fn: TaskFn(WriteResult, @TypeOf(task_ctx)),
        fd: std.posix.fd_t,
        buffer: []const u8,
        offset: usize,
    ) !void {
        const rt: *Runtime = @alignCast(@fieldParentPtr("fs", self));
        const index = try rt.scheduler.spawn(WriteResult, task_ctx, task_fn, .waiting);
        try rt.aio.queue_write(index, fd, buffer, offset);
    }

    pub fn close(
        self: *Filesystem,
        task_ctx: anytype,
        comptime task_fn: TaskFn(void, @TypeOf(task_ctx)),
        fd: std.posix.fd_t,
    ) !void {
        const rt: *Runtime = @alignCast(@fieldParentPtr("fs", self));
        const index = try rt.scheduler.spawn(void, task_ctx, task_fn, .waiting);
        try rt.aio.queue_close(index, fd);
    }
};
