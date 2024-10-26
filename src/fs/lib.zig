const std = @import("std");

const Runtime = @import("../runtime/lib.zig").Runtime;
const TaskFn = @import("../runtime/task.zig").TaskFn;

pub const Filesystem = struct {
    pub fn open(
        self: *Filesystem,
        comptime Context: type,
        comptime task_fn: TaskFn(Context),
        task_ctx: Context,
        path: [:0]const u8,
    ) !void {
        const rt: *Runtime = @alignCast(@fieldParentPtr("fs", self));
        const index = try rt.scheduler.spawn(Context, task_fn, task_ctx, .waiting);
        try rt.aio.queue_open(index, path);
    }

    pub fn stat(
        self: *Filesystem,
        comptime Context: type,
        comptime task_fn: TaskFn(Context),
        task_ctx: Context,
        fd: std.posix.fd_t,
    ) !void {
        const rt: *Runtime = @alignCast(@fieldParentPtr("fs", self));
        const index = try rt.scheduler.spawn(Context, task_fn, task_ctx, .waiting);
        try rt.aio.queue_stat(index, fd);
    }

    pub fn read(
        self: *Filesystem,
        comptime Context: type,
        comptime task_fn: TaskFn(Context),
        task_ctx: Context,
        fd: std.posix.fd_t,
        buffer: []u8,
        offset: usize,
    ) !void {
        const rt: *Runtime = @alignCast(@fieldParentPtr("fs", self));
        const index = try rt.scheduler.spawn(Context, task_fn, task_ctx, .waiting);
        try rt.aio.queue_read(index, fd, buffer, offset);
    }

    pub fn write(
        self: *Filesystem,
        comptime Context: type,
        comptime task_fn: TaskFn(Context),
        task_ctx: Context,
        fd: std.posix.fd_t,
        buffer: []const u8,
        offset: usize,
    ) !void {
        const rt: *Runtime = @alignCast(@fieldParentPtr("fs", self));
        const index = try rt.scheduler.spawn(Context, task_fn, task_ctx, .waiting);
        try rt.aio.queue_write(index, fd, buffer, offset);
    }

    pub fn close(
        self: *Filesystem,
        comptime Context: type,
        comptime task_fn: TaskFn(Context),
        task_ctx: Context,
        fd: std.posix.fd_t,
    ) !void {
        const rt: *Runtime = @alignCast(@fieldParentPtr("fs", self));
        const index = try rt.scheduler.spawn(Context, task_fn, task_ctx, .waiting);
        try rt.aio.queue_close(index, fd);
    }
};
