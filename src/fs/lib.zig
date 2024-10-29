const std = @import("std");

const Runtime = @import("../runtime/lib.zig").Runtime;
const TaskFn = @import("../runtime/task.zig").TaskFn;

const Stat = @import("../aio/completion.zig").Stat;

pub const Filesystem = struct {
    pub fn open(
        self: *Filesystem,
        comptime C: type,
        comptime task_fn: TaskFn(std.posix.fd_t, C),
        task_ctx: C,
        path: [:0]const u8,
    ) !void {
        const rt: *Runtime = @alignCast(@fieldParentPtr("fs", self));
        const index = try rt.scheduler.spawn(std.posix.fd_t, C, task_fn, task_ctx, .waiting);
        try rt.aio.queue_open(index, path);
    }

    pub fn stat(
        self: *Filesystem,
        comptime C: type,
        comptime task_fn: TaskFn(Stat, C),
        task_ctx: C,
        fd: std.posix.fd_t,
    ) !void {
        const rt: *Runtime = @alignCast(@fieldParentPtr("fs", self));
        const index = try rt.scheduler.spawn(Stat, C, task_fn, task_ctx, .waiting);
        try rt.aio.queue_stat(index, fd);
    }

    pub fn read(
        self: *Filesystem,
        comptime C: type,
        comptime task_fn: TaskFn(i32, C),
        task_ctx: C,
        fd: std.posix.fd_t,
        buffer: []u8,
        offset: usize,
    ) !void {
        const rt: *Runtime = @alignCast(@fieldParentPtr("fs", self));
        const index = try rt.scheduler.spawn(i32, C, task_fn, task_ctx, .waiting);
        try rt.aio.queue_read(index, fd, buffer, offset);
    }

    pub fn write(
        self: *Filesystem,
        comptime C: type,
        comptime task_fn: TaskFn(i32, C),
        task_ctx: C,
        fd: std.posix.fd_t,
        buffer: []const u8,
        offset: usize,
    ) !void {
        const rt: *Runtime = @alignCast(@fieldParentPtr("fs", self));
        const index = try rt.scheduler.spawn(i32, C, task_fn, task_ctx, .waiting);
        try rt.aio.queue_write(index, fd, buffer, offset);
    }

    pub fn close(
        self: *Filesystem,
        comptime C: type,
        comptime task_fn: TaskFn(void, C),
        task_ctx: C,
        fd: std.posix.fd_t,
    ) !void {
        const rt: *Runtime = @alignCast(@fieldParentPtr("fs", self));
        const index = try rt.scheduler.spawn(void, C, task_fn, task_ctx, .waiting);
        try rt.aio.queue_close(index, fd);
    }
};
