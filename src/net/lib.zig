const std = @import("std");

const Runtime = @import("../runtime/lib.zig").Runtime;
const TaskFn = @import("../runtime/task.zig").TaskFn;

pub const Net = struct {
    pub fn accept(
        self: *Net,
        comptime C: type,
        comptime task_fn: TaskFn(std.posix.socket_t, C),
        task_ctx: C,
        socket: std.posix.socket_t,
    ) !void {
        const rt: *Runtime = @alignCast(@fieldParentPtr("net", self));
        const index = try rt.scheduler.spawn(std.posix.socket_t, C, task_fn, task_ctx, .waiting);
        try rt.aio.queue_accept(index, socket);
    }

    pub fn recv(
        self: *Net,
        comptime C: type,
        comptime task_fn: TaskFn(i32, C),
        task_ctx: C,
        socket: std.posix.socket_t,
        buffer: []u8,
    ) !void {
        const rt: *Runtime = @alignCast(@fieldParentPtr("net", self));
        const index = try rt.scheduler.spawn(i32, C, task_fn, task_ctx, .waiting);
        try rt.aio.queue_recv(index, socket, buffer);
    }

    pub fn send(
        self: *Net,
        comptime C: type,
        comptime task_fn: TaskFn(i32, C),
        task_ctx: C,
        socket: std.posix.socket_t,
        buffer: []const u8,
    ) !void {
        const rt: *Runtime = @alignCast(@fieldParentPtr("net", self));
        const index = try rt.scheduler.spawn(i32, C, task_fn, task_ctx, .waiting);
        try rt.aio.queue_send(index, socket, buffer);
    }

    pub fn close(
        self: *Net,
        comptime C: type,
        comptime task_fn: TaskFn(void, C),
        task_ctx: C,
        socket: std.posix.socket_t,
    ) !void {
        const rt: *Runtime = @alignCast(@fieldParentPtr("net", self));
        const index = try rt.scheduler.spawn(void, C, task_fn, task_ctx, .waiting);
        try rt.aio.queue_close(index, socket);
    }
};
