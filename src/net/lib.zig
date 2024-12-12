const std = @import("std");

const Runtime = @import("../runtime/lib.zig").Runtime;
const TaskFn = @import("../runtime/task.zig").TaskFn;

const AcceptResult = @import("../aio/completion.zig").AcceptResult;
const ConnectResult = @import("../aio/completion.zig").ConnectResult;
const RecvResult = @import("../aio/completion.zig").RecvResult;
const SendResult = @import("../aio/completion.zig").SendResult;

pub const Net = struct {
    pub fn accept(
        self: *Net,
        task_ctx: anytype,
        comptime task_fn: TaskFn(AcceptResult, @TypeOf(task_ctx)),
        socket: std.posix.socket_t,
    ) !void {
        const rt: *Runtime = @alignCast(@fieldParentPtr("net", self));
        const index = try rt.scheduler.spawn(AcceptResult, task_ctx, task_fn, .waiting);
        try rt.aio.queue_accept(index, socket);
    }

    pub fn connect(
        self: *Net,
        task_ctx: anytype,
        comptime task_fn: TaskFn(ConnectResult, @TypeOf(task_ctx)),
        socket: std.posix.socket_t,
        host: []const u8,
        port: u16,
    ) !void {
        const rt: *Runtime = @alignCast(@fieldParentPtr("net", self));
        const index = try rt.scheduler.spawn(ConnectResult, task_ctx, task_fn, .waiting);
        try rt.aio.queue_connect(index, socket, host, port);
    }

    pub fn recv(
        self: *Net,
        task_ctx: anytype,
        comptime task_fn: TaskFn(RecvResult, @TypeOf(task_ctx)),
        socket: std.posix.socket_t,
        buffer: []u8,
    ) !void {
        const rt: *Runtime = @alignCast(@fieldParentPtr("net", self));
        const index = try rt.scheduler.spawn(RecvResult, task_ctx, task_fn, .waiting);
        try rt.aio.queue_recv(index, socket, buffer);
    }

    pub fn send(
        self: *Net,
        task_ctx: anytype,
        comptime task_fn: TaskFn(SendResult, @TypeOf(task_ctx)),
        socket: std.posix.socket_t,
        buffer: []const u8,
    ) !void {
        const rt: *Runtime = @alignCast(@fieldParentPtr("net", self));
        const index = try rt.scheduler.spawn(SendResult, task_ctx, task_fn, .waiting);
        try rt.aio.queue_send(index, socket, buffer);
    }

    pub fn close(
        self: *Net,
        task_ctx: anytype,
        comptime task_fn: TaskFn(void, @TypeOf(task_ctx)),
        socket: std.posix.socket_t,
    ) !void {
        const rt: *Runtime = @alignCast(@fieldParentPtr("net", self));
        const index = try rt.scheduler.spawn(void, task_ctx, task_fn, .waiting);
        try rt.aio.queue_close(index, socket);
    }
};
