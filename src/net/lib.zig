const std = @import("std");

const Runtime = @import("../runtime/lib.zig").Runtime;
const TaskFn = @import("../runtime/task.zig").TaskFn;

pub const Net = struct {
    const AcceptParams = struct {
        socket: std.posix.socket_t,
        func: TaskFn,
        ctx: ?*anyopaque = null,
    };

    pub fn accept(self: *Net, params: AcceptParams) !void {
        const rt: *Runtime = @alignCast(@fieldParentPtr("net", self));
        const index = try rt.scheduler.spawn(
            params.func,
            params.ctx,
            .waiting,
        );
        try rt.aio.queue_accept(index, params.socket);
    }

    const RecvParams = struct {
        socket: std.posix.socket_t,
        buffer: []u8,
        func: TaskFn,
        ctx: ?*anyopaque = null,
    };

    pub fn recv(self: *Net, params: RecvParams) !void {
        const rt: *Runtime = @alignCast(@fieldParentPtr("net", self));
        const index = try rt.scheduler.spawn(
            params.func,
            params.ctx,
            .waiting,
        );
        try rt.aio.queue_recv(index, params.socket, params.buffer);
    }

    const SendParams = struct {
        socket: std.posix.socket_t,
        buffer: []const u8,
        func: TaskFn,
        ctx: ?*anyopaque = null,
    };

    pub fn send(self: *Net, params: SendParams) !void {
        const rt: *Runtime = @alignCast(@fieldParentPtr("net", self));
        const index = try rt.scheduler.spawn(
            params.func,
            params.ctx,
            .waiting,
        );
        try rt.aio.queue_send(index, params.socket, params.buffer);
    }

    const CloseParams = struct {
        socket: std.posix.socket_t,
        func: TaskFn,
        ctx: ?*anyopaque = null,
    };

    pub fn close(self: *Net, params: CloseParams) !void {
        const rt: *Runtime = @alignCast(@fieldParentPtr("net", self));
        const index = try rt.scheduler.spawn(
            params.func,
            params.ctx,
            .waiting,
        );

        try rt.aio.queue_close(index, params.socket);
    }
};
