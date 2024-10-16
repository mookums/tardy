const std = @import("std");

const Runtime = @import("../runtime/lib.zig").Runtime;
const TaskFn = @import("../runtime/task.zig").TaskFn;

pub const Filesystem = struct {
    const OpenParams = struct {
        path: []const u8,
        func: TaskFn,
        ctx: ?*anyopaque = null,
    };

    pub fn open(self: *Filesystem, params: OpenParams) !void {
        const rt: *Runtime = @alignCast(@fieldParentPtr("fs", self));
        const index = try rt.scheduler.spawn(
            params.func,
            params.ctx,
            .waiting,
        );

        try rt.aio.queue_open(index, params.path);
    }

    const ReadParams = struct {
        fd: std.posix.fd_t,
        buffer: []u8,
        offset: usize,
        func: TaskFn,
        ctx: ?*anyopaque = null,
    };

    pub fn read(self: *Filesystem, params: ReadParams) !void {
        const rt: *Runtime = @alignCast(@fieldParentPtr("fs", self));
        const index = try rt.scheduler.spawn(
            params.func,
            params.ctx,
            .waiting,
        );

        try rt.aio.queue_read(index, params.fd, params.buffer, params.offset);
    }

    const WriteParams = struct {
        fd: std.posix.fd_t,
        buffer: []const u8,
        offset: usize,
        func: TaskFn,
        ctx: ?*anyopaque = null,
    };

    pub fn write(self: *Filesystem, params: WriteParams) !void {
        const rt: *Runtime = @alignCast(@fieldParentPtr("fs", self));
        const index = try rt.scheduler.spawn(
            params.func,
            params.ctx,
            .waiting,
        );

        try rt.aio.queue_write(index, params.fd, params.buffer, params.offset);
    }

    const CloseParams = struct {
        fd: std.posix.fd_t,
        func: TaskFn,
        ctx: ?*anyopaque = null,
    };

    pub fn close(self: *Filesystem, params: CloseParams) !void {
        const rt: *Runtime = @alignCast(@fieldParentPtr("fs", self));
        const index = try rt.scheduler.spawn(
            params.func,
            params.ctx,
            .waiting,
        );

        try rt.aio.queue_close(index, params.fd);
    }
};
