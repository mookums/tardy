const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;

const Runtime = @import("../runtime/lib.zig").Runtime;
const TaskFn = @import("../runtime/task.zig").TaskFn;

const AcceptResult = @import("../aio/completion.zig").AcceptResult;
const ConnectResult = @import("../aio/completion.zig").ConnectResult;
const RecvResult = @import("../aio/completion.zig").RecvResult;
const SendResult = @import("../aio/completion.zig").SendResult;

pub const Socket = struct {
    pub const Kind = enum {
        tcp,
        udp,
        unix,

        pub fn listenable(self: Kind) bool {
            return switch (self) {
                .tcp, .unix => true,
                else => false,
            };
        }
    };

    handle: std.posix.socket_t,
    addr: std.net.Address,
    kind: Kind,

    pub fn init(kind: Kind, host: []const u8, port: ?u16) !Socket {
        const addr = switch (kind) {
            .tcp, .udp => blk: {
                assert(port != null);
                break :blk if (comptime builtin.os.tag == .linux)
                    try std.net.Address.resolveIp(host, port.?)
                else
                    try std.net.Address.parseIp(host, port.?);
            },
            .unix => try std.net.Address.initUnix(host),
        };

        const sock_type: u32 = switch (kind) {
            .tcp, .unix => std.posix.SOCK.STREAM,
            .udp => std.posix.SOCK.DGRAM,
        };

        const protocol: u32 = switch (kind) {
            .tcp => std.posix.IPPROTO.TCP,
            .udp => std.posix.IPPROTO.UDP,
            .unix => 0,
        };

        const socket = try std.posix.socket(
            addr.any.family,
            sock_type | std.posix.SOCK.CLOEXEC | std.posix.SOCK.NONBLOCK,
            protocol,
        );

        if (@hasDecl(std.posix.SO, "REUSEPORT_LB")) {
            try std.posix.setsockopt(
                socket,
                std.posix.SOL.SOCKET,
                std.posix.SO.REUSEPORT_LB,
                &std.mem.toBytes(@as(c_int, 1)),
            );
        } else if (@hasDecl(std.posix.SO, "REUSEPORT")) {
            try std.posix.setsockopt(
                socket,
                std.posix.SOL.SOCKET,
                std.posix.SO.REUSEPORT,
                &std.mem.toBytes(@as(c_int, 1)),
            );
        } else {
            try std.posix.setsockopt(
                socket,
                std.posix.SOL.SOCKET,
                std.posix.SO.REUSEADDR,
                &std.mem.toBytes(@as(c_int, 1)),
            );
        }

        return .{ .handle = socket, .addr = addr, .kind = kind };
    }

    /// Bind the current Socket
    pub fn bind(self: *const Socket) !void {
        try std.posix.bind(self.handle, &self.addr.any, self.addr.getOsSockLen());
    }

    /// Listen on the Current Socket.
    pub fn listen(self: *const Socket, backlog: usize) !void {
        assert(self.kind.listenable());
        try std.posix.listen(self.handle, @truncate(backlog));
    }

    pub fn close(
        self: *const Socket,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(void, @TypeOf(task_ctx)),
    ) !void {
        try rt.scheduler.spawn(
            void,
            task_ctx,
            task_fn,
            .wait_for_io,
            .{ .close = self.handle },
        );
    }

    pub fn close_blocking(self: *const Socket) void {
        // todo: delete the unix socket if the
        // server is being closed
        std.posix.close(self.handle);
    }

    pub fn accept(
        self: *const Socket,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(AcceptResult, @TypeOf(task_ctx)),
    ) !void {
        assert(self.kind.listenable());

        try rt.scheduler.spawn(
            AcceptResult,
            task_ctx,
            task_fn,
            .wait_for_io,
            .{ .accept = .{ .socket = self.handle, .kind = self.kind } },
        );
    }

    pub fn connect(
        self: *const Socket,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(ConnectResult, @TypeOf(task_ctx)),
    ) !void {
        try rt.scheduler.spawn(
            ConnectResult,
            task_ctx,
            task_fn,
            .wait_for_io,
            .{ .connect = .{ .socket = self.handle, .addr = self.addr, .kind = self.kind } },
        );
    }

    pub fn recv(
        self: *const Socket,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(RecvResult, @TypeOf(task_ctx)),
        buffer: []u8,
    ) !void {
        try rt.scheduler.spawn(
            RecvResult,
            task_ctx,
            task_fn,
            .wait_for_io,
            .{ .recv = .{ .socket = self.handle, .buffer = buffer } },
        );
    }

    pub fn recv_all(
        self: *const Socket,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(RecvResult, @TypeOf(task_ctx)),
        buffer: []u8,
    ) !void {
        const Provision = struct {
            const Self = @This();
            buffer: []u8,
            recv: usize,
            socket: *const Socket,
            task_ctx: @TypeOf(task_ctx),

            fn recv_all_task(runtime: *Runtime, res: RecvResult, p: *Self) !void {
                var run_task = false;

                scope: {
                    errdefer runtime.allocator.destroy(p);
                    const length = res.unwrap() catch |e| {
                        switch (e) {
                            error.Closed => {
                                run_task = true;
                                break :scope;
                            },
                            else => {
                                try task_fn(runtime, .{ .err = @errorCast(e) }, p.task_ctx);
                                return;
                            },
                        }
                    };
                    p.recv += length;

                    assert(p.recv <= p.buffer.len);
                    if (p.recv == p.buffer.len)
                        run_task = true
                    else
                        try p.socket.recv(runtime, p, recv_all_task, p.buffer[p.recv..]);
                }

                if (run_task) {
                    defer runtime.allocator.destroy(p);
                    try task_fn(runtime, .{ .actual = p.recv }, p.task_ctx);
                }
            }
        };

        const p = try rt.allocator.create(Provision);
        errdefer rt.allocator.destroy(p);
        p.* = Provision{
            .buffer = buffer,
            .recv = 0,
            .socket = self,
            .task_ctx = task_ctx,
        };

        try self.recv(rt, p, Provision.recv_all_task, buffer);
    }

    pub fn send(
        self: *const Socket,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(SendResult, @TypeOf(task_ctx)),
        buffer: []const u8,
    ) !void {
        try rt.scheduler.spawn(
            SendResult,
            task_ctx,
            task_fn,
            .wait_for_io,
            .{ .send = .{ .socket = self.handle, .buffer = buffer } },
        );
    }

    pub fn send_all(
        self: *const Socket,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(SendResult, @TypeOf(task_ctx)),
        buffer: []const u8,
    ) !void {
        const Provision = struct {
            const Self = @This();
            buffer: []const u8,
            send: usize,
            socket: *const Socket,
            task_ctx: @TypeOf(task_ctx),

            fn send_all_task(runtime: *Runtime, res: SendResult, p: *Self) !void {
                var run_task = false;
                {
                    errdefer runtime.allocator.destroy(p);
                    const length = res.unwrap() catch |e| {
                        try task_fn(runtime, .{ .err = @errorCast(e) }, p.task_ctx);
                        return e;
                    };

                    p.send += length;
                    if (p.send >= p.buffer.len)
                        run_task = true
                    else
                        try p.socket.send(runtime, p, send_all_task, p.buffer[p.send..]);
                }

                if (run_task) {
                    defer runtime.allocator.destroy(p);
                    try task_fn(runtime, .{ .actual = {} }, p.task_ctx);
                }
            }
        };

        const p = try rt.allocator.create(Provision);
        errdefer rt.allocator.destroy(p);
        p.* = Provision{
            .buffer = buffer,
            .send = 0,
            .socket = self,
            .task_ctx = task_ctx,
        };

        try self.send(rt, p, Provision.send_all_task, buffer);
    }
};
