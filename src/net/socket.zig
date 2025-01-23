const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;

const Runtime = @import("../runtime/lib.zig").Runtime;
const TaskFn = @import("../runtime/task.zig").TaskFn;

const AcceptResult = @import("../aio/completion.zig").AcceptResult;
const AcceptError = @import("../aio/completion.zig").AcceptError;
const ConnectResult = @import("../aio/completion.zig").ConnectResult;
const ConnectError = @import("../aio/completion.zig").ConnectError;
const RecvResult = @import("../aio/completion.zig").RecvResult;
const RecvError = @import("../aio/completion.zig").RecvError;
const SendResult = @import("../aio/completion.zig").SendResult;
const SendError = @import("../aio/completion.zig").SendError;

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

    const HostPort = struct {
        host: []const u8,
        port: u16,
    };

    pub const InitKind = union(Kind) {
        tcp: HostPort,
        udp: HostPort,
        unix: []const u8,
    };

    handle: std.posix.socket_t,
    addr: std.net.Address,
    kind: Kind,

    pub fn init(kind: InitKind) !Socket {
        const addr = switch (kind) {
            .tcp, .udp => |inner| blk: {
                break :blk if (comptime builtin.os.tag == .linux)
                    try std.net.Address.resolveIp(inner.host, inner.port)
                else
                    try std.net.Address.parseIp(inner.host, inner.port);
            },
            // Not supported on Windows at the moment.
            .unix => |path| if (builtin.os.tag == .windows) unreachable else try std.net.Address.initUnix(path),
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
            // Windows can't be nonblocking since connect likes to freak out.
            //sock_type | std.posix.SOCK.CLOEXEC | if (builtin.os.tag == .windows) 0 else std.posix.SOCK.NONBLOCK,
            sock_type | std.posix.SOCK.CLOEXEC,
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
    pub fn bind(self: Socket) !void {
        try std.posix.bind(self.handle, &self.addr.any, self.addr.getOsSockLen());
    }

    /// Listen on the Current Socket.
    pub fn listen(self: Socket, backlog: usize) !void {
        assert(self.kind.listenable());
        try std.posix.listen(self.handle, @truncate(backlog));
    }

    const CloseAction = struct {
        socket: Socket,

        pub fn resolve(self: *const CloseAction, rt: *Runtime) !void {
            if (rt.aio.features.has_capability(.close))
                try rt.scheduler.frame_await(.{ .close = self.socket.handle })
            else
                std.posix.close(self.socket.handle);
        }

        pub fn callback(
            self: *const CloseAction,
            rt: *Runtime,
            task_ctx: anytype,
            comptime task_fn: TaskFn(void, @TypeOf(task_ctx)),
        ) !void {
            try rt.scheduler.spawn(
                void,
                task_ctx,
                task_fn,
                .wait_for_io,
                .{ .close = self.socket.handle },
            );
        }
    };

    pub fn close(self: Socket) CloseAction {
        return .{ .socket = self };
    }

    pub fn close_blocking(self: Socket) void {
        // todo: delete the unix socket if the
        // server is being closed
        std.posix.close(self.handle);
    }

    const AcceptAction = struct {
        socket: Socket,

        pub fn resolve(self: *const AcceptAction, rt: *Runtime) !Socket {
            if (rt.aio.features.has_capability(.accept)) {
                try rt.scheduler.frame_await(.{
                    .accept = .{
                        .socket = self.socket.handle,
                        .kind = self.socket.kind,
                    },
                });

                const index = rt.current_task.?;
                const task = rt.scheduler.tasks.get(index);
                return try task.result.accept.unwrap();
            } else {
                var addr: std.net.Address = undefined;
                var addr_len = addr.getOsSockLen();

                const socket = std.posix.accept(
                    self.socket.handle,
                    &addr.any,
                    &addr_len,
                    0,
                ) catch |e| return switch (e) {
                    std.posix.AcceptError.ConnectionAborted,
                    std.posix.AcceptError.ConnectionResetByPeer,
                    => AcceptError.ConnectionAborted,
                    std.posix.AcceptError.SocketNotListening => AcceptError.NotListening,
                    std.posix.AcceptError.ProcessFdQuotaExceeded => AcceptError.ProcessFdQuotaExceeded,
                    std.posix.AcceptError.SystemFdQuotaExceeded => AcceptError.SystemFdQuotaExceeded,
                    std.posix.AcceptError.FileDescriptorNotASocket => AcceptError.NotASocket,
                    std.posix.AcceptError.OperationNotSupported => AcceptError.OperationNotSupported,
                    else => AcceptError.Unexpected,
                };

                return .{
                    .handle = socket,
                    .addr = addr,
                    .kind = self.socket.kind,
                };
            }
        }

        pub fn callback(
            self: *const AcceptAction,
            rt: *Runtime,
            task_ctx: anytype,
            comptime task_fn: TaskFn(AcceptResult, @TypeOf(task_ctx)),
        ) !void {
            try rt.scheduler.spawn(
                AcceptResult,
                task_ctx,
                task_fn,
                .wait_for_io,
                .{ .accept = .{ .socket = self.socket.*, .kind = self.kind } },
            );
        }
    };

    pub fn accept(self: Socket) AcceptAction {
        assert(self.kind.listenable());
        return .{ .socket = self };
    }

    const ConnectAction = struct {
        socket: Socket,

        pub fn resolve(self: *const ConnectAction, rt: *Runtime) !Socket {
            if (rt.aio.features.has_capability(.connect)) {
                try rt.scheduler.frame_await(.{
                    .connect = .{
                        .socket = self.socket.handle,
                        .addr = self.socket.addr,
                        .kind = self.socket.kind,
                    },
                });

                const index = rt.current_task.?;
                const task = rt.scheduler.tasks.get(index);
                return try task.result.connect.unwrap();
            } else {
                std.posix.connect(
                    self.socket.handle,
                    &self.socket.addr.any,
                    self.socket.addr.getOsSockLen(),
                ) catch |e| return switch (e) {
                    else => ConnectError.Unexpected,
                };

                return self.socket;
            }
        }

        pub fn callback(
            self: *const ConnectAction,
            rt: *Runtime,
            task_ctx: anytype,
            comptime task_fn: TaskFn(ConnectResult, @TypeOf(task_ctx)),
        ) !void {
            try rt.scheduler.spawn(
                ConnectResult,
                task_ctx,
                task_fn,
                .wait_for_io,
                .{
                    .connect = .{
                        .socket = self.socket.handle,
                        .addr = self.socket.addr,
                        .kind = self.socket.kind,
                    },
                },
            );
        }
    };

    pub fn connect(self: Socket) ConnectAction {
        return .{ .socket = self };
    }

    const RecvAction = struct {
        socket: Socket,
        buffer: []u8,

        pub fn resolve(self: *const RecvAction, rt: *Runtime) !usize {
            if (rt.aio.features.has_capability(.recv)) {
                try rt.scheduler.frame_await(.{ .recv = .{ .socket = self.socket.handle, .buffer = self.buffer } });

                const index = rt.current_task.?;
                const task = rt.scheduler.tasks.get(index);
                return try task.result.recv.unwrap();
            } else {
                const count = std.posix.recv(self.socket.handle, self.buffer, 0) catch |e| return switch (e) {
                    else => RecvError.Unexpected,
                };

                if (count == 0) return RecvError.Closed;
                return count;
            }
        }

        pub fn callback(
            self: *const RecvAction,
            rt: *Runtime,
            task_ctx: anytype,
            comptime task_fn: TaskFn(RecvResult, @TypeOf(task_ctx)),
        ) !void {
            try rt.scheduler.spawn(
                RecvResult,
                task_ctx,
                task_fn,
                .wait_for_io,
                .{ .recv = .{ .socket = self.socket.handle, .buffer = self.buffer } },
            );
        }
    };

    pub fn recv(self: Socket, buffer: []u8) RecvAction {
        return .{ .socket = self, .buffer = buffer };
    }

    const RecvAllAction = struct {
        socket: Socket,
        buffer: []u8,

        pub fn resolve(self: *const RecvAllAction, rt: *Runtime) !usize {
            var length: usize = 0;

            while (length < self.buffer.len) {
                const result = self.socket.recv(self.buffer[length..]).resolve(rt) catch |e| switch (e) {
                    error.Closed => return length,
                    else => return e,
                };

                length += result;
            }

            return length;
        }

        pub fn callback(
            self: *const RecvAllAction,
            rt: *Runtime,
            task_ctx: anytype,
            comptime task_fn: TaskFn(RecvResult, @TypeOf(task_ctx)),
        ) !void {
            const Provision = struct {
                const Self = @This();
                buffer: []u8,
                count: usize,
                socket: Socket,
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
                        p.count += length;

                        assert(p.count <= p.buffer.len);
                        if (p.count == p.buffer.len)
                            run_task = true
                        else
                            try p.socket.recv(p.buffer[p.count..]).callback(runtime, p, recv_all_task);
                    }

                    if (run_task) {
                        defer runtime.allocator.destroy(p);
                        try task_fn(runtime, .{ .actual = p.count }, p.task_ctx);
                    }
                }
            };

            const p = try rt.allocator.create(Provision);
            errdefer rt.allocator.destroy(p);
            p.* = Provision{
                .buffer = self.buffer,
                .count = 0,
                .socket = self.*,
                .task_ctx = task_ctx,
            };

            try self.socket.recv(self.buffer).callback(rt, p, Provision.recv_all_task);
        }
    };

    pub fn recv_all(self: Socket, buffer: []u8) RecvAllAction {
        return .{ .socket = self, .buffer = buffer };
    }

    const SendAction = struct {
        socket: Socket,
        buffer: []const u8,

        pub fn resolve(self: *const SendAction, rt: *Runtime) !usize {
            if (rt.aio.features.has_capability(.send)) {
                try rt.scheduler.frame_await(.{ .send = .{
                    .socket = self.socket.handle,
                    .buffer = self.buffer,
                } });

                const index = rt.current_task.?;
                const task = rt.scheduler.tasks.get(index);
                return try task.result.send.unwrap();
            } else {
                const count = std.posix.send(self.socket.handle, self.buffer, 0) catch |e| return switch (e) {
                    std.posix.SendError.ConnectionResetByPeer => SendError.ConnectionReset,
                    else => SendError.Unexpected,
                };

                return count;
            }
        }

        pub fn callback(
            self: *const SendAction,
            rt: *Runtime,
            task_ctx: anytype,
            comptime task_fn: TaskFn(SendResult, @TypeOf(task_ctx)),
        ) !void {
            try rt.scheduler.spawn(
                SendResult,
                task_ctx,
                task_fn,
                .wait_for_io,
                .{ .send = .{ .socket = self.socket.handle, .buffer = self.buffer } },
            );
        }
    };

    pub fn send(self: Socket, buffer: []const u8) SendAction {
        return .{ .socket = self, .buffer = buffer };
    }

    const SendAllAction = struct {
        socket: Socket,
        buffer: []const u8,

        pub fn resolve(self: *const SendAllAction, rt: *Runtime) !usize {
            var length: usize = 0;

            while (length < self.buffer.len) {
                const result = self.socket.send(self.buffer[length..]).resolve(rt) catch |e| switch (e) {
                    error.ConnectionReset => return length,
                    else => return e,
                };
                length += result;
            }

            return length;
        }

        pub fn callback(
            self: *const SendAllAction,
            rt: *Runtime,
            task_ctx: anytype,
            comptime task_fn: TaskFn(SendResult, @TypeOf(task_ctx)),
        ) !void {
            const Provision = struct {
                const Self = @This();
                buffer: []const u8,
                count: usize,
                socket: Socket,
                task_ctx: @TypeOf(task_ctx),

                fn send_all_task(runtime: *Runtime, res: SendResult, p: *Self) !void {
                    var run_task = false;
                    {
                        errdefer runtime.allocator.destroy(p);
                        const length = res.unwrap() catch |e| {
                            try task_fn(runtime, .{ .err = @errorCast(e) }, p.task_ctx);
                            return e;
                        };

                        p.count += length;
                        if (p.count >= p.buffer.len)
                            run_task = true
                        else
                            try p.socket.send(p.buffer[p.count..]).callback(runtime, p, send_all_task);
                    }

                    if (run_task) {
                        defer runtime.allocator.destroy(p);
                        try task_fn(runtime, .{ .actual = p.count }, p.task_ctx);
                    }
                }
            };

            const p = try rt.allocator.create(Provision);
            errdefer rt.allocator.destroy(p);
            p.* = Provision{
                .buffer = self.buffer,
                .count = 0,
                .socket = self.*,
                .task_ctx = task_ctx,
            };

            try self.socket.send(self.buffer).callback(rt, p, Provision.send_all_task);
        }
    };

    pub fn send_all(self: Socket, buffer: []const u8) SendAllAction {
        return .{ .socket = self, .buffer = buffer };
    }
};
