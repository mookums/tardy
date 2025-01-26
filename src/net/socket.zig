const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;

const Runtime = @import("../runtime/lib.zig").Runtime;

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
            sock_type | std.posix.SOCK.CLOEXEC,
            protocol,
        );

        if (kind != .unix) {
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

    pub fn close(self: Socket, rt: *Runtime) !void {
        if (rt.aio.features.has_capability(.close))
            try rt.scheduler.io_await(.{ .close = self.handle })
        else
            std.posix.close(self.handle);
    }

    pub fn close_blocking(self: Socket) void {
        // todo: delete the unix socket if the
        // server is being closed
        std.posix.close(self.handle);
    }

    pub fn accept(self: Socket, rt: *Runtime) !Socket {
        assert(self.kind.listenable());
        if (rt.aio.features.has_capability(.accept)) {
            try rt.scheduler.io_await(.{
                .accept = .{
                    .socket = self.handle,
                    .kind = self.kind,
                },
            });

            const index = rt.current_task.?;
            const task = rt.scheduler.tasks.get(index);
            return try task.result.accept.unwrap();
        } else {
            var addr: std.net.Address = undefined;
            var addr_len = addr.getOsSockLen();

            const socket = std.posix.accept(
                self.handle,
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
                .kind = self.kind,
            };
        }
    }

    pub fn connect(self: Socket, rt: *Runtime) !Socket {
        if (rt.aio.features.has_capability(.connect)) {
            try rt.scheduler.io_await(.{
                .connect = .{
                    .socket = self.handle,
                    .addr = self.addr,
                    .kind = self.kind,
                },
            });

            const index = rt.current_task.?;
            const task = rt.scheduler.tasks.get(index);
            return try task.result.connect.unwrap();
        } else {
            std.posix.connect(
                self.handle,
                &self.addr.any,
                self.addr.getOsSockLen(),
            ) catch |e| return switch (e) {
                else => ConnectError.Unexpected,
            };

            return self;
        }
    }

    pub fn recv(self: Socket, rt: *Runtime, buffer: []u8) !usize {
        if (rt.aio.features.has_capability(.recv)) {
            try rt.scheduler.io_await(.{
                .recv = .{
                    .socket = self.handle,
                    .buffer = buffer,
                },
            });

            const index = rt.current_task.?;
            const task = rt.scheduler.tasks.get(index);
            return try task.result.recv.unwrap();
        } else {
            const count = std.posix.recv(self.handle, buffer, 0) catch |e| return switch (e) {
                else => RecvError.Unexpected,
            };

            if (count == 0) return RecvError.Closed;
            return count;
        }
    }

    pub fn recv_all(self: Socket, rt: *Runtime, buffer: []u8) !usize {
        var length: usize = 0;

        while (length < buffer.len) {
            const result = self.recv(rt, buffer[length..]) catch |e| switch (e) {
                RecvError.Closed => return length,
                else => return e,
            };

            length += result;
        }

        return length;
    }

    pub fn send(self: Socket, rt: *Runtime, buffer: []const u8) !usize {
        if (rt.aio.features.has_capability(.send)) {
            try rt.scheduler.io_await(.{
                .send = .{
                    .socket = self.handle,
                    .buffer = buffer,
                },
            });

            const index = rt.current_task.?;
            const task = rt.scheduler.tasks.get(index);
            return try task.result.send.unwrap();
        } else {
            const count = std.posix.send(self.handle, buffer, 0) catch |e| return switch (e) {
                std.posix.SendError.ConnectionResetByPeer => SendError.Closed,
                else => SendError.Unexpected,
            };

            return count;
        }
    }

    pub fn send_all(self: Socket, rt: *Runtime, buffer: []const u8) !usize {
        var length: usize = 0;

        while (length < buffer.len) {
            const result = self.send(rt, buffer[length..]) catch |e| switch (e) {
                SendError.Closed => return length,
                else => return e,
            };
            length += result;
        }

        return length;
    }
};
