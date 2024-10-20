const std = @import("std");

pub const Job = struct {
    type: union(enum) {
        open: [:0]const u8,
        read: struct { fd: std.posix.fd_t, buffer: []u8, offset: usize },
        write: struct { fd: std.posix.fd_t, buffer: []const u8, offset: usize },
        close: std.posix.fd_t,
        accept: std.posix.socket_t,
        connect: struct { socket: std.posix.socket_t, addr: std.posix.sockaddr },
        send: struct { socket: std.posix.socket_t, buffer: []const u8 },
        recv: struct { socket: std.posix.socket_t, buffer: []u8 },
    },

    index: usize = 0,
    task: usize,
};
