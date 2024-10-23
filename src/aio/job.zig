const std = @import("std");
const Timespec = @import("timespec.zig").Timespec;

pub const Job = struct {
    type: union(enum) {
        timer: union(enum) { none, fd: std.posix.fd_t, ns: i128 },
        open: [:0]const u8,
        stat: std.posix.fd_t,
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
