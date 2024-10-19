const std = @import("std");

pub const Job = struct {
    type: union(enum) {
        open: [:0]const u8,
        accept,
        connect: std.posix.sockaddr,
        recv: []u8,
        read: struct { buffer: []u8, offset: usize },
        send: []const u8,
        write: struct { buffer: []const u8, offset: usize },
        close,
    },

    index: usize = 0,
    fd: std.posix.fd_t,
    task: usize,
};
