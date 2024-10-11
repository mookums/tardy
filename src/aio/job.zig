const std = @import("std");

pub const Job = struct {
    type: union(enum) {
        open,
        accept,
        connect: std.posix.sockaddr,
        recv: []u8,
        read: []u8,
        send: []const u8,
        write: []const u8,
        close,
    },

    index: usize = 0,
    fd: std.posix.fd_t,
    task: usize,
};
