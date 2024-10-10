const std = @import("std");

pub const Job = struct {
    type: union(enum) {
        accept,
        recv: []u8,
        send: []const u8,
        close,
    },

    index: usize = 0,
    socket: std.posix.socket_t,
    task: usize,
};
