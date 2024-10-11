const std = @import("std");

pub const Result = union(enum) {
    /// If we have returned a socket.
    socket: std.posix.socket_t,
    /// If we have a file descriptor.
    fd: std.posix.fd_t,
    /// If we have returned a value.
    value: i32,
};

pub const Completion = struct {
    task: usize,
    result: Result,
};
