const std = @import("std");

pub const Timestamp = struct {
    seconds: u64,
    nanos: u64,
};

// This interface is missing a lot of the stuff you get from `stat()` normally.
// This is minimally what I need.
pub const Stat = struct {
    size: u64,
    mode: u64 = 0,
    accessed: ?Timestamp = null,
    modified: ?Timestamp = null,
    changed: ?Timestamp = null,
};

pub const Result = union(enum) {
    /// If we have returned a stat object.
    stat: Stat,
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
