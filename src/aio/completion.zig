const std = @import("std");
const Timespec = @import("timespec.zig").Timespec;

// This interface is missing a lot of the stuff you get from `stat()` normally.
// This is minimally what I need.
pub const Stat = struct {
    size: u64,
    mode: u64 = 0,
    accessed: ?Timespec = null,
    modified: ?Timespec = null,
    changed: ?Timespec = null,
};

pub const Result = union(enum) {
    none,
    /// If we have returned a stat object.
    stat: Stat,
    /// If we have returned a socket.
    socket: std.posix.socket_t,
    /// If we have a file descriptor.
    fd: std.posix.fd_t,
    /// If we have returned a value.
    value: i32,
    /// If we have returned a ptr.
    ptr: *anyopaque,
};

pub const Completion = struct {
    task: usize,
    result: Result,
};
