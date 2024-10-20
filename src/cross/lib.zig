const std = @import("std");
pub const fd = @import("fd.zig");
pub const socket = @import("socket.zig");

/// Get the `fd_t` for `stdout`.
pub fn get_std_out() !std.posix.fd_t {
    return std.io.getStdOut().handle;
}
