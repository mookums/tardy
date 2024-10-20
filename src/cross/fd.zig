const std = @import("std");
const builtin = @import("builtin");
const os = builtin.os.tag;

/// Invalid `fd_t`.
pub const INVALID_FD = if (os == .windows) std.os.windows.INVALID_HANDLE_VALUE else -1;

/// Ensures that the `std.posix.fd_t` is valid.
pub fn is_valid(fd: std.posix.fd_t) bool {
    switch (comptime os) {
        .windows => return fd != std.os.windows.INVALID_HANDLE_VALUE,
        else => return fd >= 0,
    }
}
