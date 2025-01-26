const std = @import("std");
const builtin = @import("builtin");
const os = builtin.os.tag;

const Timespec = @import("../lib.zig").Timespec;

/// Invalid `socket_t`.
pub const INVALID_SOCKET = if (os == .windows) std.os.windows.ws2_32.INVALID_SOCKET else -1;

/// Ensures that the `std.posix.socket_t` is valid.
pub fn is_valid(socket: std.posix.socket_t) bool {
    switch (comptime os) {
        .windows => return socket != std.os.windows.ws2_32.INVALID_SOCKET,
        else => return socket >= 0,
    }
}

/// Sets the `std.posix.socket_t` to nonblocking.
pub fn to_nonblock(socket: std.posix.socket_t) !void {
    if (comptime os == .windows) {
        var mode: u32 = 1;
        _ = std.os.windows.ws2_32.ioctlsocket(
            socket,
            std.os.windows.ws2_32.FIONBIO,
            &mode,
        );
    } else {
        const current_flags = try std.posix.fcntl(socket, std.posix.F.GETFL, 0);
        var new_flags = @as(
            std.posix.O,
            @bitCast(@as(u32, @intCast(current_flags))),
        );
        new_flags.NONBLOCK = true;
        const arg: u32 = @bitCast(new_flags);
        _ = try std.posix.fcntl(socket, std.posix.F.SETFL, arg);
    }
}

pub fn disable_nagle(socket: std.posix.socket_t) !void {
    if (comptime os.isBSD()) {
        // system.TCP is weird on MacOS.
        try std.posix.setsockopt(
            socket,
            std.posix.IPPROTO.TCP,
            1,
            &std.mem.toBytes(@as(c_int, 1)),
        );
    } else {
        try std.posix.setsockopt(
            socket,
            std.posix.IPPROTO.TCP,
            std.posix.TCP.NODELAY,
            &std.mem.toBytes(@as(c_int, 1)),
        );
    }
}
