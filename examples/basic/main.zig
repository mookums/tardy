const std = @import("std");
const assert = std.debug.assert;
const tardy = @import("tardy");
const log = std.log.scoped(.@"tardy/example/basic");

const Runtime = tardy.Runtime(.auto);
const Task = Runtime.RuntimeTask;

fn log_task(rt: *Runtime, t: *Task, ctx: ?*anyopaque) void {
    _ = ctx;
    _ = t;
    log.debug("{d} - tardy example", .{std.time.milliTimestamp()});
    std.time.sleep(1 * std.time.ns_per_s);
    rt.spawn(log_task, null) catch unreachable;
}

fn accept_task(rt: *Runtime, t: *Task, ctx: ?*anyopaque) void {
    const server_socket: *std.posix.socket_t = @ptrCast(@alignCast(ctx.?));
    const child_socket = t.result.?.socket;
    defer std.posix.close(child_socket);

    log.debug("{d} - accepted socket fd={d}", .{ std.time.milliTimestamp(), child_socket });
    rt.accept(server_socket.*, accept_task, ctx) catch unreachable;
}

pub fn main() !void {
    const allocator = std.heap.page_allocator;
    var runtime = try Runtime.init(allocator, 1024);
    defer runtime.deinit();

    const host = "0.0.0.0";
    const port = 9862;

    const addr = try std.net.Address.resolveIp(host, port);

    var socket: std.posix.socket_t = blk: {
        const socket_flags = std.posix.SOCK.STREAM | std.posix.SOCK.CLOEXEC | std.posix.SOCK.NONBLOCK;
        break :blk try std.posix.socket(
            addr.any.family,
            socket_flags,
            std.posix.IPPROTO.TCP,
        );
    };

    if (@hasDecl(std.posix.SO, "REUSEPORT_LB")) {
        try std.posix.setsockopt(
            socket,
            std.posix.SOL.SOCKET,
            std.posix.SO.REUSEPORT_LB,
            &std.mem.toBytes(@as(c_int, 1)),
        );
    } else if (@hasDecl(std.posix.SO, "REUSEPORT")) {
        try std.posix.setsockopt(
            socket,
            std.posix.SOL.SOCKET,
            std.posix.SO.REUSEPORT,
            &std.mem.toBytes(@as(c_int, 1)),
        );
    } else {
        try std.posix.setsockopt(
            socket,
            std.posix.SOL.SOCKET,
            std.posix.SO.REUSEADDR,
            &std.mem.toBytes(@as(c_int, 1)),
        );
    }

    try std.posix.bind(socket, &addr.any, addr.getOsSockLen());
    try std.posix.listen(socket, 1024);

    try runtime.spawn(log_task, null);
    try runtime.accept(socket, accept_task, &socket);
    try runtime.run();
}
