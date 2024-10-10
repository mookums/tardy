const std = @import("std");
const assert = std.debug.assert;
const tardy = @import("tardy");
const log = std.log.scoped(.@"tardy/example/http");
const Pool = @import("../../src/core/pool.zig").Pool;

const Runtime = tardy.Runtime(.auto);
const Task = Runtime.RuntimeTask;

const Provision = struct {
    index: usize,
    socket: std.posix.socket_t,
    buffer: []u8,
};

const HTTP_RESPONSE = "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 27\r\nContent-Type: text/plain\r\n\r\nThis is an HTTP benchmark\r\n";

fn socket_to_nonblocking(socket: std.posix.socket_t) !void {
    const current_flags = try std.posix.fcntl(socket, std.posix.F.GETFL, 0);
    var new_flags = @as(
        std.posix.O,
        @bitCast(@as(u32, @intCast(current_flags))),
    );
    new_flags.NONBLOCK = true;
    const arg: u32 = @bitCast(new_flags);
    _ = try std.posix.fcntl(socket, std.posix.F.SETFL, arg);
}

fn close_connection(provision_pool: *Pool(Provision), provision: *const Provision) void {
    log.debug("closed connection fd={d}", .{provision.socket});
    std.posix.close(provision.socket);
    provision_pool.release(provision.index);
}

fn accept_task(rt: *Runtime, t: *Task, ctx: ?*anyopaque) void {
    const server_socket: *std.posix.socket_t = @ptrCast(@alignCast(ctx.?));
    const child_socket = t.result.?.socket;
    socket_to_nonblocking(child_socket) catch unreachable;

    log.debug("{d} - accepted socket fd={d}", .{ std.time.milliTimestamp(), child_socket });
    rt.accept(server_socket.*, accept_task, ctx) catch unreachable;

    // get provision
    // assign based on index
    // get buffer
    const provision_pool: *Pool(Provision) = @ptrCast(@alignCast(rt.storage.get("provision_pool").?));
    const borrowed = provision_pool.borrow() catch unreachable;
    borrowed.item.index = borrowed.index;
    borrowed.item.socket = child_socket;
    rt.recv(child_socket, borrowed.item.buffer, recv_task, borrowed.item) catch unreachable;
}

fn recv_task(rt: *Runtime, t: *Task, ctx: ?*anyopaque) void {
    const provision: *Provision = @ptrCast(@alignCast(ctx.?));
    const length = t.result.?.value;

    log.debug("{d} - recv socket fd={d}", .{ std.time.milliTimestamp(), provision.socket });

    if (length <= 0) {
        const provision_pool: *Pool(Provision) = @ptrCast(@alignCast(rt.storage.get("provision_pool").?));
        close_connection(provision_pool, provision);
        return;
    }

    rt.send(provision.socket, HTTP_RESPONSE[0..], send_task, ctx) catch unreachable;
}

fn send_task(rt: *Runtime, t: *Task, ctx: ?*anyopaque) void {
    const provision: *Provision = @ptrCast(@alignCast(ctx.?));
    const length = t.result.?.value;

    log.debug("{d} - send socket fd={d}", .{ std.time.milliTimestamp(), provision.socket });

    if (length <= 0) {
        const provision_pool: *Pool(Provision) = @ptrCast(@alignCast(rt.storage.get("provision_pool").?));
        close_connection(provision_pool, provision);
        return;
    }

    rt.recv(provision.socket, provision.buffer, recv_task, ctx) catch unreachable;
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .thread_safe = true }){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    const size = 1024;

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

    try socket_to_nonblocking(socket);
    try std.posix.bind(socket, &addr.any, addr.getOsSockLen());
    try std.posix.listen(socket, size);

    var runtime = try Runtime.init(allocator, size);
    defer runtime.deinit();

    var threads = std.ArrayList(std.Thread).init(allocator);
    const thread_count = (try std.Thread.getCpuCount() / 2) - 2;

    for (0..thread_count - 1) |_| {
        const handle = try std.Thread.spawn(.{ .allocator = allocator }, struct {
            fn thread_init(t_allocator: std.mem.Allocator, t_socket: *std.posix.socket_t) void {
                var thread_rt = Runtime.init(t_allocator, size) catch return;
                defer thread_rt.deinit();

                var thread_pool: Pool(Provision) = Pool(Provision).init(t_allocator, size, struct {
                    fn init(items: []Provision, all: anytype) void {
                        for (items) |*item| {
                            item.buffer = all.alloc(u8, size) catch return;
                        }
                    }
                }.init, t_allocator) catch unreachable;

                thread_rt.storage.put("provision_pool", &thread_pool) catch return;

                thread_rt.accept(t_socket.*, accept_task, t_socket) catch return;
                thread_rt.run() catch return;
            }
        }.thread_init, .{
            allocator,
            &socket,
        });

        try threads.append(handle);
    }

    errdefer for (threads.items) |thread| thread.join();

    var pool: Pool(Provision) = try Pool(Provision).init(allocator, size, struct {
        fn init(items: []Provision, all: anytype) void {
            for (items) |*item| {
                item.buffer = all.alloc(u8, size) catch unreachable;
            }
        }
    }.init, allocator);

    try runtime.storage.put("provision_pool", &pool);
    try runtime.accept(socket, accept_task, &socket);
    try runtime.run();
}
