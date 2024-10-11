const std = @import("std");
const assert = std.debug.assert;
const tardy = @import("tardy");
const log = std.log.scoped(.@"tardy/example/http");
const Pool = @import("../../src/core/pool.zig").Pool;

const Runtime = tardy.Runtime(.auto);
const Task = Runtime.Task;

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

fn accept_predicate(rt: *Runtime, _: *Task) bool {
    const provision_pool: *Pool(Provision) = @ptrCast(@alignCast(rt.storage.get("provision_pool").?));
    const remaining = provision_pool.items.len - provision_pool.dirty.count();
    // We need atleast two because we requeue the accept and the recv.
    return remaining >= 2;
}

fn accept_task(rt: *Runtime, t: *Task, ctx: ?*anyopaque) void {
    const server_socket: *std.posix.socket_t = @ptrCast(@alignCast(ctx.?));
    const child_socket = t.result.?.socket;
    socket_to_nonblocking(child_socket) catch unreachable;

    log.debug("{d} - accepted socket fd={d}", .{ std.time.milliTimestamp(), child_socket });
    rt.accept(.{
        .socket = server_socket.*,
        .func = accept_task,
        .ctx = ctx,
        .predicate = accept_predicate,
    }) catch unreachable;

    // get provision
    // assign based on index
    // get buffer
    const provision_pool: *Pool(Provision) = @ptrCast(@alignCast(rt.storage.get("provision_pool").?));
    const borrowed = provision_pool.borrow() catch unreachable;
    borrowed.item.index = borrowed.index;
    borrowed.item.socket = child_socket;
    rt.recv(.{
        .socket = child_socket,
        .buffer = borrowed.item.buffer,
        .func = recv_task,
        .ctx = borrowed.item,
    }) catch unreachable;
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

    rt.send(.{
        .socket = provision.socket,
        .buffer = HTTP_RESPONSE[0..],
        .func = send_task,
        .ctx = ctx,
    }) catch unreachable;
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

    rt.recv(.{
        .socket = provision.socket,
        .buffer = provision.buffer,
        .func = recv_task,
        .ctx = ctx,
    }) catch unreachable;
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .thread_safe = true }){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    const thread_count = (try std.Thread.getCpuCount() / 2) - 2;
    const conn_per_thread: usize = try std.math.divCeil(usize, 2000, thread_count);

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
    try std.posix.listen(socket, 1024);

    var runtime = try Runtime.init(.{
        .allocator = allocator,
        .size_tasks_max = @intCast(conn_per_thread + 2),
        .size_aio_jobs_max = @intCast(conn_per_thread),
        .size_aio_reap_max = 128,
    });
    defer runtime.deinit();

    var threads = std.ArrayList(std.Thread).init(allocator);
    defer threads.deinit();

    for (0..thread_count - 1) |_| {
        const handle = try std.Thread.spawn(.{ .allocator = allocator }, struct {
            fn thread_init(
                t_runtime: *const Runtime,
                t_allocator: std.mem.Allocator,
                t_socket: *std.posix.socket_t,
                t_conn_per: usize,
            ) void {
                var thread_rt = Runtime.init(.{
                    .allocator = t_allocator,
                    .parent_async = &t_runtime.aio,
                    .size_tasks_max = @intCast(t_conn_per + 2),
                    .size_aio_jobs_max = @intCast(t_conn_per),
                    .size_aio_reap_max = 128,
                }) catch return;
                defer thread_rt.deinit();

                var thread_pool: Pool(Provision) = Pool(Provision).init(t_allocator, t_conn_per, struct {
                    fn init(items: []Provision, ctx: anytype) void {
                        for (items) |*item| {
                            item.buffer = ctx.allocator.alloc(u8, 512) catch return;
                        }
                    }
                }.init, .{ .allocator = t_allocator }) catch unreachable;

                thread_rt.storage.put("provision_pool", &thread_pool) catch return;

                thread_rt.accept(.{
                    .socket = t_socket.*,
                    .func = accept_task,
                    .ctx = t_socket,
                    .predicate = accept_predicate,
                }) catch return;
                thread_rt.run() catch return;
            }
        }.thread_init, .{
            &runtime,
            allocator,
            &socket,
            conn_per_thread,
        });

        try threads.append(handle);
    }
    errdefer for (threads.items) |thread| thread.join();

    var pool: Pool(Provision) = try Pool(Provision).init(allocator, conn_per_thread, struct {
        fn init(items: []Provision, ctx: anytype) void {
            for (items) |*item| {
                item.buffer = ctx.allocator.alloc(u8, 512) catch return;
            }
        }
    }.init, .{ .allocator = allocator });

    try runtime.storage.put("provision_pool", &pool);
    try runtime.accept(.{
        .socket = socket,
        .func = accept_task,
        .ctx = &socket,
        .predicate = accept_predicate,
    });
    try runtime.run();
}
