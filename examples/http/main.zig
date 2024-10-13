const std = @import("std");
const log = std.log.scoped(.@"tardy/example/http");

const Pool = @import("../../src/core/pool.zig").Pool;

const Runtime = @import("tardy").Runtime;
const Task = @import("tardy").Task;
const Tardy = @import("tardy").Tardy(.auto);

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
    // We need atleast three because the new tasks and the underlying task.
    return remaining > 2;
}

fn accept_task(rt: *Runtime, t: *Task, ctx: ?*anyopaque) void {
    const server_socket: *std.posix.socket_t = @ptrCast(@alignCast(ctx.?));
    const child_socket = t.result.?.socket;

    if (child_socket <= 0) {
        log.err("failed to accept socket", .{});
        rt.stop();
        return;
    }

    socket_to_nonblocking(child_socket) catch unreachable;

    log.debug("accepted socket fd={d}", .{child_socket});
    rt.net.accept(.{
        .socket = server_socket.*,
        .func = accept_task,
        .ctx = ctx,
        .predicate = accept_predicate,
    }) catch unreachable;

    // get provision
    // assign based on index
    // get buffer
    const provision_pool: *Pool(Provision) = @ptrCast(@alignCast(rt.storage.get("provision_pool").?));
    const borrowed = provision_pool.borrow_hint(@intCast(child_socket)) catch unreachable;
    borrowed.item.index = borrowed.index;
    borrowed.item.socket = child_socket;
    rt.net.recv(.{
        .socket = child_socket,
        .buffer = borrowed.item.buffer,
        .func = recv_task,
        .ctx = borrowed.item,
    }) catch unreachable;
}

fn recv_task(rt: *Runtime, t: *Task, ctx: ?*anyopaque) void {
    const provision: *Provision = @ptrCast(@alignCast(ctx.?));
    const length = t.result.?.value;

    log.debug("recv socket fd={d}", .{provision.socket});

    if (length <= 0) {
        const provision_pool: *Pool(Provision) = @ptrCast(@alignCast(rt.storage.get("provision_pool").?));
        close_connection(provision_pool, provision);
        return;
    }

    rt.net.send(.{
        .socket = provision.socket,
        .buffer = HTTP_RESPONSE[0..],
        .func = send_task,
        .ctx = ctx,
    }) catch unreachable;
}

fn send_task(rt: *Runtime, t: *Task, ctx: ?*anyopaque) void {
    const provision: *Provision = @ptrCast(@alignCast(ctx.?));
    const length = t.result.?.value;

    log.debug("send socket fd={d}", .{provision.socket});

    if (length <= 0) {
        const provision_pool: *Pool(Provision) = @ptrCast(@alignCast(rt.storage.get("provision_pool").?));
        close_connection(provision_pool, provision);
        return;
    }

    rt.net.recv(.{
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

    const thread_count = @max(@as(u16, @intCast(try std.Thread.getCpuCount() / 2 - 1)), 1);
    const conn_per_thread = try std.math.divCeil(u16, 2000, thread_count);

    var tardy = try Tardy.init(.{
        .allocator = allocator,
        .threading = .auto,
        .size_tasks_max = conn_per_thread,
        .size_aio_jobs_max = conn_per_thread,
        .size_aio_reap_max = 128,
    });
    defer tardy.deinit();

    const EntryParams = struct {
        size: u16,
        socket: *std.posix.socket_t,
    };

    try tardy.entry(
        struct {
            fn rt_start(rt: *Runtime, alloc: std.mem.Allocator, params: EntryParams) !void {
                const pool: *Pool(Provision) = try alloc.create(Pool(Provision));
                pool.* = try Pool(Provision).init(alloc, params.size, struct {
                    fn init(items: []Provision, all: anytype) void {
                        for (items) |*item| {
                            item.buffer = all.alloc(u8, 512) catch unreachable;
                        }
                    }
                }.init, alloc);

                try rt.storage.put("provision_pool", pool);
                try rt.net.accept(.{
                    .socket = params.socket.*,
                    .func = accept_task,
                    .ctx = params.socket,
                    .predicate = accept_predicate,
                });
            }
        }.rt_start,
        EntryParams{ .size = conn_per_thread, .socket = &socket },
    );
}
