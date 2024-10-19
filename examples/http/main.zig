const std = @import("std");
const log = std.log.scoped(.@"tardy/example/http");

const Pool = @import("tardy").Pool;
const Runtime = @import("tardy").Runtime;
const Task = @import("tardy").Task;
const Tardy = @import("tardy").Tardy(.auto);

const Provision = struct {
    index: usize,
    socket: std.posix.socket_t,
    buffer: []u8,
};

const HTTP_RESPONSE = "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 27\r\nContent-Type: text/plain\r\n\r\nThis is an HTTP benchmark\r\n";

fn create_socket(addr: std.net.Address) !std.posix.socket_t {
    const socket: std.posix.socket_t = blk: {
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

    return socket;
}

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

fn accept_task(rt: *Runtime, t: *const Task, _: ?*anyopaque) !void {
    const child_socket = t.result.?.socket;

    if (child_socket <= 0) {
        log.err("failed to accept socket", .{});
        rt.stop();
        return;
    }

    try socket_to_nonblocking(child_socket);
    log.debug("accepted socket fd={d}", .{child_socket});

    const provision_pool: *Pool(Provision) = @ptrCast(@alignCast(rt.storage.get("provision_pool").?));
    const borrowed = try provision_pool.borrow_hint(@intCast(child_socket));
    borrowed.item.index = borrowed.index;
    borrowed.item.socket = child_socket;
    try rt.net.recv(.{
        .socket = child_socket,
        .buffer = borrowed.item.buffer,
        .func = recv_task,
        .ctx = borrowed.item,
    });
}

fn recv_task(rt: *Runtime, t: *const Task, ctx: ?*anyopaque) !void {
    const provision: *Provision = @ptrCast(@alignCast(ctx.?));
    const length = t.result.?.value;

    log.debug("recv socket fd={d}", .{provision.socket});

    if (length <= 0) {
        log.debug("recv closed fd={d}", .{provision.socket});
        log.debug("queueing close with ctx ptr: {*}", .{ctx});
        try rt.net.close(.{
            .fd = provision.socket,
            .func = close_task,
            .ctx = ctx,
        });

        return;
    }

    try rt.net.send(.{
        .socket = provision.socket,
        .buffer = HTTP_RESPONSE[0..],
        .func = send_task,
        .ctx = ctx,
    });
}

fn send_task(rt: *Runtime, t: *const Task, ctx: ?*anyopaque) !void {
    const provision: *Provision = @ptrCast(@alignCast(ctx.?));
    const length = t.result.?.value;

    log.debug("send socket fd={d}", .{provision.socket});

    if (length <= 0) {
        log.debug("send closed fd={d}", .{provision.socket});
        log.debug("queueing close with ctx ptr: {*}", .{ctx});
        try rt.net.close(.{
            .fd = provision.socket,
            .func = close_task,
            .ctx = ctx,
        });

        return;
    }

    try rt.net.recv(.{
        .socket = provision.socket,
        .buffer = provision.buffer,
        .func = recv_task,
        .ctx = ctx,
    });
}

fn close_task(rt: *Runtime, _: *const Task, ctx: ?*anyopaque) !void {
    const provision: *Provision = @ptrCast(@alignCast(ctx.?));
    const provision_pool: *Pool(Provision) = @ptrCast(@alignCast(rt.storage.get("provision_pool").?));

    log.debug("close socket fd={d}", .{provision.socket});
    provision_pool.release(provision.index);
    const server_socket: *std.posix.socket_t = @ptrCast(@alignCast(rt.storage.get("server_socket").?));

    // requeue accept
    try rt.net.accept(.{
        .socket = server_socket.*,
        .func = accept_task,
    });
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .thread_safe = true }){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    const host = "0.0.0.0";
    const port = 9862;

    const thread_count = @max(@as(u16, @intCast(try std.Thread.getCpuCount() / 2 - 1)), 1);
    const conn_per_thread = try std.math.divCeil(u16, 2000, thread_count);

    var tardy = try Tardy.init(.{
        .allocator = allocator,
        .threading = .{ .multi = thread_count },
        .size_tasks_max = conn_per_thread,
        .size_aio_jobs_max = conn_per_thread,
        .size_aio_reap_max = conn_per_thread,
    });
    defer tardy.deinit();

    try tardy.entry(
        struct {
            fn rt_start(rt: *Runtime, alloc: std.mem.Allocator, size: u16) !void {
                // socket per thread.
                const addr = try std.net.Address.resolveIp(host, port);
                const socket = try alloc.create(std.posix.socket_t);
                socket.* = try create_socket(addr);
                try socket_to_nonblocking(socket.*);
                try std.posix.bind(socket.*, &addr.any, addr.getOsSockLen());
                try std.posix.listen(socket.*, size);

                const pool: *Pool(Provision) = try alloc.create(Pool(Provision));
                pool.* = try Pool(Provision).init(alloc, size, struct {
                    fn init(items: []Provision, all: anytype) void {
                        for (items) |*item| {
                            item.buffer = all.alloc(u8, 512) catch unreachable;
                        }
                    }
                }.init, alloc);

                try rt.storage.put("provision_pool", pool);
                try rt.storage.put("server_socket", socket);

                for (0..size) |_| {
                    try rt.net.accept(.{
                        .socket = socket.*,
                        .func = accept_task,
                    });
                }
            }
        }.rt_start,
        conn_per_thread,
        struct {
            fn rt_end(rt: *Runtime, alloc: std.mem.Allocator, _: anytype) void {
                const server_socket: *std.posix.socket_t = @ptrCast(@alignCast(rt.storage.get("server_socket").?));
                alloc.destroy(server_socket);

                const provision_pool: *Pool(Provision) = @ptrCast(@alignCast(rt.storage.get("provision_pool").?));
                provision_pool.deinit(struct {
                    fn pool_deinit(items: []Provision, a: anytype) void {
                        for (items) |item| {
                            a.free(item.buffer);
                        }
                    }
                }.pool_deinit, alloc);
                alloc.destroy(provision_pool);
            }
        }.rt_end,
        void,
    );
}
