const std = @import("std");
const log = std.log.scoped(.@"tardy/example/echo");

const Pool = @import("tardy").Pool;
const Runtime = @import("tardy").Runtime;
const Task = @import("tardy").Task;
const Tardy = @import("tardy").Tardy(.auto);

const Provision = struct {
    index: usize,
    socket: std.posix.socket_t,
    buffer: []u8,
};

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

fn accept_task(rt: *Runtime, t: *const Task, ctx: ?*anyopaque) !void {
    const server_socket: *std.posix.socket_t = @ptrCast(@alignCast(ctx.?));
    const child_socket = t.result.?.socket;
    try socket_to_nonblocking(child_socket);

    log.debug("{d} - accepted socket fd={d}", .{ std.time.milliTimestamp(), child_socket });
    try rt.net.accept(.{
        .socket = server_socket.*,
        .func = accept_task,
        .ctx = ctx,
    });

    // get provision
    // assign based on index
    // get buffer
    const provision_pool: *Pool(Provision) = @ptrCast(@alignCast(rt.storage.get("provision_pool").?));
    const borrowed = try provision_pool.borrow();
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

    if (length <= 0) {
        const provision_pool: *Pool(Provision) = @ptrCast(@alignCast(rt.storage.get("provision_pool").?));
        close_connection(provision_pool, provision);
        return;
    }

    try rt.net.send(.{
        .socket = provision.socket,
        .buffer = provision.buffer[0..@intCast(length)],
        .func = send_task,
        .ctx = ctx,
    });
}

fn send_task(rt: *Runtime, t: *const Task, ctx: ?*anyopaque) !void {
    const provision: *Provision = @ptrCast(@alignCast(ctx.?));
    const length = t.result.?.value;

    if (length <= 0) {
        const provision_pool: *Pool(Provision) = @ptrCast(@alignCast(rt.storage.get("provision_pool").?));
        close_connection(provision_pool, provision);
        return;
    }

    log.debug("Echoed: {s}", .{provision.buffer[0..@intCast(length)]});
    try rt.net.recv(.{
        .socket = provision.socket,
        .buffer = provision.buffer,
        .func = recv_task,
        .ctx = ctx,
    });
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();
    const size = 1024;

    var tardy = try Tardy.init(.{
        .allocator = allocator,
        .threading = .single,
    });
    defer tardy.deinit();

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

    try tardy.entry(
        struct {
            fn rt_start(rt: *Runtime, alloc: std.mem.Allocator, t_socket: *std.posix.socket_t) !void {
                const pool: *Pool(Provision) = try alloc.create(Pool(Provision));
                pool.* = try Pool(Provision).init(alloc, size, struct {
                    fn init(items: []Provision, all: anytype) void {
                        for (items) |*item| {
                            item.buffer = all.alloc(u8, size) catch unreachable;
                        }
                    }
                }.init, alloc);

                try rt.storage.put("provision_pool", pool);
                try rt.net.accept(.{
                    .socket = t_socket.*,
                    .func = accept_task,
                    .ctx = t_socket,
                });
            }
        }.rt_start,
        &socket,
        struct {
            fn rt_end(rt: *Runtime, alloc: std.mem.Allocator, _: anytype) void {
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
