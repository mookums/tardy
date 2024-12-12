const std = @import("std");
const log = std.log.scoped(.@"tardy/example/echo");

const Pool = @import("tardy").Pool;
const Runtime = @import("tardy").Runtime;
const Task = @import("tardy").Task;
const Tardy = @import("tardy").Tardy(.auto);
const Cross = @import("tardy").Cross;

const AcceptResult = @import("tardy").AcceptResult;
const RecvResult = @import("tardy").RecvResult;
const SendResult = @import("tardy").SendResult;

const Provision = struct {
    index: usize,
    socket: std.posix.socket_t,
    buffer: []u8,
};

fn close_connection(provision_pool: *Pool(Provision), provision: *const Provision) void {
    log.debug("closed connection fd={d}", .{provision.socket});
    std.posix.close(provision.socket);
    provision_pool.release(provision.index);
}

fn accept_task(rt: *Runtime, result: AcceptResult, socket: std.posix.socket_t) !void {
    const child_socket = result.unwrap() catch |e| {
        log.err("Failed to accept on socket | {}", .{e});
        rt.stop();
        return;
    };

    try Cross.socket.to_nonblock(child_socket);

    log.debug("{d} - accepted socket fd={d}", .{ std.time.milliTimestamp(), child_socket });
    try rt.net.accept(socket, accept_task, socket);

    // get provision
    // assign based on index
    // get buffer
    const provision_pool = rt.storage.get_ptr("provision_pool", Pool(Provision));
    const borrowed = try provision_pool.borrow();
    borrowed.item.index = borrowed.index;
    borrowed.item.socket = child_socket;

    try rt.net.recv(
        borrowed.item,
        recv_task,
        child_socket,
        borrowed.item.buffer,
    );
}

fn recv_task(rt: *Runtime, result: RecvResult, provision: *Provision) !void {
    _ = result.unwrap() catch |e| {
        log.err("Failed to recv on socket | {}", .{e});
        const provision_pool = rt.storage.get_ptr("provision_pool", Pool(Provision));
        close_connection(provision_pool, provision);
        return;
    };

    try rt.net.send(
        provision,
        send_task,
        provision.socket,
        provision.buffer,
    );
}

fn send_task(rt: *Runtime, result: SendResult, provision: *Provision) !void {
    const length = result.unwrap() catch |e| {
        log.err("Failed to send on socket | {}", .{e});
        const provision_pool = rt.storage.get_ptr("provision_pool", Pool(Provision));
        close_connection(provision_pool, provision);
        return;
    };

    log.debug("Echoed: {s}", .{provision.buffer[0..@intCast(length)]});
    try rt.net.recv(
        provision,
        recv_task,
        provision.socket,
        provision.buffer,
    );
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

    const addr = try std.net.Address.parseIp(host, port);

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

    try Cross.socket.to_nonblock(socket);
    try std.posix.bind(socket, &addr.any, addr.getOsSockLen());
    try std.posix.listen(socket, size);

    try tardy.entry(
        socket,
        struct {
            fn rt_start(rt: *Runtime, t_socket: std.posix.socket_t) !void {
                const pool = try Pool(Provision).init(rt.allocator, size, rt.allocator, struct {
                    fn init(items: []Provision, a: std.mem.Allocator) void {
                        for (items) |*item| {
                            item.buffer = a.alloc(u8, size) catch unreachable;
                        }
                    }
                }.init);

                try rt.storage.store_alloc("provision_pool", pool);

                try rt.net.accept(t_socket, accept_task, t_socket);
            }
        }.rt_start,
        {},
        struct {
            fn rt_end(rt: *Runtime, _: void) !void {
                const provision_pool = rt.storage.get_ptr("provision_pool", Pool(Provision));
                provision_pool.deinit(rt.allocator, struct {
                    fn pool_deinit(items: []Provision, a: std.mem.Allocator) void {
                        for (items) |item| {
                            a.free(item.buffer);
                        }
                    }
                }.pool_deinit);
            }
        }.rt_end,
    );
}
