const std = @import("std");
const log = std.log.scoped(.@"tardy/example/http");

const Pool = @import("tardy").Pool;
const Runtime = @import("tardy").Runtime;
const Task = @import("tardy").Task;
const Tardy = @import("tardy").Tardy(.auto);
const Cross = @import("tardy").Cross;

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

fn accept_task(rt: *Runtime, child_socket: std.posix.socket_t, _: void) !void {
    if (!Cross.socket.is_valid(child_socket)) {
        log.err("failed to accept socket", .{});
        rt.stop();
        return;
    }

    try Cross.socket.to_nonblock(child_socket);
    log.debug("accepted socket fd={d}", .{child_socket});

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

fn recv_task(rt: *Runtime, length: i32, provision: *Provision) !void {
    log.debug("recv socket fd={d}", .{provision.socket});

    if (length <= 0) {
        log.debug("recv closed fd={d}", .{provision.socket});
        log.debug("queueing close with ctx ptr: {*}", .{provision});
        try rt.net.close(provision, close_task, provision.socket);
        return;
    }

    try rt.net.send(
        provision,
        send_task,
        provision.socket,
        HTTP_RESPONSE[0..],
    );
}

fn send_task(rt: *Runtime, length: i32, provision: *Provision) !void {
    log.debug("send socket fd={d}", .{provision.socket});

    if (length <= 0) {
        log.debug("send closed fd={d}", .{provision.socket});
        log.debug("queueing close with ctx ptr: {*}", .{provision});
        try rt.net.close(provision, close_task, provision.socket);
        return;
    }

    try rt.net.recv(
        provision,
        recv_task,
        provision.socket,
        provision.buffer,
    );
}

fn close_task(rt: *Runtime, _: void, provision: *Provision) !void {
    const provision_pool = rt.storage.get_ptr("provision_pool", Pool(Provision));

    log.debug("close socket fd={d}", .{provision.socket});
    provision_pool.release(provision.index);

    const socket = rt.storage.get("server_socket", std.posix.socket_t);
    try rt.net.accept({}, accept_task, socket);
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
        conn_per_thread,
        struct {
            fn rt_start(rt: *Runtime, size: u16) !void {
                // socket per thread.
                const addr = try std.net.Address.parseIp(host, port);
                const socket = try create_socket(addr);
                try Cross.socket.to_nonblock(socket);
                try std.posix.bind(socket, &addr.any, addr.getOsSockLen());
                try std.posix.listen(socket, size);

                const pool = try Pool(Provision).init(rt.allocator, size, rt.allocator, struct {
                    fn init(items: []Provision, a: std.mem.Allocator) void {
                        for (items) |*item| {
                            item.buffer = a.alloc(u8, 512) catch unreachable;
                        }
                    }
                }.init);

                try rt.storage.store_alloc("provision_pool", pool);
                try rt.storage.store_alloc("server_socket", socket);

                for (0..size) |_| {
                    try rt.net.accept(
                        {},
                        accept_task,
                        socket,
                    );
                }
            }
        }.rt_start,
        {},
        struct {
            fn rt_end(rt: *Runtime, _: void) !void {
                const socket = rt.storage.get("server_socket", std.posix.socket_t);
                std.posix.close(socket);

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
