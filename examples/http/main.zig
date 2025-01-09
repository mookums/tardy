const std = @import("std");
const log = std.log.scoped(.@"tardy/example/http");

const Pool = @import("tardy").Pool;
const Runtime = @import("tardy").Runtime;
const Task = @import("tardy").Task;
const Tardy = @import("tardy").Tardy(.auto);
const Cross = @import("tardy").Cross;

const TcpServer = @import("tardy").TcpServer;
const TcpSocket = @import("tardy").TcpSocket;

const AcceptTcpResult = @import("tardy").AcceptTcpResult;
const RecvResult = @import("tardy").RecvResult;
const SendResult = @import("tardy").SendResult;

const Provision = struct {
    index: usize,
    socket: TcpSocket,
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

fn accept_task(rt: *Runtime, result: AcceptTcpResult, _: void) !void {
    const socket = result.unwrap() catch |e| {
        log.err("failed to accept socket | {}", .{e});
        rt.stop();
        return;
    };

    try Cross.socket.to_nonblock(socket.socket);
    log.debug("accepted socket", .{});

    const provision_pool = rt.storage.get_ptr("provision_pool", Pool(Provision));
    const index = try provision_pool.borrow();

    const item = provision_pool.get_ptr(index);
    item.index = index;
    item.socket = socket;

    try socket.recv(rt, item, recv_task, item.buffer);
}

fn recv_task(rt: *Runtime, result: RecvResult, provision: *Provision) !void {
    log.debug("recv socket", .{});
    _ = result.unwrap() catch |e| {
        log.debug("recv closed | {}", .{e});
        log.debug("queueing close with ctx ptr: {*}", .{provision});
        return try provision.socket.close(rt, provision, close_task);
    };

    try provision.socket.send(rt, provision, send_task, HTTP_RESPONSE[0..]);
}

fn send_task(rt: *Runtime, result: SendResult, provision: *Provision) !void {
    log.debug("send socket", .{});

    _ = result.unwrap() catch |e| {
        log.debug("send closed | {}", .{e});
        log.debug("queueing close with ctx ptr: {*}", .{provision});
        return try provision.socket.close(rt, provision, close_task);
    };

    try provision.socket.recv(rt, provision, recv_task, provision.buffer);
}

fn close_task(rt: *Runtime, _: void, provision: *Provision) !void {
    const provision_pool = rt.storage.get_ptr("provision_pool", Pool(Provision));

    log.debug("close socket", .{});
    provision_pool.release(provision.index);

    const server = rt.storage.get("tcp_server", TcpServer);
    try server.accept(rt, {}, accept_task);
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .thread_safe = true }){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    const host = "0.0.0.0";
    const port = 9862;

    const thread_count = @max(@as(u16, @intCast(try std.Thread.getCpuCount() / 2 - 1)), 1);
    const conn_per_thread = try std.math.divCeil(u16, 2000, thread_count);

    var tardy = try Tardy.init(allocator, .{
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
                const server = try TcpServer.init(host, port);
                try server.listen(size);
                try Cross.socket.to_nonblock(server.socket);

                const pool = try Pool(Provision).init(rt.allocator, size);
                for (pool.items) |*item| {
                    item.buffer = try rt.allocator.alloc(u8, 512);
                }

                try rt.storage.store_alloc("provision_pool", pool);
                try rt.storage.store_alloc("tcp_server", server);

                for (0..size) |_| try server.accept(rt, {}, accept_task);
            }
        }.rt_start,
        {},
        struct {
            fn rt_end(rt: *Runtime, _: void) !void {
                const socket = rt.storage.get("tcp_server", TcpServer);
                std.posix.close(socket.socket);

                const provision_pool = rt.storage.get_ptr("provision_pool", Pool(Provision));
                for (provision_pool.items) |*item| {
                    rt.allocator.free(item.buffer);
                }
            }
        }.rt_end,
    );
}
