const std = @import("std");
const log = std.log.scoped(.@"tardy/example/echo");

const Pool = @import("tardy").Pool;
const Runtime = @import("tardy").Runtime;
const Task = @import("tardy").Task;
const Tardy = @import("tardy").Tardy(.auto);
const Cross = @import("tardy").Cross;

const Socket = @import("tardy").Socket;

const AcceptResult = @import("tardy").AcceptResult;
const RecvResult = @import("tardy").RecvResult;
const SendResult = @import("tardy").SendResult;

const Provision = struct {
    index: usize,
    socket: Socket,
    buffer: []u8,
};

fn close_connection(provision_pool: *Pool(Provision), provision: *const Provision) void {
    log.debug("closed connection", .{});
    std.posix.close(provision.socket.handle);
    provision_pool.release(provision.index);
}

fn accept_task(rt: *Runtime, result: AcceptResult, server: *const Socket) !void {
    const socket = result.unwrap() catch |e| {
        log.err("Failed to accept on socket | {}", .{e});
        rt.stop();
        return;
    };

    try Cross.socket.to_nonblock(socket.handle);

    log.debug(
        "{d} - accepted socket [{}]",
        .{ std.time.milliTimestamp(), socket.addr.in },
    );
    try server.accept(rt, server, accept_task);

    // get provision
    // assign based on index
    // get buffer
    const provision_pool = rt.storage.get_ptr("provision_pool", Pool(Provision));
    const index = try provision_pool.borrow();

    const item = provision_pool.get_ptr(index);
    item.index = index;
    item.socket = socket;

    try socket.recv(rt, item, recv_task, item.buffer);
}

fn recv_task(rt: *Runtime, result: RecvResult, provision: *Provision) !void {
    const length = result.unwrap() catch |e| {
        log.err("Failed to recv on socket | {}", .{e});
        const provision_pool = rt.storage.get_ptr("provision_pool", Pool(Provision));
        close_connection(provision_pool, provision);
        return;
    };

    try provision.socket.send_all(rt, provision, send_task, provision.buffer[0..length]);
}

fn send_task(rt: *Runtime, result: SendResult, provision: *Provision) !void {
    const length = result.unwrap() catch |e| {
        log.err("Failed to send on socket | {}", .{e});
        const provision_pool = rt.storage.get_ptr("provision_pool", Pool(Provision));
        close_connection(provision_pool, provision);
        return;
    };

    log.debug("Echoed: {s}", .{provision.buffer[0..@intCast(length)]});
    try provision.socket.recv(rt, provision, recv_task, provision.buffer);
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();
    const size = 1024;

    var tardy = try Tardy.init(allocator, .{
        .threading = .single,
        .pooling = .static,
    });
    defer tardy.deinit();

    const host = "0.0.0.0";
    const port = 9862;

    const server = try Socket.init(.{ .tcp = .{ .host = host, .port = port } });
    try server.bind();
    try server.listen(1024);

    try tardy.entry(
        &server,
        struct {
            fn rt_start(rt: *Runtime, tcp_server: *const Socket) !void {
                const pool = try Pool(Provision).init(rt.allocator, size, .static);
                for (pool.items) |*item| item.buffer = try rt.allocator.alloc(u8, size);
                try rt.storage.store_alloc("provision_pool", pool);

                try tcp_server.accept(rt, tcp_server, accept_task);
            }
        }.rt_start,
        {},
        struct {
            fn rt_end(rt: *Runtime, _: void) !void {
                const provision_pool = rt.storage.get_ptr("provision_pool", Pool(Provision));
                provision_pool.deinit_with_hook(rt.allocator, struct {
                    fn pool_deinit(items: []Provision, a: std.mem.Allocator) void {
                        for (items) |item| a.free(item.buffer);
                    }
                }.pool_deinit);
            }
        }.rt_end,
    );
}
