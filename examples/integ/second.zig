const std = @import("std");
const assert = std.debug.assert;
const log = @import("lib.zig").log;

const Runtime = @import("tardy").Runtime;

const TcpServer = @import("tardy").TcpServer;
const TcpSocket = @import("tardy").TcpSocket;

const AcceptTcpResult = @import("tardy").AcceptTcpResult;
const RecvResult = @import("tardy").RecvResult;
const SendResult = @import("tardy").SendResult;

const IntegParams = @import("lib.zig").IntegParams;

const Params = struct {
    integ: *const IntegParams,
    buffer: []u8 = undefined,
    server: TcpServer = undefined,
    socket: TcpSocket = undefined,
};

pub fn start(rt: *Runtime, _: void, integ_params: *const IntegParams) !void {
    const server = try TcpServer.init("127.0.0.1", 9988);
    // The server is ready after this, listening on the socket.
    try server.listen(256);
    defer integ_params.server_ready.store(true, .release);

    log.debug("created new integ dir (seed={d})", .{integ_params.seed});

    const params = try rt.allocator.create(Params);
    errdefer rt.allocator.destroy(params);
    params.* = .{
        .integ = integ_params,
        .server = server,
    };

    try server.accept(rt, params, post_accept);
}

fn post_accept(rt: *Runtime, res: AcceptTcpResult, params: *Params) !void {
    const socket = try res.unwrap();
    errdefer socket.close_blocking();

    params.socket = socket;
    params.buffer = try rt.allocator.alloc(u8, params.integ.socket_buffer_size);
    errdefer rt.allocator.free(params.buffer);

    try socket.recv_all(rt, params, post_recv, params.buffer);
}

fn post_recv(rt: *Runtime, res: RecvResult, params: *Params) !void {
    const read = try res.unwrap();
    _ = read;

    params.socket.close_blocking();
    params.server.close_blocking();

    defer rt.allocator.destroy(params);
    defer rt.allocator.free(params.buffer);
}
