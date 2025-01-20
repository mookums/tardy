const std = @import("std");
const log = std.log.scoped(.@"tardy/example/echo");

const Pool = @import("tardy").Pool;
const Runtime = @import("tardy").Runtime;
const Task = @import("tardy").Task;
const Tardy = @import("tardy").Tardy(.auto);
const Cross = @import("tardy").Cross;

const Socket = @import("tardy").Socket;
const Timer = @import("tardy").Timer;

const AcceptResult = @import("tardy").AcceptResult;
const RecvResult = @import("tardy").RecvResult;
const SendResult = @import("tardy").SendResult;

fn echo_frame(rt: *Runtime, server: *const Socket) !void {
    const socket = try server.accept().resolve(rt);

    defer socket.close_blocking();
    defer {
        const index = rt.current_task orelse unreachable;
        const task = rt.scheduler.tasks.get_ptr(index);
        task.state = .dead;
        rt.scheduler.release(index) catch unreachable;
    }

    log.debug(
        "{d} - accepted socket [{}]",
        .{ std.time.milliTimestamp(), socket.addr.in },
    );

    // spawn off a new frame.
    try rt.spawn_frame(.{ rt, server }, echo_frame, 1024 * 16);

    var buffer: [1024]u8 = undefined;
    while (true) {
        const recv_length = socket.recv(&buffer).resolve(rt) catch |e| {
            log.err("Failed to recv on socket | {}", .{e});
            return;
        };

        const send_length = socket.send_all(buffer[0..recv_length]).resolve(rt) catch |e| {
            log.err("Failed to send on socket | {}", .{e});
            return;
        };

        log.debug("Echoed: {s}", .{buffer[0..send_length]});
    }
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    var tardy = try Tardy.init(allocator, .{
        .threading = .single,
        .pooling = .static,
        .size_tasks_initial = 256,
        .size_aio_reap_max = 256,
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
            fn start(rt: *Runtime, tcp_server: *const Socket) !void {
                try rt.spawn_frame(.{ rt, tcp_server }, echo_frame, 1024 * 16);
            }
        }.start,
        {},
        struct {
            fn end(_: *Runtime, _: void) !void {}
        }.end,
    );
}
