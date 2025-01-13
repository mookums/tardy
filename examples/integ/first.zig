const std = @import("std");
const assert = std.debug.assert;
const log = @import("lib.zig").log;

const Runtime = @import("tardy").Runtime;

const Dir = @import("tardy").Dir;
const File = @import("tardy").File;
const Timer = @import("tardy").Timer;
const TcpSocket = @import("tardy").TcpSocket;

const OpenFileResult = @import("tardy").OpenFileResult;
const OpenDirResult = @import("tardy").OpenDirResult;

const CreateFileResult = @import("tardy").OpenFileResult;
const CreateDirResult = @import("tardy").CreateDirResult;

const StatResult = @import("tardy").StatResult;
const ReadResult = @import("tardy").ReadResult;
const WriteResult = @import("tardy").WriteResult;
const DeleteTreeResult = @import("tardy").DeleteTreeResult;

const ConnectResult = @import("tardy").ConnectResult;

const IntegParams = @import("lib.zig").IntegParams;

const Params = struct {
    integ: *const IntegParams,
    root_dir: Dir = undefined,
    file: File = undefined,
    buffer: []u8 = undefined,
    socket: TcpSocket = undefined,
};

pub fn start(rt: *Runtime, res: CreateDirResult, integ_params: *const IntegParams) !void {
    const new_dir = try res.unwrap();
    errdefer new_dir.close_blocking();
    log.debug("created new integ dir (seed={d})", .{integ_params.seed});

    const params = try rt.allocator.create(Params);
    errdefer rt.allocator.destroy(params);
    params.* = .{
        .integ = integ_params,
        .root_dir = new_dir,
    };

    try new_dir.create_file(rt, params, post_create_dummy, "dummy", .{ .mode = .read_write });
}

fn post_create_dummy(rt: *Runtime, res: OpenFileResult, params: *Params) !void {
    const file = try res.unwrap();
    errdefer file.close_blocking();
    params.file = file;

    params.buffer = try rt.allocator.alloc(u8, params.integ.file_buffer_size);
    errdefer rt.allocator.free(params.buffer);

    for (params.buffer) |*item| item.* = @truncate(params.integ.seed);
    try file.write_all(rt, params, post_write, params.buffer, null);
}

fn post_write(rt: *Runtime, res: WriteResult, params: *Params) !void {
    assert(try res.unwrap() == params.buffer.len);
    try params.file.stat(rt, params, post_stat);
}

fn post_stat(rt: *Runtime, res: StatResult, params: *Params) !void {
    const stat = try res.unwrap();

    // asserts we wrote the whole buffer into it
    assert(stat.size == params.buffer.len);
    try Timer.delay(rt, params, wait_for_server, .{ .seconds = 1 });
}

fn wait_for_server(rt: *Runtime, _: void, params: *Params) !void {
    if (params.integ.server_ready.load(.acquire))
        try TcpSocket.connect(rt, params, post_connect, "127.0.0.1", 9988)
    else
        try Timer.delay(rt, params, wait_for_server, .{ .seconds = 1 });
}

fn post_connect(rt: *Runtime, res: ConnectResult, params: *Params) !void {
    const socket = try res.unwrap();
    socket.close_blocking();

    try Dir.cwd().delete_tree(rt, params, finish, params.integ.seed_string, 1);
}

fn finish(rt: *Runtime, res: DeleteTreeResult, params: *Params) !void {
    defer rt.stop();
    defer rt.allocator.destroy(params);
    defer rt.allocator.free(params.buffer);
    defer params.root_dir.close_blocking();
    defer params.file.close_blocking();

    try res.unwrap();
    log.debug("deleted integ tree (seed={d})", .{params.integ.seed});
}
