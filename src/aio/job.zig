const std = @import("std");

const Timespec = @import("../lib.zig").Timespec;
const Path = @import("../fs/lib.zig").Path;

pub const Job = struct {
    type: union(enum) {
        wake,
        timer: TimerJob,
        open: OpenJob,
        mkdir: MkdirJob,
        delete: Path,
        stat: std.posix.fd_t,
        read: ReadJob,
        write: WriteJob,
        close: std.posix.fd_t,
        accept: std.posix.socket_t,
        connect: ConnectJob,
        send: SendJob,
        recv: RecvJob,
    },

    index: usize,
    task: usize,
};

const TimerJob = union(enum) {
    none,
    fd: std.posix.fd_t,
    ns: i128,
};

const OpenJob = struct {
    path: Path,
    kind: enum { file, dir },
};

const MkdirJob = struct {
    path: Path,
    mode: std.posix.mode_t,
};

const ReadJob = struct {
    fd: std.posix.fd_t,
    buffer: []u8,
    offset: ?usize,
};

const WriteJob = struct {
    fd: std.posix.fd_t,
    buffer: []const u8,
    offset: ?usize,
};

const ConnectJob = struct {
    socket: std.posix.socket_t,
    addr: std.net.Address,
};

const SendJob = struct {
    socket: std.posix.socket_t,
    buffer: []const u8,
};

const RecvJob = struct {
    socket: std.posix.socket_t,
    buffer: []u8,
};
