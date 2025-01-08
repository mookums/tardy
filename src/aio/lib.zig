const std = @import("std");
const assert = std.debug.assert;
const builtin = @import("builtin");
const Completion = @import("completion.zig").Completion;

const Timespec = @import("../lib.zig").Timespec;
const Path = @import("../fs/lib.zig").Path;

const Atomic = std.atomic.Value;

pub const AsyncIOType = union(enum) {
    /// Attempts to automatically match
    /// the best backend.
    ///
    /// `Linux: io_uring -> epoll -> busy_loop
    /// Windows: busy_loop
    /// Darwin & BSD: kqueue
    /// Solaris: busy_loop`
    auto,
    /// Available on Linux >= 5.1
    ///
    /// Utilizes the io_uring interface for handling I/O.
    /// `https://kernel.dk/io_uring.pdf`
    io_uring,
    /// Available on Linux >= 2.5.45
    ///
    /// Utilizes the epoll interface for handling I/O.
    epoll,
    /// Available on Darwin & BSD systems
    ///
    /// Utilizes the kqueue interface for handling I/O.
    kqueue,
    /// Available on most targets.
    /// Relies on non-blocking fd operations and busy loop polling.
    busy_loop,
    /// Available on all targets.
    custom: type,
};

pub fn auto_async_match() AsyncIOType {
    switch (comptime builtin.target.os.tag) {
        .linux => {
            const version = comptime builtin.target.os.getVersionRange().linux;

            if (version.isAtLeast(.{
                .major = 5,
                .minor = 1,
                .patch = 0,
            }) orelse @compileError("Unable to determine kernel version. Specify an AsyncIO Backend.")) {
                return AsyncIOType.io_uring;
            }

            if (version.isAtLeast(.{ .major = 2, .minor = 5, .patch = 45 }) orelse unreachable) {
                return AsyncIOType.epoll;
            }

            return AsyncIOType.busy_loop;
        },
        .windows => return AsyncIOType.busy_loop,
        .ios, .macos, .watchos, .tvos, .visionos => return AsyncIOType.kqueue,
        .kfreebsd, .freebsd, .openbsd, .netbsd, .dragonfly => return AsyncIOType.kqueue,
        .solaris, .illumos => return AsyncIOType.busy_loop,
        else => @compileError("Unsupported platform! Provide a custom AsyncIO backend."),
    }
}

pub fn async_to_type(comptime aio: AsyncIOType) type {
    return comptime switch (aio) {
        .io_uring => @import("../aio/apis/io_uring.zig").AsyncIoUring,
        .epoll => @import("../aio/apis/epoll.zig").AsyncEpoll,
        .busy_loop => @import("../aio/apis/busy_loop.zig").AsyncBusyLoop,
        .kqueue => @import("../aio/apis/kqueue.zig").AsyncKQueue,
        .custom => |inner| {
            assert(std.meta.hasMethod(inner, "init"));
            assert(std.meta.hasMethod(inner, "to_async"));
            return inner;
        },
        .auto => unreachable,
    };
}

pub const AsyncIOOptions = struct {
    /// The parent AsyncIO that this should
    /// inherit parameters from.
    parent_async: ?*const AsyncIO = null,
    /// Maximum number of aio jobs.
    size_aio_jobs_max: usize,
    /// Maximum number of completions reaped.
    size_aio_reap_max: usize,
};

pub const AsyncIO = struct {
    runner: *anyopaque,
    attached: bool = false,
    completions: []Completion = undefined,
    asleep: Atomic(bool) = Atomic(bool).init(false),

    _deinit: *const fn (
        self: *AsyncIO,
        allocator: std.mem.Allocator,
    ) void,

    _queue_timer: *const fn (
        self: *AsyncIO,
        task: usize,
        timespec: Timespec,
    ) anyerror!void,

    // Filesystem Operations
    _queue_open: *const fn (
        self: *AsyncIO,
        task: usize,
        path: Path,
        flags: AioOpenFlags,
    ) anyerror!void,

    _queue_delete: *const fn (
        self: *AsyncIO,
        task: usize,
        path: Path,
        is_dir: bool,
    ) anyerror!void,

    _queue_mkdir: *const fn (
        self: *AsyncIO,
        task: usize,
        path: Path,
        mode: std.posix.mode_t,
    ) anyerror!void,

    _queue_stat: *const fn (
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
    ) anyerror!void,

    _queue_read: *const fn (
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
        buffer: []u8,
        offset: ?usize,
    ) anyerror!void,

    _queue_write: *const fn (
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
        buffer: []const u8,
        offset: ?usize,
    ) anyerror!void,

    _queue_close: *const fn (
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
    ) anyerror!void,

    // Network Operations
    _queue_accept: *const fn (
        self: *AsyncIO,
        task: usize,
        socket: std.posix.socket_t,
    ) anyerror!void,

    _queue_connect: *const fn (
        self: *AsyncIO,
        task: usize,
        socket: std.posix.socket_t,
        host: []const u8,
        port: u16,
    ) anyerror!void,

    _queue_recv: *const fn (
        self: *AsyncIO,
        task: usize,
        socket: std.posix.socket_t,
        buffer: []u8,
    ) anyerror!void,

    _queue_send: *const fn (
        self: *AsyncIO,
        task: usize,
        socket: std.posix.socket_t,
        buffer: []const u8,
    ) anyerror!void,

    _wake: *const fn (self: *AsyncIO) anyerror!void,
    _reap: *const fn (self: *AsyncIO, wait: bool) anyerror![]Completion,
    _submit: *const fn (self: *AsyncIO) anyerror!void,

    /// This provides the completions that the backend will utilize when
    /// submitting and reaping. This MUST be called before any other
    /// methods on this AsyncIO instance.
    pub fn attach(self: *AsyncIO, completions: []Completion) void {
        self.completions = completions;
        self.attached = true;
    }

    pub fn deinit(
        self: *AsyncIO,
        allocator: std.mem.Allocator,
    ) void {
        @call(.auto, self._deinit, .{ self, allocator });
    }

    pub fn queue_timer(
        self: *AsyncIO,
        task: usize,
        timespec: Timespec,
    ) !void {
        assert(self.attached);
        try @call(.auto, self._queue_timer, .{ self, task, timespec });
    }

    pub fn queue_open(
        self: *AsyncIO,
        task: usize,
        path: Path,
        flags: AioOpenFlags,
    ) !void {
        assert(self.attached);
        try @call(.auto, self._queue_open, .{ self, task, path, flags });
    }

    pub fn queue_delete(
        self: *AsyncIO,
        task: usize,
        path: Path,
        is_dir: bool,
    ) !void {
        assert(self.attached);
        try @call(.auto, self._queue_delete, .{ self, task, path, is_dir });
    }

    pub fn queue_mkdir(
        self: *AsyncIO,
        task: usize,
        path: Path,
        mode: std.posix.mode_t,
    ) !void {
        assert(self.attached);
        try @call(.auto, self._queue_mkdir, .{ self, task, path, mode });
    }

    pub fn queue_stat(
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
    ) !void {
        assert(self.attached);
        try @call(.auto, self._queue_stat, .{ self, task, fd });
    }

    pub fn queue_read(
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
        buffer: []u8,
        offset: ?usize,
    ) !void {
        assert(self.attached);
        try @call(.auto, self._queue_read, .{ self, task, fd, buffer, offset });
    }

    pub fn queue_write(
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
        buffer: []const u8,
        offset: ?usize,
    ) !void {
        assert(self.attached);
        try @call(.auto, self._queue_write, .{ self, task, fd, buffer, offset });
    }

    pub fn queue_close(
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
    ) !void {
        assert(self.attached);
        try @call(.auto, self._queue_close, .{ self, task, fd });
    }

    pub fn queue_accept(
        self: *AsyncIO,
        task: usize,
        socket: std.posix.socket_t,
    ) !void {
        assert(self.attached);
        try @call(.auto, self._queue_accept, .{ self, task, socket });
    }

    pub fn queue_connect(
        self: *AsyncIO,
        task: usize,
        socket: std.posix.socket_t,
        host: []const u8,
        port: u16,
    ) !void {
        assert(self.attached);
        try @call(.auto, self._queue_connect, .{ self, task, socket, host, port });
    }

    pub fn queue_recv(
        self: *AsyncIO,
        task: usize,
        socket: std.posix.socket_t,
        buffer: []u8,
    ) !void {
        assert(self.attached);
        try @call(.auto, self._queue_recv, .{ self, task, socket, buffer });
    }

    pub fn queue_send(
        self: *AsyncIO,
        task: usize,
        socket: std.posix.socket_t,
        buffer: []const u8,
    ) !void {
        assert(self.attached);
        try @call(.auto, self._queue_send, .{ self, task, socket, buffer });
    }

    pub fn wake(self: *AsyncIO) !void {
        assert(self.attached);
        self.asleep.store(false, .release);
        try @call(.auto, self._wake, .{self});
    }

    pub fn reap(self: *AsyncIO, wait: bool) ![]Completion {
        assert(self.attached);
        if (wait) self.asleep.store(true, .release);
        return try @call(.auto, self._reap, .{ self, wait });
    }

    pub fn submit(self: *AsyncIO) !void {
        assert(self.attached);
        try @call(.auto, self._submit, .{self});
    }
};

pub const FileMode = enum {
    read,
    write,
    read_write,
};

/// These are the OpenFlags used internally.
/// This allows us to abstract out various different FS calls
/// that are all backed by the same underlying call.
pub const AioOpenFlags = struct {
    mode: FileMode = .read,
    /// Open the file for appending.
    /// This will force writing permissions.
    append: bool = false,
    /// Create the file if it doesn't exist.
    create: bool = false,
    /// Truncate the file to the start.
    truncate: bool = false,
    /// Fail if the file already exists.
    exclusive: bool = false,
    /// Open the file for non-blocking I/O.
    non_block: bool = false,
    /// Ensure data is physically written to disk immediately.
    sync: bool = false,
    /// Ensure that the file is a directory.
    directory: bool = false,
};
