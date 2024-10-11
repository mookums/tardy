const std = @import("std");
const assert = std.debug.assert;
const builtin = @import("builtin");
const Completion = @import("completion.zig").Completion;

pub const AsyncIOType = union(enum) {
    /// Attempts to automatically match
    /// the best backend.
    ///
    /// `Linux: io_uring -> epoll -> busy_loop
    /// Windows: busy_loop
    /// Darwin & BSD: busy_loop
    /// Solaris: busy_loop`
    auto,
    /// Only available on Linux >= 5.1
    ///
    /// Utilizes the io_uring interface for handling I/O.
    /// `https://kernel.dk/io_uring.pdf`
    io_uring,
    /// Only available on Linux >= 2.5.45
    ///
    /// Utilizes the epoll interface for handling I/O.
    epoll,
    /// Available on most targets.
    /// Relies on non-blocking fds and busy loop polling.
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

            if (version.isAtLeast(.{
                .major = 2,
                .minor = 5,
                .patch = 45,
            }) orelse @compileError("Unable to determine kernel version. Specify an AsyncIO Backend.")) {
                return AsyncIOType.epoll;
            }

            return AsyncIOType.busy_loop;
        },
        .windows => return AsyncIOType.busy_loop,
        .ios, .macos, .watchos, .tvos, .visionos => return AsyncIOType.busy_loop,
        .kfreebsd, .freebsd, .openbsd, .netbsd, .dragonfly => return AsyncIOType.busy_loop,
        .solaris, .illumos => return AsyncIOType.busy_loop,
        else => @compileError("Unsupported platform! Provide a custom AsyncIO backend."),
    }
}

pub const AsyncIOError = error{
    QueueFull,
};

pub const AsyncIOOptions = struct {
    /// The parent AsyncIO that this should
    /// inherit parameters from.
    parent_async: ?*const AsyncIO = null,
    /// Maximum number of aio jobs.
    size_aio_jobs_max: u16,
    /// Maximum number of completions reaped.
    size_aio_reap_max: u16,
};

pub const AsyncIO = struct {
    runner: *anyopaque,
    attached: bool = false,
    completions: []Completion = undefined,

    _deinit: *const fn (
        self: *AsyncIO,
        allocator: std.mem.Allocator,
    ) void,

    // Filesystem Operations
    _queue_open: *const fn (
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
        path: []const u8,
    ) AsyncIOError!void,

    _queue_read: *const fn (
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
        buffer: []u8,
        offset: usize,
    ) AsyncIOError!void,

    _queue_write: *const fn (
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
        buffer: []const u8,
        offset: usize,
    ) AsyncIOError!void,

    _queue_close: *const fn (
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
    ) AsyncIOError!void,

    // Network Operations
    _queue_accept: *const fn (
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
    ) AsyncIOError!void,

    _queue_connect: *const fn (
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
        host: []const u8,
        port: u16,
    ) AsyncIOError!void,

    _queue_recv: *const fn (
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
        buffer: []u8,
    ) AsyncIOError!void,

    _queue_send: *const fn (
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
        buffer: []const u8,
    ) AsyncIOError!void,

    _reap: *const fn (self: *AsyncIO, min: usize) AsyncIOError![]Completion,
    _submit: *const fn (self: *AsyncIO) AsyncIOError!void,

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

    pub fn queue_open(
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
        path: []const u8,
    ) AsyncIOError!void {
        assert(self.attached);
        try @call(.auto, self._queue_open, .{ self, task, fd, path });
    }

    pub fn queue_read(
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
        buffer: []u8,
        offset: usize,
    ) AsyncIOError!void {
        assert(self.attached);
        try @call(.auto, self._queue_read, .{ self, task, fd, buffer, offset });
    }

    pub fn queue_write(
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
        buffer: []const u8,
        offset: usize,
    ) AsyncIOError!void {
        assert(self.attached);
        try @call(.auto, self._queue_write, .{ self, task, fd, buffer, offset });
    }

    pub fn queue_close(
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
    ) AsyncIOError!void {
        assert(self.attached);
        try @call(.auto, self._queue_close, .{ self, task, fd });
    }

    pub fn queue_accept(
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
    ) AsyncIOError!void {
        assert(self.attached);
        try @call(.auto, self._queue_accept, .{ self, task, fd });
    }

    pub fn queue_connect(
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
        host: []const u8,
        port: u16,
    ) AsyncIOError!void {
        assert(self.attached);
        try @call(.auto, self._queue_connect, .{ self, task, fd, host, port });
    }

    pub fn queue_recv(
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
        buffer: []u8,
    ) AsyncIOError!void {
        assert(self.attached);
        try @call(.auto, self._queue_recv, .{ self, task, fd, buffer });
    }

    pub fn queue_send(
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
        buffer: []const u8,
    ) AsyncIOError!void {
        assert(self.attached);
        try @call(.auto, self._queue_send, .{ self, task, fd, buffer });
    }

    pub fn reap(self: *AsyncIO, min: usize) AsyncIOError![]Completion {
        assert(self.attached);
        return try @call(.auto, self._reap, .{ self, min });
    }

    pub fn submit(self: *AsyncIO) AsyncIOError!void {
        assert(self.attached);
        try @call(.auto, self._submit, .{self});
    }
};
