const std = @import("std");
const log = std.log.scoped(.@"tardy/aio");
const assert = std.debug.assert;
const builtin = @import("builtin");
const Completion = @import("completion.zig").Completion;

const Timespec = @import("../lib.zig").Timespec;
const Path = @import("../fs/lib.zig").Path;

const Atomic = std.atomic.Value;

const PoolKind = @import("../core/pool.zig").PoolKind;
const Socket = @import("../net/lib.zig").Socket;

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
            }) orelse @compileError("Unable to determine kernel version. Specify an Async I/O Backend.")) {
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
        else => @compileError("Unsupported platform! Provide a custom Async I/O backend."),
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
    // Pooling
    pooling: PoolKind,
    size_tasks_initial: usize,
    /// Maximum number of completions reaped.
    size_aio_reap_max: usize,
};

pub const AsyncSubmission = union(enum) {
    timer: Timespec,
    open: struct {
        path: Path,
        flags: AioOpenFlags,
    },
    delete: struct {
        path: Path,
        is_dir: bool,
    },
    mkdir: struct {
        path: Path,
        mode: std.posix.mode_t,
    },
    stat: std.posix.fd_t,
    read: struct {
        fd: std.posix.fd_t,
        buffer: []u8,
        offset: ?usize,
    },
    write: struct {
        fd: std.posix.fd_t,
        buffer: []const u8,
        offset: ?usize,
    },
    close: std.posix.fd_t,
    accept: struct {
        socket: std.posix.socket_t,
        kind: Socket.Kind,
    },
    connect: struct {
        socket: std.posix.socket_t,
        addr: std.net.Address,
        kind: Socket.Kind,
    },
    recv: struct {
        socket: std.posix.socket_t,
        buffer: []u8,
    },
    send: struct {
        socket: std.posix.socket_t,
        buffer: []const u8,
    },
};

pub const AsyncIO = struct {
    runner: *anyopaque,
    attached: bool = false,
    completions: []Completion = undefined,
    asleep: Atomic(bool) = Atomic(bool).init(false),

    _queue_job: *const fn (
        self: *AsyncIO,
        index: usize,
        job: AsyncSubmission,
    ) anyerror!void,

    _deinit: *const fn (
        self: *AsyncIO,
        allocator: std.mem.Allocator,
    ) void,

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

    pub fn queue_job(self: *AsyncIO, task: usize, job: AsyncSubmission) !void {
        assert(self.attached);
        log.debug("queuing up job={s} at index={d}", .{ @tagName(job), task });
        try @call(.auto, self._queue_job, .{ self, task, job });
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
    /// Permissions used for creating files.
    perms: ?std.posix.mode_t = null,
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
    non_block: bool = true,
    /// Ensure data is physically written to disk immediately.
    sync: bool = false,
    /// Ensure that the file is a directory.
    directory: bool = false,
};
