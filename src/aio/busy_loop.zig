const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.@"tardy/aio/busy_loop");

const builtin = @import("builtin");
const Completion = @import("completion.zig").Completion;

const AsyncIO = @import("lib.zig").AsyncIO;
const AsyncIOOptions = @import("lib.zig").AsyncIOOptions;
const Job = @import("job.zig").Job;

pub const AsyncBusyLoop = struct {
    inner: std.ArrayListUnmanaged(Job),

    pub fn init(allocator: std.mem.Allocator, options: AsyncIOOptions) !AsyncBusyLoop {
        const list = try std.ArrayListUnmanaged(Job).initCapacity(allocator, options.size_aio_jobs_max);
        return AsyncBusyLoop{ .inner = list };
    }

    pub fn deinit(self: *AsyncIO, allocator: std.mem.Allocator) void {
        const loop: *AsyncBusyLoop = @ptrCast(@alignCast(self.runner));
        loop.inner.deinit(allocator);
    }

    pub fn queue_open(
        self: *AsyncIO,
        task: usize,
        path: []const u8,
    ) !void {
        const loop: *AsyncBusyLoop = @ptrCast(@alignCast(self.runner));
        loop.inner.appendAssumeCapacity(.{
            .type = .{ .open = path },
            .fd = undefined,
            .task = task,
        });
    }

    pub fn queue_read(
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
        buffer: []u8,
        offset: usize,
    ) !void {
        const loop: *AsyncBusyLoop = @ptrCast(@alignCast(self.runner));
        loop.inner.appendAssumeCapacity(.{
            .type = .{ .read = .{ .buffer = buffer, .offset = offset } },
            .fd = fd,
            .task = task,
        });
    }

    pub fn queue_write(
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
        buffer: []const u8,
        offset: usize,
    ) !void {
        const loop: *AsyncBusyLoop = @ptrCast(@alignCast(self.runner));
        loop.inner.appendAssumeCapacity(.{
            .type = .{ .write = .{ .buffer = buffer, .offset = offset } },
            .fd = fd,
            .task = task,
        });
    }

    pub fn queue_close(
        self: *AsyncIO,
        task: usize,
        fd: std.posix.fd_t,
    ) !void {
        const loop: *AsyncBusyLoop = @ptrCast(@alignCast(self.runner));
        loop.inner.appendAssumeCapacity(.{
            .type = .close,
            .fd = fd,
            .task = task,
        });
    }

    pub fn queue_accept(
        self: *AsyncIO,
        task: usize,
        socket: std.posix.socket_t,
    ) !void {
        const loop: *AsyncBusyLoop = @ptrCast(@alignCast(self.runner));
        loop.inner.appendAssumeCapacity(.{
            .type = .accept,
            .fd = socket,
            .task = task,
        });
    }

    pub fn queue_connect(
        self: *AsyncIO,
        task: usize,
        socket: std.posix.socket_t,
        host: []const u8,
        port: u16,
    ) !void {
        const loop: *AsyncBusyLoop = @ptrCast(@alignCast(self.runner));

        const addr = try std.net.Address.parseIp(host, port);

        loop.inner.appendAssumeCapacity(.{
            .type = .{ .connect = addr.any },
            .fd = socket,
            .task = task,
        });
    }

    pub fn queue_recv(
        self: *AsyncIO,
        task: usize,
        socket: std.posix.socket_t,
        buffer: []u8,
    ) !void {
        const loop: *AsyncBusyLoop = @ptrCast(@alignCast(self.runner));
        loop.inner.appendAssumeCapacity(.{
            .type = .{ .recv = buffer },
            .fd = socket,
            .task = task,
        });
    }

    pub fn queue_send(
        self: *AsyncIO,
        task: usize,
        socket: std.posix.socket_t,
        buffer: []const u8,
    ) !void {
        const loop: *AsyncBusyLoop = @ptrCast(@alignCast(self.runner));
        loop.inner.appendAssumeCapacity(.{
            .type = .{ .send = buffer },
            .fd = socket,
            .task = task,
        });
    }

    pub fn submit(self: *AsyncIO) !void {
        const loop: *AsyncBusyLoop = @ptrCast(@alignCast(self.runner));
        _ = loop;
    }

    pub fn reap(self: *AsyncIO, wait: bool) ![]Completion {
        const loop: *AsyncBusyLoop = @ptrCast(@alignCast(self.runner));
        var reaped: usize = 0;
        var first_run: bool = true;

        while ((reaped < 1 and wait) or first_run) {
            var i: usize = 0;
            while (i < loop.inner.items.len and reaped < self.completions.len) : (i += 1) {
                const job = loop.inner.items[i];

                switch (job.type) {
                    .open => |path| {
                        const com_ptr = &self.completions[reaped];

                        const res: std.posix.fd_t = blk: {
                            const open_result = std.posix.openat(std.posix.AT.FDCWD, path, .{}, 0) catch |e| {
                                switch (e) {
                                    error.WouldBlock => continue,
                                    else => {
                                        log.debug("open failed: {}", .{e});
                                        break :blk -1;
                                    },
                                }
                            };

                            break :blk open_result;
                        };

                        com_ptr.result = .{ .fd = res };
                        com_ptr.task = job.task;
                        _ = loop.inner.swapRemove(i);
                        i -|= 1;
                        reaped += 1;
                    },
                    .read => |inner| {
                        const com_ptr = &self.completions[reaped];

                        const res: std.posix.fd_t = blk: {
                            const pread_result = std.posix.pread(job.fd, inner.buffer, inner.offset) catch |e| {
                                switch (e) {
                                    error.WouldBlock => continue,
                                    error.Unseekable => {
                                        const read_result = std.posix.read(job.fd, inner.buffer) catch |re| switch (re) {
                                            error.WouldBlock => continue,
                                            else => {
                                                log.debug("read failed: {}", .{re});
                                                break :blk -1;
                                            },
                                        };

                                        break :blk @intCast(read_result);
                                    },
                                    else => {
                                        log.debug("pread failed: {}", .{e});
                                        break :blk -1;
                                    },
                                }
                            };

                            break :blk @intCast(pread_result);
                        };

                        com_ptr.result = .{ .value = res };
                        com_ptr.task = job.task;
                        _ = loop.inner.swapRemove(i);
                        i -|= 1;
                        reaped += 1;
                    },
                    .write => |inner| {
                        const com_ptr = &self.completions[reaped];

                        const res: std.posix.fd_t = blk: {
                            const pwrite_result = std.posix.pwrite(
                                job.fd,
                                inner.buffer,
                                inner.offset,
                            ) catch |e| {
                                switch (e) {
                                    error.WouldBlock => continue,
                                    error.Unseekable => {
                                        const write_result = std.posix.write(job.fd, inner.buffer) catch |we| switch (we) {
                                            error.WouldBlock => continue,
                                            else => {
                                                log.debug("write failed: {}", .{we});
                                                break :blk -1;
                                            },
                                        };

                                        break :blk @intCast(write_result);
                                    },
                                    else => {
                                        log.debug("pwrite failed: {}", .{e});
                                        break :blk -1;
                                    },
                                }
                            };

                            break :blk @intCast(pwrite_result);
                        };

                        com_ptr.result = .{ .value = res };
                        com_ptr.task = job.task;
                        _ = loop.inner.swapRemove(i);
                        i -|= 1;
                        reaped += 1;
                    },
                    .close => {
                        const com_ptr = &self.completions[reaped];
                        std.posix.close(job.fd);
                        com_ptr.result = .{ .value = 0 };
                        com_ptr.task = job.task;
                        _ = loop.inner.swapRemove(i);
                        i -|= 1;
                        reaped += 1;
                    },
                    .accept => {
                        const com_ptr = &self.completions[reaped];

                        const res: std.posix.fd_t = blk: {
                            const accept_result = std.posix.accept(job.fd, null, null, 0) catch |e| {
                                switch (e) {
                                    error.WouldBlock => continue,
                                    error.ConnectionResetByPeer => switch (comptime builtin.target.os.tag) {
                                        .windows => break :blk std.os.windows.ws2_32.INVALID_SOCKET,
                                        else => break :blk 0,
                                    },
                                    else => {
                                        log.debug("accept failed: {}", .{e});
                                        switch (comptime builtin.target.os.tag) {
                                            .windows => break :blk std.os.windows.ws2_32.INVALID_SOCKET,
                                            else => break :blk -1,
                                        }
                                    },
                                }
                            };

                            break :blk accept_result;
                        };

                        com_ptr.result = .{ .socket = res };
                        com_ptr.task = job.task;
                        _ = loop.inner.swapRemove(i);
                        i -|= 1;
                        reaped += 1;
                    },
                    .connect => |addr| {
                        const com_ptr = &self.completions[reaped];

                        const addr_len: std.posix.socklen_t = switch (addr.family) {
                            std.posix.AF.INET => @sizeOf(std.posix.sockaddr.in),
                            std.posix.AF.INET6 => @sizeOf(std.posix.sockaddr.in6),
                            std.posix.AF.UNIX => @sizeOf(std.posix.sockaddr.un),
                            else => @panic("Unsupported!"),
                        };

                        const res: std.posix.fd_t = blk: {
                            _ = std.posix.connect(job.fd, &addr, addr_len) catch |e| {
                                switch (e) {
                                    error.WouldBlock => continue,
                                    error.ConnectionResetByPeer => switch (comptime builtin.target.os.tag) {
                                        .windows => break :blk std.os.windows.ws2_32.INVALID_SOCKET,
                                        else => break :blk 0,
                                    },
                                    else => {
                                        log.debug("connect failed: {}", .{e});
                                        switch (comptime builtin.target.os.tag) {
                                            .windows => break :blk std.os.windows.ws2_32.INVALID_SOCKET,
                                            else => break :blk -1,
                                        }
                                    },
                                }
                            };

                            break :blk job.fd;
                        };

                        com_ptr.result = .{ .fd = res };
                        com_ptr.task = job.task;
                        _ = loop.inner.swapRemove(i);
                        i -|= 1;
                        reaped += 1;
                    },
                    .recv => |buffer| {
                        const com_ptr = &self.completions[reaped];
                        const len: i32 = blk: {
                            const read_len = std.posix.recv(job.fd, buffer, 0) catch |e| {
                                switch (e) {
                                    error.WouldBlock => continue,
                                    error.ConnectionResetByPeer => break :blk 0,
                                    else => {
                                        log.debug("recv failed: {}", .{e});
                                        break :blk -1;
                                    },
                                }
                            };

                            break :blk @intCast(read_len);
                        };

                        com_ptr.result = .{ .value = @intCast(len) };
                        com_ptr.task = job.task;
                        _ = loop.inner.swapRemove(i);
                        i -|= 1;
                        reaped += 1;
                    },
                    .send => |buffer| {
                        const com_ptr = &self.completions[reaped];
                        const len: i32 = blk: {
                            const send_len = std.posix.send(job.fd, buffer, 0) catch |e| {
                                switch (e) {
                                    error.WouldBlock => continue,
                                    error.ConnectionResetByPeer => break :blk 0,
                                    else => {
                                        log.debug("send failed: {}", .{e});
                                        break :blk -1;
                                    },
                                }
                            };

                            break :blk @intCast(send_len);
                        };

                        com_ptr.result = .{ .value = @intCast(len) };
                        com_ptr.task = job.task;
                        _ = loop.inner.swapRemove(i);
                        i -|= 1;
                        reaped += 1;
                    },
                }
            }

            first_run = false;
        }

        return self.completions[0..reaped];
    }

    pub fn to_async(self: *AsyncBusyLoop) AsyncIO {
        return AsyncIO{
            .runner = self,
            ._deinit = deinit,
            ._queue_open = queue_open,
            ._queue_read = queue_read,
            ._queue_write = queue_write,
            ._queue_close = queue_close,
            ._queue_accept = queue_accept,
            ._queue_connect = queue_connect,
            ._queue_recv = queue_recv,
            ._queue_send = queue_send,
            ._submit = submit,
            ._reap = reap,
        };
    }
};
