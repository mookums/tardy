const std = @import("std");
const assert = std.debug.assert;

const Runtime = @import("../runtime/lib.zig").Runtime;
const Task = @import("../runtime/task.zig").Task;

const FileOpenFn = *const fn (File, *Runtime, *const Task, ?*anyopaque) anyerror!void;
const FileReadFn = *const fn ([]const u8, File, *Runtime, *const Task, ?*anyopaque) anyerror!void;
const FileWriteFn = *const fn (usize, File, *Runtime, *const Task, ?*anyopaque) anyerror!void;
const FileCloseFn = *const fn (*Runtime, *const Task, ?*anyopaque) anyerror!void;

/// A higher level API for interacting with Files.
///
/// If you are seeking maximum performance, it is better to utilize
/// `Runtime.fs` and manually handle File operations.
pub const File = struct {
    runtime: *Runtime,
    handle: std.posix.socket_t,
    context: *FileContext,
    closed: bool = false,
    read_count: usize = 0,
    write_count: usize = 0,

    pub fn init(runtime: *Runtime, handle: std.posix.socket_t) File {
        return .{
            .runtime = runtime,
            .handle = handle,
            .context = undefined,
            .closed = false,
            .read_count = 0,
            .write_count = 0,
        };
    }

    pub fn deinit(self: File) void {
        self.runtime.allocator.destroy(self.context);
    }

    /// Converts the `std.fs.File` type from the standard library into the Tardy version.
    pub fn from_std(runtime: *Runtime, file: std.fs.File) File {
        var new_file: File = .{
            .runtime = runtime,
            .handle = file.handle,
            .context = undefined,
            .closed = false,
            .read_count = 0,
            .write_count = 0,
        };

        const context: *FileContext = try runtime.allocator.create(FileContext);
        context.* = .{
            .func = undefined,
            .ctx = undefined,
            .file = new_file,
        };

        new_file.context = context;
        return new_file;
    }

    /// Converts the Tardy version into the `std.fs.File` type from the standard library.
    pub fn to_std(file: File) std.fs.File {
        return .{ .handle = file.handle };
    }

    const FileContext = struct {
        func: union(enum) {
            open: FileOpenFn,
            read: FileReadFn,
            write: FileWriteFn,
            close: FileCloseFn,
        },
        ctx: ?*anyopaque = null,
        buffer: []u8 = undefined,
        file: File = undefined,
    };

    fn file_open_task(rt: *Runtime, t: *const Task, ctx: ?*anyopaque) !void {
        var file: File = File.init(rt, t.result.?.fd);
        const context: *FileContext = @ptrCast(@alignCast(ctx.?));
        file.context = context;
        context.file = file;
        try @call(.auto, context.func.open, .{ context.file, rt, t, context.ctx });
    }

    pub fn open(
        path: [:0]const u8,
        runtime: *Runtime,
        ctx: ?*anyopaque,
        then: *const fn (File, *Runtime, *const Task, ?*anyopaque) anyerror!void,
    ) !void {
        const context: *FileContext = try runtime.allocator.create(FileContext);
        context.* = .{ .func = .{ .open = then }, .ctx = ctx };

        try runtime.fs.open(.{
            .path = path,
            .func = file_open_task,
            .ctx = context,
        });
    }

    fn file_read_task(rt: *Runtime, t: *const Task, ctx: ?*anyopaque) !void {
        const result: i32 = t.result.?.value;
        const context: *FileContext = @ptrCast(@alignCast(ctx.?));
        const length: usize = if (result >= 0) @intCast(result) else 0;
        context.file.read_count += length;

        try @call(
            .auto,
            context.func.read,
            .{ context.buffer[0..length], context.file, rt, t, context.ctx },
        );
    }

    pub fn read(
        self: File,
        buffer: []u8,
        ctx: ?*anyopaque,
        then: *const fn ([]const u8, *Runtime, *const Task, ?*anyopaque) anyerror!void,
    ) !void {
        assert(!self.closed);
        const context: *FileContext = self.context;
        context.func = .{ .read = then };
        context.ctx = ctx;
        context.buffer = buffer;
        context.file = self;

        try self.runtime.fs.read(.{
            .fd = self.handle,
            .buffer = buffer,
            .offset = self.read_count,
            .func = file_read_task,
            .ctx = context,
        });
    }

    fn file_write_task(rt: *Runtime, t: *const Task, ctx: ?*anyopaque) !void {
        const result: i32 = t.result.?.value;
        const context: *FileContext = @ptrCast(@alignCast(ctx.?));
        const length: usize = if (result >= 0) @intCast(result) else 0;
        context.file.write_count += length;

        try @call(
            .auto,
            context.func.write,
            .{ length, context.file, rt, t, context.ctx },
        );
    }

    pub fn write(
        self: File,
        buffer: []const u8,
        ctx: ?*anyopaque,
        then: *const fn (usize, File, *Runtime, *const Task, ?*anyopaque) anyerror!void,
    ) !void {
        assert(!self.closed);
        const context: *FileContext = self.context;
        context.* = .{
            .file = self,
            .func = then,
            .ctx = ctx,
        };

        try self.runtime.fs.write(.{
            .fd = self.handle,
            .buffer = buffer,
            .offset = self.write_count,
            .func = file_write_task,
            .ctx = context,
        });
    }

    fn file_close_task(rt: *Runtime, t: *const Task, ctx: ?*anyopaque) !void {
        const context: *FileContext = @ptrCast(@alignCast(ctx.?));
        defer rt.allocator.destroy(context);
        try @call(.auto, context.func.close, .{ rt, t, context.ctx });
    }

    pub fn close(
        self: File,
        ctx: ?*anyopaque,
        then: *const fn (*Runtime, *const Task, ?*anyopaque) anyerror!void,
    ) !void {
        assert(!self.closed);
        const context: *FileContext = self.context;
        context.func = .{ .close = then };
        context.ctx = ctx;
        context.file = self;
        context.file.closed = true;

        try self.runtime.fs.close(.{
            .fd = self.handle,
            .func = file_close_task,
            .ctx = context,
        });
    }
};
