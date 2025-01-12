const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.@"tardy/fs/dir");
const builtin = @import("builtin");

const Runtime = @import("../runtime/lib.zig").Runtime;
const TaskFn = @import("../runtime/task.zig").TaskFn;

const Path = @import("lib.zig").Path;
const File = @import("lib.zig").File;

const FileMode = @import("../aio/lib.zig").FileMode;
const AioOpenFlags = @import("../aio/lib.zig").AioOpenFlags;

const Resulted = @import("../aio/completion.zig").Resulted;
const OpenFileResult = @import("../aio/completion.zig").OpenFileResult;
const OpenDirResult = @import("../aio/completion.zig").OpenDirResult;
const OpenError = @import("../aio/completion.zig").OpenError;

const DeleteError = @import("../aio/completion.zig").DeleteError;
const DeleteResult = @import("../aio/completion.zig").DeleteResult;
const StatResult = @import("../aio/completion.zig").StatResult;
const ReadResult = @import("../aio/completion.zig").ReadResult;
const WriteResult = @import("../aio/completion.zig").WriteResult;

pub const DeleteTreeError = OpenError || DeleteError || error{InternalFailure};
pub const DeleteTreeResult = Resulted(void, DeleteTreeError);

pub const Dir = struct {
    handle: std.posix.fd_t,

    /// Create a std.fs.Dir from a Dir.
    pub fn to_std(self: Dir) std.fs.Dir {
        return std.fs.Dir{ .fd = self.handle };
    }

    /// Create a Dir from the std.fs.Dir
    pub fn from_std(self: std.fs.Dir) Dir {
        return .{ .handle = self.fd };
    }

    /// Create a Directory.
    pub fn create(
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(OpenDirResult, @TypeOf(task_ctx)),
        path: Path,
    ) !void {
        const index = try rt.scheduler.spawn(OpenDirResult, task_ctx, task_fn, .waiting);
        try rt.aio.queue_mkdir(index, path);
    }

    /// Open a Directory.
    pub fn open(
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(OpenDirResult, @TypeOf(task_ctx)),
        path: Path,
    ) !void {
        const aio_flags: AioOpenFlags = .{
            .mode = .read,
            .create = false,
            .directory = true,
        };

        try rt.scheduler.spawn2(
            OpenDirResult,
            task_ctx,
            task_fn,
            .waiting,
            .{ .open = .{
                .path = path,
                .flags = aio_flags,
            } },
        );
    }

    /// Create a File relative to this Dir.
    pub fn create_file(
        self: *const Dir,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(OpenFileResult, @TypeOf(task_ctx)),
        subpath: [:0]const u8,
        flags: File.CreateFlags,
    ) !void {
        try File.create(rt, task_ctx, task_fn, .{ .rel = .{ .dir = self.handle, .path = subpath } }, flags);
    }

    /// Open a File relative to this Dir.
    pub fn open_file(
        self: *const Dir,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(OpenFileResult, @TypeOf(task_ctx)),
        subpath: [:0]const u8,
        flags: File.OpenFlags,
    ) !void {
        try File.open(rt, task_ctx, task_fn, .{ .rel = .{ .dir = self.handle, .path = subpath } }, flags);
    }

    /// Create a Dir relative to this Dir.
    pub fn create_dir(
        self: *const Dir,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(OpenDirResult, @TypeOf(task_ctx)),
        subpath: [:0]const u8,
    ) !void {
        try Dir.create(rt, task_ctx, task_fn, .{ .rel = .{ .dir = self.handle, .path = subpath } });
    }

    /// Open a Dir relative to this Dir.
    pub fn open_dir(
        self: *const Dir,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(OpenDirResult, @TypeOf(task_ctx)),
        subpath: [:0]const u8,
    ) !void {
        try Dir.open(rt, task_ctx, task_fn, .{ .rel = .{ .dir = self.handle, .path = subpath } });
    }

    /// Get Stat information of this Dir.
    pub fn stat(
        self: *const Dir,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(StatResult, @TypeOf(task_ctx)),
    ) !void {
        const index = try rt.scheduler.spawn(StatResult, task_ctx, task_fn, .waiting);
        try rt.aio.queue_stat(index, self.handle);
    }

    /// TODO: This needs to basically walk through the directory. This will end up
    /// having to be blocking?
    ///
    /// Might as well just use the std lib walker then and queue ops from there?
    pub fn walk(self: *const Dir, allocator: std.mem.Allocator) !void {
        _ = self;
        _ = allocator;
        @panic("Not implemented yet");
    }

    /// Delete a File within this Dir.
    pub fn delete_file(
        self: *const Dir,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(DeleteResult, @TypeOf(task_ctx)),
        sub_path: [:0]const u8,
    ) !void {
        try rt.scheduler.spawn2(
            DeleteResult,
            task_ctx,
            task_fn,
            .waiting,
            .{
                .delete = .{
                    .path = .{ .rel = .{ .dir = self.handle, .path = sub_path } },
                    .is_dir = false,
                },
            },
        );
    }

    /// Delete a Dir within this Dir.
    pub fn delete_dir(
        self: *const Dir,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(DeleteResult, @TypeOf(task_ctx)),
        sub_path: [:0]const u8,
    ) !void {
        try rt.scheduler.spawn2(
            DeleteResult,
            task_ctx,
            task_fn,
            .waiting,
            .{
                .delete = .{
                    .path = .{ .rel = .{ .dir = self.handle, .path = sub_path } },
                    .is_dir = true,
                },
            },
        );
    }

    pub fn delete_tree(
        self: *const Dir,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(DeleteTreeResult, @TypeOf(task_ctx)),
        sub_path: [:0]const u8,
    ) !void {
        const StackItem = struct {
            name: [:0]const u8,
            dir: Dir,
            iter: std.fs.Dir.Iterator,
        };

        const Provision = struct {
            const Self = @This();
            // this is a clone of the original `self`.
            root: Dir,
            root_subpath: [:0]const u8,
            stack: std.ArrayListUnmanaged(StackItem),
            current_path: [:0]const u8,

            // all fundamental error handling is up a level from this... :)
            fn process_next_entry(runtime: *Runtime, p: *Self) !void {
                assert(p.stack.items.len > 0);

                const top = &p.stack.items[p.stack.items.len - 1];
                const dir = top.dir;
                log.debug("processing top of stack... (name={s})", .{top.name});

                if (try top.iter.next()) |entry| {
                    log.debug("entry in stack={s}", .{entry.name});
                    p.current_path = try runtime.allocator.dupeZ(u8, entry.name);
                    switch (entry.kind) {
                        .directory => try dir.open_dir(runtime, p, process_dir_task, p.current_path),
                        else => {
                            log.debug("deleting {s} from current layer", .{p.current_path});
                            try dir.delete_file(runtime, p, delete_task, p.current_path);
                        },
                    }
                } else {
                    const current = p.stack.pop();
                    log.debug("top of stack is empty, popping {s}", .{current.name});
                    log.debug("stack length: {d}", .{p.stack.items.len});

                    if (p.stack.items.len > 0) {
                        const parent = p.stack.items[p.stack.items.len - 1].dir;
                        p.current_path = current.name;
                        try parent.delete_dir(runtime, p, delete_task, current.name);
                    } else {
                        log.debug("deleting the root dir @ {s}", .{current.name});
                        p.root_subpath = current.name;
                        try p.root.delete_dir(runtime, p, delete_root_task, p.root_subpath);
                    }
                }
            }

            fn clean_on_error(runtime: *Runtime, p: *Self) void {
                for (p.stack.items) |item| {
                    runtime.allocator.free(item.name);
                    std.posix.close(item.dir.handle);
                }

                p.stack.deinit(runtime.allocator);
                runtime.allocator.destroy(p);
            }

            fn open_dir_task(runtime: *Runtime, res: OpenDirResult, p: *Self) !void {
                const dir = res.unwrap() catch |e| {
                    runtime.allocator.destroy(p);
                    try task_fn(runtime, .{ .err = @errorCast(e) }, task_ctx);
                    return e;
                };

                const std_dir = dir.to_std();
                const iter = std_dir.iterate();

                p.stack = try std.ArrayListUnmanaged(StackItem).initCapacity(runtime.allocator, 0);
                errdefer clean_on_error(runtime, p);

                log.debug("starting the delete tree at {s}", .{p.current_path});
                p.stack.append(runtime.allocator, .{
                    .name = try runtime.allocator.dupeZ(u8, p.current_path),
                    .dir = dir,
                    .iter = iter,
                }) catch |e| {
                    try task_fn(runtime, .{ .err = error.InternalFailure }, task_ctx);
                    return e;
                };

                process_next_entry(runtime, p) catch |e| {
                    try task_fn(runtime, .{ .err = error.InternalFailure }, task_ctx);
                    return e;
                };
            }

            fn process_dir_task(runtime: *Runtime, res: OpenDirResult, p: *Self) !void {
                errdefer clean_on_error(runtime, p);

                const new_dir = res.unwrap() catch |e| {
                    try task_fn(runtime, .{ .err = @errorCast(e) }, task_ctx);
                    return e;
                };
                const new_std_dir = new_dir.to_std();
                const new_iter = new_std_dir.iterate();

                log.debug("appending new dir to the stack... {s}", .{p.current_path});
                p.stack.append(runtime.allocator, .{
                    .name = p.current_path,
                    .dir = new_dir,
                    .iter = new_iter,
                }) catch |e| {
                    try task_fn(runtime, .{ .err = error.InternalFailure }, task_ctx);
                    return e;
                };

                process_next_entry(runtime, p) catch |e| {
                    try task_fn(runtime, .{ .err = error.InternalFailure }, task_ctx);
                    return e;
                };
            }

            fn delete_task(runtime: *Runtime, result: DeleteResult, p: *Self) !void {
                errdefer clean_on_error(runtime, p);

                result.unwrap() catch |e| {
                    try task_fn(runtime, .{ .err = @errorCast(e) }, task_ctx);
                    return e;
                };
                log.debug("deleted path={s}", .{p.current_path});
                runtime.allocator.free(p.current_path);
                process_next_entry(runtime, p) catch |e| {
                    try task_fn(runtime, .{ .err = error.InternalFailure }, task_ctx);
                    return e;
                };
            }

            fn delete_root_task(runtime: *Runtime, result: DeleteResult, p: *Self) !void {
                defer runtime.allocator.destroy(p);
                defer p.stack.deinit(runtime.allocator);
                defer runtime.allocator.free(p.root_subpath);

                result.unwrap() catch |e| {
                    try task_fn(runtime, .{ .err = @errorCast(e) }, task_ctx);
                    return e;
                };

                log.debug("deleted tree root @ {s}", .{p.root_subpath});
                try task_fn(runtime, .{ .actual = {} }, task_ctx);
            }
        };

        const p = try rt.allocator.create(Provision);
        errdefer rt.allocator.destroy(p);
        p.root = self.*;
        p.current_path = sub_path;
        try self.open_dir(rt, p, Provision.open_dir_task, sub_path);
    }

    /// Close the underlying Handle of this Dir.
    pub fn close(
        self: *const Dir,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(void, @TypeOf(task_ctx)),
    ) !void {
        const index = try rt.scheduler.spawn(void, task_ctx, task_fn, .waiting);
        try rt.aio.queue_close(index, self.handle);
    }
};
