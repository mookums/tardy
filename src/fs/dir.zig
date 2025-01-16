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
const DeleteTreeResult = @import("../aio/completion.zig").DeleteTreeResult;

const StatResult = @import("../aio/completion.zig").StatResult;
const ReadResult = @import("../aio/completion.zig").ReadResult;
const WriteResult = @import("../aio/completion.zig").WriteResult;

const CreateDirResult = @import("../aio/completion.zig").CreateDirResult;
const MkdirResult = @import("../aio/completion.zig").MkdirResult;
const MkdirError = @import("../aio/completion.zig").MkdirError;

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

    /// Get `cwd` as a Dir.
    pub fn cwd() Dir {
        return .{ .handle = std.fs.cwd().fd };
    }

    /// Creates and opens a Directory.
    pub fn create(
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(CreateDirResult, @TypeOf(task_ctx)),
        path: Path,
    ) !void {
        const Provision = struct {
            const Self = @This();
            path: Path,
            task_ctx: @TypeOf(task_ctx),

            pub fn create_task(runtime: *Runtime, res: MkdirResult, p: *Self) !void {
                {
                    errdefer runtime.allocator.destroy(p);

                    _ = res.unwrap() catch |e| {
                        try task_fn(runtime, .{ .err = @errorCast(e) }, p.task_ctx);
                        return e;
                    };
                }

                try Dir.open(runtime, p, open_task, p.path);
            }

            pub fn open_task(runtime: *Runtime, res: OpenDirResult, p: *Self) !void {
                defer runtime.allocator.destroy(p);

                const dir = res.unwrap() catch |e| {
                    try task_fn(runtime, .{ .err = @errorCast(e) }, p.task_ctx);
                    return e;
                };

                try task_fn(runtime, .{ .actual = dir }, p.task_ctx);
            }
        };

        const provision = try rt.allocator.create(Provision);
        provision.* = .{ .path = path, .task_ctx = task_ctx };
        errdefer rt.allocator.destroy(provision);

        try rt.scheduler.spawn(
            MkdirResult,
            provision,
            Provision.create_task,
            .wait_for_io,
            .{ .mkdir = .{ .path = path, .mode = 0o755 } },
        );
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

        try rt.scheduler.spawn(
            OpenDirResult,
            task_ctx,
            task_fn,
            .wait_for_io,
            .{ .open = .{
                .path = path,
                .flags = aio_flags,
            } },
        );
    }

    /// Create a File relative to this Dir.
    pub fn create_file(
        self: Dir,
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
        self: Dir,
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
        self: Dir,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(CreateDirResult, @TypeOf(task_ctx)),
        subpath: [:0]const u8,
    ) !void {
        try Dir.create(rt, task_ctx, task_fn, .{ .rel = .{ .dir = self.handle, .path = subpath } });
    }

    /// Open a Dir relative to this Dir.
    pub fn open_dir(
        self: Dir,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(OpenDirResult, @TypeOf(task_ctx)),
        subpath: [:0]const u8,
    ) !void {
        try Dir.open(rt, task_ctx, task_fn, .{ .rel = .{ .dir = self.handle, .path = subpath } });
    }

    /// Get Stat information of this Dir.
    pub fn stat(
        self: Dir,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(StatResult, @TypeOf(task_ctx)),
    ) !void {
        try rt.scheduler.spawn(StatResult, task_ctx, task_fn, .wait_for_io, .{ .stat = self.handle });
    }

    /// TODO: This needs to basically walk through the directory. This will end up
    /// having to be blocking?
    ///
    /// Might as well just use the std lib walker then and queue ops from there?
    ///
    /// We can write our own with the Iterator API. This would be awkward though
    /// as we'd have to do walking callbacks weird.
    pub fn walk(self: *const Dir, allocator: std.mem.Allocator) !void {
        _ = self;
        _ = allocator;
        @panic("Not implemented yet");
    }

    /// Delete a File within this Dir.
    pub fn delete_file(
        self: Dir,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(DeleteResult, @TypeOf(task_ctx)),
        sub_path: [:0]const u8,
    ) !void {
        try rt.scheduler.spawn(
            DeleteResult,
            task_ctx,
            task_fn,
            .wait_for_io,
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
        try rt.scheduler.spawn(
            DeleteResult,
            task_ctx,
            task_fn,
            .wait_for_io,
            .{
                .delete = .{
                    .path = .{ .rel = .{ .dir = self.handle, .path = sub_path } },
                    .is_dir = true,
                },
            },
        );
    }

    /// This will iterate through the Directory at the path given,
    /// deleting all files within it and then deleting the Directory.
    ///
    /// This does allocate within it using the `rt.allocator`.
    pub fn delete_tree(
        self: *const Dir,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(DeleteTreeResult, @TypeOf(task_ctx)),
        sub_path: [:0]const u8,
        comptime max_stack_depth: usize,
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
            task_ctx: @TypeOf(task_ctx),
            stack: [max_stack_depth]StackItem = undefined,
            // This represents the next free slot.
            stack_len: usize = 0,
            current_path_buf: [std.fs.max_path_bytes]u8 = undefined,
            current_path: [:0]const u8,

            inline fn stack_peek(p: *Self) *StackItem {
                assert(p.stack_len > 0);
                return &p.stack[p.stack_len - 1];
            }

            inline fn stack_pop(p: *Self) StackItem {
                assert(p.stack_len > 0);
                p.stack_len -= 1;
                return p.stack[p.stack_len];
            }

            inline fn stack_push(p: *Self, item: StackItem) void {
                assert(p.stack_len < p.stack.len);
                p.stack[p.stack_len] = item;
                p.stack_len += 1;
            }

            inline fn copy_to_current_path(p: *Self, path: []const u8) void {
                const len = path.len;
                assert(len < p.current_path_buf.len - 1);
                std.mem.copyForwards(u8, p.current_path_buf[0..len], path[0..len]);
                p.current_path_buf[len] = 0;
                p.current_path = p.current_path_buf[0..len :0];
            }

            fn process_next_entry(runtime: *Runtime, p: *Self) !void {
                assert(p.stack_len > 0);

                const top = p.stack_peek();
                const dir = top.dir;
                log.debug("processing top of stack... (name={s})", .{top.name});

                if (try top.iter.next()) |entry| {
                    log.debug("entry in stack={s}", .{entry.name});

                    copy_to_current_path(p, entry.name);
                    log.debug("current path: {s}", .{p.current_path});

                    switch (entry.kind) {
                        .directory => try dir.open_dir(runtime, p, process_dir_task, p.current_path),
                        else => {
                            log.debug("deleting {s} from current layer", .{p.current_path});
                            try dir.delete_file(runtime, p, delete_task, p.current_path);
                        },
                    }
                } else {
                    const current = p.stack_pop();
                    defer runtime.allocator.free(current.name);
                    // once its popped off, it has been deleted.
                    // that means we can safely close the fd :)
                    defer std.posix.close(current.dir.handle);

                    copy_to_current_path(p, current.name);
                    log.debug("top of stack is empty, popping {s}", .{p.current_path});
                    log.debug("stack length: {d}", .{p.stack_len});

                    if (p.stack_len > 0) {
                        const parent = p.stack_peek().dir;
                        try parent.delete_dir(runtime, p, delete_task, p.current_path);
                    } else {
                        log.debug("deleting the root dir @ {s}", .{p.current_path});
                        try p.root.delete_dir(runtime, p, delete_root_task, p.current_path);
                    }
                }
            }

            fn clean_on_error(runtime: *Runtime, p: *Self) void {
                for (p.stack[0..p.stack_len]) |item| {
                    runtime.allocator.free(item.name);
                    std.posix.close(item.dir.handle);
                }

                runtime.allocator.destroy(p);
            }

            fn open_dir_task(runtime: *Runtime, res: OpenDirResult, p: *Self) !void {
                const dir = res.unwrap() catch |e| {
                    runtime.allocator.destroy(p);
                    try task_fn(runtime, .{ .err = @errorCast(e) }, p.task_ctx);
                    return e;
                };

                const std_dir = dir.to_std();
                const iter = std_dir.iterate();

                errdefer clean_on_error(runtime, p);

                log.debug("starting the delete tree at {s}", .{p.current_path});

                const current_path_copy = try runtime.allocator.dupeZ(u8, p.current_path);
                errdefer runtime.allocator.free(current_path_copy);
                p.stack_push(.{
                    .name = current_path_copy,
                    .dir = dir,
                    .iter = iter,
                });

                process_next_entry(runtime, p) catch |e| {
                    try task_fn(runtime, .{ .err = error.InternalFailure }, p.task_ctx);
                    return e;
                };
            }

            fn process_dir_task(runtime: *Runtime, res: OpenDirResult, p: *Self) !void {
                errdefer clean_on_error(runtime, p);

                const new_dir = res.unwrap() catch |e| {
                    try task_fn(runtime, .{ .err = @errorCast(e) }, p.task_ctx);
                    return e;
                };
                const new_std_dir = new_dir.to_std();
                const new_iter = new_std_dir.iterate();

                log.debug("appending new dir to the stack... {s}", .{p.current_path});

                const current_path_copy = try runtime.allocator.dupeZ(u8, p.current_path);
                errdefer runtime.allocator.free(current_path_copy);
                p.stack_push(.{
                    .name = current_path_copy,
                    .dir = new_dir,
                    .iter = new_iter,
                });

                process_next_entry(runtime, p) catch |e| {
                    try task_fn(runtime, .{ .err = error.InternalFailure }, p.task_ctx);
                    return e;
                };
            }

            fn delete_task(runtime: *Runtime, result: DeleteResult, p: *Self) !void {
                errdefer clean_on_error(runtime, p);

                result.unwrap() catch |e| {
                    try task_fn(runtime, .{ .err = @errorCast(e) }, p.task_ctx);
                    return e;
                };

                log.debug("deleted path={s}", .{p.current_path});

                process_next_entry(runtime, p) catch |e| {
                    try task_fn(runtime, .{ .err = error.InternalFailure }, p.task_ctx);
                    return e;
                };
            }

            fn delete_root_task(runtime: *Runtime, result: DeleteResult, p: *Self) !void {
                defer runtime.allocator.destroy(p);

                result.unwrap() catch |e| {
                    try task_fn(runtime, .{ .err = @errorCast(e) }, p.task_ctx);
                    return e;
                };

                log.debug("deleted tree root @ {s}", .{p.current_path});
                try task_fn(runtime, .{ .actual = {} }, p.task_ctx);
            }
        };

        const p = try rt.allocator.create(Provision);
        errdefer rt.allocator.destroy(p);
        p.* = .{ .root = self.*, .current_path = sub_path, .task_ctx = task_ctx };

        try self.open_dir(rt, p, Provision.open_dir_task, sub_path);
    }

    /// Close the underlying Handle of this Dir.
    pub fn close(
        self: *const Dir,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(void, @TypeOf(task_ctx)),
    ) !void {
        try rt.scheduler.spawn(void, task_ctx, task_fn, .wait_for_io, .{ .close = self.handle });
    }

    pub fn close_blocking(self: *const Dir) void {
        std.posix.close(self.handle);
    }
};
