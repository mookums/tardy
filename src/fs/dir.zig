const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.@"tardy/fs/dir");
const builtin = @import("builtin");

const Runtime = @import("../runtime/lib.zig").Runtime;
const TaskFn = @import("../runtime/task.zig").TaskFn;

const Path = @import("lib.zig").Path;
const File = @import("lib.zig").File;
const Stat = @import("lib.zig").Stat;

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

    const OpenAction = struct {
        path: Path,

        const flags: AioOpenFlags = .{
            .mode = .read,
            .create = false,
            .directory = true,
        };

        pub fn resolve(self: *const OpenAction, rt: *Runtime) !Dir {
            try rt.scheduler.frame_await(.{ .open = .{ .path = self.path, .flags = flags } });

            const index = rt.current_task orelse unreachable;
            const task = rt.scheduler.tasks.get_ptr(index);

            const result: OpenDirResult = switch (task.result) {
                .open => |inner| switch (inner) {
                    .actual => |actual| .{ .actual = actual.dir },
                    .err => |err| .{ .err = err },
                },
                else => unreachable,
            };

            return try result.unwrap();
        }

        pub fn callback(
            self: *const OpenAction,
            rt: *Runtime,
            task_ctx: anytype,
            comptime task_fn: TaskFn(OpenDirResult, @TypeOf(task_ctx)),
        ) !void {
            try rt.scheduler.spawn(
                OpenDirResult,
                task_ctx,
                task_fn,
                .wait_for_io,
                .{ .open = .{
                    .path = self.path,
                    .flags = flags,
                } },
            );
        }
    };

    /// Open a Directory.
    pub fn open(path: Path) OpenAction {
        return .{ .path = path };
    }

    const CreateAction = struct {
        path: Path,

        pub fn resolve(self: *const CreateAction, rt: *Runtime) !Dir {
            try rt.scheduler.frame_await(.{ .mkdir = .{ .path = self.path, .mode = 0o775 } });

            const index = rt.current_task orelse unreachable;
            const task = rt.scheduler.tasks.get_ptr(index);
            try task.result.mkdir.unwrap();

            try Dir.open(self.path).resolve(rt);
        }

        pub fn callback(
            self: *const CreateAction,
            rt: *Runtime,
            task_ctx: anytype,
            comptime task_fn: TaskFn(CreateDirResult, @TypeOf(task_ctx)),
        ) !void {
            const Provision = struct {
                const Self = @This();
                path: Path,
                task_ctx: @TypeOf(task_ctx),

                pub fn create_task(runtime: *Runtime, res: MkdirResult, p: *Self) !void {
                    {
                        errdefer runtime.allocator.destroy(p);

                        res.unwrap() catch |e| {
                            try task_fn(runtime, .{ .err = @errorCast(e) }, p.task_ctx);
                            return e;
                        };
                    }

                    try Dir.open(p.path).callback(runtime, p, open_task);
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
            provision.* = .{ .path = self.path, .task_ctx = task_ctx };
            errdefer rt.allocator.destroy(provision);

            try rt.scheduler.spawn(
                MkdirResult,
                provision,
                Provision.create_task,
                .wait_for_io,
                .{ .mkdir = .{ .path = self.path, .mode = 0o755 } },
            );
        }
    };

    /// Creates and opens a Directory.
    pub fn create(path: Path) CreateAction {
        return .{ .path = path };
    }

    const CreateFileAction = struct {
        dir: Dir,
        subpath: [:0]const u8,
        flags: File.CreateFlags,

        pub fn resolve(self: *const CreateFileAction, rt: *Runtime) !File {
            return try File.create(
                .{ .rel = .{ .dir = self.dir.handle, .path = self.subpath } },
                self.flags,
            ).resolve(rt);
        }

        pub fn callback(
            self: *const CreateFileAction,
            rt: *Runtime,
            task_ctx: anytype,
            comptime task_fn: TaskFn(OpenFileResult, @TypeOf(task_ctx)),
        ) !void {
            try File.create(
                .{ .rel = .{ .dir = self.dir.handle, .path = self.subpath } },
                self.flags,
            ).callback(rt, task_ctx, task_fn);
        }
    };

    /// Create a File relative to this Dir.
    pub fn create_file(self: Dir, subpath: [:0]const u8, flags: File.CreateFlags) CreateFileAction {
        return .{ .dir = self, .subpath = subpath, .flags = flags };
    }

    const OpenFileAction = struct {
        dir: Dir,
        subpath: [:0]const u8,
        flags: File.OpenFlags,

        pub fn resolve(self: *const OpenFileAction, rt: *Runtime) !File {
            return try File.open(
                .{ .rel = .{ .dir = self.dir.handle, .path = self.subpath } },
                self.flags,
            ).resolve(rt);
        }

        pub fn callback(
            self: *const OpenFileAction,
            rt: *Runtime,
            task_ctx: anytype,
            comptime task_fn: TaskFn(OpenFileResult, @TypeOf(task_ctx)),
        ) !void {
            return try File.open(
                .{ .rel = .{ .dir = self.dir.handle, .path = self.subpath } },
                self.flags,
            ).callback(rt, task_ctx, task_fn);
        }
    };

    /// Open a File relative to this Dir.
    pub fn open_file(self: Dir, subpath: [:0]const u8, flags: File.OpenFlags) OpenFileAction {
        return .{ .dir = self, .subpath = subpath, .flags = flags };
    }

    const CreateDirAction = struct {
        dir: Dir,
        subpath: [:0]const u8,

        pub fn resolve(self: *const CreateDirAction, rt: *Runtime) !Dir {
            try Dir.create(rt, .{
                .rel = .{ .dir = self.dir.handle, .path = self.subpath },
            }).resolve(rt);
        }

        pub fn callback(
            self: *const CreateDirAction,
            rt: *Runtime,
            task_ctx: anytype,
            comptime task_fn: TaskFn(CreateDirResult, @TypeOf(task_ctx)),
        ) !void {
            try Dir.create(rt, .{
                .rel = .{ .dir = self.dir.handle, .path = self.subpath },
            }).callback(rt, task_ctx, task_fn);
        }
    };

    /// Create a Dir relative to this Dir.
    pub fn create_dir(self: Dir, subpath: [:0]const u8) CreateDirAction {
        return .{ .dir = self, .subpath = subpath };
    }

    const OpenDirAction = struct {
        dir: Dir,
        subpath: [:0]const u8,

        pub fn resolve(self: *const OpenDirAction, rt: *Runtime) !Dir {
            return try Dir.open(.{
                .rel = .{ .dir = self.dir.handle, .path = self.subpath },
            }).resolve(rt);
        }

        pub fn callback(
            self: *const OpenDirAction,
            rt: *Runtime,
            task_ctx: anytype,
            comptime task_fn: TaskFn(OpenDirResult, @TypeOf(task_ctx)),
        ) !void {
            return try Dir.open(.{
                .rel = .{ .dir = self.dir.handle, .path = self.subpath },
            }).callback(rt, task_ctx, task_fn);
        }
    };

    /// Open a Dir relative to this Dir.
    pub fn open_dir(self: Dir, subpath: [:0]const u8) OpenDirAction {
        return .{ .dir = self, .subpath = subpath };
    }

    const StatAction = struct {
        dir: Dir,

        pub fn resolve(self: *const StatAction, rt: *Runtime) !Stat {
            try rt.scheduler.frame_await(.{ .stat = self.dir.handle });

            const index = rt.current_task orelse unreachable;
            const task = rt.scheduler.tasks.get_ptr(index);
            return try task.result.stat.unwrap();
        }

        pub fn callback(
            self: *const StatAction,
            rt: *Runtime,
            task_ctx: anytype,
            comptime task_fn: TaskFn(StatResult, @TypeOf(task_ctx)),
        ) !void {
            try rt.scheduler.spawn(
                StatResult,
                task_ctx,
                task_fn,
                .wait_for_io,
                .{ .stat = self.handle },
            );
        }
    };

    /// Get Stat information of this Dir.
    pub fn stat(self: Dir) StatAction {
        return .{ .dir = self };
    }

    const DeleteFileAction = struct {
        dir: Dir,
        subpath: [:0]const u8,

        pub fn resolve(self: *const DeleteFileAction, rt: *Runtime) !void {
            try rt.scheduler.frame_await(.{ .delete = .{
                .path = .{ .rel = .{ .dir = self.dir.handle, .path = self.subpath } },
                .is_dir = false,
            } });
        }

        pub fn callback(
            self: *const DeleteFileAction,
            rt: *Runtime,
            task_ctx: anytype,
            comptime task_fn: TaskFn(DeleteResult, @TypeOf(task_ctx)),
        ) !void {
            try rt.scheduler.spawn(
                DeleteResult,
                task_ctx,
                task_fn,
                .wait_for_io,
                .{
                    .delete = .{
                        .path = .{ .rel = .{ .dir = self.dir.handle, .path = self.subpath } },
                        .is_dir = false,
                    },
                },
            );
        }
    };

    /// Delete a File within this Dir.
    pub fn delete_file(self: Dir, subpath: [:0]const u8) DeleteFileAction {
        return .{ .dir = self, .subpath = subpath };
    }

    const DeleteDirAction = struct {
        dir: Dir,
        subpath: [:0]const u8,

        pub fn resolve(self: *const DeleteDirAction, rt: *Runtime) !void {
            try rt.scheduler.frame_await(.{ .delete = .{
                .path = .{ .rel = .{ .dir = self.dir.handle, .path = self.subpath } },
                .is_dir = true,
            } });
        }

        pub fn callback(
            self: *const DeleteDirAction,
            rt: *Runtime,
            task_ctx: anytype,
            comptime task_fn: TaskFn(DeleteResult, @TypeOf(task_ctx)),
        ) !void {
            try rt.scheduler.spawn(
                DeleteResult,
                task_ctx,
                task_fn,
                .wait_for_io,
                .{
                    .delete = .{
                        .path = .{ .rel = .{ .dir = self.dir.handle, .path = self.subpath } },
                        .is_dir = true,
                    },
                },
            );
        }
    };

    /// Delete a Dir within this Dir.
    pub fn delete_dir(self: Dir, subpath: [:0]const u8) DeleteDirAction {
        return .{ .dir = self, .subpath = subpath };
    }

    const DeleteTreeAction = struct {
        dir: Dir,
        subpath: [:0]const u8,

        pub fn resolve(self: *const DeleteTreeAction, rt: *Runtime) !void {
            const base_dir = try self.dir.open_dir(self.subpath).resolve(rt);

            const base_std_dir = base_dir.to_std();
            var walker = try base_std_dir.walk(rt.allocator);
            defer walker.deinit();

            while (try walker.next()) |entry| {
                const new_dir = Dir.from_std(entry.dir);
                switch (entry.kind) {
                    .directory => try new_dir.delete_tree(entry.basename).resolve(rt),
                    else => try new_dir.delete_file(entry.basename).resolve(rt),
                }
            }

            try base_dir.close().resolve(rt);
            try self.dir.delete_dir(self.subpath).resolve(rt);
        }

        pub fn callback(
            self: *const DeleteTreeAction,
            rt: *Runtime,
            task_ctx: anytype,
            comptime task_fn: TaskFn(DeleteTreeResult, @TypeOf(task_ctx)),
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
                            .directory => try dir.open_dir(p.current_path).callback(runtime, p, process_dir_task),
                            else => {
                                log.debug("deleting {s} from current layer", .{p.current_path});
                                try dir.delete_file(p.current_path).callback(runtime, p, delete_task);
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
                            try parent.delete_dir(p.current_path).callback(runtime, p, delete_task);
                        } else {
                            log.debug("deleting the root dir @ {s}", .{p.current_path});
                            try p.root.delete_dir(p.current_path).callback(runtime, p, delete_root_task);
                        }
                    }
                }

                fn clean_on_error(runtime: *Runtime, p: *Self) void {
                    for (p.stack[0..p.stack_len]) |item| {
                        runtime.allocator.free(item.name);
                        item.dir.close_blocking();
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
            p.* = .{ .root = self.dir, .current_path = self.subpath, .task_ctx = task_ctx };

            try self.dir.open_dir(self.subpath).callback(rt, p, Provision.open_dir_task);
        }
    };

    /// This will iterate through the Directory at the path given,
    /// deleting all files within it and then deleting the Directory.
    ///
    /// This does allocate within it using the `rt.allocator`.
    pub fn delete_tree(
        self: Dir,
        subpath: [:0]const u8,
    ) DeleteTreeAction {
        return .{ .dir = self, .subpath = subpath };
    }

    const CloseAction = struct {
        dir: Dir,

        pub fn resolve(self: *const CloseAction, rt: *Runtime) !void {
            try rt.scheduler.frame_await(.{ .close = self.dir.handle });
        }

        pub fn callback(
            self: *const CloseAction,
            rt: *Runtime,
            task_ctx: anytype,
            comptime task_fn: TaskFn(void, @TypeOf(task_ctx)),
        ) !void {
            try rt.scheduler.spawn(void, task_ctx, task_fn, .wait_for_io, .{ .close = self.dir.handle });
        }
    };

    /// Close the underlying Handle of this Dir.
    pub fn close(self: Dir) CloseAction {
        return .{ .dir = self };
    }

    pub fn close_blocking(self: *const Dir) void {
        std.posix.close(self.handle);
    }
};
