const std = @import("std");

pub const File = @import("file.zig").File;
pub const Dir = @import("dir.zig").Dir;

pub const Path = union(enum) {
    /// Relative to given Directory
    rel: struct {
        dir: std.posix.fd_t,
        path: [:0]const u8,
    },
    /// Absolute Path
    abs: [:0]const u8,

    pub fn dupe(self: *const Path, allocator: std.mem.Allocator) !Path {
        switch (self.*) {
            .rel => |inner| {
                const path_dupe = try allocator.dupeZ(u8, inner.path);
                errdefer allocator.free(path_dupe);
                return .{ .rel = .{ .dir = inner.dir, .path = path_dupe } };
            },
            .abs => |path| return .{ .abs = try allocator.dupeZ(u8, path) },
        }
    }
};

const Timespec = @import("../lib.zig").Timespec;
pub const Stat = struct {
    size: u64,
    mode: u64 = 0,
    accessed: ?Timespec = null,
    modified: ?Timespec = null,
    changed: ?Timespec = null,
};
