const std = @import("std");
const assert = std.debug.assert;

const Runtime = @import("tardy").Runtime;

const Path = @import("tardy").Path;
const File = @import("tardy").File;
const Dir = @import("tardy").Dir;

const OpenFileResult = @import("tardy").OpenFileResult;
const ReadResult = @import("tardy").ReadResult;
const WriteResult = @import("tardy").WriteResult;
const DeleteResult = @import("tardy").DeleteResult;

const TaskFn = @import("tardy").TaskFn;

pub const FileChain = struct {
    const Step = enum {
        create,
        open,
        read,
        write,
        close,
        delete,
    };

    allocator: std.mem.Allocator,
    file: ?File = null,
    path: Path,
    steps: []Step,
    index: usize = 0,
    buffer: []u8,

    pub fn validate_chain(chain: []const Step) bool {
        var exists = false;
        var opened = false;
        for (chain) |*step| {
            switch (step.*) {
                .create => {
                    if (exists) return false;
                    exists = true;
                    opened = true;
                },
                .open => {
                    if (!exists or opened) return false;
                    opened = true;
                },
                .read => if (!exists or !opened) return false,
                .write => if (!exists or !opened) return false,
                .close => {
                    if (!opened) return false;
                    opened = false;
                },
                .delete => {
                    if (!exists) return false;
                    exists = false;
                },
            }
        }

        return (!exists and !opened);
    }

    pub fn generate_random_chain(allocator: std.mem.Allocator, seed: u64) ![]Step {
        var prng = std.Random.DefaultPrng.init(seed);
        const rand = prng.random();

        var list = try std.ArrayListUnmanaged(Step).initCapacity(allocator, 0);
        defer list.deinit(allocator);

        while (true) {
            const potentials = next_potential_entry(list.items);
            if (potentials.len == 0) break;
            const potential = rand.intRangeLessThan(usize, 0, potentials.len);
            try list.append(allocator, potentials[potential]);
        }

        assert(validate_chain(list.items));
        return try list.toOwnedSlice(allocator);
    }

    pub fn next_potential_entry(chain: []const Step) []const Step {
        if (chain.len == 0) return &.{.create};

        var exists = false;
        var opened = false;
        for (chain) |*step| {
            switch (step.*) {
                .create => {
                    assert(!exists);
                    exists = true;
                    opened = true;
                },
                .open => {
                    assert(exists and !opened);
                    opened = true;
                },
                .read => assert(exists and opened),
                .write => assert(exists and opened),
                .close => {
                    assert(opened);
                    opened = false;
                },
                .delete => {
                    assert(exists);
                    exists = false;
                    return &.{};
                },
            }
        }

        if (exists and opened) return &.{ .read, .write, .close };
        if (exists and !opened) return &.{.delete};
        unreachable;
    }

    // Path is expected to remain valid.
    pub fn init(allocator: std.mem.Allocator, chain: []const Step, path: Path, buffer_size: usize) !FileChain {
        assert(chain.len > 0);

        const chain_dupe = try allocator.dupe(Step, chain);
        errdefer allocator.free(chain_dupe);

        const path_dupe = try path.dupe(allocator);
        errdefer switch (path_dupe) {
            .rel => |inner| allocator.free(inner.path),
            .abs => |p| allocator.free(p),
        };

        assert(validate_chain(chain));

        const buffer = try allocator.alloc(u8, buffer_size);
        errdefer allocator.free(buffer);

        return .{
            .allocator = allocator,
            .steps = chain_dupe,
            .path = path_dupe,
            .buffer = buffer,
        };
    }

    pub fn deinit(self: *const FileChain) void {
        defer self.allocator.free(self.steps);
        defer self.allocator.free(self.buffer);
        defer switch (self.path) {
            .rel => |inner| self.allocator.free(inner.path),
            .abs => |p| self.allocator.free(p),
        };
    }

    pub fn run(
        self: *FileChain,
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(void, @TypeOf(task_ctx)),
    ) !void {
        const Provision = struct {
            const Self = @This();

            chain: *FileChain,
            task_ctx: @TypeOf(task_ctx),

            fn next_step(runtime: *Runtime, p: *Self) !void {
                switch (p.chain.steps[p.chain.index]) {
                    .create => try File.create(runtime, p, open_task, p.chain.path, .{ .mode = .read_write }),
                    .open => try File.open(runtime, p, open_task, p.chain.path, .{ .mode = .read_write }),
                    .read => try p.chain.file.?.read_all(runtime, p, read_task, p.chain.buffer, null),
                    .write => {
                        for (p.chain.buffer[0..]) |*item| item.* = 123;
                        try p.chain.file.?.write_all(runtime, p, write_task, p.chain.buffer, null);
                    },
                    .close => try p.chain.file.?.close(runtime, p, close_task),
                    .delete => {
                        const dir = Dir{ .handle = p.chain.path.rel.dir };
                        try dir.delete_file(runtime, p, delete_task, p.chain.path.rel.path);
                    },
                }
            }

            fn open_task(runtime: *Runtime, res: OpenFileResult, p: *Self) !void {
                errdefer unreachable;

                const step = p.chain.steps[p.chain.index];
                p.chain.index += 1;
                assert(step == .create or step == .open);

                const file = try res.unwrap();
                p.chain.file = file;

                try next_step(runtime, p);
            }

            fn read_task(runtime: *Runtime, res: ReadResult, p: *Self) !void {
                errdefer unreachable;

                const step = p.chain.steps[p.chain.index];
                p.chain.index += 1;
                assert(step == .read);

                // assert we wrote the correct thing.
                const length = try res.unwrap();
                for (p.chain.buffer[0..length]) |item| assert(item == 123);

                try next_step(runtime, p);
            }

            fn write_task(runtime: *Runtime, res: WriteResult, p: *Self) !void {
                errdefer unreachable;

                const step = p.chain.steps[p.chain.index];
                p.chain.index += 1;
                assert(step == .write);

                const length = try res.unwrap();
                for (p.chain.buffer[0..length]) |*item| {
                    assert(item.* == 123);
                    item.* = 0;
                }

                try next_step(runtime, p);
            }

            fn close_task(runtime: *Runtime, _: void, p: *Self) !void {
                errdefer unreachable;

                const step = p.chain.steps[p.chain.index];
                p.chain.index += 1;
                assert(step == .close);

                try next_step(runtime, p);
            }

            fn delete_task(runtime: *Runtime, res: DeleteResult, p: *Self) !void {
                errdefer unreachable;

                try res.unwrap();
                defer runtime.allocator.destroy(p);

                try task_fn(runtime, {}, p.task_ctx);
            }
        };

        const provision = try rt.allocator.create(Provision);
        errdefer rt.allocator.destroy(provision);
        provision.* = .{ .chain = self, .task_ctx = task_ctx };

        try Provision.next_step(rt, provision);
    }
};

const testing = std.testing;

test "FileChain: Invalid Exists" {
    const chain: []const FileChain.Step = &.{
        .open,
        .read,
        .write,
        .close,
        .delete,
    };

    try testing.expect(!FileChain.validate_chain(chain));
}

test "FileChain: Invalid Opened" {
    const chain: []const FileChain.Step = &.{
        .create,
        .close,
        .read,
        .write,
    };

    try testing.expect(!FileChain.validate_chain(chain));
}

test "FileChain: Never Closed" {
    const chain: []const FileChain.Step = &.{
        .create,
        .delete,
    };

    try testing.expect(!FileChain.validate_chain(chain));
}

test "FileChain: Never Deleted" {
    const chain: []const FileChain.Step = &.{
        .create,
        .read,
        .write,
        .close,
    };

    try testing.expect(!FileChain.validate_chain(chain));
}

test "FileChain: Verify Double Close" {
    const chain: []const FileChain.Step = &.{
        .create,
        .read,
        .write,
        .close,
        .open,
        .read,
        .read,
        .read,
        .close,
        .delete,
    };

    try testing.expect(FileChain.validate_chain(chain));
}

test "FileChain: Validate Random Chain" {
    // Actually generates and tests a random FileChain :)
    var seed: u64 = undefined;
    try std.posix.getrandom(std.mem.asBytes(&seed));
    errdefer std.debug.print("failed seed: {d}\n", .{seed});

    const chain = try FileChain.generate_random_chain(testing.allocator, seed);
    defer testing.allocator.free(chain);
    try testing.expect(FileChain.validate_chain(chain));
}
