const std = @import("std");
const assert = std.debug.assert;
const log = @import("lib.zig").log;

const Atomic = std.atomic.Value;

const Runtime = @import("tardy").Runtime;

const Dir = @import("tardy").Dir;
const File = @import("tardy").File;
const Timer = @import("tardy").Timer;

const OpenFileResult = @import("tardy").OpenFileResult;
const OpenDirResult = @import("tardy").OpenDirResult;

const CreateFileResult = @import("tardy").OpenFileResult;
const CreateDirResult = @import("tardy").CreateDirResult;

const StatResult = @import("tardy").StatResult;
const ReadResult = @import("tardy").ReadResult;
const WriteResult = @import("tardy").WriteResult;
const DeleteTreeResult = @import("tardy").DeleteTreeResult;

const ConnectResult = @import("tardy").ConnectResult;

const SharedParams = @import("lib.zig").SharedParams;

const FileChain = @import("file_chain.zig").FileChain;

const Params = struct {
    shared: *const SharedParams,
    root_dir: Dir,
    file_chain: *FileChain,
};

threadlocal var file_chain_counter: usize = 0;

pub fn start(rt: *Runtime, res: CreateDirResult, shared_params: *const SharedParams) !void {
    const new_dir = res.unwrap() catch |e| {
        log.err("failed due to {}", .{e});
        unreachable;
    };

    log.debug("created new shared dir (seed={d})", .{shared_params.seed});

    var prng = std.Random.DefaultPrng.init(shared_params.seed);
    const rand = prng.random();

    const chain_count = shared_params.size_tasks_initial * rand.intRangeLessThan(usize, 1, 3);
    file_chain_counter = chain_count;
    for (0..chain_count) |i| {
        var prng2 = std.Random.DefaultPrng.init(shared_params.seed);
        const rand2 = prng2.random();

        const chain_ptr = try rt.allocator.create(FileChain);
        errdefer rt.allocator.destroy(chain_ptr);

        const sub_chain = try FileChain.generate_random_chain(
            rt.allocator,
            (shared_params.seed + i) % std.math.maxInt(usize),
        );
        defer rt.allocator.free(sub_chain);

        const sub_path = try std.fmt.allocPrintZ(rt.allocator, "{s}-{d}", .{ shared_params.seed_string, i });
        defer rt.allocator.free(sub_path);

        chain_ptr.* = try FileChain.init(
            rt.allocator,
            sub_chain,
            .{ .rel = .{ .dir = new_dir.handle, .path = sub_path } },
            rand2.intRangeLessThan(usize, 1, 8 * 1024),
        );
        errdefer chain_ptr.deinit();

        const params = try rt.allocator.create(Params);
        errdefer rt.allocator.destroy(params);
        params.* = .{
            .shared = shared_params,
            .root_dir = new_dir,
            .file_chain = chain_ptr,
        };

        chain_ptr.run(rt, params, post_chain) catch |e| {
            log.err("failed due to {}", .{e});
            unreachable;
        };
    }
}

fn post_chain(rt: *Runtime, _: void, params: *Params) !void {
    errdefer unreachable;

    // This is fine because it is threadlocal AND
    // only one version of `post_chain` runs at a time.
    file_chain_counter -= 1;
    if (file_chain_counter == 0) {
        log.debug("deleting the e2e tree...", .{});
        try Dir.cwd().delete_tree(rt, params, finish, params.shared.seed_string, 1);
    } else {
        // if not the last, clean it here.
        params.file_chain.deinit();
        rt.allocator.destroy(params.file_chain);
        rt.allocator.destroy(params);
    }
}

fn finish(rt: *Runtime, res: DeleteTreeResult, params: *Params) !void {
    defer rt.stop();
    defer rt.allocator.destroy(params);
    defer rt.allocator.destroy(params.file_chain);
    defer params.file_chain.deinit();
    defer params.root_dir.close_blocking();

    res.unwrap() catch |e| {
        log.err("failed due to {}", .{e});
        unreachable;
    };

    log.debug("deleted shared tree (seed={d})", .{params.shared.seed});
}
