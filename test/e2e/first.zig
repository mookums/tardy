const std = @import("std");
const assert = std.debug.assert;
const log = @import("lib.zig").log;

const Atomic = std.atomic.Value;
const Runtime = @import("tardy").Runtime;

const Dir = @import("tardy").Dir;
const SharedParams = @import("lib.zig").SharedParams;
const FileChain = @import("file_chain.zig").FileChain;

pub const STACK_SIZE = 1024 * 32;
threadlocal var file_chain_counter: usize = 0;

pub fn start_frame(rt: *Runtime, shared_params: *const SharedParams) !void {
    const new_dir = try Dir.cwd().create_dir(shared_params.seed_string).resolve(rt);
    log.debug("created new shared dir (seed={d})", .{shared_params.seed});

    var prng = std.Random.DefaultPrng.init(shared_params.seed);
    const rand = prng.random();

    const chain_count = shared_params.size_tasks_initial * rand.intRangeLessThan(usize, 1, 3);
    file_chain_counter = chain_count;

    for (0..chain_count) |i| {
        var prng2 = std.Random.DefaultPrng.init(shared_params.seed + i);
        const rand2 = prng2.random();

        const chain_ptr = try rt.allocator.create(FileChain);
        errdefer rt.allocator.destroy(chain_ptr);

        const sub_chain = try FileChain.generate_random_chain(
            rt.allocator,
            (shared_params.seed + i) % std.math.maxInt(usize),
        );
        defer rt.allocator.free(sub_chain);

        const subpath = try std.fmt.allocPrintZ(rt.allocator, "{s}-{d}", .{ shared_params.seed_string, i });
        defer rt.allocator.free(subpath);

        chain_ptr.* = try FileChain.init(
            rt.allocator,
            sub_chain,
            .{ .rel = .{ .dir = new_dir.handle, .path = subpath } },
            rand2.intRangeLessThan(usize, 1, 8 * 1024),
        );
        errdefer chain_ptr.deinit();

        try rt.spawn_frame(
            .{ chain_ptr, rt, &file_chain_counter, shared_params.seed_string },
            FileChain.chain_frame,
            STACK_SIZE,
        );
    }
}
