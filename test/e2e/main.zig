const std = @import("std");
const assert = std.debug.assert;
const log = @import("lib.zig").log;

const Runtime = @import("tardy").Runtime;
const Task = @import("tardy").Task;
const Timer = @import("tardy").Timer;

const options = @import("options");
const AsyncType = @import("tardy").AsyncType;
const backend: AsyncType = switch (options.async_option) {
    .auto => .auto,
    .kqueue => .kqueue,
    .io_uring => .io_uring,
    .epoll => .epoll,
    .poll => .poll,
    .custom => unreachable,
};
const Tardy = @import("tardy").Tardy(backend);

const Dir = @import("tardy").Dir;

const SharedParams = @import("lib.zig").SharedParams;

const First = @import("first.zig");
const Second = @import("second.zig");

pub const std_options: std.Options = .{ .log_level = .info };

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    _ = args.next().?;

    // max u64 is 21 characters long :p
    var maybe_seed_buffer: [21]u8 = undefined;
    const seed_string = args.next() orelse blk: {
        const stdin = std.io.getStdIn();
        const bytes = try stdin.readToEndAlloc(allocator, std.math.maxInt(usize));
        defer allocator.free(bytes);

        var iter = std.mem.splitScalar(u8, bytes, '\n');
        const not_passed_in = "seed not passed in: ./e2e [seed]";
        const pre_new = iter.next() orelse @panic(not_passed_in);
        const length = pre_new.len;

        if (length <= 1) @panic(not_passed_in);
        if (length >= maybe_seed_buffer.len) @panic("seed too long to be a u64");

        assert(length < maybe_seed_buffer.len);
        std.mem.copyForwards(u8, &maybe_seed_buffer, pre_new);
        maybe_seed_buffer[length] = 0;
        break :blk maybe_seed_buffer[0..length :0];
    };

    const seed = std.fmt.parseUnsigned(u64, seed_string, 10) catch @panic("seed passed in is not u64");
    var prng = std.Random.DefaultPrng.init(seed);
    const rand = prng.random();

    const shared: SharedParams = blk: {
        var p: SharedParams = undefined;
        p.seed_string = seed_string;
        p.seed = seed;

        p.size_tasks_initial = rand.intRangeAtMost(usize, 1, 64);
        p.size_aio_reap_max = rand.intRangeAtMost(usize, 1, p.size_tasks_initial * 2);
        break :blk p;
    };
    log.debug("{s}", .{std.json.fmt(shared, .{ .whitespace = .indent_1 })});

    var tardy = try Tardy.init(allocator, .{
        .threading = .single,
        .pooling = .grow,
        .size_tasks_initial = shared.size_tasks_initial,
        .size_aio_reap_max = shared.size_aio_reap_max,
    });
    defer tardy.deinit();

    try tardy.entry(
        &shared,
        struct {
            fn start(rt: *Runtime, p: *const SharedParams) !void {
                try rt.spawn(.{ rt, p }, First.start_frame, First.STACK_SIZE);
                try rt.spawn(.{ rt, p }, Second.start_frame, Second.STACK_SIZE);
            }
        }.start,
    );

    std.debug.print("seed={d} passed\n", .{seed});
}
