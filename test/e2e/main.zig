const std = @import("std");
const assert = std.debug.assert;
const log = @import("lib.zig").log;

const Atomic = std.atomic.Value;

const Runtime = @import("tardy").Runtime;
const Task = @import("tardy").Task;
const Tardy = @import("tardy").Tardy(.io_uring);

const Dir = @import("tardy").Dir;

const SharedParams = @import("lib.zig").SharedParams;

const First = @import("first.zig");
const Second = @import("second.zig");

pub const std_options = .{
    .log_level = .err,
};

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
        const pre_new = iter.next() orelse {
            return log.err("seed not passed in: ./e2e [seed]", .{});
        };
        const length = pre_new.len;

        if (length <= 1) {
            return log.err("seed not passed in: ./e2e [seed]", .{});
        }

        if (length >= maybe_seed_buffer.len) {
            return log.err("seed too long to be a u64", .{});
        }

        assert(length < maybe_seed_buffer.len);
        std.mem.copyForwards(u8, &maybe_seed_buffer, pre_new);
        maybe_seed_buffer[length] = 0;
        break :blk maybe_seed_buffer[0..length :0];
    };

    const seed = std.fmt.parseUnsigned(u64, seed_string, 10) catch {
        log.err("seed passed in is not u64", .{});
        return;
    };
    var prng = std.Random.DefaultPrng.init(seed);
    const rand = prng.random();

    const shared: SharedParams = blk: {
        var p: SharedParams = undefined;
        p.seed_string = seed_string;
        p.seed = seed;

        p.size_tasks_initial = rand.intRangeAtMost(usize, 1, 1024 * 4);
        p.size_aio_reap_max = rand.intRangeAtMost(usize, 1, p.size_tasks_initial);
        break :blk p;
    };
    log.debug("{s}\n\n", .{std.json.fmt(shared, .{ .whitespace = .indent_1 })});

    var tardy = try Tardy.init(allocator, .{
        .threading = .{ .multi = 2 },
        .pooling = .grow,
        .size_tasks_initial = shared.size_tasks_initial,
        //.size_aio_reap_max = shared.size_aio_reap_max,
        .size_aio_reap_max = 1,
    });
    defer tardy.deinit();

    const EntryParams = struct {
        shared: *const SharedParams,
        runtime_id: *Atomic(usize),
    };

    var runtime_id = Atomic(usize).init(0);

    const params = EntryParams{
        .shared = &shared,
        .runtime_id = &runtime_id,
    };

    try tardy.entry(
        &params,
        struct {
            fn start(rt: *Runtime, p: *const EntryParams) !void {
                switch (p.runtime_id.fetchAdd(1, .acquire)) {
                    0 => try rt.spawn_frame(.{ rt, p.shared }, First.start_frame, First.STACK_SIZE),
                    1 => try rt.spawn_frame(.{ rt, p.shared }, Second.start_frame, Second.STACK_SIZE),
                    //1 => try rt.scheduler.spawn(void, p.shared, Second.start, .runnable, null),
                    else => unreachable,
                }
            }
        }.start,
        {},
        struct {
            fn end(_: *Runtime, _: void) !void {}
        }.end,
    );
}
