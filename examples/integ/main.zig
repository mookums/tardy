const std = @import("std");
const assert = std.debug.assert;
const log = @import("lib.zig").log;

const Atomic = std.atomic.Value;

const Runtime = @import("tardy").Runtime;
const Task = @import("tardy").Task;
const Tardy = @import("tardy").Tardy(.io_uring);

const Dir = @import("tardy").Dir;
const TcpServer = @import("tardy").TcpServer;

const IntegParams = @import("lib.zig").IntegParams;

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

    var maybe_seed_buffer: [21]u8 = undefined;
    const seed_string = args.next() orelse blk: {
        const stdin = std.io.getStdIn();
        const bytes = try stdin.readToEndAlloc(allocator, std.math.maxInt(usize));
        defer allocator.free(bytes);

        var iter = std.mem.splitScalar(u8, bytes, '\n');
        const pre_new = iter.next() orelse {
            return log.err("seed not passed in: ./integ [seed]", .{});
        };
        const length = pre_new.len;

        if (length <= 1) {
            return log.err("seed not passed in: ./integ [seed]", .{});
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

    var server_ready = Atomic(bool).init(false);

    const integ_values: IntegParams = blk: {
        var p: IntegParams = undefined;
        p.seed_string = seed_string;
        p.seed = seed;

        p.max_task_count = rand.intRangeAtMost(usize, 1, 1024 * 4);
        p.max_aio_jobs = rand.intRangeAtMost(usize, 1, p.max_task_count);
        p.max_aio_reap = rand.intRangeAtMost(usize, 1, p.max_aio_jobs);

        p.file_buffer_size = rand.intRangeAtMost(usize, 1, 1024 * 32);
        p.socket_buffer_size = rand.intRangeAtMost(usize, 1, 1024 * 32);

        p.server_ready = &server_ready;
        break :blk p;
    };
    //integ_values.log_to_stderr();

    var tardy = try Tardy.init(allocator, .{
        .threading = .{ .multi = 2 },
        .size_tasks_max = integ_values.max_task_count,
        .size_aio_jobs_max = integ_values.max_aio_jobs,
        .size_aio_reap_max = integ_values.max_aio_reap,
    });
    defer tardy.deinit();

    const EntryParams = struct {
        integ: *const IntegParams,
        runtime_id: *Atomic(usize),
    };

    var runtime_id = Atomic(usize).init(0);

    const params = EntryParams{
        .integ = &integ_values,
        .runtime_id = &runtime_id,
    };

    try tardy.entry(
        &params,
        struct {
            fn start(rt: *Runtime, p: *const EntryParams) !void {
                try rt.storage.store_ptr("params", @constCast(p.integ));

                switch (p.runtime_id.fetchAdd(1, .acquire)) {
                    0 => try Dir.cwd().create_dir(rt, p.integ, First.start, p.integ.seed_string),
                    1 => try rt.scheduler.spawn2(void, p.integ, Second.start, .runnable, null),
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
