// This is just a general E2E test that should test various Tardy actions,
// behaviors, functions across a randomized (but *kinda* deterministic) test order.
//
// We don't 100% care about the stuff that happens in the middle (and how the OS decides to order AIO calls)
// but the end results are VERY important.

const std = @import("std");
const log = std.log.scoped(.@"tardy/example/integ");

const Runtime = @import("tardy").Runtime;
const Task = @import("tardy").Task;
const Tardy = @import("tardy").Tardy(.auto);
const Cross = @import("tardy").Cross;

const Dir = @import("tardy").Dir;

const OpenFileResult = @import("tardy").OpenFileResult;
const OpenDirResult = @import("tardy").OpenDirResult;
const CreateDirResult = @import("tardy").CreateDirResult;

const ReadResult = @import("tardy").ReadResult;
const WriteResult = @import("tardy").WriteResult;

const DeleteTreeResult = @import("tardy").DeleteTreeResult;

fn integ_start(rt: *Runtime, res: CreateDirResult, params: *IntegParams) !void {
    const new_dir = try res.unwrap();
    errdefer new_dir.close_blocking();
    log.debug("created new integ dir (seed={d})", .{params.seed});

    // create file
    // write some garbage data based on seed into file
    //
    // in runtime 2,
    // create a TCP socket and open for accept
    // set an atomic flag saying we are ready for connect
    //
    // after fs stuff on runtime1,
    // connect to runtime 2
    // runtime 2 creates a new dir
    // stream the file over tcp across the two runtimes
    //
    // ensure the two files are byte for byte identical
    // server then takes the checksum, sends it back
    // client then compares the checksum then asserts sameness
    // close connections
    //
    // delete temporary tree
    //
    // Randomize the file sizes
    // Randomize the TCP buffer/chunk sizes used during transfer
    // Randomly inject delays during the transfer to test partial reads/writes
    // Randomly choose between different checksum algorithms
    // Occasionally do multiple transfers in parallel
    // Randomly close and reopen the connection mid-transfer to test reconnection
    // Randomly force GC/memory pressure during transfers
    // Randomize directory depths/file paths

    try Dir.cwd().delete_tree(rt, params, integ_finish, params.seed_string, 1);
}

fn integ_finish(rt: *Runtime, res: DeleteTreeResult, params: *IntegParams) !void {
    defer rt.stop();
    try res.unwrap();
    log.debug("deleted integ tree (seed={d})", .{params.seed});
}

const IntegParams = struct {
    seed_string: [:0]const u8,
    seed: u64,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    var tardy = try Tardy.init(allocator, .{
        .threading = .single,
        .size_tasks_max = 1,
        .size_aio_jobs_max = 1,
        .size_aio_reap_max = 1,
    });
    defer tardy.deinit();

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    _ = args.next().?;
    const seed_string = args.next() orelse {
        log.err("seed not passed in: ./integ [seed]", .{});
        return;
    };

    const seed = try std.fmt.parseUnsigned(u64, seed_string, 10);

    const EntryParams = struct {
        seed_string: [:0]const u8,
        seed: u64,
    };

    try tardy.entry(
        &EntryParams{ .seed = seed, .seed_string = seed_string },
        struct {
            fn start(rt: *Runtime, params: *const EntryParams) !void {
                const integ_params = try rt.allocator.create(IntegParams);
                integ_params.* = .{ .seed = params.seed, .seed_string = params.seed_string };
                try rt.storage.store_ptr("params", integ_params);

                try Dir.create(
                    rt,
                    integ_params,
                    integ_start,
                    .{ .rel = .{ .dir = std.fs.cwd().fd, .path = params.seed_string } },
                );
            }
        }.start,
        {},
        struct {
            fn end(rt: *Runtime, _: void) !void {
                const integ_params = rt.storage.get_ptr("params", IntegParams);
                rt.allocator.destroy(integ_params);
            }
        }.end,
    );
}
