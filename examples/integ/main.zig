// This is our main testing example binary.
//
// This is meant to touch as much of the active Tardy API as possible
// and essentially ensure that things are happening and behaving correctly.
//
// Things that need to occur during the test:
//
// Directory:
//  - Creation
//  - Stat
//  - Open
//  - Close
//  - Remove
//
// File:
//  - Creation
//  - Stat
//  - Open
//  - Read
//  - Read Offset
//  - Write
//  - Write Offset
//  - Close
//  - Remove
//
// Network:
//  - Accept
//  - Connect
//  - Open
//  - Recv
//  - Write
//  - Close
//
// These can occur concurrently (which is what we will be aiming for).
// There will likely be a variety of test tasks that each run within an independent Runtime
// and when they are all done, we can confirm the results of the test run.
//
// There will be a seed generated/provided at the start that will determine all of the behavior of the program.
// This will allow us to rerun the exact same tests later on if something fails or is fixed.
// This should also be compiled and run on each supported platform ensuring all Async I/O backends behave similarly.

const std = @import("std");
const log = std.log.scoped(.@"tardy/example/integ");

const Runtime = @import("tardy").Runtime;
const Task = @import("tardy").Task;
const Tardy = @import("tardy").Tardy(.auto);
const Cross = @import("tardy").Cross;

const File = @import("tardy").File;
const Dir = @import("tardy").Dir;

const OpenFileResult = @import("tardy").OpenFileResult;
const OpenDirResult = @import("tardy").OpenDirResult;
const ReadResult = @import("tardy").ReadResult;
const WriteResult = @import("tardy").WriteResult;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .thread_safe = true }){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    var tardy = try Tardy.init(allocator, .{
        .threading = .single,
        .size_tasks_max = 1,
        .size_aio_jobs_max = 1,
        .size_aio_reap_max = 1,
    });
    defer tardy.deinit();

    try tardy.entry(
        {},
        struct {
            fn start(rt: *Runtime, _: void) !void {
                _ = rt;
                const dir = Dir.from_std(std.fs.cwd());
                _ = dir;
            }
        }.start,
        {},
        struct {
            fn end(_: *Runtime, _: void) !void {}
        }.end,
    );
}
