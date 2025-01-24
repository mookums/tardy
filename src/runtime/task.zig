const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.@"tardy/runtime/task");

const Frame = @import("../frame/lib.zig").Frame;
const Runtime = @import("../runtime/lib.zig").Runtime;
const Result = @import("../aio/completion.zig").Result;

pub const Task = struct {
    pub const State = union(enum) {
        channel: struct {
            check: *const fn (*anyopaque) bool,
            gen: *const fn (*anyopaque) ?*anyopaque,
            ctx: *anyopaque,
        },
        /// Waiting for an Async I/O Event.
        wait_for_io,
        /// Immediately Runnable.
        runnable,
        /// Dead.
        dead,
    };
    // 1 byte
    state: State = .dead,
    // no idea on bytes.
    result: Result = .none,
    // 8 bytes
    index: usize,
    // 8 bytes
    frame: *Frame,
};
