const Runtime = @import("lib.zig").Runtime;
const Timespec = @import("../lib.zig").Timespec;

const Frame = @import("../frame/lib.zig").Frame;

pub const Timer = struct {
    pub fn delay(rt: *Runtime, timespec: Timespec) !void {
        try rt.scheduler.io_await(.{ .timer = timespec });
    }
};
