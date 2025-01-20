const std = @import("std");
const assert = std.debug.assert;
const log = @import("lib.zig").log;

const Runtime = @import("tardy").Runtime;
const SharedParams = @import("lib.zig").SharedParams;

threadlocal var tcp_chain_counter: usize = 0;

pub fn start_frame(rt: *Runtime, shared_params: *const SharedParams) !void {
    _ = rt;
    _ = shared_params;
}
