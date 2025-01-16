const std = @import("std");
pub const log = std.log.scoped(.@"tardy/e2e");

const Atomic = std.atomic.Value;

pub const SharedParams = struct {
    // Seed Info
    seed_string: [:0]const u8,
    seed: u64,

    // Tardy Initalization
    size_tasks_initial: usize,
    size_aio_reap_max: usize,
};
