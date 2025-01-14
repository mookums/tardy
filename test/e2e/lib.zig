const std = @import("std");
pub const log = std.log.scoped(.@"tardy/e2e");

const Atomic = std.atomic.Value;

pub const SharedParams = struct {
    // Seed Info
    seed_string: [:0]const u8,
    seed: u64,

    // Tardy Initalization
    max_task_count: usize,
    max_aio_jobs: usize,
    max_aio_reap: usize,
};
