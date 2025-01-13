const std = @import("std");
pub const log = std.log.scoped(.@"tardy/example/integ");

const Atomic = std.atomic.Value;

pub const IntegParams = struct {
    // Seed Info
    seed_string: [:0]const u8,
    seed: u64,
    rand: std.Random,

    server_ready: *Atomic(bool),

    // Tardy Initalization
    max_task_count: usize,
    max_aio_jobs: usize,
    max_aio_reap: usize,

    // File Operations
    file_buffer_size: usize,

    // Network Operations
    socket_buffer_size: usize,

    pub fn log_to_stderr(self: *const IntegParams) void {
        const fmt =
            \\seed={d}
            \\max_task_count={d}
            \\max_aio_jobs={d}
            \\max_aio_reap={d}
            \\file_buffer_size={d}
            \\socket_buffer_size={d}
        ;

        std.debug.print(
            fmt ++ "\n\n",
            .{
                self.seed,
                self.max_task_count,
                self.max_aio_jobs,
                self.max_aio_reap,
                self.file_buffer_size,
                self.socket_buffer_size,
            },
        );
    }
};
