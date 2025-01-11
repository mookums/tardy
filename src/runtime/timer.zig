const Runtime = @import("lib.zig").Runtime;
const TaskFn = @import("task.zig").TaskFn;
const Timespec = @import("lib.zig").Timespec;

pub const Timer = struct {
    /// Spawns a new Task. This task will be set as runnable
    /// after the `timespec` amount of time has elasped.
    pub fn delay(
        self: *Runtime,
        comptime R: type,
        task_ctx: anytype,
        comptime task_fn: TaskFn(R, @TypeOf(task_ctx)),
        timespec: Timespec,
    ) !void {
        const index = try self.scheduler.spawn(R, task_ctx, task_fn, .waiting);
        try self.aio.queue_timer(index, timespec);
    }
};
