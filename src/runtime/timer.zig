const Runtime = @import("lib.zig").Runtime;
const TaskFn = @import("task.zig").TaskFn;
const Timespec = @import("../lib.zig").Timespec;

pub const Timer = struct {
    /// Spawns a new Task. This task will be set as runnable
    /// after the `timespec` amount of time has elasped.
    pub fn delay(
        rt: *Runtime,
        task_ctx: anytype,
        comptime task_fn: TaskFn(void, @TypeOf(task_ctx)),
        timespec: Timespec,
    ) !void {
        try rt.scheduler.spawn(void, task_ctx, task_fn, .wait_for_io, .{ .timer = timespec });
    }
};
