const Runtime = @import("lib.zig").Runtime;
const TaskFn = @import("task.zig").TaskFn;
const Timespec = @import("../lib.zig").Timespec;

pub const Timer = struct {
    /// Spawns a new Task. This task will be set as runnable
    /// after the `timespec` amount of time has elasped.
    pub fn delay(
        rt: *Runtime,
        comptime R: type,
        task_ctx: anytype,
        comptime task_fn: TaskFn(R, @TypeOf(task_ctx)),
        timespec: Timespec,
    ) !void {
        try rt.scheduler.spawn2(R, task_ctx, task_fn, .waiting, .{ .timer = timespec });
    }
};
