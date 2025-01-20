const Runtime = @import("lib.zig").Runtime;
const TaskFn = @import("task.zig").TaskFn;
const Timespec = @import("../lib.zig").Timespec;

const Frame = @import("../frame/lib.zig").Frame;

pub const Timer = struct {
    const DelayAction = struct {
        timespec: Timespec,

        pub fn resolve(self: *const DelayAction, rt: *Runtime) !void {
            try rt.scheduler.frame_await(.{ .timer = self.timespec });
        }

        pub fn callback(
            self: *const DelayAction,
            rt: *Runtime,
            task_ctx: anytype,
            comptime task_fn: TaskFn(void, @TypeOf(task_ctx)),
        ) !void {
            try rt.scheduler.spawn(
                void,
                task_ctx,
                task_fn,
                .wait_for_io,
                .{ .timer = self.timespec },
            );
        }
    };

    // This is meant for using with the Frames as it just queues a job for the current task.
    pub fn delay(timespec: Timespec) DelayAction {
        return .{ .timespec = timespec };
    }
};
