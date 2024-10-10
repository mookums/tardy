const std = @import("std");
const assert = std.debug.assert;

const Task = @import("./task.zig").Task;
const State = @import("./task.zig").State;
const TaskFn = @import("./task.zig").TaskFn;
const Pool = @import("../core/pool.zig").Pool;

pub fn Scheduler(comptime Runtime: type) type {
    const RuntimeTask = Task(Runtime);
    return struct {
        const Self = @This();
        allocator: std.mem.Allocator,
        tasks: Pool(RuntimeTask),

        pub fn init(allocator: std.mem.Allocator, size: usize) !Self {
            return .{
                .allocator = allocator,
                .tasks = try Pool(RuntimeTask).init(allocator, size, null, null),
            };
        }

        /// Spawns a Task by adding it into the scheduler pool.
        /// This means that if the predicate is true that it might run.
        pub fn spawn(self: *Self, task_fn: RuntimeTask.TaskFn, task_ctx: ?*anyopaque, task_state: State) !usize {
            const borrowed = try self.tasks.borrow();
            borrowed.item.* = .{
                .index = borrowed.index,
                .state = task_state,
                .func = task_fn,
                .context = task_ctx,
            };

            return borrowed.index;
        }

        pub fn release(self: *Self, index: usize) void {
            self.tasks.release(index);
        }

        /// Get the next Task that has been scheduled.
        /// Returns null if it has not been scheduled.
        pub fn next(self: *Self) ?*RuntimeTask {
            var iter = self.tasks.iterator();
            return iter.next();
        }
    };
}
