const std = @import("std");
const assert = std.debug.assert;

const Task = @import("./task.zig").Task;
const Pool = @import("../core/pool.zig").Pool;

pub const Scheduler = struct {
    allocator: std.mem.Allocator,
    tasks: Pool(Task),

    pub fn init(allocator: std.mem.Allocator, size: usize) !Scheduler {
        return .{
            .allocator = allocator,
            .tasks = try Pool(Task).init(allocator, size, null, null),
        };
    }

    /// Spawns a Task by adding it into the scheduler pool.
    /// This means that if the predicate is true that it might run.
    pub fn spawn(self: *Scheduler, task: Task) !void {
        const borrowed = try self.tasks.borrow();
        borrowed.item.* = task;
        borrowed.item.index = borrowed.index;
    }

    pub fn release(self: *Scheduler, index: usize) void {
        self.tasks.release(index);
    }

    /// Get the next Task that has been scheduled.
    /// Returns null if it has not been scheduled.
    pub fn next(self: *Scheduler) ?*Task {
        var iter = self.tasks.iterator();
        return iter.next();
    }
};
