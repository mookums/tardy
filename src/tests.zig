const std = @import("std");
const testing = std.testing;

test "tardy unit tests" {
    // Core
    testing.refAllDecls(@import("./core/atomic_ring.zig"));
    testing.refAllDecls(@import("./core/pool.zig"));
    testing.refAllDecls(@import("./core/ring.zig"));
    testing.refAllDecls(@import("./core/zero_copy.zig"));

    // Runtime
    testing.refAllDecls(@import("./runtime/storage.zig"));
}
