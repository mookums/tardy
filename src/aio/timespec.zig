const std = @import("std");

pub const Timespec = struct {
    seconds: u64 = 0,
    nanos: u64 = 0,
};
