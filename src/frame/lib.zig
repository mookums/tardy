const std = @import("std");
const builtin = @import("builtin");
const log = std.log.scoped(.@"tardy/frame");
const assert = std.debug.assert;

const Hardware = switch (builtin.cpu.arch) {
    .x86_64 => switch (builtin.os.tag) {
        .windows => x64Windows,
        else => x64SysV,
    },
    .aarch64 => aarch64General,
    else => @panic("Architecture not currently supported!"),
};

/// Swap the first stack out and and the second stack in.
extern fn tardy_swap_frame(noalias *[*]u8, noalias *[*]u8) callconv(.C) void;

const FrameEntryFn = *const fn () callconv(.C) noreturn;
fn EntryFn(args: anytype, comptime func: anytype) FrameEntryFn {
    const Args = @TypeOf(args);
    return struct {
        fn inner() callconv(.C) noreturn {
            const frame = active_frame.?;

            //const args_ptr: *align(1) Args = @ptrFromInt(@intFromPtr(frame) - @sizeOf(Args));
            const args_ptr: *Args = @ptrFromInt(@intFromPtr(frame) - @sizeOf(Args));
            @call(.auto, func, args_ptr.*) catch |e| {
                log.warn("frame failed | {}", .{e});
                frame.status = .errored;
                Frame.yield();
                unreachable;
            };

            // When our func is done running, just yield.
            frame.status = .done;
            Frame.yield();
            unreachable;
        }
    }.inner;
}

threadlocal var active_frame: ?*Frame = null;

pub const Frame = extern struct {
    const Status = enum(u8) {
        in_progress,
        done,
        errored,
    };

    /// The previous SP.
    caller_sp: [*]u8,
    /// The current SP.
    current_sp: [*]u8,
    /// Stack Info
    stack_ptr: [*]u8,
    stack_len: usize,
    /// Is the Frame done?
    status: Status = .in_progress,

    pub fn init(
        allocator: std.mem.Allocator,
        stack_size: usize,
        args: anytype,
        comptime func: anytype,
    ) !*Frame {
        const stack = try allocator.alloc(u8, stack_size);
        errdefer allocator.free(stack);
        const Args = @TypeOf(args);

        if (comptime builtin.mode == .Debug) {
            // this should mark it easily for the debugger.
            for (stack) |*byte| byte.* = 0xAA;
        }

        const stack_base = @intFromPtr(stack.ptr);
        const stack_end = @intFromPtr(stack.ptr + stack.len);

        // space for the frame
        var stack_ptr = std.mem.alignBackward(
            usize,
            stack_end - @sizeOf(Frame),
            Hardware.alignment,
        );
        if (stack_ptr < stack_base) return error.StackTooSmall;
        const frame: *Frame = @ptrFromInt(stack_ptr);

        // space for the args
        stack_ptr -= @sizeOf(Args);
        const arg_ptr: *Args = @ptrFromInt(stack_ptr);
        arg_ptr.* = args;

        // space for the saved registers (pushed)
        stack_ptr = std.mem.alignBackward(
            usize,
            stack_ptr - @sizeOf(usize) * Hardware.stack_count,
            Hardware.alignment,
        );
        if (stack_ptr < stack_base) return error.StackTooSmall;
        assert(std.mem.isAligned(stack_ptr, Hardware.alignment));

        // set the return address appropriately
        const entries: [*]FrameEntryFn = @ptrFromInt(stack_ptr);
        entries[Hardware.entry] = EntryFn(args, func);

        frame.* = .{
            .caller_sp = undefined,
            .current_sp = @ptrFromInt(stack_ptr),
            .stack_ptr = stack.ptr,
            .stack_len = stack.len,
        };

        return frame;
    }

    pub fn deinit(self: Frame, allocator: std.mem.Allocator) void {
        const stack = self.stack_ptr[0..self.stack_len];
        allocator.free(stack);
    }

    /// This runs/continues a Frame.
    pub fn proceed(frame: *Frame) void {
        const old_frame = active_frame;
        assert(old_frame != frame);
        active_frame = frame;
        defer active_frame = old_frame;

        tardy_swap_frame(&frame.caller_sp, &frame.current_sp);
    }

    /// This yields/pauses a Frame.
    pub fn yield() void {
        const current = active_frame.?;
        tardy_swap_frame(&current.current_sp, &current.caller_sp);
    }
};

const x64SysV = struct {
    pub const stack_count = 7;
    pub const entry = stack_count - 1;
    pub const alignment = 16;

    comptime {
        asm (@embedFile("asm/x86_64_sysv.s"));
    }
};

const x64Windows = struct {
    pub const stack_count = 31;
    pub const entry = stack_count - 1;
    pub const alignment = 16;

    comptime {
        asm (@embedFile("asm/x86_64_win.s"));
    }
};

const aarch64General = struct {
    pub const stack_count = 20;
    pub const entry = 0;
    pub const alignment = 16;

    comptime {
        asm (@embedFile("asm/aarch64_gen.s"));
    }
};
