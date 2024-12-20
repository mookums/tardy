const std = @import("std");
const Timespec = @import("timespec.zig").Timespec;

pub fn Resulted(comptime T: type, comptime E: type) type {
    return struct {
        const Self = @This();
        actual: ?T = null,
        err: ?E = null,

        pub fn unwrap(self: *const Self) E!T {
            return self.actual orelse self.err.?;
        }
    };
}

// This interface is missing a lot of the stuff you get from `stat()` normally.
// This is minimally what I need.
pub const Stat = struct {
    size: u64,
    mode: u64 = 0,
    accessed: ?Timespec = null,
    modified: ?Timespec = null,
    changed: ?Timespec = null,
};

pub const AcceptError = error{
    WouldBlock,
    InvalidFd,
    ConnectionAborted,
    InvalidAddress,
    Interrupted,
    NotListening,
    ProcessFdQuotaExceeded,
    SystemFdQuotaExceeded,
    OutOfMemory,
    NotASocket,
    OperationNotSupported,
    Unexpected,
};

pub const ConnectError = error{
    AccessDenied,
    AddressInUse,
    AddressNotAvailable,
    AddressFamilyNotSupported,
    WouldBlock,
    InvalidFd,
    ConnectionRefused,
    InvalidAddress,
    Interrupted,
    AlreadyConnected,
    NetworkUnreachable,
    NotASocket,
    ProtocolFamilyNotSupported,
    TimedOut,
    Unexpected,
};

pub const RecvError = error{
    Closed,
    WouldBlock,
    InvalidFd,
    ConnectionRefused,
    /// Invalid Receive Buffer Address.
    InvalidAddress,
    Interrupted,
    InvalidArguments,
    OutOfMemory,
    NotConnected,
    NotASocket,
    Unexpected,
};

pub const SendError = error{
    AccessDenied,
    WouldBlock,
    OpenInProgress,
    InvalidFd,
    ConnectionReset,
    NoDestinationAddress,
    InvalidAddress,
    Interrupted,
    InvalidArguments,
    AlreadyConnected,
    InvalidSize,
    OutOfMemory,
    NotConnected,
    OperationNotSupported,
    BrokenPipe,
    Unexpected,
};

pub const OpenError = error{
    AccessDenied,
    InvalidFd,
    FileBusy,
    DiskQuotaExceeded,
    FileAlreadyExists,
    InvalidAddress,
    FileTooBig,
    Interrupted,
    InvalidArguments,
    IsDirectory,
    Loop,
    ProcessFdQuotaExceeded,
    NameTooLong,
    SystemFdQuotaExceeded,
    DeviceNotFound,
    FileNotFound,
    OutOfMemory,
    NoSpace,
    NotADirectory,
    OperationNotSupported,
    ReadOnlyFileSystem,
    FileLocked,
    WouldBlock,
    Unexpected,
};

pub const ReadError = error{
    AccessDenied,
    EndOfFile,
    WouldBlock,
    InvalidFd,
    InvalidAddress,
    Interrupted,
    InvalidArguments,
    IoError,
    IsDirectory,
    Unexpected,
};

pub const WriteError = error{
    WouldBlock,
    InvalidFd,
    NoDestinationAddress,
    DiskQuotaExceeded,
    InvalidAddress,
    FileTooBig,
    Interrupted,
    InvalidArguments,
    IoError,
    NoSpace,
    AccessDenied,
    BrokenPipe,
    Unexpected,
};

pub const StatError = error{
    AccessDenied,
    InvalidFd,
    InvalidAddress,
    InvalidArguments,
    Loop,
    NameTooLong,
    FileNotFound,
    OutOfMemory,
    NotADirectory,
    Unexpected,
};

pub const AcceptResult = Resulted(std.posix.socket_t, AcceptError);
pub const ConnectResult = Resulted(std.posix.socket_t, ConnectError);
pub const RecvResult = Resulted(usize, RecvError);
pub const SendResult = Resulted(usize, SendError);

pub const OpenResult = Resulted(std.posix.fd_t, OpenError);
pub const ReadResult = Resulted(usize, ReadError);
pub const WriteResult = Resulted(usize, WriteError);

pub const StatResult = Resulted(Stat, StatError);

pub const Result = union(enum) {
    none,
    /// If we want to wake the runtime up.
    wake,
    /// If we have returned a stat object.
    stat: StatResult,
    accept: AcceptResult,
    connect: ConnectResult,
    recv: RecvResult,
    send: SendResult,
    open: OpenResult,
    read: ReadResult,
    write: WriteResult,
    close,
    /// If we have returned a ptr.
    ptr: ?*anyopaque,
};

pub const Completion = struct {
    task: usize,
    result: Result,
};
