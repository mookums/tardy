const std = @import("std");
const Timespec = @import("../lib.zig").Timespec;

const File = @import("../fs/lib.zig").File;
const Dir = @import("../fs/lib.zig").Dir;
const Stat = @import("../fs/lib.zig").Stat;

const TcpSocket = @import("../net/lib.zig").TcpSocket;

pub fn Resulted(comptime T: type, comptime E: type) type {
    return union(enum) {
        const Self = @This();
        actual: T,
        err: E,

        pub fn unwrap(self: *const Self) E!T {
            switch (self.*) {
                .actual => |a| return a,
                .err => |e| return e,
            }
        }
    };
}

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
    Busy,
    DiskQuotaExceeded,
    AlreadyExists,
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
    NotFound,
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
    NotFound,
    OutOfMemory,
    NotADirectory,
    Unexpected,
};

pub const MkdirError = error{
    AccessDenied,
    AlreadyExists,
    Loop,
    NameTooLong,
    NotFound,
    NoSpace,
    NotADirectory,
    ReadOnlyFileSystem,
    Unexpected,
};

pub const DeleteError = error{
    AccessDenied,
    Busy,
    InvalidAddress,
    IoError,
    IsDirectory,
    Loop,
    NameTooLong,
    NotFound,
    OutOfMemory,
    IsNotDirectory,
    ReadOnlyFileSystem,
    InvalidArguments,
    NotEmpty,
    InvalidFd,
    Unexpected,
};

const AcceptResultType = union(enum) { tcp: TcpSocket };
pub const InnerAcceptResult = Resulted(AcceptResultType, AcceptError);
pub const AcceptTcpResult = Resulted(TcpSocket, AcceptError);

pub const ConnectResult = Resulted(std.posix.socket_t, ConnectError);
pub const RecvResult = Resulted(usize, RecvError);
pub const SendResult = Resulted(usize, SendError);

// This is ONLY used internally. This helps us avoid Result enum bloat
// by encoding multiple possibilities within one Result.
const OpenResultType = union(enum) { file: File, dir: Dir };
pub const InnerOpenResult = Resulted(OpenResultType, OpenError);
pub const OpenFileResult = Resulted(File, OpenError);
pub const OpenDirResult = Resulted(Dir, OpenError);

pub const MkdirResult = Resulted(void, MkdirError);
pub const CreateDirError = MkdirError || OpenError || error{InternalFailure};
pub const CreateDirResult = Resulted(Dir, CreateDirError);

pub const DeleteResult = Resulted(void, DeleteError);
pub const DeleteTreeError = OpenError || DeleteError || error{InternalFailure};
pub const DeleteTreeResult = Resulted(void, DeleteTreeError);

pub const ReadResult = Resulted(usize, ReadError);
pub const WriteResult = Resulted(usize, WriteError);
pub const WriteAllResult = Resulted(void, WriteError);

pub const StatResult = Resulted(Stat, StatError);

pub const Result = union(enum) {
    none,
    /// If we want to wake the runtime up.
    wake,
    /// If we have returned a stat object.
    stat: StatResult,
    accept: InnerAcceptResult,
    connect: ConnectResult,
    recv: RecvResult,
    send: SendResult,
    open: InnerOpenResult,
    mkdir: MkdirResult,
    delete: DeleteResult,
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
