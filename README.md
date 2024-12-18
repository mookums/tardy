# Tardy

Tardy *(def: delaying or delayed beyond the right or expected time; late.)* is an asynchronous runtime for writing applications and services in Zig.
Most of the code for this project originated in [zzz](https://github.com/mookums/zzz), a performance oriented networking framework.

- **Performant**: Tardy utilizes the latest Asynchronous APIs while minimizing allocations.
- **Portable**: Tardy natively supports Linux, Mac and Windows. Easy to port through the custom Async I/O system.
- **Scalable**: Tardy uses very little memory initially and can be tuned using various configuration options.

## Installing
Latest Zig Stable: `0.13.0`

Latest Tardy Release: `0.2.0`
```
zig fetch --save git+https://github.com/mookums/tardy#v0.2.0
```

You can then add the dependency in your `build.zig` file:
```zig
const tardy = b.dependency("tardy", .{
    .target = target,
    .optimize = optimize,
}).module("tardy");

exe.root_module.addImport(tardy);
```

> [!IMPORTANT]
> Tardy is currently **alpha** software. It's fast and it works but is still experimental!

## Features
- Modular Asynchronous Implementation
    - `io_uring` for Linux (>= 5.1.0).
    - `epoll` for Linux (>= 2.5.45).
    - `kqueue` for BSD & Mac.
    - `busy_loop` for Linux, Mac and Windows.
- Single and Multi-threaded Support
- Callbacks on Async I/O events (through `runtime.[net/fs]`)
- Green Threads (through `runtime.spawn`)
- Channels for Asynchronous Communcation across Tasks

## Ecosystem
- [zzz](https://github.com/mookums/zzz): a framework for writing performant and reliable networked services.

## Contribution
Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in Tardy by you, shall be licensed as MPL2.0, without any additional terms or conditions.
