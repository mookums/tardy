
# Tardy

Tardy *(def: delaying or delayed beyond the right or expected time; late.)* is an asynchronous runtime for writing applications and services in Zig.
Most of the code for this project originated in [zzz](https://github.com/mookums/zzz), a performance oriented networking framework.

- Tardy utilizes the latest Asynchronous APIs while minimizing allocations.
- Tardy natively supports Linux, Mac, BSD, and Windows.
- Tardy is configurable, allowing you to optimize the runtime for your specific use-case.

[![Static Badge](https://img.shields.io/badge/license-MPL2-blue)](https://www.mozilla.org/en-US/MPL/2.0/)
[![Discord](https://img.shields.io/discord/1294761432922980392?logo=discord)](https://discord.gg/FP9Xb7WGPK)

## ?
Tardy is a thread-local asynchronous runtime for Zig, providing the core implementation for asynchronous libraries and services.
- Per-thread Runtime isolation for minimal contention
- Native async I/O (io_uring, epoll, kqueue, poll)
    - `File` and `Dir` for Filesystem Operations
    - `Socket` for Network Operatons

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
    - `poll` for POSIX-compliant systems.
- Single and Multi-threaded Support
- Stackful Coroutines (Frames)
- Asynchronous Primitives (such as `File`, `Dir` and `Socket`).
- Channels for Asynchronous Communcation across Tasks and Threads

## Ecosystem
- [zzz](https://github.com/mookums/zzz): a framework for writing performant and reliable networked services.

## Contribution
Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in Tardy by you, shall be licensed as MPL2.0, without any additional terms or conditions.
