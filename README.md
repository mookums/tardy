# tardy 

## Installing
Tracking Latest Zig Stable: `0.13.0`
```
zig fetch --save git+https://github.com/mookums/tardy#main
```

You can then add the dependency in your `build.zig` file:
```zig
const tardy = b.dependency("tardy", .{
    .target = target,
    .optimize = optimize,
}).module("tardy");

exe.root_module.addImport(tardy);
```

## tardy?
tardy (def: delaying or delayed beyond the right or expected time; late.) is an asynchronous runtime written for Zig. Most of the code for this project originated in [zzz]("https://github.com/mookums/zzz"), a performance oriented networking framework.

tardy natively supports Linux, Mac and Windows. You can also port tardy to any target by providing an implementation for the Async I/O.

> [!IMPORTANT]
> tardy is currently **alpha** software and there is still a lot changing at a fairly quick pace and certain places where things are less polished.

## Performance
tardy is pretty quick. you can reference the [zzz]("https://github.com/mookums/zzz") benchmarks as it uses tardy internally.

## Features
- Modular Asynchronous Implementation
    - Allows for passing in your own Async I/O implementation.
    - Comes with:
        - `io_uring` for Linux (>= 5.1.0).
        - `epoll` for Linux (>= 2.5.45).
        - `busy_loop` for Linux, Mac and Windows.
- Single and Multi-threaded Support
- Callbacks on Async I/O events (through `runtime.[net/fs]`)
- Green Threads (through `runtime.spawn`)
