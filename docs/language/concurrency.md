# Concurrency

ThinkingLanguage provides concurrency primitives for parallel and asynchronous workloads.

## Spawn and Await

Launch a concurrent task with `spawn` and retrieve its result with `await`:

```tl
let task = spawn {
    expensive_computation()
}

// ... do other work ...

let result = await task
```

The spawned block runs concurrently. `await` blocks until the task completes and returns its result.

## Channels

Channels provide message-passing communication between concurrent tasks:

```tl
let [tx, rx] = channel()

spawn {
    send(tx, "hello from task")
}

let msg = recv(rx)
print(msg)  // "hello from task"
```

- `channel()` -- creates a sender/receiver pair
- `send(tx, value)` -- sends a value through the channel
- `recv(rx)` -- blocks until a message is available
- `try_recv(rx)` -- non-blocking receive, returns `None` if no message is ready

## Combinators

- `await_all([task1, task2])` -- waits for all tasks and returns a list of results
- `pmap(list, fn)` -- parallel map over a list
- `timeout(task, ms)` -- waits for a task with a time limit (in milliseconds)
- `sleep(1000)` -- pauses execution for the given number of milliseconds

```tl
let tasks = [
    spawn { fetch("https://api.example.com/a") },
    spawn { fetch("https://api.example.com/b") },
]
let results = await_all(tasks)
```

## Error Propagation

Errors thrown inside a spawned task are propagated when you `await` the task. Wrap await calls in `try/catch` to handle them:

```tl
let task = spawn {
    throw "something went wrong"
}

try {
    let result = await task
} catch e {
    print("Task failed: {e}")
}
```

## Async/Await (Feature-Gated)

For true asynchronous I/O, TL provides a tokio-backed async runtime behind the `async-runtime` feature flag.

### Declaring Async Functions

```tl
async fn fetch_data(url) {
    let response = async_http_get(url)
    return response
}
```

### Async I/O Builtins

These builtins perform non-blocking I/O:

- `async_read_file(path)` -- read a file asynchronously
- `async_write_file(path, content)` -- write a file asynchronously
- `async_http_get(url)` -- async HTTP GET request
- `async_http_post(url, body)` -- async HTTP POST request
- `async_sleep(ms)` -- async sleep

### Async Combinators

- `select([task1, task2])` -- returns the result of the first task to complete
- `race_all([task1, task2, task3])` -- returns the first completed result, cancels the rest
- `async_map(list, fn)` -- concurrent map over a collection
- `async_filter(list, fn)` -- concurrent filter over a collection

```tl
let fastest = select([
    spawn { async_http_get("https://api1.example.com/data") },
    spawn { async_http_get("https://api2.example.com/data") },
])
```

### Building with Async Support

The async runtime requires the `async-runtime` feature flag:

```sh
cargo build --features async-runtime
```

Without this feature, async builtins return stub errors.

## Important Limitation

Shared mutable state between closures is not supported. Each closure gets its own copy of captured variables when the upvalue is closed. If you need to share state between tasks, use channels.

```tl
// This does NOT work as expected:
let count = 0
let inc = fn() { count = count + 1 }
let get = fn() { count }

inc()
print(get())  // prints 0, not 1 — each closure has its own copy

// Use channels instead for cross-task communication
```
