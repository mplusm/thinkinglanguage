"""Python scalar benchmarks — equivalent to TL scalar_bench.rs"""
import time

def fib(n):
    if n <= 1:
        return n
    return fib(n - 1) + fib(n - 2)

def bench_fib():
    start = time.perf_counter()
    iterations = 5
    for _ in range(iterations):
        result = fib(25)
    elapsed = (time.perf_counter() - start) / iterations
    print(f"fib(25) Python:       {elapsed*1000:.2f} ms  (result={result})")

def bench_sum_loop():
    start = time.perf_counter()
    iterations = 5
    for _ in range(iterations):
        total = 0
        for i in range(1_000_000):
            total += i
    elapsed = (time.perf_counter() - start) / iterations
    print(f"sum 1M Python:        {elapsed*1000:.2f} ms  (result={total})")

def bench_map_filter():
    start = time.perf_counter()
    iterations = 5
    for _ in range(iterations):
        nums = list(range(100_000))
        doubled = list(map(lambda x: x * 2, nums))
        evens = list(filter(lambda x: x % 4 == 0, doubled))
        result = sum(evens)
    elapsed = (time.perf_counter() - start) / iterations
    print(f"map+filter 100k Python: {elapsed*1000:.2f} ms  (result={result})")

if __name__ == "__main__":
    bench_fib()
    bench_sum_loop()
    bench_map_filter()
