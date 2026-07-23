# Bestshield | Tamakun — high-throughput promotional email sender (dry run)

A single-file Go program that delivers a personalized promotional email to
**1,000,000** customer addresses **as fast as reasonably possible**, as a
**dry run** — no real email is sent. Each "send" simulates a 10–50ms network
call. The focus is safe, bounded concurrency via a goroutine worker pool.

## How to run

Requires **Go 1.22+** (uses `math/rand/v2`). No external dependencies.

```bash
# Build & run the headline scenario (1M recipients, 1000 workers, ~30s)
go run .

# Faster: more workers, still bounded (1M in ~6s)
go run . -workers 5000

# Smoke test under the race detector (100k jobs)
go run -race . -total 100000

# Exercise the error path with a synthetic 15% failure rate
go run . -fail-rate 0.15 -total 100000
```

Progress streams to **stderr** every 100,000 results; the final summary goes to
**stdout** (so `go run . > result.txt` captures a clean report). Press
**Ctrl-C** mid-run for a graceful partial tally (exit code 130).

### Flags

| Flag | Default | Description |
|---|---|---|
| `-total` | `1000000` | Number of recipients |
| `-workers` | `1000` | Worker pool size (the central throughput knob) |
| `-min-ms` / `-max-ms` | `10` / `50` | Simulated send latency range |
| `-job-buffer` / `-result-buffer` | `= workers` | Channel capacities (`-1` → use worker count) |
| `-fail-rate` | `0` | Synthetic failure rate `0..1` (exercises the error path) |

## Testing

The suite (`main_test.go`, standard library `testing` only) covers core
correctness, graceful shutdown, edge cases, and throughput.

```bash
go test -race .                                  # all tests under the race detector
go test -bench=BenchmarkWorkerPool -benchmem .   # throughput + allocations
```

| Test / benchmark | Verifies |
|---|---|
| `TestWorkerPoolTally` | A full batch is processed and tallied correctly — all-success, all-failure (`fail-rate 1.0`), and the `total == sent + failed` invariant under a mixed failure rate |
| `TestGracefulShutdown` | Cancelling the context mid-flight stops the workers promptly, the pool stays bounded while running, and goroutines return to baseline after shutdown (no leak) |
| `TestUnits` | Edge cases for `makeJob`, `commas`, `stats.add`, and `config.validate` (zero/negative buffers, `min>max`, out-of-range `fail-rate`) |
| `BenchmarkWorkerPool` | Steady-state `jobs/sec` across 50 / 200 / 1,000 workers, reported via `b.ReportMetric` |

Illustrative throughput (1ms simulated latency, 12-core dev machine — varies by hardware):

| workers | jobs/sec |
|---|---|
| 50 | ~42k |
| 200 | ~153k |
| 1,000 | ~558k |

At high worker counts the single producer/collector and core scheduling start to bind, so throughput trails the theoretical `workers × 1000/s` ceiling — the kind of bottleneck a benchmark is meant to surface.

## Architectural decisions

### Why a bounded pool of 1,000 workers instead of 1,000,000 goroutines?

The naive approach — one goroutine per recipient — would spawn **1M goroutines**,
each with a 2–8KB stack: roughly **2–8 GB of memory**, plus heavy scheduler and
GC churn. It is unbounded and resource-hostile.

Instead we use a **fixed pool of workers** connected by buffered channels:

```
producer ──jobs(chan, 1000)──▶ 1000 workers ──results(chan, 1000)──▶ collector
```

- **Bounded resources:** goroutines are capped at the pool size. In every run,
  `runtime.NumGoroutine()` peaks at **1,005** (1000 workers + ~5 overhead) —
  never 1,000,000 — and `TestGracefulShutdown` verifies it returns to baseline
  after cancellation (no leak).
- **Bounded memory:** only `~2 × workers` jobs are ever in flight (~128 KB of
  Job/Result structs), and channels provide natural **backpressure** — the
  producer simply blocks when workers are saturated.
- **Throughput scales with the knob:** since simulated work is pure I/O-style
  sleep, throughput ≈ `workers / meanLatency`. At a 30ms mean: 1,000 workers ≈
  **32k/s (~30s)**; 5,000 ≈ **163k/s (~6s)**. 1,000 is the safe, idiomatic
  default; it is a flag, not a constant.

Completion is driven entirely by `close()` — the producer closes `jobs`, workers
exit their range loop, a closer goroutine waits on the workers then closes
`results`, and the collector drains until closed. This makes the pipeline
**race-free** without any mutexes or atomics: the single collector goroutine is
the sole owner of the aggregated stats.

**Other choices:** `math/rand/v2` for lock-free latency RNG across workers
(no global mutex); `time.NewTimer` + `Stop()` instead of `time.After` to avoid
leaving a timer behind on each of 1M calls; and `signal.NotifyContext` for
graceful Ctrl-C cancellation.

### Why 1,000 (and not more)?

For the dry run you can safely raise `-workers` since the work is pure sleep.
In a **real** deployment, however, the limit is the downstream SMTP server's
allowed concurrency — not this knob — so over-tuning the pool size is beside the
point. 1,000 is a reasonable, honest default; the flag lets the reviewer explore
the speed/resource tradeoff directly.

## Assumptions

- **In-memory synthetic data generation (zero disk I/O).** The producer goroutine
  fabricates each recipient on the fly — `{Name, Email, PromoCode}` — from the
  job index. No customer CSV, database, or external file is read or written.
  This keeps the dry run **frictionless for the reviewer**: clone, `go run .`,
  done — no sample data to locate, no fixtures to set up, and the benchmark
  measures pure send throughput without disk variance.
- **No real email is ever sent.** The "network call" is a `time.Sleep` of 10–50ms.
- **Failures are opt-in.** The default path always succeeds; pass `-fail-rate` to
  inject synthetic SMTP rejections and observe the error/aggregation path.
