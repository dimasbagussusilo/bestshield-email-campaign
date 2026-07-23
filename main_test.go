package main

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"
)

// TestWorkerPoolTally is the core-logic test: the worker pool must process an
// entire batch of jobs and tally results correctly. We use deterministic
// configurations (fail-rate 0 and 1.0) so the expected tally is exact,
// independent of the random latency.
func TestWorkerPoolTally(t *testing.T) {
	ctx := context.Background()

	t.Run("all succeed", func(t *testing.T) {
		const total = 5000
		s, peakGo := run(ctx, config{total: total, workers: 50, jobBuf: 50, resBuf: 50, minMs: 1, maxMs: 2})
		if s.total != total || s.sent != total || s.failed != 0 {
			t.Errorf("tally = {total:%d sent:%d failed:%d}, want %d sent, 0 failed", s.total, s.sent, s.failed, total)
		}
		if peakGo < 50 {
			t.Errorf("peakGo=%d, expected the worker pool to have spawned", peakGo)
		}
	})

	t.Run("all fail (fail-rate 1.0)", func(t *testing.T) {
		const total = 5000
		s, _ := run(ctx, config{total: total, workers: 50, jobBuf: 50, resBuf: 50, minMs: 1, maxMs: 2, failRate: 1.0})
		if s.total != total || s.sent != 0 || s.failed != total {
			t.Errorf("tally = {total:%d sent:%d failed:%d}, want %d failed, 0 sent", s.total, s.sent, s.failed, total)
		}
	})

	t.Run("tally invariant total == sent + failed", func(t *testing.T) {
		// fail-rate 0.3 -> a mix; every job still produces exactly one result.
		s, _ := run(ctx, config{total: 4000, workers: 25, jobBuf: 25, resBuf: 25, minMs: 1, maxMs: 2, failRate: 0.3})
		if s.total != 4000 {
			t.Errorf("processed=%d, want every job tallied (4000)", s.total)
		}
		if s.total != s.sent+s.failed {
			t.Errorf("invariant broken: total=%d but sent+failed=%d", s.total, s.sent+s.failed)
		}
	})
}

// TestUnits covers the small helpers and edge cases: Job fabrication,
// number formatting, result aggregation, and config validation.
func TestUnits(t *testing.T) {
	t.Run("makeJob", func(t *testing.T) {
		j := makeJob(7)
		want := Job{ID: 7, Email: "customer000007@example.com", PromoCode: "PROMO-000007", Name: firstNames[7]}
		if j != want {
			t.Errorf("makeJob(7) = %+v, want %+v", j, want)
		}
	})

	t.Run("commas", func(t *testing.T) {
		for _, c := range []struct {
			in   int
			want string
		}{
			{0, "0"}, {42, "42"}, {999, "999"}, {1000, "1,000"},
			{1234567, "1,234,567"}, {1000000, "1,000,000"},
		} {
			if got := commas(c.in); got != c.want {
				t.Errorf("commas(%d) = %q, want %q", c.in, got, c.want)
			}
		}
	})

	t.Run("stats.add", func(t *testing.T) {
		var s stats
		s.add(Result{OK: true})
		s.add(Result{OK: false, Err: "boom"})
		s.add(Result{OK: true})
		if s.total != 3 || s.sent != 2 || s.failed != 1 {
			t.Errorf("stats = {total:%d sent:%d failed:%d}, want {3 2 1}", s.total, s.sent, s.failed)
		}
	})

	t.Run("config.validate", func(t *testing.T) {
		for _, c := range []struct {
			name    string
			cfg     config
			wantErr bool
		}{
			{"valid", config{total: 1, workers: 1, jobBuf: 1, resBuf: 1, minMs: 1, maxMs: 1}, false},
			{"zero buffer ok", config{total: 1, workers: 1, jobBuf: 0, resBuf: 0, minMs: 0, maxMs: 0}, false},
			{"failRate boundary ok", config{total: 1, workers: 1, jobBuf: 1, resBuf: 1, minMs: 1, maxMs: 1, failRate: 1}, false},
			{"total<=0", config{total: 0, workers: 1, jobBuf: 1, resBuf: 1, minMs: 1, maxMs: 1}, true},
			{"workers<=0", config{total: 1, workers: 0, jobBuf: 1, resBuf: 1, minMs: 1, maxMs: 1}, true},
			{"negative buffer", config{total: 1, workers: 1, jobBuf: -1, resBuf: 1, minMs: 1, maxMs: 1}, true},
			{"min>max", config{total: 1, workers: 1, jobBuf: 1, resBuf: 1, minMs: 5, maxMs: 1}, true},
			{"failRate<0", config{total: 1, workers: 1, jobBuf: 1, resBuf: 1, minMs: 1, maxMs: 1, failRate: -0.1}, true},
			{"failRate>1", config{total: 1, workers: 1, jobBuf: 1, resBuf: 1, minMs: 1, maxMs: 1, failRate: 1.5}, true},
		} {
			t.Run(c.name, func(t *testing.T) {
				if err := c.cfg.validate(); (err != nil) != c.wantErr {
					t.Errorf("validate() err=%v, wantErr=%v", err, c.wantErr)
				}
			})
		}
	})
}

// TestGracefulShutdown verifies that cancelling the context mid-flight stops the
// workers safely and without leaking goroutines. It uses a total large enough
// that the run cannot finish on its own, so it can only complete via cancel.
func TestGracefulShutdown(t *testing.T) {
	baseline := runtime.NumGoroutine()
	const workers = 50

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		run(ctx, config{total: 1 << 30, workers: workers, jobBuf: workers, resBuf: workers, minMs: 5, maxMs: 10})
		close(done)
	}()

	// Wait for the pool to spin up, and confirm it is bounded at ~workers
	// (not one goroutine per job, which would explode toward the huge total).
	var mid int
	for i := 0; i < 200; i++ {
		if mid = runtime.NumGoroutine(); mid >= baseline+workers {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if mid < baseline+workers-5 {
		t.Fatalf("worker pool did not start: baseline=%d mid=%d", baseline, mid)
	}
	if mid > baseline+workers+30 {
		t.Fatalf("goroutine count not bounded (leaking per job?): baseline=%d mid=%d", baseline, mid)
	}

	cancel()

	select {
	case <-done:
		// run() returned: producer, all workers, and the closer have exited.
	case <-time.After(3 * time.Second):
		t.Fatal("run did not return after cancel — goroutine leak or hang")
	}

	// Allow the closer goroutine (it calls close(results) just before run
	// returns) a moment to fully exit, then require the count to settle back
	// near baseline. Poll briefly to avoid flakiness from runtime background
	// goroutines.
	var final int
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if final = runtime.NumGoroutine(); final <= baseline+5 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if leaked := final - baseline; leaked > 5 {
		t.Errorf("goroutine leak after shutdown: baseline=%d final=%d (leaked=%d)", baseline, final, leaked)
	}
}

// BenchmarkWorkerPool measures steady-state throughput (jobs/sec) of the pool
// across a few worker counts, exercising the full producer -> workers ->
// collector path. Run with: go test -bench=BenchmarkWorkerPool -benchmem
func BenchmarkWorkerPool(b *testing.B) {
	const total = 10_000
	for _, workers := range []int{50, 200, 1000} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			ctx := context.Background()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				run(ctx, config{total: total, workers: workers, jobBuf: workers, resBuf: workers, minMs: 1, maxMs: 1})
			}
			throughput := float64(total) * float64(b.N) / b.Elapsed().Seconds()
			b.ReportMetric(throughput, "jobs/sec")
		})
	}
}
