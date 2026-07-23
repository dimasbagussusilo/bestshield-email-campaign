// Command tamakun is a high-throughput promotional-email sender DRY RUN.
//
// It streams 1,000,000 personalized Jobs through a fixed-size worker pool of
// goroutines connected by buffered channels, simulating each network "send" as
// a 10-50ms sleep. No real email is ever sent. The pool bounds goroutines and
// memory (the explicit alternative to spawning 1 goroutine per recipient).
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"sync"
	"time"
)

// Job is a single unit of work: one personalized promotional email.
type Job struct {
	ID        int    // sequential recipient index 0..N-1
	Email     string // e.g. "customer000123@example.com"
	Name      string // first name for personalization
	PromoCode string // per-recipient code, e.g. "PROMO-1A2B3C"
}

// Result is the outcome of processing one Job.
type Result struct {
	JobID    int
	OK       bool          // true on success
	Err      string        // "" when OK; populated on failure/cancel
	Duration time.Duration // measured simulated send latency
}

// config bundles the tunables for a run.
type config struct {
	total    int     // number of recipients
	workers  int     // worker pool size (the central knob)
	jobBuf   int     // jobs channel capacity
	resBuf   int     // results channel capacity
	minMs    int     // min simulated latency
	maxMs    int     // max simulated latency
	failRate float64 // synthetic failure rate 0..1 (exercises the error path)
}

// stats is the aggregated tally. Owned solely by the collector goroutine,
// so it needs no synchronization.
type stats struct {
	total, sent, failed int
}

func (s *stats) add(r Result) {
	s.total++
	if r.OK {
		s.sent++
	} else {
		s.failed++
	}
}

func main() {
	// Define flags, THEN Parse, THEN dereference — otherwise cfg would capture
	// the defaults and ignore the command line.
	total := flag.Int("total", 1_000_000, "number of recipients")
	workers := flag.Int("workers", 1000, "worker pool size")
	minMs := flag.Int("min-ms", 10, "min simulated send latency in ms")
	maxMs := flag.Int("max-ms", 50, "max simulated send latency in ms")
	failRate := flag.Float64("fail-rate", 0, "synthetic failure rate 0..1")
	// -1 means "default to the worker count".
	jobBuf := flag.Int("job-buffer", -1, "jobs channel buffer (default = workers)")
	resBuf := flag.Int("result-buffer", -1, "results channel buffer (default = workers)")
	flag.Parse()

	cfg := config{
		total:    *total,
		workers:  *workers,
		minMs:    *minMs,
		maxMs:    *maxMs,
		failRate: *failRate,
		jobBuf:   *jobBuf,
		resBuf:   *resBuf,
	}
	if cfg.jobBuf < 0 {
		cfg.jobBuf = cfg.workers
	}
	if cfg.resBuf < 0 {
		cfg.resBuf = cfg.workers
	}

	if err := cfg.validate(); err != nil {
		fmt.Fprintf(os.Stderr, "invalid flags: %v\n", err)
		flag.Usage()
		os.Exit(2)
	}

	// Ctrl-C cancels the run; in-flight jobs finish/cancel and a partial
	// tally is reported.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	start := time.Now()
	s, peakGo := run(ctx, cfg)
	elapsed := time.Since(start)

	report(os.Stdout, cfg, s, elapsed, peakGo)

	if ctx.Err() != nil {
		os.Exit(130) // 128 + SIGINT(2)
	}
}

func (c config) validate() error {
	switch {
	case c.total <= 0:
		return fmt.Errorf("-total must be > 0")
	case c.workers <= 0:
		return fmt.Errorf("-workers must be > 0")
	case c.jobBuf < 0 || c.resBuf < 0:
		return fmt.Errorf("buffer sizes must be >= 0")
	case c.minMs < 0 || c.maxMs < c.minMs:
		return fmt.Errorf("-min-ms/-max-ms must satisfy 0 <= min <= max")
	case c.failRate < 0 || c.failRate > 1:
		return fmt.Errorf("-fail-rate must be in [0, 1]")
	}
	return nil
}

// run builds the pipeline and blocks until all results are collected.
//
//	producer ──jobs(chan, jobBuf)──▶ N workers ──results(chan, resBuf)──▶ collector
//
// Completion is driven entirely by close():
//  1. producer closes jobs when done (or ctx cancelled);
//  2. workers exit their range loop when jobs closes, then wg.Done();
//  3. the closer goroutine waits on all workers, then closes results;
//  4. the collector (this goroutine) ranges results until closed.
func run(ctx context.Context, cfg config) (stats, int) {
	jobs := make(chan Job, cfg.jobBuf)
	results := make(chan Result, cfg.resBuf)

	// Spawn a fixed pool of workers.
	var wg sync.WaitGroup
	for i := 0; i < cfg.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs { // exits when jobs is closed
				res := sendEmail(ctx, job, cfg.minMs, cfg.maxMs)
				// Optionally inject a synthetic failure AFTER the simulated
				// latency, to exercise the error path.
				if res.OK && cfg.failRate > 0 && rand.Float64() < cfg.failRate {
					res.OK = false
					res.Err = "simulated SMTP 4xx rejection"
				}
				results <- res
			}
		}()
	}

	// Producer: stream synthetic Jobs. Backpressure-safe (blocks when the jobs
	// buffer is full) and cancellation-responsive even while blocked.
	go func() {
		defer close(jobs)
		for i := 0; i < cfg.total; i++ {
			select {
			case jobs <- makeJob(i):
			case <-ctx.Done():
				return
			}
		}
	}()

	// Closer: once every worker has exited, no more results will be written.
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collector: sole owner of stats. Drains results until results is closed.
	var s stats
	peakGo := runtime.NumGoroutine()
	for r := range results {
		s.add(r)
		if s.total%100_000 == 0 {
			if ng := runtime.NumGoroutine(); ng > peakGo {
				peakGo = ng
			}
			fmt.Fprintf(os.Stderr, "  progress: %s results, %s sent, %s failed (goroutines=%d)\n",
				commas(s.total), commas(s.sent), commas(s.failed), peakGo)
		}
	}
	return s, peakGo
}

// sendEmail simulates one personalized send: a random 10-50ms network delay,
// interruptible by ctx. Uses time.NewTimer (not time.After) so we Stop() it
// and avoid leaving a timer behind on every one of 1M calls.
func sendEmail(ctx context.Context, job Job, minMs, maxMs int) Result {
	start := time.Now()
	span := maxMs - minMs + 1 // inclusive upper bound
	delay := time.Duration(minMs+rand.IntN(span)) * time.Millisecond

	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-t.C:
		return Result{JobID: job.ID, OK: true, Duration: time.Since(start)}
	case <-ctx.Done():
		return Result{JobID: job.ID, OK: false, Err: ctx.Err().Error(), Duration: time.Since(start)}
	}
}

// makeJob fabricates a synthetic, personalized Job for recipient i.
func makeJob(i int) Job {
	return Job{
		ID:        i,
		Name:      firstNames[i%len(firstNames)],
		Email:     fmt.Sprintf("customer%06d@example.com", i),
		PromoCode: fmt.Sprintf("PROMO-%06X", i),
	}
}

func report(w *os.File, cfg config, s stats, elapsed time.Duration, peakGo int) {
	throughput := 0.0
	if elapsed > 0 {
		throughput = float64(s.sent) / elapsed.Seconds()
	}
	status := "COMPLETE"
	if s.failed > 0 || s.sent < cfg.total {
		status = "PARTIAL"
	}
	fmt.Fprintf(w, "====== Dry-run summary (%s) ======\n", status)
	fmt.Fprintf(w, "recipients       : %s\n", commas(cfg.total))
	fmt.Fprintf(w, "sent             : %s\n", commas(s.sent))
	fmt.Fprintf(w, "failed           : %s\n", commas(s.failed))
	fmt.Fprintf(w, "workers          : %s\n", commas(cfg.workers))
	fmt.Fprintf(w, "job buffer       : %s\n", commas(cfg.jobBuf))
	fmt.Fprintf(w, "result buffer    : %s\n", commas(cfg.resBuf))
	fmt.Fprintf(w, "latency range    : %d-%dms\n", cfg.minMs, cfg.maxMs)
	fmt.Fprintf(w, "elapsed          : %s\n", elapsed.Round(time.Millisecond))
	fmt.Fprintf(w, "throughput       : %s sends/sec\n", commas(int(throughput)))
	fmt.Fprintf(w, "GOMAXPROCS       : %d\n", runtime.GOMAXPROCS(0))
	fmt.Fprintf(w, "peak goroutines  : %d\n", peakGo)
}

// commas formats a non-negative integer with thousands separators.
func commas(n int) string {
	s := strconv.Itoa(n)
	var b []byte
	for i := 0; i < len(s); i++ {
		if i > 0 && (len(s)-i)%3 == 0 {
			b = append(b, ',')
		}
		b = append(b, s[i])
	}
	return string(b)
}

var firstNames = []string{
	"Aiko", "Boris", "Camila", "Dmitri", "Esme", "Farid", "Greta", "Hiro",
	"Ines", "Javed", "Kofi", "Lena", "Mateo", "Nadia", "Omar", "Priya",
	"Quinn", "Ravi", "Sora", "Tariq", "Uma", "Viktor", "Wei", "Xiomara",
	"Yuki", "Zane",
}
