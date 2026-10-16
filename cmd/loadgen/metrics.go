package main

import (
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Live Prometheus metrics for loadgen, so a run is visible WHILE it happens
// rather than only in the JSON report afterwards.
//
// Why this is here at all: loadgen's report is the authoritative record of a
// window, but it arrives after the window closes, and the most common way this
// harness produces a confident wrong answer is a load generator that quietly
// stopped holding its offered rate (METHODOLOGY 29). Watching send_lateness
// against the pacing interval catches that in the first seconds instead of
// after the fact.
//
// Deliberately hand-rolled rather than pulling in prometheus/client_golang:
//
//   - loadgen is the instrument every published number comes from. Adding a
//     dependency to it means the measuring device changed, and the exposition
//     format is a few dozen lines of text that is far easier to audit than a
//     library's collector machinery.
//   - go.mod stays at one direct dependency (franz-go).
//
// Off unless -metrics-listen is given, and when off the only cost is one
// already-false bool check per sample. The counters are plain atomics, so the
// hot path pays two atomic adds per order, which is nanoseconds against a
// 1,000us pacing interval.
//
// These are SEPARATE from the atomics main() keeps for the report. main resets
// recordsSent between the warmup and timed phases, which is right for a
// per-window report and wrong for a Prometheus counter: a counter that goes
// backwards makes rate() produce a spike at the reset. Keeping our own
// monotonic set costs one more atomic and removes that coupling entirely.

// Bucket upper bounds in SECONDS. Prometheus convention is base units, and
// mixing microseconds in here would mean every dashboard needs a unit
// conversion that someone eventually forgets.
//
// Resolution is deliberately densest between 100us and 2.5ms: the pacing
// interval at the published RATE=1000 is 1,000us, and the question these
// buckets exist to answer is whether the p99 is approaching it. Buckets an
// order of magnitude apart would answer "yes or no" but not "by how much".
var latenessBounds = []float64{
	0.00001, 0.000025, 0.00005, // 10, 25, 50us
	0.0001, 0.00025, 0.0005, // 100, 250, 500us
	0.001, 0.0025, 0.005, // 1, 2.5, 5ms
	0.01, 0.025, 0.05, // 10, 25, 50ms
	0.1, 0.25, 0.5, 1.0, // 100, 250, 500ms, 1s
}

type liveMetrics struct {
	enabled bool

	ordersSent    atomic.Int64
	ordersAcked   atomic.Int64
	produceErrors atomic.Int64
	receipts      atomic.Int64

	// Set once from the flags, exported as gauges so a dashboard can draw the
	// pacing interval as a threshold instead of having it hardcoded in a panel
	// that then disagrees with the run.
	offeredRate    float64
	pacingInterval float64
	inbrokerFanout int
	arm            string
	// The run's -label (the runner's level tag, e.g. bookdepth-10000), so a
	// dashboard can name and shade each level, not just each autorun phase.
	label string

	// Guarded by the caller's existing lateness mutex - see recordLateness.
	// No mutex of its own on purpose: the send loop already takes one to append
	// to its lateness slice, and adding a second lock to the same critical
	// section would be pure overhead.
	buckets []uint64
	count   uint64
	sum     float64

	// Only for the phase gauge, which is written from a different goroutine
	// than the one recording lateness.
	phaseMu sync.Mutex
	phase   string
}

func newLiveMetrics() *liveMetrics {
	return &liveMetrics{buckets: make([]uint64, len(latenessBounds))}
}

// recordLateness files one send-lateness sample, in MICROSECONDS to match what
// the send loop already computes.
//
// MUST be called with the send loop's lateness mutex held. That is why it takes
// no lock: it is called from inside an existing critical section, and the
// alternative is a second lock acquisition per sample on the hot path.
func (m *liveMetrics) recordLateness(micros float64) {
	if !m.enabled {
		return
	}
	secs := micros / 1e6
	m.count++
	m.sum += secs
	// Linear scan over 16 bounds. A binary search would be asymptotically
	// better and measurably slower at this size.
	for i, b := range latenessBounds {
		if secs <= b {
			m.buckets[i]++
			return
		}
	}
	// Above the last bound: counted in _count and the +Inf bucket only, which
	// is what makes the +Inf bucket equal _count below.
}

func (m *liveMetrics) setPhase(p string) {
	if !m.enabled {
		return
	}
	m.phaseMu.Lock()
	m.phase = p
	m.phaseMu.Unlock()
}

// render writes the exposition format. latenessMu must be held by the caller
// for the same reason recordLateness needs it.
func (m *liveMetrics) render(latenessMu *sync.Mutex) string {
	var b []byte
	add := func(format string, args ...any) {
		b = append(b, fmt.Sprintf(format, args...)...)
	}

	counter := func(name, help string, v int64) {
		add("# HELP %s %s\n# TYPE %s counter\n%s %d\n", name, help, name, name, v)
	}
	gauge := func(name, help string, v float64) {
		add("# HELP %s %s\n# TYPE %s gauge\n%s %s\n", name, help, name, name,
			strconv.FormatFloat(v, 'g', -1, 64))
	}

	counter("loadgen_orders_sent_total",
		"Orders offered to the cluster. One order is a crossing pair, so this is half the record count.",
		m.ordersSent.Load())
	counter("loadgen_orders_acked_total",
		"Orders whose aggressor leg was acknowledged by the broker.",
		m.ordersAcked.Load())
	counter("loadgen_produce_errors_total",
		"Produce callbacks that returned an error.",
		m.produceErrors.Load())
	counter("loadgen_receipts_total",
		"Receipts observed coming back from the transform output, warmup and timed phases combined.",
		m.receipts.Load())

	gauge("loadgen_offered_rate_orders_per_second",
		"The configured offered rate. Compare against rate(loadgen_orders_sent_total) to see whether it is actually being held.",
		m.offeredRate)
	gauge("loadgen_pacing_interval_seconds",
		"Seconds between scheduled sends at the offered rate. send_lateness approaching this means the generator is no longer pacing and its latency figures include its own queueing.",
		m.pacingInterval)

	latenessMu.Lock()
	cnt, sum := m.count, m.sum
	buckets := append([]uint64(nil), m.buckets...)
	latenessMu.Unlock()

	add("# HELP loadgen_send_lateness_seconds How late each produce call was against its scheduled slot. This harness's coordinated-omission detector: if it grows, the CLIENT is the bottleneck and the latency it reports is not the cluster's.\n")
	add("# TYPE loadgen_send_lateness_seconds histogram\n")
	var cum uint64
	for i, bound := range latenessBounds {
		cum += buckets[i]
		add("loadgen_send_lateness_seconds_bucket{le=\"%s\"} %d\n",
			strconv.FormatFloat(bound, 'g', -1, 64), cum)
	}
	add("loadgen_send_lateness_seconds_bucket{le=\"+Inf\"} %d\n", cnt)
	add("loadgen_send_lateness_seconds_sum %s\n", strconv.FormatFloat(sum, 'g', -1, 64))
	add("loadgen_send_lateness_seconds_count %d\n", cnt)

	m.phaseMu.Lock()
	phase := m.phase
	m.phaseMu.Unlock()
	if phase != "" {
		add("# HELP loadgen_phase_info Which phase the run is in (preload, warmup, measure, drain), as a label on a constant 1.\n")
		add("# TYPE loadgen_phase_info gauge\n")
		add("loadgen_phase_info{phase=%q,arm=%q,label=%q} 1\n", phase, m.arm, m.label)
	}
	return string(b)
}

// serve starts the metrics endpoint in the background. A failure to bind is
// logged and otherwise ignored: losing the observer must never fail a run that
// is otherwise measuring correctly, and the scrape job is allowed to be down.
func (m *liveMetrics) serve(addr string, latenessMu *sync.Mutex) {
	if !m.enabled {
		return
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = w.Write([]byte(m.render(latenessMu)))
	})
	srv := &http.Server{
		Addr:              listenAddr(addr),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Printf("loadgen: live metrics on %s/metrics", addr)
		if err := srv.ListenAndServe(); err != nil {
			log.Printf("loadgen: metrics endpoint stopped: %v", err)
		}
	}()
}

// listenAddr normalises a bare port ("9110") to ":9110", so -metrics-listen
// accepts either form.
func listenAddr(addr string) string {
	if addr == "" {
		return ""
	}
	if _, err := strconv.Atoi(addr); err == nil {
		return ":" + addr
	}
	return addr
}
