// Command loadgen is the open-loop, rate-controlled load generator this
// project did not have. Every other harness here is closed-loop with one
// record in flight (cmd/bench sends one order and waits for its specific
// fill; cmd/relay-wasm-fanout-bench produces N crossing pairs as fast as it
// can with no pacing), which is the right methodology for "is in-broker
// faster than over-the-wire" and the wrong one for any question about
// throughput. A closed-loop harness cannot raise offered load - bigger N
// only makes the run longer - so it can never find a saturation point, and
// cannot tell "slow" apart from "saturated".
//
// What open loop means here, concretely:
//
//   - Offered rate is an INPUT (-rate). Achieved rate is an OUTPUT. When
//     they diverge the run is past saturation and its latency number is a
//     queue depth, not a latency. The report says so via clean:false.
//   - Sends are paced against an ABSOLUTE schedule anchored at the window's
//     start, not by sleeping a fixed interval per iteration. Per-iteration
//     sleeping drifts, and the drift silently lowers the real offered rate
//     exactly when the system slows down - which is the moment the number
//     matters most.
//   - The gap between when a send was scheduled and when it actually went
//     out is measured and reported as send_lateness. That is this harness's
//     own coordinated-omission detector: if lateness grows, the CLIENT is
//     the bottleneck and the run says nothing about the broker.
//
// Correlation without a map: at 100k orders/sec a map of order-id -> send
// time is hundreds of MB and becomes its own bottleneck. Instead the send
// timestamp is encoded into the aggressor order's own id, which
// rust/src/bin/relay-probe.rs already echoes back verbatim, so latency is
// recovered by parsing the id off the receipt - no map, no lock, O(1) memory
// on the hot path. Only the sampled subset (-sample-every) keeps any
// per-order state at all.
//
// Requires the caller to have already deployed wasm-matcher and every
// relay-probe instance, and to have pinned placement per
// bench/run-relay-wasm-fanout-crossshard.sh. Same prerequisites as
// cmd/relay-wasm-fanout-bench; this only changes how load is offered.
//
// CLOCK PROVENANCE - read before trusting a cross-clock stage.
// send_lateness and produce are measured entirely on the client's own clock.
// relay_consume is measured entirely on the broker's guest clock (both
// endpoints stamped in-guest, same machine). Those three are sound. match
// and total each subtract a client-clock reading from a broker-clock
// reading, so they carry the client-to-broker clock offset in full. At the
// microsecond scale this project targets, ordinary NTP sync (often hundreds
// of microseconds off, sometimes milliseconds) would dominate those two
// numbers entirely. This tool reports a coarse bracket-based skew estimate
// as a gross-error check only - it is NOT a correction, and its uncertainty
// is milliseconds by construction. Real cross-clock numbers need PTP-grade
// sync on both hosts with the residual offset recorded per run; see
// bench/SCALING-TEST-PLAN.md (P0.5).
package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/vuldin/redpanda-wasm-bench/wire"
)

// ---------------------------------------------------------------------------
// stats
// ---------------------------------------------------------------------------

type stageStats struct {
	Samples    int     `json:"samples"`
	MinMicros  float64 `json:"min_micros"`
	MeanMicros float64 `json:"mean_micros"`
	P50Micros  float64 `json:"p50_micros"`
	P90Micros  float64 `json:"p90_micros"`
	P99Micros  float64 `json:"p99_micros"`
	P999Micros float64 `json:"p999_micros"`
	MaxMicros  float64 `json:"max_micros"`
}

func newStageStats(xs []float64) stageStats {
	sort.Float64s(xs)
	s := stageStats{Samples: len(xs)}
	if len(xs) == 0 {
		return s
	}
	s.MinMicros = xs[0]
	s.MeanMicros = mean(xs)
	s.P50Micros = percentile(xs, 0.50)
	s.P90Micros = percentile(xs, 0.90)
	s.P99Micros = percentile(xs, 0.99)
	s.P999Micros = percentile(xs, 0.999)
	s.MaxMicros = xs[len(xs)-1]
	return s
}

func mean(xs []float64) float64 {
	var sum float64
	for _, x := range xs {
		sum += x
	}
	return sum / float64(len(xs))
}

// percentile expects xs sorted ascending.
func percentile(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	if len(xs) == 1 {
		return xs[0]
	}
	idx := p * float64(len(xs)-1)
	lo := int(idx)
	hi := lo + 1
	if hi >= len(xs) {
		return xs[lo]
	}
	frac := idx - float64(lo)
	return xs[lo]*(1-frac) + xs[hi]*frac
}

// ---------------------------------------------------------------------------
// report
// ---------------------------------------------------------------------------

type configReport struct {
	OfferedRate  float64 `json:"offered_rate_orders_per_sec"`
	Duration     string  `json:"duration"`
	Warmup       string  `json:"warmup"`
	Pacing       string  `json:"pacing"`
	PayloadBytes int     `json:"payload_bytes"`
	NumProbes    int     `json:"num_probes"`
	SampleEvery  int     `json:"sample_every"`
	MaxBuffered  int     `json:"max_buffered_records"`
	KeyByPair    bool    `json:"key_by_pair"`
	LingerMs     int     `json:"producer_linger_ms"`
	// Acks is fingerprinted because it is the DOMINANT e2e term - a run at
	// acks=leader is not comparable with one at acks=all, and the difference
	// is a durability trade rather than a tuning choice.
	Acks          string `json:"producer_acks"`
	Producers     int    `json:"producers"`
	InputTopic    string `json:"input_topic"`
	ProbeTopic    string `json:"probe_topic"`
	TransformName string `json:"transform_name"`
}

// errorTally counts produce failures by their error string, bounded so a
// pathological run cannot grow it without limit.
//
// Added because a run reported "400,000 produce errors" with no indication of
// what they were, which made the cause unguessable - and two successive
// hypotheses (fsync cost, then producer linger) were both wrong. Recording the
// actual error is cheaper than another round of guessing.
type errorTally struct {
	mu     sync.Mutex
	counts map[string]int64
	total  int64
}

const maxDistinctErrors = 12

func newErrorTally() *errorTally {
	return &errorTally{counts: make(map[string]int64, maxDistinctErrors)}
}

func (e *errorTally) add(err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.total++
	msg := err.Error()
	if _, seen := e.counts[msg]; !seen && len(e.counts) >= maxDistinctErrors {
		e.counts["(other, distinct-error cap reached)"]++
		return
	}
	e.counts[msg]++
}

func (e *errorTally) snapshot() map[string]int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make(map[string]int64, len(e.counts))
	for k, v := range e.counts {
		out[k] = v
	}
	return out
}

type ratesReport struct {
	OfferedPerSec   float64 `json:"offered_orders_per_sec"`
	AttemptedPerSec float64 `json:"attempted_orders_per_sec"`
	// AchievedOrdersPerSec divides receipts by num_probes, putting it in the
	// same unit as the offered rate so the two compare directly.
	AchievedOrdersPerSec   float64          `json:"achieved_orders_per_sec"`
	AchievedReceiptsPerSec float64          `json:"achieved_receipts_per_sec"`
	AttemptedRatio         float64          `json:"attempted_over_offered"`
	AchievedRatio          float64          `json:"achieved_over_offered"`
	OrdersSent             int64            `json:"orders_sent"`
	RecordsSent            int64            `json:"records_sent"`
	ProduceErrors          int64            `json:"produce_errors"`
	ProduceErrorsByKind    map[string]int64 `json:"produce_errors_by_kind"`
	ElapsedSeconds         float64          `json:"elapsed_seconds"`
}

type clockReport struct {
	Note                  string   `json:"note"`
	SingleClockStages     []string `json:"single_clock_stages"`
	CrossClockStages      []string `json:"cross_clock_stages"`
	SkewEstimateMicros    float64  `json:"skew_estimate_micros"`
	SkewUncertaintyMicros float64  `json:"skew_uncertainty_micros"`
	SkewSamples           int      `json:"skew_samples"`
	// NegativeMatchMagnitudeMicros is how far below zero the `match` stage
	// went. It is NOT a skew bound: on 2026-09-02 it read 227-394us while
	// chrony had every host synced to 2-9us against the same AWS reference,
	// because the cause was the in-broker pre-commit read, not the clocks.
	// See judgeNegativeMatch.
	NegativeMatchMagnitudeMicros float64 `json:"negative_match_magnitude_micros"`
	NegativeMatchNote            string  `json:"negative_match_note"`
	// CrossClockUsable reports whether `match` and `total` mean anything for
	// THIS level. They always carry a +/-400-650us error bar on chrony-synced
	// hosts, which is already the same order as the numbers themselves - that
	// is a known, permanent limitation and is NOT what this flags. What it
	// flags is a clock STEP: an offset so large that the cross-clock stages
	// stop being a noisy measurement and become a reading of the clock error.
	//
	// Found the hard way on 2026-09-01 (run 1788316828, spread arm, fanout 12):
	// skew_estimate jumped to 12,493us against 19-81us on every other level,
	// and total_p50 duly reported 25,456us - a 37x apparent blow-up - while the
	// single-clock stages at that level were the BEST in the arm (produce
	// 338us, the lowest of five). `clean` was true, so the summary table
	// invited exactly the wrong conclusion and very nearly got it.
	// Deliberately phrased as the NEGATIVE so the zero value means "no problem
	// detected". As `CrossClockUsable bool` this defaulted to false, and every
	// report built without running the detector - including several in
	// main_test.go - was marked not-clean with an empty reason. A safety check
	// whose default state is "broken" is worse than no check.
	CrossClockUnusable bool   `json:"cross_clock_unusable"`
	CrossClockVerdict  string `json:"cross_clock_verdict"`
}

type funcResource struct {
	CPUSecondsDelta float64 `json:"cpu_seconds_delta"`
	// Invocations is this function's own guest-invocation count over the
	// window, read from redpanda_transform_execution_latency_sec_count -
	// measured per function, never inferred from what the client sent.
	Invocations float64 `json:"invocations"`
	// CPUMicrosPerInvocation is the honest per-record figure: CPU over the
	// guest's own invocation count.
	CPUMicrosPerInvocation float64 `json:"cpu_micros_per_invocation"`
	// CPUMicrosPerOrder is CPU over ORDERS offered. One order is a crossing
	// pair, so it is two input records and two matcher invocations - making
	// this roughly twice the per-invocation figure. Reported because -rate is
	// expressed in orders, so a capacity claim in orders/sec must use this.
	CPUMicrosPerOrder float64 `json:"cpu_micros_per_order"`
	MemoryUsageEnd    float64 `json:"memory_usage_end_bytes"`
	MaxMemoryEnd      float64 `json:"max_memory_end_bytes"`
}

type resourcesReport struct {
	// Keyed by the function_name label the broker exports.
	PerFunction map[string]funcResource `json:"per_function"`

	TransformReadBytesDelta  float64 `json:"transform_read_bytes_delta"`
	TransformWriteBytesDelta float64 `json:"transform_write_bytes_delta"`
	TransformFailuresDelta   float64 `json:"transform_failures_delta"`
	TransformGivenUpDelta    float64 `json:"transform_batches_given_up_delta"`

	// RelayPushesDelta counts SHARD-LOCAL DELIVERY PASSES, not producer
	// pushes: redpanda_relay_pushes_total is incremented inside
	// relay::service::deliver_locally(), which push() invokes once for the
	// producer's own shard plus once per other shard holding a subscriber. It
	// therefore grows with how widely subscribers are spread, independently of
	// the offered rate.
	RelayPushesDelta    float64 `json:"relay_pushes_delta"`
	RelayDeliveredDelta float64 `json:"relay_delivered_delta"`
	RelayDroppedDelta   float64 `json:"relay_dropped_delta"`
	RelayActiveSubsEnd  float64 `json:"relay_active_subscriptions_end"`
	// RelayDeliveredPerShardPass is delivered/pushes. It is NOT a fan-out
	// ratio and was previously mis-named relay_delivered_per_push: since each
	// shard pass delivers to that shard's own subscribers, the quotient sits
	// near 1 at every fan-out level. Measured 2026-08-29: fanout 10 reported
	// 1.0 while the true ratio was 10.
	RelayDeliveredPerShardPass float64 `json:"relay_delivered_per_shard_pass"`
	// RelayDeliveredPerLogicalPush is the real fan-out ratio. This workload
	// emits two records per order, so logical pushes = orders x 2 and we can
	// compute it directly rather than inferring it from a broker counter.
	RelayDeliveredPerLogicalPush float64 `json:"relay_delivered_per_logical_push"`

	ScrapedBeforeAndAfter bool `json:"scraped_before_and_after"`
	// WindowCoverage is the producer's invocation count over records sent.
	// Below ~1.0 means the after-snapshot was taken before the pipeline
	// finished, so every per-record figure is truncated.
	WindowCoverage float64 `json:"window_coverage"`
	AdminEndpoints int     `json:"admin_endpoints_scraped"`
}

type lagReport struct {
	Samples  int     `json:"samples"`
	MaxLag   float64 `json:"max_lag"`
	FirstLag float64 `json:"first_lag"`
	FinalLag float64 `json:"final_lag"`
	// BacklogThreshold is the record count above which a remaining backlog is
	// treated as real saturation rather than noise. Recorded so the verdict can
	// be re-derived instead of taken on trust.
	BacklogThreshold float64 `json:"backlog_threshold_records"`
	// GrewDuringRun means the transform ended the window meaningfully behind:
	// lag both increased AND finished above BacklogThreshold.
	GrewDuringRun bool `json:"grew_during_run"`
	// PeakedAboveThreshold means lag rose above BacklogThreshold at ANY point,
	// even if it drained again before the window closed. This is a separate
	// condition from GrewDuringRun and it needs its own check: on 2026-08-29 a
	// 50,000 orders/sec level peaked at 50,843 records of lag - twice the
	// threshold - then drained to 123 by the end, so FinalLag > FirstLag was
	// false and the run reported CLEAN while its total_p50 was 73.7ms and its
	// p99 was 491ms. Those are queue depths, not latencies. Throughput in such
	// a run is still valid (every receipt arrived); only the latency is not.
	PeakedAboveThreshold bool `json:"peaked_above_threshold"`
}

type report struct {
	Label  string       `json:"label"`
	RunID  int64        `json:"run_id"`
	Config configReport `json:"config"`
	Clock  clockReport  `json:"clock"`
	Rates  ratesReport  `json:"rates"`

	ExpectedReceipts int64 `json:"expected_receipts"`
	ReceivedReceipts int64 `json:"received_receipts"`
	// DuplicateReceipts counts sampled orders that produced MORE than
	// num_probes receipts. A matching engine yields one fill per crossing
	// pair, so a duplicate means the record was processed more than once -
	// which is what a processor restart does, since it resumes from the last
	// committed offset and replays up to commit_interval_ms of work.
	// Recorded because received_receipts once EXCEEDED expected_receipts and
	// the cause could not be distinguished from a short drain without it.
	DuplicateReceipts      int64 `json:"duplicate_receipts_sampled"`
	MaxReceiptsForAnyOrder int   `json:"max_receipts_for_any_sampled_order"`
	MissingReceipts        int64 `json:"missing_receipts"`
	WarmupReceipts         int64 `json:"warmup_receipts"`
	// ClientBound means the pace loop aborted early because the producer buffer
	// saturated - the level is a statement about the load generator, not the
	// broker.
	ClientBound bool `json:"client_bound"`

	SendLateness stageStats `json:"send_lateness"`
	Produce      stageStats `json:"produce"`
	Match        stageStats `json:"match"`
	RelayConsume stageStats `json:"relay_consume"`
	Total        stageStats `json:"total"`

	Lag       lagReport       `json:"lag"`
	Resources resourcesReport `json:"resources"`

	Clean          bool     `json:"clean"`
	UncleanReasons []string `json:"unclean_reasons"`
}

// ---------------------------------------------------------------------------
// order ids: the send timestamp rides in the id, so correlation needs no map
// ---------------------------------------------------------------------------

// Aggressor (buy) id layout: "<runID><phase>-b<seq>-<sendUnixNanos>".
// Resting  (sell) id layout: "<runID><phase>-s<seq>-0".
//
// phase is 'w' (warmup, discarded) or 't' (timed). The run-id prefix keeps
// ids unique across runs against a long-lived matcher, which METHODOLOGY.md
// #8 records as a real failure that hangs the harness silently when missing.
func aggressorID(runID int64, phase byte, seq int64, sendNanos int64) string {
	var b strings.Builder
	b.Grow(48)
	b.WriteString(strconv.FormatInt(runID, 10))
	b.WriteByte(phase)
	b.WriteString("-b")
	b.WriteString(strconv.FormatInt(seq, 10))
	b.WriteByte('-')
	b.WriteString(strconv.FormatInt(sendNanos, 10))
	return b.String()
}

func restingID(runID int64, phase byte, seq int64) string {
	var b strings.Builder
	b.Grow(32)
	b.WriteString(strconv.FormatInt(runID, 10))
	b.WriteByte(phase)
	b.WriteString("-s")
	b.WriteString(strconv.FormatInt(seq, 10))
	b.WriteString("-0")
	return b.String()
}

type parsedID struct {
	runID     int64
	phase     byte
	seq       int64
	sendNanos int64
	ok        bool
}

func parseAggressorID(id string) parsedID {
	first := strings.IndexByte(id, '-')
	if first < 2 {
		return parsedID{}
	}
	head := id[:first]
	runID, err := strconv.ParseInt(head[:len(head)-1], 10, 64)
	if err != nil {
		return parsedID{}
	}
	phase := head[len(head)-1]
	rest := id[first+1:]
	if len(rest) < 2 || rest[0] != 'b' {
		return parsedID{}
	}
	rest = rest[1:]
	dash := strings.IndexByte(rest, '-')
	if dash < 0 {
		return parsedID{}
	}
	seq, err := strconv.ParseInt(rest[:dash], 10, 64)
	if err != nil {
		return parsedID{}
	}
	sendNanos, err := strconv.ParseInt(rest[dash+1:], 10, 64)
	if err != nil {
		return parsedID{}
	}
	return parsedID{runID: runID, phase: phase, seq: seq, sendNanos: sendNanos, ok: true}
}

// ---------------------------------------------------------------------------
// prometheus text scraping
// ---------------------------------------------------------------------------

type sample struct {
	labels string
	value  float64
}

type snapshot struct {
	at      time.Time
	metrics map[string][]sample
}

// wanted lists the metric families scraped. Every one is on
// /public_metrics; nothing here needs the internal /metrics endpoint.
var wanted = map[string]bool{
	"redpanda_wasm_engine_cpu_seconds_total":     true,
	"redpanda_wasm_engine_memory_usage":          true,
	"redpanda_wasm_engine_max_memory":            true,
	"redpanda_transform_read_bytes":              true,
	"redpanda_transform_write_bytes":             true,
	"redpanda_transform_failures":                true,
	"redpanda_transform_batches_given_up":        true,
	"redpanda_transform_state_recovery_failures": true,
	"redpanda_transform_lag":                     true,
	// Exact per-function guest invocation count. This is the correct
	// denominator for per-record CPU - see buildResources.
	"redpanda_transform_execution_latency_sec_count": true,
	"redpanda_relay_pushes_total":                    true,
	"redpanda_relay_delivered_total":                 true,
	"redpanda_relay_dropped_total":                   true,
	"redpanda_relay_active_subscriptions":            true,
}

// scrape merges /public_metrics from every listed broker.
//
// Merging is required, not an optimisation: these metrics are per-node. A
// transform's wasm_engine and transform_* series exist only on the broker
// running that processor, and which broker that is follows partition
// leadership. Scraping a single endpoint therefore produces per-function CPU
// that is missing (n/a) or belongs to an idle duplicate, depending on where
// leadership happened to land - measured both ways on the same cluster.
//
// Summing is the right merge for every family here: counters and gauges are
// per-(node, function), and only one node runs the real processor, so the sum
// is that node's value plus ~0 from idle duplicates.
func scrape(adminURLs string) (*snapshot, error) {
	urls := strings.Split(adminURLs, ",")
	merged := &snapshot{at: time.Now(), metrics: make(map[string][]sample)}
	var firstErr error
	ok := 0
	for _, u := range urls {
		u = strings.TrimSpace(u)
		if u == "" {
			continue
		}
		snap, err := scrapeOne(u)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		ok++
		for name, samples := range snap.metrics {
			merged.metrics[name] = append(merged.metrics[name], samples...)
		}
	}
	if ok == 0 {
		if firstErr != nil {
			return nil, firstErr
		}
		return nil, fmt.Errorf("no admin endpoints given")
	}
	return merged, nil
}

func scrapeOne(adminURL string) (*snapshot, error) {
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Get(strings.TrimRight(adminURL, "/") + "/public_metrics")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("scrape %s: status %d", adminURL, resp.StatusCode)
	}
	return parseMetricsBody(resp.Body), nil
}

// parseMetricsBody keeps only the families in wanted. Split out from scrape so
// it can be tested against a real-shaped body without a broker.
func parseMetricsBody(r io.Reader) *snapshot {
	snap := &snapshot{at: time.Now(), metrics: make(map[string][]sample, len(wanted))}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 256*1024), 8*1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if len(line) == 0 || line[0] == '#' {
			continue
		}
		// "<name>{<labels>} <value>" or "<name> <value>"
		sp := strings.LastIndexByte(line, ' ')
		if sp < 0 {
			continue
		}
		head, valStr := line[:sp], line[sp+1:]
		name, labels := head, ""
		if brace := strings.IndexByte(head, '{'); brace >= 0 {
			name = head[:brace]
			labels = head[brace:]
		}
		if !wanted[name] {
			continue
		}
		v, err := strconv.ParseFloat(valStr, 64)
		if err != nil || math.IsNaN(v) {
			continue
		}
		snap.metrics[name] = append(snap.metrics[name], sample{labels: labels, value: v})
	}
	return snap
}

func (s *snapshot) sum(name string) float64 {
	var total float64
	for _, smp := range s.metrics[name] {
		total += smp.value
	}
	return total
}

// byLabel sums a metric per distinct value of one label, so CPU and memory
// can be attributed per function_name without modelling the whole label
// space.
//
// The match is anchored at a label boundary ('{' or ','), not a plain
// substring: searching for `function_name="` would also hit inside a label
// named `redpanda_function_name`, silently folding an unrelated series into
// the wrong bucket and inflating whatever capacity number is computed from it.
func (s *snapshot) byLabel(name, label string) map[string]float64 {
	out := make(map[string]float64)
	needle := label + `="`
	for _, smp := range s.metrics[name] {
		for i := 0; ; {
			rel := strings.Index(smp.labels[i:], needle)
			if rel < 0 {
				break
			}
			at := i + rel
			i = at + len(needle)
			if at == 0 {
				continue // no opening brace before it; malformed, skip
			}
			if c := smp.labels[at-1]; c != '{' && c != ',' {
				continue // a longer label ending in our name, not our label
			}
			rest := smp.labels[i:]
			j := strings.IndexByte(rest, '"')
			if j < 0 {
				break
			}
			out[rest[:j]] += smp.value
			break
		}
	}
	return out
}

func deltaSum(before, after *snapshot, name string) float64 {
	if before == nil || after == nil {
		return 0
	}
	return after.sum(name) - before.sum(name)
}

// ---------------------------------------------------------------------------
// pacing
// ---------------------------------------------------------------------------

// waitUntil blocks until t. Go's timers are not accurate enough to pace a
// 10us inter-arrival gap by sleeping it (the wakeup overshoot is itself
// larger than the gap, which would silently cap the real offered rate well
// below the requested one), so anything under spinThreshold is spun out
// instead. That costs a busy core on the client and is the deliberate trade:
// an inaccurate offered rate makes every number in the run meaningless,
// while a busy client core is merely a resource the report accounts for.
const spinThreshold = 500 * time.Microsecond

func waitUntil(t time.Time) {
	for {
		d := time.Until(t)
		if d <= 0 {
			return
		}
		if d > spinThreshold {
			time.Sleep(d - spinThreshold)
			continue
		}
		runtime.Gosched()
	}
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

type collector struct {
	runID       int64
	numProbes   int
	sampleEvery int

	timedReceipts  atomic.Int64
	warmupReceipts atomic.Int64

	mu          sync.Mutex
	totalLat    []float64
	matchLat    []float64
	relayLat    []float64
	skewEst     []float64
	skewUnc     []float64
	sampledSeen map[int64]int
	sendNanos   map[int64]int64     // sampled timed orders only
	ackTimes    map[int64]time.Time // sampled timed orders only
}

func newCollector(runID int64, numProbes, sampleEvery int) *collector {
	return &collector{
		runID: runID, numProbes: numProbes, sampleEvery: sampleEvery,
		sampledSeen: map[int64]int{},
		sendNanos:   map[int64]int64{},
		ackTimes:    map[int64]time.Time{},
	}
}

func (c *collector) recordSend(seq, nanos int64) {
	c.mu.Lock()
	c.sendNanos[seq] = nanos
	c.mu.Unlock()
}

func (c *collector) recordAck(seq int64, at time.Time) {
	c.mu.Lock()
	c.ackTimes[seq] = at
	c.mu.Unlock()
}

// produceStage is ack - send, both readings taken on the client's own clock,
// so it is free of any cross-machine skew.
func (c *collector) produceStage() []float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]float64, 0, len(c.ackTimes))
	for seq, ackT := range c.ackTimes {
		sn, ok := c.sendNanos[seq]
		if !ok {
			continue
		}
		out = append(out, float64(ackT.Sub(time.Unix(0, sn)).Nanoseconds())/1000)
	}
	return out
}

// decodeFills parses the matcher's fill-list wire format, the same shape
// relay-probe and relay-sink decode guest-side: a u16-BE count, then per fill
// four u16-BE-length-prefixed strings (aggressor_id, resting_id,
// aggressor_party, resting_party) followed by 16 bytes, then a trailing 8-byte
// matched_at_nanos the matcher stamps unconditionally.
//
// Needed because the external arm consumes the fills topic directly - there is
// no relay-probe in that arm to render a text receipt.
func decodeFills(buf []byte) (ids []string, matchedAtNanos uint64, ok bool) {
	if len(buf) < 2 {
		return nil, 0, false
	}
	count := int(binary.BigEndian.Uint16(buf[0:2]))
	rest := buf[2:]
	readString := func(b []byte) (string, []byte, bool) {
		if len(b) < 2 {
			return "", nil, false
		}
		n := int(binary.BigEndian.Uint16(b[0:2]))
		b = b[2:]
		if len(b) < n {
			return "", nil, false
		}
		return string(b[:n]), b[n:], true
	}
	for i := 0; i < count; i++ {
		var aggressor string
		var okk bool
		if aggressor, rest, okk = readString(rest); !okk {
			return nil, 0, false
		}
		if _, rest, okk = readString(rest); !okk { // resting_id
			return nil, 0, false
		}
		if _, rest, okk = readString(rest); !okk { // aggressor_party
			return nil, 0, false
		}
		if _, rest, okk = readString(rest); !okk { // resting_party
			return nil, 0, false
		}
		if len(rest) < 16 {
			return nil, 0, false
		}
		rest = rest[16:]
		ids = append(ids, aggressor)
	}
	if len(rest) < 8 {
		return nil, 0, false
	}
	return ids, binary.BigEndian.Uint64(rest[0:8]), true
}

// onFill is the external arm's counterpart to onReceipt: loadgen is itself the
// external Kafka consumer, so the receipt timestamp is loadgen's OWN clock -
// the same clock the send timestamp came from.
//
// That makes `total` a SINGLE-CLOCK measurement in this arm, with no skew
// residual at all, which is the whole reason the comparison is done this way.
// On comparing this arm's total against the in-broker arm's, which is subtler
// than it looks and was stated backwards here until 2026-08-31.
//
// This arm's total ends when THIS process observes the fill, so it includes the
// fill's write plus the client's own fetch. The in-broker arm's total ends at a
// stamp the probe guest takes in the broker at the instant the record is
// delivered to it - so it does NOT include the probe -> probe_out -> loadgen hop,
// which exists only to carry the timestamp out. The earlier comment claimed the
// opposite and concluded the ratio understates the in-broker path; it does not.
//
// Each arm does measure the honest "consumer has the data" instant for its own
// architecture, which is the comparison worth making - the endpoints differ
// because the architectures genuinely differ in where the consumer lives. Two
// caveats survive: the in-broker total is cross-clock while this one is not, and
// the in-broker arm's broker does extra work (writing 150k receipts) that this
// arm's does not, which if anything inflates the in-broker latency.
//
// The clock-free way to compare, preferred over the raw ratio: measure each
// arm's total against its OWN single-clock produce-ack from the same run.
//
// relay_consume and match are deliberately NOT recorded here. Both would need a
// broker-clock reading, and mixing clocks is exactly what this arm exists to
// avoid. Absent is honest; a subtly cross-clock number is not.
func (c *collector) onFill(value []byte, observedAt time.Time) {
	ids, _, ok := decodeFills(value)
	if !ok {
		return
	}
	for _, id := range ids {
		pid := parseAggressorID(id)
		if !pid.ok || pid.runID != c.runID {
			continue
		}
		if pid.phase == 'w' {
			c.warmupReceipts.Add(1)
			continue
		}
		c.timedReceipts.Add(1)
		if pid.seq%int64(c.sampleEvery) != 0 {
			continue
		}
		sendT := time.Unix(0, pid.sendNanos)
		c.mu.Lock()
		c.totalLat = append(c.totalLat, float64(observedAt.Sub(sendT).Nanoseconds())/1000)
		c.sampledSeen[pid.seq]++
		c.mu.Unlock()
	}
}

func (c *collector) onReceipt(value string, observedAt time.Time) {
	// relay-probe writes "<aggressor_id>:<receipt_nanos>:<matched_at_nanos>"
	c1 := strings.IndexByte(value, ':')
	if c1 < 0 {
		return
	}
	c2 := strings.IndexByte(value[c1+1:], ':')
	if c2 < 0 {
		return
	}
	c2 += c1 + 1

	pid := parseAggressorID(value[:c1])
	if !pid.ok || pid.runID != c.runID {
		return
	}
	if pid.phase == 'w' {
		c.warmupReceipts.Add(1)
		return
	}
	c.timedReceipts.Add(1)
	if pid.seq%int64(c.sampleEvery) != 0 {
		return
	}

	receiptNanos, err := strconv.ParseInt(value[c1+1:c2], 10, 64)
	if err != nil {
		return
	}
	matchedNanos, err := strconv.ParseInt(value[c2+1:], 10, 64)
	if err != nil {
		return
	}
	sendT := time.Unix(0, pid.sendNanos)
	receiptT := time.Unix(0, receiptNanos)
	matchedT := time.Unix(0, matchedNanos)

	c.mu.Lock()
	defer c.mu.Unlock()

	// total and relay_consume get one sample per (order, probe) pair - those
	// are the two that scale with fan-out, so the full N-wide distribution is
	// the point. match does not vary per probe (same order, same matcher
	// run), so it is recorded once, on the first receipt seen for the order.
	c.totalLat = append(c.totalLat, float64(receiptT.Sub(sendT).Nanoseconds())/1000)
	c.relayLat = append(c.relayLat, float64(receiptT.Sub(matchedT).Nanoseconds())/1000)

	if c.sampledSeen[pid.seq] == 0 {
		if ackT, ok := c.ackTimes[pid.seq]; ok {
			c.matchLat = append(c.matchLat, float64(matchedT.Sub(ackT).Nanoseconds())/1000)
		}
		// Coarse skew bracket: the broker-clock reading receiptNanos is known
		// to fall between two client-clock readings - the send, and our own
		// observation of the receipt record. Midpoint is the estimate, half
		// the bracket width the uncertainty. The bracket spans the probe's
		// own Kafka produce plus our fetch, so this catches gross skew
		// (seconds) and is useless as a microsecond-scale correction.
		mid := sendT.Add(observedAt.Sub(sendT) / 2)
		c.skewEst = append(c.skewEst, float64(receiptT.Sub(mid).Nanoseconds())/1000)
		c.skewUnc = append(c.skewUnc, float64(observedAt.Sub(sendT).Nanoseconds())/2000)
	}
	c.sampledSeen[pid.seq]++
}

func main() {
	var (
		brokers        = flag.String("brokers", "127.0.0.1:19093", "comma-separated seed brokers")
		inputTopic     = flag.String("input-topic", "orders", "topic to produce crossing orders to")
		probeTopic     = flag.String("probe-topic", "relay-probe-out", "shared topic every relay-probe instance writes receipts to")
		fillsTopic     = flag.String("fills-topic", "fills", "the matcher's OUTPUT topic. Only consumed when -receipt-source=fills, where loadgen acts as an ordinary external Kafka consumer of the matcher's output")
		receiptSrc     = flag.String("receipt-source", "probe", `where receipts come from: "probe" (default - consume -probe-topic, correlating on relay-probe's "id:receipt:matched_at" text) or "fills" (consume -output-topic directly and decode the fill wire format, i.e. measure what an ORDINARY EXTERNAL KAFKA CONSUMER sees). "fills" is the external arm of the in-broker-vs-external comparison`)
		numProbes      = flag.Int("num-probes", 1, "number of relay-probe instances deployed; expected receipts per order")
		skewStepMicros = flag.Float64("skew-step-micros", 1000,
			"Client-to-broker clock offset above which the cross-clock stages (match, total) "+
				"are reported unusable and the run is marked not-clean. This is a CLOCK STEP detector, "+
				"not a sync-quality one: chrony-synced hosts here show 19-81us offsets, so 1000us is ~12x "+
				"above the worst normal reading and ~12x below the 12,493us step observed on 2026-09-01. "+
				"Single-clock stages are unaffected and stay valid.")
		clockRmsMicros = flag.Float64("clock-rms-micros", 0,
			"Independently measured host clock offset in microseconds (chrony RMS, as captured "+
				"in clock-sync.json). Used to tell the two causes of a negative `match` apart: "+
				"clock error, versus the in-broker pre-commit read where the matcher genuinely "+
				"stamps before the producer's quorum ack returns. 0 means unknown, and the report "+
				"then says it cannot distinguish them rather than guessing.")

		rate     = flag.Float64("rate", 1000, "offered rate in ORDERS (crossing pairs, so two records) per second. 0 sends as fast as possible, which measures the client's own ceiling as much as the broker's")
		duration = flag.Duration("duration", 30*time.Second, "length of the timed window")
		warmup   = flag.Duration("warmup", 5*time.Second, "length of the warmup window; its records are produced and its receipts discarded, absorbing VM cold start and connection/metadata setup")
		pacing   = flag.String("pacing", "fixed", "inter-arrival pacing: fixed (constant interval) or poisson (exponential, bursty - closer to a real arrival process and harder on queues)")

		payloadBytes = flag.Int("payload-bytes", 0, "pad each order's value to at least this many bytes with filler. Both wire decoders stop at the fixed layout and ignore trailing bytes, so this changes record size without changing semantics. 0 leaves orders at their natural size")
		sampleEvery  = flag.Int("sample-every", 1, "compute per-record latency for 1 in N orders. Receipt counts and rates always cover every order; this only bounds the memory the latency distributions take at high rates")
		maxBuffered  = flag.Int("max-buffered", 200000, "franz-go producer buffer ceiling. Hitting it makes Produce block, which surfaces as send_lateness instead of being silently absorbed")
		acksMode     = flag.String("acks", "all",
			"produce acknowledgement level: all (quorum, the default and the only durable choice), "+
				"leader (leader append only), or none. THIS IS THE DOMINANT e2e TERM: at RF=3 with "+
				"linger=0 every produce awaits a quorum round trip, measured at 641-1010us against "+
				"78us for the entire relay path. leader is a DURABILITY TRADE, defensible only because "+
				"in-broker relay consumers already read before replication completes, so it does not "+
				"change what they see. Never a default. Anything other than all also disables "+
				"idempotent writes, since franz-go requires AllISRAcks for idempotency.")
		numProducers = flag.Int("producers", 1, "number of independent producer clients (and pacing goroutines).\n\tONE client cannot saturate a single partition: franz-go caps in-flight produce requests per broker under idempotency, so at RF=3 a single client stalled at a hard ~2,739 orders/sec regardless of whether 10k or 100k was offered. N clients give N times the in-flight budget, which is also how a real deployment drives a hot partition - many producers, one partition.\n\tPairs are assigned to clients by sequence (seq mod producers) so BOTH legs of a crossing pair always go through the SAME client. That is required, not incidental: franz-go preserves order per client per partition, and splitting the legs across clients lets the buy arrive before its sell, making the SELL the aggressor - whose id this tool never recorded a send time for, silently destroying correlation")
		lingerMs     = flag.Int("linger-ms", 0, "producer linger. 0 is correct for LATENCY measurement (METHODOLOGY.md #5: franz-go's 10ms default was this project's single biggest measurement error) and is WRONG for throughput measurement at RF>1.\n\tWith linger=0 every produce is a tiny request, and at RF=3 each one awaits a quorum acknowledgement, so client throughput collapses to roughly one request per quorum round trip - measured at ~2,739 orders/sec regardless of whether 5k or 50k was offered, and unchanged by write caching. Set 1-5ms to measure the BROKER at RF=3; the produce stage then reports a batching artifact rather than a per-record latency, which is the deliberate trade. Always recorded in the fingerprint")
		keyByPair    = flag.Bool("key-by-pair", true, "give both legs of a crossing pair the same record key, so they hash to the same partition. REQUIRED for any multi-partition run: the matcher's order book is per-processor and there is one processor per partition, so legs that land on different partitions land in different books and can never match - which silently halves the fill rate and looks like a broker scaling limit. Set false only to reproduce that failure deliberately")
		drain        = flag.Duration("drain", 10*time.Second, "how long to keep collecting receipts after the send window closes")

		adminURL      = flag.String("admin-url", "", "comma-separated broker admin endpoints, e.g. http://10.0.0.33:9644,http://10.0.0.123:9644 - scrapes per-function CPU/memory, relay counters and transform lag before and after the window.\n\tPASS EVERY BROKER. These metrics are per-node: a transform's engine metrics exist only on the broker actually running it, and which broker that is follows partition leadership, which is not controlled. Scraping one broker silently omits the transform entirely (reported as n/a) or, worse, reports an IDLE duplicate instance pinned there by RELAY_TARGET_SHARD instead of the working one")
		transformName = flag.String("transform-name", "", "matcher transform name, recorded in the report and used to label its per-function resource row")
		lagInterval   = flag.Duration("lag-interval", 2*time.Second, "how often to sample transform lag during the timed window")

		label = flag.String("label", "loadgen", "label for this run, used in the report")
		out   = flag.String("out", "", "path to write the JSON report (stdout if empty)")
	)
	flag.Parse()

	if *sampleEvery < 1 {
		log.Fatalf("loadgen: -sample-every must be >= 1")
	}
	if *numProbes < 1 {
		log.Fatalf("loadgen: -num-probes must be >= 1")
	}
	if *pacing != "fixed" && *pacing != "poisson" {
		log.Fatalf("loadgen: -pacing must be fixed or poisson, got %q", *pacing)
	}

	seeds := strings.Split(*brokers, ",")
	runID := time.Now().UnixNano()
	coll := newCollector(runID, *numProbes, *sampleEvery)

	if *numProducers < 1 {
		log.Fatalf("loadgen: -producers must be >= 1")
	}
	var acks kgo.Acks
	switch *acksMode {
	case "all":
		acks = kgo.AllISRAcks()
	case "leader":
		acks = kgo.LeaderAck()
	case "none":
		acks = kgo.NoAck()
	default:
		log.Fatalf("loadgen: -acks must be all, leader or none (got %q)", *acksMode)
	}

	newProducer := func() (*kgo.Client, error) {
		opts := []kgo.Opt{
			kgo.SeedBrokers(seeds...),
			kgo.RequiredAcks(acks),
		}
		// franz-go's idempotent producer REQUIRES AllISRAcks; asking for anything
		// weaker without disabling it fails at client construction rather than at
		// produce time, which is a confusing place to discover it.
		if *acksMode != "all" {
			opts = append(opts, kgo.DisableIdempotentWrite())
		}
		opts = append(opts,
			// METHODOLOGY.md #5: franz-go's 10ms default linger was the single
			// biggest measurement error this project ever made, so the default here
			// is 0 and latency runs must keep it there.
			//
			// But 0 is not universally right. At RF=3 every produce awaits a quorum
			// acknowledgement, so linger=0 caps client throughput at about one
			// request per quorum round trip - measured as a hard ~2,739 orders/sec
			// whether 5k or 50k was offered, and write caching did not move it.
			// That is the CLIENT's ceiling, not the broker's, and it makes the
			// broker unmeasurable at RF=3. -linger-ms exists for throughput runs;
			// the value is always recorded in the fingerprint so a batched run is
			// never compared against an unbatched one.
			kgo.ProducerLinger(time.Duration(*lingerMs)*time.Millisecond),
			// Per client. Total client-side buffering is this times -producers,
			// which is part of why more clients raises the achievable rate.
			kgo.MaxBufferedRecords(*maxBuffered),
		)
		return kgo.NewClient(opts...)
	}
	producers := make([]*kgo.Client, *numProducers)
	for i := range producers {
		pc, err := newProducer()
		if err != nil {
			log.Fatalf("loadgen: creating producer %d: %v", i, err)
		}
		producers[i] = pc
		defer pc.Close()
	}
	flushAll := func() {
		for i, pc := range producers {
			if err := pc.Flush(context.Background()); err != nil {
				log.Printf("loadgen: flush producer %d: %v", i, err)
			}
		}
	}

	// Consume the receipt topic from its end BEFORE producing anything, so
	// no receipt written during the run can be missed.
	// The external arm consumes the matcher's OUTPUT topic directly - it is an
	// ordinary Kafka consumer, which is the point. The in-broker arm consumes
	// the probe's receipt topic.
	consumeTopic := *probeTopic
	if *receiptSrc == "fills" {
		consumeTopic = *fillsTopic
	} else if *receiptSrc != "probe" {
		log.Fatalf("loadgen: -receipt-source must be \"probe\" or \"fills\", got %q", *receiptSrc)
	}
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumeTopics(consumeTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)
	if err != nil {
		log.Fatalf("loadgen: creating consumer: %v", err)
	}
	defer consumer.Close()

	var filler []byte
	if *payloadBytes > 0 {
		filler = make([]byte, *payloadBytes)
		for i := range filler {
			filler[i] = 'x'
		}
	}
	pad := func(b []byte) []byte {
		if *payloadBytes > len(b) {
			return append(b, filler[:*payloadBytes-len(b)]...)
		}
		return b
	}

	// ----- receipt collector -----
	collectCtx, stopCollect := context.WithCancel(context.Background())
	defer stopCollect()
	collectorDone := make(chan struct{})
	go func() {
		defer close(collectorDone)
		for {
			fetches := consumer.PollFetches(collectCtx)
			fetches.EachError(func(t string, p int32, err error) {
				if collectCtx.Err() == nil {
					log.Printf("loadgen: fetch error on %s/%d: %v", t, p, err)
				}
			})
			observedAt := time.Now()
			fetches.EachRecord(func(r *kgo.Record) {
				if *receiptSrc == "fills" {
					coll.onFill(r.Value, observedAt)
					return
				}
				coll.onReceipt(string(r.Value), observedAt)
			})
			if collectCtx.Err() != nil {
				return
			}
		}
	}()

	// ----- lag sampler -----
	var (
		lagMu      sync.Mutex
		lagSamples []float64
	)
	lagCtx, stopLag := context.WithCancel(context.Background())
	defer stopLag()
	if *adminURL != "" {
		go func() {
			t := time.NewTicker(*lagInterval)
			defer t.Stop()
			for {
				select {
				case <-lagCtx.Done():
					return
				case <-t.C:
					snap, err := scrape(*adminURL)
					if err != nil {
						continue
					}
					lagMu.Lock()
					lagSamples = append(lagSamples, snap.sum("redpanda_transform_lag"))
					lagMu.Unlock()
				}
			}
		}()
	}

	// ----- send loop -----
	errTally := newErrorTally()
	var (
		produceErrors atomic.Int64
		recordsSent   atomic.Int64
		lateness      []float64
		latenessMu    sync.Mutex
	)

	sendOne := func(pc *kgo.Client, phase byte, seq int64, sampled bool) {
		// Both legs share one key so franz-go's hash partitioner routes them to
		// the same partition, and therefore to the same processor and the same
		// order book. Without this the two legs scatter (the keyless partitioner
		// is sticky-random), a crossing pair straddles two independent books,
		// and no fill is ever produced - measured as ~50% achieved rate at every
		// partition count above 1, which reads exactly like a broker scaling
		// ceiling and is not one. A real exchange partitions by instrument for
		// the same reason.
		var pairKey []byte
		if *keyByPair {
			pairKey = []byte(strconv.FormatInt(seq, 10))
		}
		sell := pad(wire.EncodeNewOrder(wire.NewOrder{
			OrderID: restingID(runID, phase, seq), Participant: "seller", Side: 1, Price: 100, Qty: 1,
		}))
		pc.Produce(context.Background(), &kgo.Record{Topic: *inputTopic, Key: pairKey, Value: sell}, func(_ *kgo.Record, err error) {
			if err != nil {
				produceErrors.Add(1)
				errTally.add(err)
			}
		})

		sendNanos := time.Now().UnixNano()
		buy := pad(wire.EncodeNewOrder(wire.NewOrder{
			OrderID: aggressorID(runID, phase, seq, sendNanos), Participant: "buyer", Side: 0, Price: 200, Qty: 1,
		}))
		timedSample := phase == 't' && sampled
		if timedSample {
			coll.recordSend(seq, sendNanos)
		}
		pc.Produce(context.Background(), &kgo.Record{Topic: *inputTopic, Key: pairKey, Value: buy}, func(_ *kgo.Record, err error) {
			if err != nil {
				produceErrors.Add(1)
				errTally.add(err)
				return
			}
			if timedSample {
				coll.recordAck(seq, time.Now())
			}
		})
		recordsSent.Add(2)
	}

	// pace runs the send loop against an absolute schedule anchored at the
	// window's start, so scheduling error never accumulates into a silently
	// reduced offered rate.
	// clientBound is set when the pace loop gives up: the producer buffer is
	// saturated and the client cannot offer the requested rate, so the level
	// says nothing about the broker.
	var clientBound bool
	var clientBoundMu sync.Mutex

	// paceOne drives ONE producer client over its own disjoint slice of the
	// sequence space, at its share of the offered rate. Running N of these in
	// parallel is what lets the offered rate exceed a single client's in-flight
	// ceiling, and keeping each pair inside one goroutine/client preserves the
	// sell-then-buy ordering the matcher depends on.
	paceOne := func(idx int, phase byte, window time.Duration, start time.Time) int64 {
		pc := producers[idx]
		n := int64(*numProducers)
		myRate := *rate / float64(*numProducers)
		rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(idx)))
		deadline := start.Add(window)
		var count int64
		var offset time.Duration
		// Bail out once the client is clearly not keeping up, rather than
		// grinding the full window and then the whole drain to reach the same
		// conclusion. A saturated producer buffer makes Produce block, so the
		// achieved send rate collapses and stays collapsed - measured at a
		// hard 2,739 orders/sec against offers of 10k, 20k, 50k and 100k, i.e.
		// entirely independent of what was asked for. Detecting that in the
		// first few seconds turns a ~2.5 minute wasted level into a few
		// seconds.
		const bailAfter = 8 * time.Second
		const bailBelow = 0.5
		for {
			now := time.Now()
			if now.After(deadline) {
				return count
			}
			if phase == 't' && myRate > 0 {
				if elapsed := now.Sub(start); elapsed > bailAfter {
					if achieved := float64(count) / elapsed.Seconds(); achieved < myRate*bailBelow {
						clientBoundMu.Lock()
						first := !clientBound
						clientBound = true
						clientBoundMu.Unlock()
						if first {
							log.Printf("loadgen: ABORTING level early - offered %.0f orders/sec across %d producer(s); producer %d managed %.0f/sec of its %.0f/sec share over %s (buffer saturated). Nothing about the broker can be measured at this rate with this client configuration; not spending the rest of the window and drain to restate that.",
								*rate, *numProducers, idx, achieved, myRate, elapsed.Round(time.Second))
						}
						return count
					}
				}
			}
			if myRate > 0 {
				if *pacing == "poisson" {
					offset += time.Duration(rng.ExpFloat64() / myRate * float64(time.Second))
				} else {
					offset += time.Duration(float64(time.Second) / myRate)
				}
				target := start.Add(offset)
				waitUntil(target)
				if phase == 't' {
					late := float64(time.Since(target).Nanoseconds()) / 1000
					if late < 0 {
						late = 0
					}
					latenessMu.Lock()
					lateness = append(lateness, late)
					latenessMu.Unlock()
				}
			}
			// Disjoint sequence space per producer, so ids stay unique and both
			// legs of a pair share a client.
			seq := count*n + int64(idx)
			sendOne(pc, phase, seq, seq%int64(*sampleEvery) == 0)
			count++
		}
	}

	// pace fans out to every producer over one shared start instant, so the
	// aggregate offered rate is the requested rate regardless of client count.
	pace := func(phase byte, window time.Duration) int64 {
		if window <= 0 {
			return 0
		}
		start := time.Now()
		var wg sync.WaitGroup
		counts := make([]int64, *numProducers)
		for i := 0; i < *numProducers; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				counts[idx] = paceOne(idx, phase, window, start)
			}(i)
		}
		wg.Wait()
		var total int64
		for _, c := range counts {
			total += c
		}
		return total
	}

	log.Printf("loadgen: warmup %s at %.0f orders/sec across %d producer(s) (%s pacing, linger %dms)",
		*warmup, *rate, *numProducers, *pacing, *lingerMs)
	pace('w', *warmup)
	flushAll()
	// Let warmup receipts land before the timed window opens, so a warmup
	// backlog is not charged to the timed run.
	time.Sleep(2 * time.Second)
	if !*keyByPair {
		log.Printf("loadgen: WARNING - -key-by-pair=false. On a multi-partition input topic the two legs of a crossing pair will land in different order books and never match, so the fill rate will be far below the offered rate for reasons that have nothing to do with broker capacity.")
	}
	if coll.warmupReceipts.Load() == 0 {
		log.Printf("loadgen: WARNING - zero warmup receipts. The pipeline is not delivering; check the transform deploy and shard pinning before trusting anything below")
	}

	var before *snapshot
	if *adminURL != "" {
		if before, err = scrape(*adminURL); err != nil {
			log.Printf("loadgen: pre-run scrape failed: %v", err)
		}
	}

	log.Printf("loadgen: timed window %s at %.0f orders/sec", *duration, *rate)
	recordsSent.Store(0)
	timedStart := time.Now()
	sent := pace('t', *duration)
	flushAll()
	sendElapsed := time.Since(timedStart)
	stopLag()

	log.Printf("loadgen: sent %d orders in %s, draining receipts for %s",
		sent, sendElapsed.Round(time.Millisecond), *drain)
	time.Sleep(*drain)
	stopCollect()
	<-collectorDone

	var after *snapshot
	if *adminURL != "" {
		if after, err = scrape(*adminURL); err != nil {
			log.Printf("loadgen: post-run scrape failed: %v", err)
		}
	}

	// ----- assemble -----
	coll.mu.Lock()
	totalCopy := append([]float64(nil), coll.totalLat...)
	matchCopy := append([]float64(nil), coll.matchLat...)
	relayCopy := append([]float64(nil), coll.relayLat...)
	skewEstCopy := append([]float64(nil), coll.skewEst...)
	skewUncCopy := append([]float64(nil), coll.skewUnc...)
	coll.mu.Unlock()
	latenessMu.Lock()
	latenessCopy := append([]float64(nil), lateness...)
	latenessMu.Unlock()
	lagMu.Lock()
	lagCopy := append([]float64(nil), lagSamples...)
	lagMu.Unlock()

	elapsed := sendElapsed.Seconds()
	received := coll.timedReceipts.Load()
	expected := sent * int64(*numProbes)

	// Which stages mix clocks depends on WHERE the receipt timestamp comes from,
	// so this cannot be a static list. It was one, and that mislabelled every
	// -receipt-source=fills run: in that arm the end stamp is taken by THIS
	// process when it observes the fill, so total is client-to-client, i.e.
	// single-clock and directly trustworthy. Reporting it as cross-clock invited
	// exactly the wrong correction, and made an apples-to-apples comparison
	// against a genuinely cross-clock probe run look unsound (Round 3,
	// 2026-08-31).
	singleClockStages := []string{"send_lateness", "produce", "relay_consume"}
	crossClockStages := []string{"match", "total"}
	clockNote := "send_lateness and produce are client-clock only; relay_consume is broker-guest-clock only; match and total each mix the two and carry the full client-to-broker offset. The skew figures here are a gross-error check with millisecond-scale uncertainty by construction, NOT a correction - cross-clock stages need PTP-grade sync to mean anything at microsecond scale."
	if *receiptSrc == "fills" {
		// total's end stamp is read by this process, on this clock.
		singleClockStages = []string{"send_lateness", "produce", "total"}
		crossClockStages = []string{}
		clockNote = "receipt-source=fills: every stage here is client-clock only, including total - its end stamp is read by this process when it observes the fill, so no broker clock enters the measurement and no skew correction applies. match and relay_consume are deliberately not recorded in this arm precisely because they would require a broker clock."
	}

	rep := report{
		Label: *label,
		RunID: runID,
		Config: configReport{
			OfferedRate: *rate, Duration: duration.String(), Warmup: warmup.String(),
			Pacing: *pacing, PayloadBytes: *payloadBytes, NumProbes: *numProbes,
			SampleEvery: *sampleEvery, MaxBuffered: *maxBuffered, KeyByPair: *keyByPair, LingerMs: *lingerMs, Acks: *acksMode, Producers: *numProducers,
			InputTopic: *inputTopic, ProbeTopic: *probeTopic, TransformName: *transformName,
		},
		Clock: clockReport{
			Note:              clockNote,
			SingleClockStages: singleClockStages,
			CrossClockStages:  crossClockStages,
			SkewSamples:       len(skewEstCopy),
		},
		Rates: ratesReport{
			OfferedPerSec:       *rate,
			OrdersSent:          sent,
			RecordsSent:         recordsSent.Load(),
			ProduceErrors:       produceErrors.Load(),
			ProduceErrorsByKind: errTally.snapshot(),
			ElapsedSeconds:      elapsed,
		},
		ExpectedReceipts: expected,
		ReceivedReceipts: received,
		WarmupReceipts:   coll.warmupReceipts.Load(),
		ClientBound:      clientBound,
		SendLateness:     newStageStats(latenessCopy),
		Produce:          newStageStats(coll.produceStage()),
		Match:            newStageStats(matchCopy),
		RelayConsume:     newStageStats(relayCopy),
		Total:            newStageStats(totalCopy),
	}
	if expected > received {
		rep.MissingReceipts = expected - received
	}
	if len(skewEstCopy) > 0 {
		sort.Float64s(skewEstCopy)
		sort.Float64s(skewUncCopy)
		rep.Clock.SkewEstimateMicros = percentile(skewEstCopy, 0.50)
		rep.Clock.SkewUncertaintyMicros = percentile(skewUncCopy, 0.50)
	}
	usable, verdict := judgeCrossClock(
		rep.Clock.SkewEstimateMicros, rep.Clock.SkewUncertaintyMicros, *skewStepMicros)
	rep.Clock.CrossClockUnusable, rep.Clock.CrossClockVerdict = !usable, verdict
	// A negative match is reported, never used to suppress `total` - see
	// judgeNegativeMatch on why the two causes need opposite responses.
	if mag, note := judgeNegativeMatch(rep.Match, *clockRmsMicros); note != "" {
		rep.Clock.NegativeMatchMagnitudeMicros = mag
		rep.Clock.NegativeMatchNote = note
	}
	if elapsed > 0 {
		rep.Rates.AttemptedPerSec = float64(sent) / elapsed
		rep.Rates.AchievedReceiptsPerSec = float64(received) / elapsed
		rep.Rates.AchievedOrdersPerSec = float64(received) / float64(*numProbes) / elapsed
		if *rate > 0 {
			rep.Rates.AttemptedRatio = rep.Rates.AttemptedPerSec / *rate
			rep.Rates.AchievedRatio = rep.Rates.AchievedOrdersPerSec / *rate
		}
	}
	coll.mu.Lock()
	for _, n := range coll.sampledSeen {
		if n > *numProbes {
			rep.DuplicateReceipts += int64(n - *numProbes)
		}
		if n > rep.MaxReceiptsForAnyOrder {
			rep.MaxReceiptsForAnyOrder = n
		}
	}
	coll.mu.Unlock()

	rep.Lag = lagReport{Samples: len(lagCopy)}
	if len(lagCopy) > 0 {
		rep.Lag.FirstLag = lagCopy[0]
		rep.Lag.FinalLag = lagCopy[len(lagCopy)-1]
		for _, l := range lagCopy {
			if l > rep.Lag.MaxLag {
				rep.Lag.MaxLag = l
			}
		}
		// A bare "final > first" is far too sensitive: at 20k orders/sec a lag
		// of 107 records is under 3ms of backlog, i.e. scheduling noise, and
		// flagging it made a comfortably-keeping-up run read NOT CLEAN. A
		// verdict that fires on every run carries no information.
		//
		// So require the backlog to be BOTH growing AND large enough to matter:
		// more than a quarter-second of offered work. Offered records/sec is
		// twice the order rate (a crossing pair is two records). The floor of
		// 1000 keeps very low rates from tripping on a handful of records.
		threshold := 1000.0
		if *rate > 0 {
			if q := *rate * 2 * 0.25; q > threshold {
				threshold = q
			}
		}
		rep.Lag.BacklogThreshold = threshold
		rep.Lag.GrewDuringRun = rep.Lag.FinalLag > rep.Lag.FirstLag &&
			rep.Lag.FinalLag > threshold
		rep.Lag.PeakedAboveThreshold = rep.Lag.MaxLag > threshold
	}
	rep.Resources = buildResources(before, after, sent)
	rep.Clean, rep.UncleanReasons = judge(&rep)

	enc, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		log.Fatalf("loadgen: encoding report: %v", err)
	}
	if *out == "" {
		fmt.Println(string(enc))
	} else if err := os.WriteFile(*out, enc, 0o644); err != nil {
		log.Fatalf("loadgen: writing report to %s: %v", *out, err)
	} else {
		fmt.Printf("loadgen: wrote report to %s\n", *out)
	}

	if !rep.Clean {
		log.Printf("loadgen: RUN NOT CLEAN - %s", strings.Join(rep.UncleanReasons, "; "))
	}
}

// buildResources attributes CPU and memory per wasm function.
//
// The denominator matters, and the first version of this tool got it wrong by
// dividing CPU by the receipt count. Receipts are not invocations: one order is
// a crossing pair, so the matcher is invoked twice per order (sell leg and buy
// leg) and writes output for both, while relay-probe only emits a receipt for
// the leg that actually produced a fill. Dividing by receipts therefore
// overstated per-record cost by about 2x - and would be wrong by a different
// factor for another guest, since E2.3's passthrough emits for both legs and so
// has a different receipt-to-invocation ratio again.
//
// Invocations are therefore read per function from
// redpanda_transform_execution_latency_sec_count rather than derived from
// anything the client counted. Both figures are reported: per invocation (the
// real per-record cost) and per order (what a claim in orders/sec needs).
func buildResources(before, after *snapshot, orders int64) resourcesReport {
	r := resourcesReport{PerFunction: map[string]funcResource{}}
	if before == nil || after == nil {
		return r
	}
	r.ScrapedBeforeAndAfter = true
	r.AdminEndpoints = len(after.metrics["redpanda_relay_active_subscriptions"])

	cpuBefore := before.byLabel("redpanda_wasm_engine_cpu_seconds_total", "function_name")
	cpuAfter := after.byLabel("redpanda_wasm_engine_cpu_seconds_total", "function_name")
	memAfter := after.byLabel("redpanda_wasm_engine_memory_usage", "function_name")
	maxMemAfter := after.byLabel("redpanda_wasm_engine_max_memory", "function_name")

	invBefore := before.byLabel("redpanda_transform_execution_latency_sec_count", "function_name")
	invAfter := after.byLabel("redpanda_transform_execution_latency_sec_count", "function_name")

	for name, cpuA := range cpuAfter {
		d := cpuA - cpuBefore[name]
		inv := invAfter[name] - invBefore[name]
		fr := funcResource{
			CPUSecondsDelta: d,
			Invocations:     inv,
			MemoryUsageEnd:  memAfter[name],
			MaxMemoryEnd:    maxMemAfter[name],
		}
		if inv > 0 {
			fr.CPUMicrosPerInvocation = d * 1e6 / inv
		}
		if orders > 0 {
			fr.CPUMicrosPerOrder = d * 1e6 / float64(orders)
		}
		r.PerFunction[name] = fr
	}

	r.TransformReadBytesDelta = deltaSum(before, after, "redpanda_transform_read_bytes")
	r.TransformWriteBytesDelta = deltaSum(before, after, "redpanda_transform_write_bytes")
	r.TransformFailuresDelta = deltaSum(before, after, "redpanda_transform_failures")
	r.TransformGivenUpDelta = deltaSum(before, after, "redpanda_transform_batches_given_up")

	r.RelayPushesDelta = deltaSum(before, after, "redpanda_relay_pushes_total")
	r.RelayDeliveredDelta = deltaSum(before, after, "redpanda_relay_delivered_total")
	r.RelayDroppedDelta = deltaSum(before, after, "redpanda_relay_dropped_total")
	r.RelayActiveSubsEnd = after.sum("redpanda_relay_active_subscriptions")
	if r.RelayPushesDelta > 0 {
		r.RelayDeliveredPerShardPass = r.RelayDeliveredDelta / r.RelayPushesDelta
	}
	if logical := float64(orders) * 2; logical > 0 {
		r.RelayDeliveredPerLogicalPush = r.RelayDeliveredDelta / logical
	}
	return r
}

// judge decides whether this run's numbers are usable, applying the same
// "get a clean run rather than report with a caveat" rule METHODOLOGY.md sets
// for the closed-loop harnesses, plus the open-loop-specific checks a
// closed-loop run has no way to make.
// judgeCrossClock decides whether this level's cross-clock stages are readable.
//
// match and total are differences between a CLIENT timestamp and a BROKER
// timestamp, so they carry the full client-to-broker offset. On chrony-synced
// hosts that offset's uncertainty is 400-650us here - already the same order as
// the 350-700us totals being measured - which is a permanent property of the
// setup, documented, and deliberately NOT flagged: flagging it would void every
// level ever run.
//
// A clock STEP is different in kind. When the offset reaches milliseconds, the
// cross-clock stages stop being noisy measurements of latency and become
// readings of the clock error itself, and they do it while every delivery check
// still passes - so clean stays true and the number looks quotable. That is the
// failure this guards.
func judgeCrossClock(estMicros, uncMicros, stepMicros float64) (bool, string) {
	abs := estMicros
	if abs < 0 {
		abs = -abs
	}
	switch {
	case stepMicros <= 0:
		return true, "step detection disabled (-skew-step-micros <= 0); cross-clock stages carry their usual +/-400-650us error bar"
	case abs > stepMicros:
		return false, fmt.Sprintf("UNUSABLE: client-to-broker clock offset %.0fus exceeds the %.0fus step threshold - match and total are reading the clock error, not the pipeline. Use the single-clock stages (send_lateness, produce, relay_consume) and the broker-side relay histograms instead; both remain valid for this level.", estMicros, stepMicros)
	case uncMicros > 2*stepMicros:
		return false, fmt.Sprintf("UNUSABLE: clock-offset uncertainty %.0fus exceeds 2x the %.0fus step threshold, so match and total have no usable error bar. Single-clock stages remain valid.", uncMicros, stepMicros)
	default:
		return true, fmt.Sprintf("usable, with the standing caveat: offset %.0fus +/-%.0fus is the error bar on match and total, which is the same order as the values themselves - never quote them to better than that.", estMicros, uncMicros)
	}
}

// judgeNegativeMatch interprets a negative `match` stage.
//
// A negative cross-clock reading has TWO possible causes, and they call for
// opposite responses:
//
//  1. Clock offset. The two clocks disagree by more than the quantity being
//     measured, so the stage is reading clock error. `total` is then junk.
//  2. A genuine event ordering. The two timestamps are stamped on events that
//     genuinely occur in that order, and the negative sign is information.
//     `total` is then FINE.
//
// For this pipeline it is (2), and that must not be mistaken for (1).
// `matched_at_nanos` is stamped inside the matcher transform on the broker
// (rust/src/main.rs) immediately after the record is appended to the leader's
// log. `ackT` is when the CLIENT received its acks=all quorum acknowledgement,
// which is a further ~510us of replication plus ~133us of network away. The
// matcher therefore normally finishes BEFORE the producer is told its write is
// durable, and `match = matchedT - ackT` is legitimately negative.
//
// That is the in-broker pre-commit read - a known, intended property of this
// architecture and part of where the latency win comes from. It is a durability
// trade, not a measurement error.
//
// So `match` is a RACE between two paths, not a duration, and its sign carries
// meaning. rmsMicros is the independently measured host clock offset (chrony
// RMS, captured in clock-sync.json); pass 0 when unknown. When the negative
// magnitude dwarfs rmsMicros, the clocks cannot explain it and cause (2) is
// established.
//
// This deliberately does NOT mark the cross-clock stages unusable. An earlier
// version did, on the theory that a negative sample proves skew - which
// suppressed a perfectly valid `total` on 2026-09-02 while chrony showed every
// host synced to 2-9us against the same AWS reference. A safety check whose
// default state is a false positive is worse than no check; this file already
// learned that lesson once, in CrossClockUnusable's own comment.
func judgeNegativeMatch(match stageStats, rmsMicros float64) (float64, string) {
	if match.Samples == 0 || (match.P50Micros >= 0 && match.MinMicros >= 0) {
		return 0, ""
	}
	mag := -match.MinMicros
	if mag < 0 {
		mag = 0
	}
	// 10x the measured clock offset: comfortably outside anything sync error
	// could account for, without needing a tight bound on rmsMicros.
	if rmsMicros > 0 && mag > 10*rmsMicros {
		return mag, fmt.Sprintf(
			"match is negative (p50 %.0fus, min %.0fus) by up to %.0fus, which is "+
				"%.0fx the measured host clock offset of %.1fus - the clocks cannot "+
				"account for it. This is the in-broker PRE-COMMIT READ: the matcher "+
				"stamps matched_at on the broker right after local append, while ackT "+
				"is the client's acks=all quorum ack ~640us later, so the consumer "+
				"sees the record before the producer is told it is durable. `match` is "+
				"a race between two paths, not a duration - do not quote it as "+
				"latency. `total` is unaffected and remains valid.",
			match.P50Micros, match.MinMicros, mag, mag/rmsMicros, rmsMicros)
	}
	if rmsMicros <= 0 {
		return mag, fmt.Sprintf(
			"match is negative (p50 %.0fus, min %.0fus) and no host clock offset was "+
				"supplied, so clock error and a genuine event ordering cannot be "+
				"distinguished. Check clock-sync.json: if the hosts are synced to "+
				"single-digit us this is the in-broker pre-commit read (expected), and "+
				"`total` is valid; if the offset is comparable to %.0fus, treat match "+
				"and total as unusable.",
			match.P50Micros, match.MinMicros, mag)
	}
	return mag, fmt.Sprintf(
		"match is negative (p50 %.0fus, min %.0fus) by up to %.0fus, which is the "+
			"same order as the measured host clock offset %.1fus - clock error is a "+
			"sufficient explanation, so match and total should not be quoted.",
		match.P50Micros, match.MinMicros, mag, rmsMicros)
}

func judge(rep *report) (bool, []string) {
	var reasons []string

	// Backlog contaminates every timestamp-derived figure in the report, the
	// skew estimator included, so the cross-clock check below needs to know
	// about it before it attributes anything to the clock.
	saturated := rep.Lag.GrewDuringRun || rep.Lag.PeakedAboveThreshold

	if rep.ReceivedReceipts == 0 {
		reasons = append(reasons, "zero receipts: the pipeline delivered nothing")
	}
	if rep.MissingReceipts > 0 {
		reasons = append(reasons, fmt.Sprintf("%d/%d receipts missing (relay drops, or a shard-locality mismatch)",
			rep.MissingReceipts, rep.ExpectedReceipts))
	}
	if rep.ClientBound {
		reasons = append(reasons, "CLIENT-BOUND: the producer buffer saturated and the level was aborted early. Raise -max-buffered, add producer clients, or add partitions - but nothing here describes the broker")
	}
	if rep.Rates.ProduceErrors > 0 {
		// Name the actual errors. "N produce errors" alone is not diagnosable,
		// and this run's cause was mis-guessed twice before the strings were
		// captured.
		var kinds []string
		for msg, n := range rep.Rates.ProduceErrorsByKind {
			note := ""
			// A saturated buffer still holds records when the harness tears the
			// topics down, and franz-go then fails them all this way. It reads
			// like a broker fault and is not one - it is downstream of the
			// buffer filling.
			if strings.Contains(msg, "UNKNOWN_TOPIC_OR_PARTITION") {
				note = " [artifact of topic teardown while records were still buffered, not a broker fault]"
			}
			kinds = append(kinds, fmt.Sprintf("%dx %q%s", n, msg, note))
		}
		sort.Strings(kinds)
		reasons = append(reasons, fmt.Sprintf("%d produce errors [%s]",
			rep.Rates.ProduceErrors, strings.Join(kinds, "; ")))
	}
	if rep.Resources.TransformFailuresDelta > 0 {
		reasons = append(reasons, fmt.Sprintf("transform failures moved by %.0f during the window",
			rep.Resources.TransformFailuresDelta))
	}
	if rep.Clock.CrossClockUnusable {
		// Not-clean rather than a mere note, deliberately. The headline this
		// project reports IS the cross-clock e2e total, and summary.txt prints
		// total_p50us beside clean - so a level whose total is unreadable must
		// not present as a clean result. The reason string names the stages
		// that survive, so the run is not thrown away wholesale.
		//
		// ORDER MATTERS: a saturated run also shows a huge apparent offset,
		// because the skew estimator is fed the same contaminated timestamps as
		// everything else - measured at 6.17s and 4.11s of "offset" on two
		// saturated fanout-12 trials (2026-09-01) whose real problem was a
		// 375,596-record backlog. Calling that a clock step would be a
		// confident misdiagnosis, so when saturation is already known the
		// verdict is reworded to name backlog as the primary cause.
		if saturated {
			reasons = append(reasons,
				fmt.Sprintf("cross-clock stages also unreadable, but SATURATION IS THE PRIMARY CAUSE - "+
					"the %.0fus apparent clock offset is backlog age contaminating the skew estimator, "+
					"not a clock step. Fix the saturation first; the offset figure means nothing until then.",
					rep.Clock.SkewEstimateMicros))
		} else {
			reasons = append(reasons, "cross-clock stages unusable: "+rep.Clock.CrossClockVerdict)
		}
	}
	if rep.Resources.RelayDroppedDelta > 0 {
		reasons = append(reasons, fmt.Sprintf("relay dropped %.0f records (a consumer was backlogged)",
			rep.Resources.RelayDroppedDelta))
	}
	// Window-coverage check. The denominator (records) is correct and the
	// invocation counter is per-record, but if the after-snapshot is taken
	// before the pipeline finishes, the invocation delta is TRUNCATED and every
	// per-record figure derived from it is silently wrong - measured at 59,948
	// invocations against 200,000 records sent, on a 10s drain at 5,000
	// orders/sec, reported as clean. With a 40s drain the same configuration
	// gave exactly 200,000. Nothing detected the truncation.
	//
	// A processor sees every input record, so its invocation count should match
	// records_sent closely. Materially fewer means the window closed early.
	if rep.Config.TransformName != "" && rep.Rates.RecordsSent > 0 {
		if fr, ok := rep.Resources.PerFunction[rep.Config.TransformName]; ok && fr.Invocations > 0 {
			coverage := fr.Invocations / float64(rep.Rates.RecordsSent)
			if coverage < 0.95 {
				reasons = append(reasons, fmt.Sprintf(
					"measurement window closed early: %s saw %.0f invocations for %d records sent (%.0f%% coverage). The per-record CPU denominator is truncated and must not be quoted - raise -drain so the pipeline finishes inside the window",
					rep.Config.TransformName, fr.Invocations, rep.Rates.RecordsSent, coverage*100))
			}
			rep.Resources.WindowCoverage = coverage
		}
	}
	if rep.DuplicateReceipts > 0 {
		reasons = append(reasons, fmt.Sprintf("%d duplicate receipts on the sampled subset (max %d for one order, expected %d): the record was processed more than once, which is what a processor restart does - it resumes from the last committed offset and replays up to data_transforms_commit_interval_ms of work. Counts and latencies both include the replay",
			rep.DuplicateReceipts, rep.MaxReceiptsForAnyOrder, rep.Config.NumProbes))
	}
	if rep.Lag.GrewDuringRun {
		reasons = append(reasons, fmt.Sprintf("saturated: transform lag grew %.0f -> %.0f (max %.0f), finishing above the %.0f-record backlog threshold (~0.25s of offered work) - the reported latency is a backlog age, not a latency",
			rep.Lag.FirstLag, rep.Lag.FinalLag, rep.Lag.MaxLag, rep.Lag.BacklogThreshold))
	} else if rep.Lag.PeakedAboveThreshold {
		// Deliberately a separate reason, not folded into the one above: this
		// run DID sustain its throughput and lose nothing, so the achieved-rate
		// figure remains usable. Only the latency percentiles are spoiled,
		// because they include time spent queued.
		reasons = append(reasons, fmt.Sprintf("latency not usable: transform lag peaked at %.0f records - above the %.0f-record backlog threshold (~0.25s of offered work) - before draining to %.0f. Throughput and receipt counts from this run are still valid; the latency percentiles are queue depths, not latencies, so this level marks the usable-latency ceiling rather than the throughput ceiling",
			rep.Lag.MaxLag, rep.Lag.BacklogThreshold, rep.Lag.FinalLag))
	}
	// The two open-loop-specific checks. Attempted below offered means the
	// CLIENT could not keep up; achieved below attempted means the BROKER
	// could not. Different findings, and they must not be conflated.
	if rep.Config.OfferedRate > 0 {
		if rep.Rates.AttemptedRatio < 0.95 {
			reasons = append(reasons, fmt.Sprintf("client could not offer the requested rate (attempted %.0f/sec vs offered %.0f/sec, %.0f%%): raise -max-buffered or add client capacity - this run says nothing about the broker",
				rep.Rates.AttemptedPerSec, rep.Config.OfferedRate, rep.Rates.AttemptedRatio*100))
		}
		if rep.Rates.AchievedRatio < 0.95 && rep.Rates.AttemptedRatio >= 0.95 {
			reasons = append(reasons, fmt.Sprintf("past saturation: achieved %.0f orders/sec against %.0f offered (%.0f%%) - a real ceiling, and the latency above is a queue depth",
				rep.Rates.AchievedOrdersPerSec, rep.Config.OfferedRate, rep.Rates.AchievedRatio*100))
		}
	}
	// Pacing fidelity, which the rate checks above cannot see. AttemptedRatio
	// counts HOW MANY orders were sent, not WHEN: a CPU-starved client can
	// still emit the right total while drifting whole pacing slots late, and
	// the same starvation delays its receipt handling - which inflates the
	// latency percentiles without tripping any other check.
	//
	// Found on 2026-09-03, traditional-deployment leg at 500 external
	// consumers: attempted_ratio 99.99%, client_bound false, lag_grew false,
	// every existing gate green - while send_lateness p90 was 534us and p99
	// 2,486us against a 1,000us pacing interval, i.e. 2.5 slots late at the
	// tail. The in-broker reference run on the same cluster had p90 2.1us.
	// total_p50 read 7,629us and there was no way to tell how much of that was
	// the architecture and how much was the client running out of CPU.
	//
	// Fixed pacing only: under -pacing burst or open-loop there is no
	// per-order schedule to be late against.
	if rep.Config.Pacing == "fixed" && rep.Config.OfferedRate > 0 && rep.SendLateness.Samples > 0 {
		intervalMicros := 1e6 / rep.Config.OfferedRate
		if rep.SendLateness.P99Micros > intervalMicros {
			reasons = append(reasons, fmt.Sprintf(
				"pacing not held: send_lateness p99 %.0fus exceeds the %.0fus pacing interval "+
					"(%.1f slots late at the tail; p90 %.0fus). The generator could not keep "+
					"its own schedule, which means the client was short of CPU - and the same "+
					"starvation delays receipt handling, so these latency percentiles include "+
					"client-side queueing and cannot be attributed to the system under test. "+
					"Reduce client-side load (fewer co-located consumers), move load to another "+
					"instance, or lower the rate - then re-run",
				rep.SendLateness.P99Micros, intervalMicros,
				rep.SendLateness.P99Micros/intervalMicros, rep.SendLateness.P90Micros))
		}
	}
	if !rep.Resources.ScrapedBeforeAndAfter {
		reasons = append(reasons, "no -admin-url: latency without resource attribution, which is the gap this tool exists to close")
	}
	return len(reasons) == 0, reasons
}
