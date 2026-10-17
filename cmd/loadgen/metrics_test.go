package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// The bucket counts must come out CUMULATIVE, and the +Inf bucket must equal
// _count. Get either wrong and Prometheus still accepts the body while
// histogram_quantile returns silently wrong numbers - there is no error to
// notice, which is why this is worth a test.
func TestSendLatenessHistogramIsCumulative(t *testing.T) {
	m := newLiveMetrics()
	m.enabled = true
	var mu sync.Mutex

	// Microseconds. 1500us and 3000us straddle the 1ms pacing interval that is
	// the whole reason this histogram exists, and 5_000_000us (5s) lands above
	// the last bound so it must appear ONLY in +Inf.
	for _, micros := range []float64{5, 40, 120, 900, 1500, 3000, 5_000_000} {
		m.recordLateness(micros)
	}

	body := m.render(&mu)
	buckets := map[string]uint64{}
	var count uint64
	for _, line := range strings.Split(body, "\n") {
		switch {
		case strings.HasPrefix(line, "loadgen_send_lateness_seconds_bucket{le=\""):
			le := line[len("loadgen_send_lateness_seconds_bucket{le=\""):]
			le = le[:strings.Index(le, "\"")]
			v, err := strconv.ParseUint(strings.Fields(line)[1], 10, 64)
			if err != nil {
				t.Fatalf("unparseable bucket line %q: %v", line, err)
			}
			buckets[le] = v
		case strings.HasPrefix(line, "loadgen_send_lateness_seconds_count "):
			v, err := strconv.ParseUint(strings.Fields(line)[1], 10, 64)
			if err != nil {
				t.Fatalf("unparseable count line %q: %v", line, err)
			}
			count = v
		}
	}

	if count != 7 {
		t.Errorf("_count = %d, want 7", count)
	}
	if got := buckets["+Inf"]; got != count {
		t.Errorf("+Inf bucket = %d, _count = %d - these must be equal", got, count)
	}

	// Monotonic non-decreasing across the declared bounds.
	var prev uint64
	for _, b := range latenessBounds {
		le := strconv.FormatFloat(b, 'g', -1, 64)
		got, ok := buckets[le]
		if !ok {
			t.Fatalf("no bucket emitted for le=%s", le)
		}
		if got < prev {
			t.Errorf("bucket le=%s went backwards: %d after %d", le, got, prev)
		}
		prev = got
	}

	// Spot-check the boundary that matters: 5, 40, 120 and 900us are all <= 1ms,
	// so le="0.001" must be exactly 4. 1500 and 3000us are above it.
	if got := buckets["0.001"]; got != 4 {
		t.Errorf("le=0.001 (1ms) = %d, want 4 (the 5, 40, 120 and 900us samples)", got)
	}
	// The 5s sample is beyond the last bound (1.0), so the last declared bucket
	// must hold 6 of the 7 samples and +Inf must hold all 7.
	if got := buckets["1"]; got != 6 {
		t.Errorf("le=1 = %d, want 6 (everything except the 5s sample)", got)
	}
}

// When disabled, recording must be a no-op. This is the property that lets the
// endpoint stay off by default for published runs without anyone having to
// reason about whether it perturbed the measurement.
func TestDisabledRecordsNothing(t *testing.T) {
	m := newLiveMetrics()
	var mu sync.Mutex
	for i := 0; i < 1000; i++ {
		m.recordLateness(float64(i))
	}
	if m.count != 0 || m.sum != 0 {
		t.Errorf("disabled metrics recorded %d samples (sum %v), want 0", m.count, m.sum)
	}
	if strings.Contains(m.render(&mu), "loadgen_phase_info") {
		t.Error("phase info emitted while disabled")
	}
}

// The dashboards shade levels and gate "measured windows only" on this exact
// line, so its labels are part of the contract.
func TestPhaseInfoCarriesPhaseArmAndLabel(t *testing.T) {
	m := newLiveMetrics()
	m.enabled = true
	m.arm, m.label = "probe", "bookdepth-10000"
	var mu sync.Mutex
	if strings.Contains(m.render(&mu), "loadgen_phase_info") {
		t.Error("phase info emitted before any phase was set")
	}
	for _, p := range []string{"preload", "warmup", "measure", "drain"} {
		m.setPhase(p)
		want := `loadgen_phase_info{phase="` + p + `",arm="probe",label="bookdepth-10000"} 1`
		if out := m.render(&mu); !strings.Contains(out, want+"\n") {
			t.Errorf("phase %s: missing %q in\n%s", p, want, out)
		}
	}
}

func TestListenAddrAcceptsBarePort(t *testing.T) {
	for in, want := range map[string]string{
		"9110":           ":9110",
		":9110":          ":9110",
		"127.0.0.1:9110": "127.0.0.1:9110",
		"":               "",
	} {
		if got := listenAddr(in); got != want {
			t.Errorf("listenAddr(%q) = %q, want %q", in, got, want)
		}
	}
}

// The failures gate must be scoped to the transform under measurement.
//
// Reproduces the real 2026-09-16 trial that was discarded: the matcher logged
// zero failures while the relay consumers logged thousands, every expected
// receipt arrived, and the unscoped sum still marked the run unclean - which
// render-tables.py turns into UNCLEAN and drops from the table.
func TestFailuresGateIsScopedToTheMeasuredTransform(t *testing.T) {
	body := func(matcher, probeEach int) string {
		var b strings.Builder
		b.WriteString("# TYPE redpanda_transform_failures counter\n")
		fmt.Fprintf(&b, "redpanda_transform_failures{function_name=\"wasm-matcher-e2-1\"} %d\n", matcher)
		for i := 0; i < 167; i++ {
			fmt.Fprintf(&b,
				"redpanda_transform_failures{function_name=\"probe-e2-1-%d\"} %d\n", i, probeEach)
		}
		return b.String()
	}
	before := parseMetricsBody(strings.NewReader(body(0, 0)))
	after := parseMetricsBody(strings.NewReader(body(0, 18)))

	const name = "wasm-matcher-e2-1"
	if got := deltaForLabel(before, after, "redpanda_transform_failures", "function_name", name); got != 0 {
		t.Errorf("matcher failure delta = %v, want 0 - the measured transform logged none", got)
	}
	want := float64(167 * 18)
	if got := deltaExcluding(before, after, "redpanda_transform_failures", "function_name", name); got != want {
		t.Errorf("other-transform failure delta = %v, want %v", got, want)
	}
	// The unscoped sum is what used to gate, and it is exactly the number that
	// discarded a flawless trial.
	if got := deltaSum(before, after, "redpanda_transform_failures"); got != want {
		t.Errorf("unscoped delta = %v, want %v", got, want)
	}

	// And a matcher that really does fail must still be caught.
	after2 := parseMetricsBody(strings.NewReader(body(7, 18)))
	if got := deltaForLabel(before, after2, "redpanda_transform_failures", "function_name", name); got != 7 {
		t.Errorf("matcher failure delta = %v, want 7 - a genuinely failing matcher must gate", got)
	}
}

// byLabel must not confuse a label whose name ends in the one being asked for.
func TestDeltaForLabelDoesNotMatchLabelSuffix(t *testing.T) {
	body := `redpanda_transform_failures{output_function_name="x",function_name="real"} 5` + "\n"
	snap := parseMetricsBody(strings.NewReader(body))
	if got := snap.byLabel("redpanda_transform_failures", "function_name")["real"]; got != 5 {
		t.Errorf("function_name=real = %v, want 5", got)
	}
	if got := snap.byLabel("redpanda_transform_failures", "function_name")["x"]; got != 0 {
		t.Errorf("output_function_name leaked into function_name: got %v, want 0", got)
	}
}

// The end-to-end histogram the dashboards draw for every deployment: labelled
// with arm and label on every line, cumulative, +Inf equal to _count, and
// absent until the first sample (an empty histogram would read as 0 latency).
func TestE2EHistogramIsLabelledAndCumulative(t *testing.T) {
	m := newLiveMetrics()
	m.enabled = true
	m.arm, m.label = "external", "trad-external-20"
	var mu sync.Mutex
	if strings.Contains(m.render(&mu), "loadgen_e2e_latency_seconds") {
		t.Fatal("no e2e series before the first sample")
	}
	for _, us := range []float64{450, 480, 1600, 2400, 45_000_000} { // the last is above every bound
		m.recordE2E(us)
	}
	body := m.render(&mu)
	for _, want := range []string{
		`loadgen_e2e_latency_seconds_bucket{arm="external",label="trad-external-20",le="0.0005"} 2`,
		`loadgen_e2e_latency_seconds_bucket{arm="external",label="trad-external-20",le="0.002"} 3`,
		`loadgen_e2e_latency_seconds_bucket{arm="external",label="trad-external-20",le="0.0025"} 4`,
		`loadgen_e2e_latency_seconds_bucket{arm="external",label="trad-external-20",le="30"} 4`,
		`loadgen_e2e_latency_seconds_bucket{arm="external",label="trad-external-20",le="+Inf"} 5`,
		`loadgen_e2e_latency_seconds_count{arm="external",label="trad-external-20"} 5`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q", want)
		}
	}
}

func TestMinInt64(t *testing.T) {
	if got := minInt64(nil); got != 0 {
		t.Errorf("minInt64(nil) = %d", got)
	}
	if got := minInt64([]int64{30, 10, 20}); got != 10 {
		t.Errorf("minInt64 = %d, want 10", got)
	}
}
