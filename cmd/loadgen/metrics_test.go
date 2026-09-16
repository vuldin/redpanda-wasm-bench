package main

import (
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
