package main

import (
	"strings"
	"testing"
	"time"
)

// Kafka's own murmur2 test vectors (clients' UtilsTest), as signed ints. The
// broker uses the same function, so matching these is what lets a gateway
// find the partition its key is written to.
func TestMurmur2MatchesKafka(t *testing.T) {
	for in, want := range map[string]int32{
		"21":                         -973932308,
		"foobar":                     -790332482,
		"a-little-bit-long-string":   -985981536,
		"a-little-bit-longer-string": -1486304829,
		"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8": -58897971,
		"abc": 479470107,
	} {
		if got := int32(murmur2([]byte(in))); got != want {
			t.Errorf("murmur2(%q) = %d, want %d", in, got, want)
		}
	}
}

// The broker takes the UNSIGNED hash modulo the partition count; Kafka
// clients take toPositive(hash). They differ whenever the top bit is set.
func TestKeyedPartitionUsesUnsignedModulo(t *testing.T) {
	h := murmur2([]byte("21")) // top bit set (negative as int32)
	if int32(h) >= 0 {
		t.Fatal("test vector should have the top bit set")
	}
	const p = 7
	broker := int32(h % p)
	kafka := int32((h & 0x7fffffff) % p)
	if got := keyedPartition(21, p); got != broker {
		t.Errorf("keyedPartition(21, 7) = %d, want the broker's %d", got, broker)
	}
	if broker == kafka {
		t.Log("note: for this vector the two rules happen to agree")
	}
}

func TestEventRoundTrip(t *testing.T) {
	buf := make([]byte, 64)
	encodeEvent(buf, 19, 123456, 987654321)
	g, seq, sent, ok := decodeEvent(buf)
	if !ok || g != 19 || seq != 123456 || sent != 987654321 {
		t.Fatalf("round trip: %v %v %v %v", g, seq, sent, ok)
	}
	if _, _, _, ok := decodeEvent(buf[:header-1]); ok {
		t.Error("a short event decoded")
	}
	buf[0] = 0
	if _, _, _, ok := decodeEvent(buf); ok {
		t.Error("an event without the magic byte decoded")
	}
}

func TestAssignment(t *testing.T) {
	all := []int32{0, 1, 2, 3}
	if topic, ps := assignment("topics", "gw-", 3, 0, nil); topic != "gw-3" || ps != nil {
		t.Errorf("topics: %s %v", topic, ps)
	}
	if topic, ps := assignment("firehose", "events", 3, 4, all); topic != "events" || len(ps) != 4 {
		t.Errorf("firehose: %s %v", topic, ps)
	}
	if topic, ps := assignment("keyed", "gw-keyed", 3, 32, all); topic != "gw-keyed" || len(ps) != 1 || ps[0] != keyedPartition(3, 32) {
		t.Errorf("keyed: %s %v", topic, ps)
	}
}

// The producer's stage marker: the stages the dashboards' "measured only"
// view keys on, in loadgen's own series and label set.
func TestStageMarker(t *testing.T) {
	w := 5 * time.Second
	for _, c := range []struct {
		elapsed time.Duration
		sending bool
		want    string
	}{{0, true, "warmup"}, {4 * time.Second, true, "warmup"}, {5 * time.Second, true, "measure"},
		{59 * time.Second, true, "measure"}, {61 * time.Second, false, "drain"}} {
		if got := stageAt(c.elapsed, w, c.sending); got != c.want {
			t.Errorf("stageAt(%v, sending=%v) = %q, want %q", c.elapsed, c.sending, got, c.want)
		}
	}
	m := &stageMarker{arm: "topics", label: "topics-4"}
	if m.render() != "" {
		t.Error("no series before the first stage")
	}
	m.set("measure")
	if !strings.Contains(m.render(), `loadgen_phase_info{phase="measure",arm="topics",label="topics-4"} 1`) {
		t.Errorf("render: %q", m.render())
	}
}

// Lag is summed for the named function only, across output-topic series, and
// a longer name sharing the prefix is not counted.
func TestTransformLag(t *testing.T) {
	body := strings.Join([]string{
		`# TYPE redpanda_transform_lag gauge`,
		`redpanda_transform_lag{function_name="gwdemux-1-topics-8",output_topic="gw-1-0"} 120`,
		`redpanda_transform_lag{output_topic="gw-1-1",function_name="gwdemux-1-topics-8"} 30`,
		`redpanda_transform_lag{function_name="gwdemux-1-topics-8x",output_topic="gw-1-0"} 999999`,
		`redpanda_transform_failures{function_name="gwdemux-1-topics-8"} 7`,
	}, "\n")
	v, ok := transformLag(strings.NewReader(body), "gwdemux-1-topics-8")
	if !ok || v != 150 {
		t.Fatalf("transformLag = %v, %v; want 150, true", v, ok)
	}
	if _, ok := transformLag(strings.NewReader(body), "other"); ok {
		t.Error("a function with no series must not count as seen")
	}
}

// The verdict: loadgen's threshold (a quarter-second of input, at least
// 1,000), no transform means usable, no samples means not.
func TestLatencyUsable(t *testing.T) {
	if got := lagThreshold(30000); got != 7500 {
		t.Errorf("lagThreshold(30000) = %v, want 7500", got)
	}
	if got := lagThreshold(2000); got != 1000 {
		t.Errorf("lagThreshold(2000) = %v, want the 1000 floor", got)
	}
	for _, c := range []struct {
		fn   string
		st   lagStats
		want bool
	}{
		{"", lagStats{}, true},
		{"d", lagStats{Samples: 50, Peak: 232, Threshold: 7500}, true},
		{"d", lagStats{Samples: 50, Peak: 123794, Threshold: 7500}, false}, // 2026-10-16 topics-8, trial 2
		{"d", lagStats{Samples: 0, Threshold: 7500}, false},
	} {
		if got := latencyUsable(c.fn, c.st); got != c.want {
			t.Errorf("latencyUsable(%q, %+v) = %v, want %v", c.fn, c.st, got, c.want)
		}
	}
}
