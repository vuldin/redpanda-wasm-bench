package main

// Transform lag during a level, and whether the level's latency is usable.
//
// The demux runs on its input partition leader's shard. When that shard is
// saturated the transform falls behind and the gateways' latency becomes a
// queue age: on 2026-10-16 a client connection landed on the demux's already
// full shard and the level reported p50 78ms / p99 849ms as clean, with the
// transform 124k records behind. loadgen guards its own runs against exactly
// this ("latency not usable: transform lag peaked at N records"); this is the
// same rule for the gateways runner: sample the level's transform lag from
// every broker's /public_metrics, and call the latency unusable if it ever
// exceeded a quarter-second of offered input (at least 1,000 records).

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// lagThreshold is loadgen's backlog threshold: a quarter-second of offered
// input records, floored at 1,000 so very low rates do not trip on a handful.
func lagThreshold(recordsPerSec float64) float64 {
	return max(1000, recordsPerSec*0.25)
}

// transformLag sums redpanda_transform_lag for one function over a
// /public_metrics body. The function_name match is anchored at a label
// boundary so a longer name sharing the prefix is not counted.
func transformLag(r io.Reader, function string) (float64, bool) {
	want := `function_name="` + function + `"`
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 256*1024), 8*1024*1024)
	var total float64
	found := false
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "redpanda_transform_lag{") {
			continue
		}
		head, val, ok := strings.Cut(line, "} ")
		if !ok || !(strings.Contains(head, "{"+want) || strings.Contains(head, ","+want)) {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(val), 64)
		if err != nil {
			continue
		}
		total += v
		found = true
	}
	return total, found
}

type lagStats struct {
	Samples   int     `json:"samples"`
	Peak      float64 `json:"peak"`
	Final     float64 `json:"final"`
	Threshold float64 `json:"threshold"`
}

// sampleLag polls every admin URL each interval until ctx ends, and returns
// the peak and last lag seen. A poll where no broker reports the function
// (not deployed yet, or between deploys) is not a sample.
func sampleLag(ctx context.Context, adminURLs []string, function string, every time.Duration) func() lagStats {
	done := make(chan lagStats, 1)
	go func() {
		client := &http.Client{Timeout: 5 * time.Second}
		var st lagStats
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				done <- st
				return
			case <-t.C:
			}
			var total float64
			seen := false
			for _, u := range adminURLs {
				resp, err := client.Get(strings.TrimRight(u, "/") + "/public_metrics")
				if err != nil {
					continue
				}
				v, ok := transformLag(resp.Body, function)
				resp.Body.Close()
				if ok {
					total += v
					seen = true
				}
			}
			if seen {
				st.Samples++
				st.Final = total
				st.Peak = max(st.Peak, total)
			}
		}
	}()
	return func() lagStats { return <-done }
}

// latencyUsable is the verdict: with no transform there is no transform lag
// to spoil anything; with one, every sample must have stayed at or under the
// threshold, and there must have been samples to say so.
func latencyUsable(function string, st lagStats) bool {
	if function == "" {
		return true
	}
	return st.Samples > 0 && st.Peak <= st.Threshold
}
