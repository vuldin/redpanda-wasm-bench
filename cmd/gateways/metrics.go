package main

// The producer's stage marker for the dashboards. It is loadgen's own series,
// loadgen_phase_info{phase,arm,label} 1, served on loadgen's metrics port: the
// producer is this runner's load generator, Prometheus already scrapes that
// port on the client hosts, and the dashboards' "measured only" view keeps a
// latency point only inside a measure stage. Without it a gateways run shows
// no latency at all in that default view (2026-10-16).

import (
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// stageAt is the producer's stage after elapsed of sending: the first warmup
// is warmup (the consumer leaves it out of latency too), then measure until
// sending stops, then drain while the last batches are flushed.
func stageAt(elapsed, warmup time.Duration, sending bool) string {
	switch {
	case !sending:
		return "drain"
	case elapsed < warmup:
		return "warmup"
	default:
		return "measure"
	}
}

type stageMarker struct {
	mu         sync.Mutex
	stage      string
	arm, label string
}

func (m *stageMarker) set(stage string) {
	m.mu.Lock()
	m.stage = stage
	m.mu.Unlock()
}

func (m *stageMarker) render() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stage == "" {
		return ""
	}
	return "# HELP loadgen_phase_info Which phase the run is in (warmup, measure, drain), as a label on a constant 1.\n" +
		"# TYPE loadgen_phase_info gauge\n" +
		fmt.Sprintf("loadgen_phase_info{phase=%q,arm=%q,label=%q} 1\n", m.stage, m.arm, m.label)
}

// serve starts the endpoint in the background; a failure to bind is logged
// and ignored, as in loadgen: losing the observer must never fail a run.
func (m *stageMarker) serve(addr string) {
	if addr == "" {
		return
	}
	if _, err := strconv.Atoi(addr); err == nil {
		addr = ":" + addr
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = w.Write([]byte(m.render()))
	})
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Printf("gateways: metrics endpoint stopped: %v", err)
		}
	}()
}
