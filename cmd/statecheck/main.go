// statecheck answers one question about a stateful matcher: after a
// leadership move, a broker kill or a fresh replay, is the book the one it
// should be?
//
// It does that with sentinels: resting asks at a price no other traffic
// touches (50,000 - above every measured buy at 200 and passive bid at
// 2..99, below the depth preload starting at 100,000). Only an order this
// tool sends can ever fill one, so after the event a buy per sentinel reads
// the book back directly: a sentinel that fills survived, one that does not
// was lost.
//
//	place   send sentinels in timed waves before an instant T, run -exec at T
//	        (the action under test), and write what was placed and when
//	verify  with other traffic stopped: sweep any stray asks below 50,000,
//	        send one buy per sentinel, read the fills, clean up, and write a
//	        verdict
//
// Waves are what turn this into a measurement of the snapshot window rather
// than a pass/fail: the broker checkpoints guest state every 5s and commits
// input offsets every 3s, independently, so on an unplanned failure the
// sentinels placed in the last few seconds before T are the ones at risk,
// and which waves survive maps that window.
//
// Ids follow loadgen's layout so the guest's replay detection treats them
// like any other traffic: sentinels are "<run>x-s<i>-0", verify buys
// "<run>y-b<i>-<nanos>" (a different phase, so their lane order is their
// own), the stray sweep "<run>z-b0-<nanos>".
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/vuldin/redpanda-wasm-bench/wire"
)

const (
	sentinelPrice = 50000
	sweepPrice    = sentinelPrice - 1
)

type placed struct {
	ID     string  `json:"id"`
	Wave   float64 `json:"wave_seconds_before_t"`
	SentMs int64   `json:"sent_unix_ms"`
	AckMs  int64   `json:"ack_unix_ms"`
	Err    string  `json:"error,omitempty"`
}

type placeReport struct {
	RunID      int64     `json:"run_id"`
	Topic      string    `json:"topic"`
	PerWave    int       `json:"per_wave"`
	Waves      []float64 `json:"waves_seconds_before_t"`
	TUnixMs    int64     `json:"t_unix_ms"`
	ExecCmd    string    `json:"exec,omitempty"`
	ExecStart  int64     `json:"exec_start_unix_ms,omitempty"`
	ExecEnd    int64     `json:"exec_end_unix_ms,omitempty"`
	ExecRC     int       `json:"exec_rc"`
	ExecOutput string    `json:"exec_output,omitempty"`
	Placed     []placed  `json:"placed"`
}

type waveVerdict struct {
	Wave      float64  `json:"wave_seconds_before_t"`
	Placed    int      `json:"placed"`
	Recovered int      `json:"recovered"`
	Lost      int      `json:"lost"`
	LostIDs   []string `json:"lost_ids,omitempty"`
}

type verdict struct {
	RunID          int64         `json:"run_id"`
	Label          string        `json:"label,omitempty"`
	Placed         int           `json:"placed"`
	Recovered      int           `json:"recovered"`
	Lost           int           `json:"lost"`
	Duplicated     int           `json:"duplicated"`
	StraysSwept    int           `json:"strays_swept"`
	StrayIDs       []string      `json:"stray_ids_sample,omitempty"`
	UnexpectedFill int           `json:"unexpected_fills"`
	Waves          []waveVerdict `json:"waves"`
	FirstFillMs    float64       `json:"first_fill_ms"`
	LastFillMs     float64       `json:"last_fill_ms"`
	TimedOut       bool          `json:"timed_out"`
	OK             bool          `json:"ok"`
	Note           string        `json:"note"`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: statecheck place|verify [flags]")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "place":
		place(os.Args[2:])
	case "verify":
		verify(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n", os.Args[1])
		os.Exit(2)
	}
}

func newProducer(brokers string) *kgo.Client {
	c, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.ProducerLinger(0),
		kgo.MetadataMinAge(250*time.Millisecond),
		kgo.RetryBackoffFn(func(int) time.Duration { return 100 * time.Millisecond }),
		kgo.RecordDeliveryTimeout(60*time.Second),
	)
	if err != nil {
		log.Fatalf("statecheck: producer: %v", err)
	}
	return c
}

func parseWaves(s string) []float64 {
	var out []float64
	for _, f := range strings.Split(s, ",") {
		v, err := strconv.ParseFloat(strings.TrimSpace(f), 64)
		if err != nil {
			log.Fatalf("statecheck: bad wave %q", f)
		}
		out = append(out, v)
	}
	sort.Sort(sort.Reverse(sort.Float64Slice(out)))
	return out
}

func sentinelID(run int64, i int) string { return fmt.Sprintf("%dx-s%d-0", run, i) }

func place(args []string) {
	fs := flag.NewFlagSet("place", flag.ExitOnError)
	brokers := fs.String("brokers", "localhost:9092", "seed brokers")
	topic := fs.String("topic", "", "the matcher's INPUT topic")
	partition := fs.Int("partition", 0, "input partition (the book under test)")
	run := fs.Int64("run-id", time.Now().Unix(), "id prefix; must differ from loadgen's")
	perWave := fs.Int("per-wave", 20, "sentinels per wave")
	waves := fs.String("waves", "10,6,3,1,0.3", "seconds before T at which each wave is sent")
	lead := fs.Duration("lead", 12*time.Second, "T = now + lead; must exceed the largest wave")
	atMs := fs.Int64("at-unix-ms", 0, "absolute T (unix ms) instead of now+lead, so another host can act at the same instant")
	execCmd := fs.String("exec", "", "shell command run at T - the action under test")
	out := fs.String("out", "placed.json", "where to write the placement record")
	fs.Parse(args)
	if *topic == "" {
		log.Fatal("statecheck place: -topic is required")
	}
	ws := parseWaves(*waves)
	pc := newProducer(*brokers)
	defer pc.Close()
	t := time.Now().Add(*lead)
	if *atMs > 0 {
		t = time.UnixMilli(*atMs)
	}
	if time.Until(t).Seconds() <= ws[0] {
		log.Fatalf("statecheck place: T is %.1fs away, less than the largest wave %.1fs", time.Until(t).Seconds(), ws[0])
	}
	rep := placeReport{RunID: *run, Topic: *topic, PerWave: *perWave, Waves: ws, TUnixMs: t.UnixMilli(), ExecCmd: *execCmd}
	var mu sync.Mutex
	rep.Placed = make([]placed, 0, len(ws)**perWave)
	i := 0
	for _, w := range ws {
		time.Sleep(time.Until(t.Add(-time.Duration(w * float64(time.Second)))))
		for k := 0; k < *perWave; k++ {
			id := sentinelID(*run, i)
			i++
			idx := len(rep.Placed)
			rep.Placed = append(rep.Placed, placed{ID: id, Wave: w, SentMs: time.Now().UnixMilli()})
			pc.Produce(context.Background(), &kgo.Record{
				Topic: *topic, Partition: int32(*partition),
				Value: wire.EncodeNewOrder(wire.NewOrder{OrderID: id, Participant: "sentinel", Side: 1, Price: sentinelPrice, Qty: 1}),
			}, func(_ *kgo.Record, err error) {
				mu.Lock()
				defer mu.Unlock()
				rep.Placed[idx].AckMs = time.Now().UnixMilli()
				if err != nil {
					rep.Placed[idx].Err = err.Error()
				}
			})
		}
	}
	time.Sleep(time.Until(t))
	if *execCmd != "" {
		rep.ExecStart = time.Now().UnixMilli()
		cmd := exec.Command("bash", "-c", *execCmd)
		b, err := cmd.CombinedOutput()
		rep.ExecEnd = time.Now().UnixMilli()
		rep.ExecOutput = tail(string(b), 4000)
		if err != nil {
			rep.ExecRC = 1
			if ee, ok := err.(*exec.ExitError); ok {
				rep.ExecRC = ee.ExitCode()
			}
		}
	}
	// Wait for every ack: a sentinel the broker never acknowledged was
	// never placed, and must not be counted as lost.
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	_ = pc.Flush(ctx)
	mu.Lock()
	defer mu.Unlock()
	writeJSON(*out, rep)
	errs := 0
	for _, p := range rep.Placed {
		if p.Err != "" || p.AckMs == 0 {
			errs++
		}
	}
	log.Printf("statecheck place: %d sentinels in %d waves, T=%d, exec rc=%d, %d unacked", len(rep.Placed), len(ws), rep.TUnixMs, rep.ExecRC, errs)
}

func verify(args []string) {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	brokers := fs.String("brokers", "localhost:9092", "seed brokers")
	in := fs.String("placed", "placed.json", "placement record from `statecheck place`")
	fillsTopic := fs.String("fills", "", "the matcher's OUTPUT topic to read fills from")
	partition := fs.Int("partition", 0, "input partition")
	timeout := fs.Duration("timeout", 30*time.Second, "how long to wait for fills")
	label := fs.String("label", "", "recorded in the verdict")
	out := fs.String("out", "verdict.json", "where to write the verdict")
	fs.Parse(args)
	if *fillsTopic == "" {
		log.Fatal("statecheck verify: -fills is required")
	}
	var pr placeReport
	b, err := os.ReadFile(*in)
	if err != nil {
		log.Fatalf("statecheck verify: %v", err)
	}
	if err := json.Unmarshal(b, &pr); err != nil {
		log.Fatalf("statecheck verify: %v", err)
	}
	cons, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		kgo.ConsumeTopics(*fillsTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)
	if err != nil {
		log.Fatalf("statecheck verify: consumer: %v", err)
	}
	defer cons.Close()
	// Make sure the consumer has resolved its end offset BEFORE anything is
	// produced, or early fills are missed.
	pctx, pcancel := context.WithTimeout(context.Background(), 2*time.Second)
	cons.PollFetches(pctx)
	pcancel()

	pc := newProducer(*brokers)
	defer pc.Close()
	send := func(v []byte) {
		if err := pc.ProduceSync(context.Background(), &kgo.Record{Topic: pr.Topic, Partition: int32(*partition), Value: v}).FirstErr(); err != nil {
			log.Fatalf("statecheck verify: produce: %v", err)
		}
	}
	run := pr.RunID
	start := time.Now()
	sweepID := fmt.Sprintf("%dz-b0-%d", run, start.UnixNano())
	send(wire.EncodeNewOrder(wire.NewOrder{OrderID: sweepID, Participant: "statecheck", Side: 0, Price: sweepPrice, Qty: 1 << 40}))
	// The sweep must not stay resting: a bid at 49,999 would cross every
	// measured sell that follows.
	send(wire.EncodeCancel(wire.Cancel{OrderID: sweepID}))
	var buyIDs []string
	for i := range pr.Placed {
		if pr.Placed[i].Err != "" || pr.Placed[i].AckMs == 0 {
			continue
		}
		id := fmt.Sprintf("%dy-b%d-%d", run, i, time.Now().UnixNano())
		buyIDs = append(buyIDs, id)
		send(wire.EncodeNewOrder(wire.NewOrder{OrderID: id, Participant: "statecheck", Side: 0, Price: sentinelPrice, Qty: 1}))
	}

	ours := map[string]bool{sweepID: true}
	for _, id := range buyIDs {
		ours[id] = true
	}
	var fills []wire.Fill
	var fillAt []time.Duration
	deadline := start.Add(*timeout)
	matched := 0
	for time.Now().Before(deadline) && matched < len(buyIDs) {
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		fs := cons.PollFetches(ctx)
		cancel()
		fs.EachRecord(func(r *kgo.Record) {
			fl, err := wire.DecodeFills(r.Value)
			if err != nil {
				return
			}
			for _, f := range fl {
				if !ours[f.AggressorID] {
					continue
				}
				fills = append(fills, f)
				fillAt = append(fillAt, time.Since(start))
				if f.AggressorID != sweepID {
					matched++
				}
			}
		})
	}
	// Clean up whatever is left so the next trial starts from a known book:
	// unfilled verify buys would rest at 50,000 and cross later traffic, and
	// surviving sentinels nobody bought would be counted by the next trial.
	for _, id := range buyIDs {
		send(wire.EncodeCancel(wire.Cancel{OrderID: id}))
	}
	for _, p := range pr.Placed {
		send(wire.EncodeCancel(wire.Cancel{OrderID: p.ID}))
	}
	v := judge(pr, sweepID, fills, fillAt)
	v.Label = *label
	v.TimedOut = matched < len(buyIDs) && v.Lost == 0
	writeJSON(*out, v)
	log.Printf("statecheck verify: placed=%d recovered=%d lost=%d duplicated=%d strays=%d ok=%v", v.Placed, v.Recovered, v.Lost, v.Duplicated, v.StraysSwept, v.OK)
}

// judge is the pure half of verify, so the accounting can be tested without
// a broker. A sentinel is recovered when one of our verify buys filled
// against it; lost when it was acknowledged and nothing filled it.
func judge(pr placeReport, sweepID string, fills []wire.Fill, at []time.Duration) verdict {
	sentinel := map[string]int{}
	waveOf := map[string]float64{}
	acked := 0
	for _, p := range pr.Placed {
		if p.Err != "" || p.AckMs == 0 {
			continue
		}
		acked++
		sentinel[p.ID] = 0
		waveOf[p.ID] = p.Wave
	}
	v := verdict{RunID: pr.RunID, Placed: acked}
	first, last := time.Duration(-1), time.Duration(0)
	for i, f := range fills {
		if f.AggressorID == sweepID {
			if _, isSentinel := sentinel[f.RestingID]; isSentinel {
				// The sweep is priced one below the sentinels; this cannot
				// happen unless the book's crossing rule is broken.
				v.UnexpectedFill++
				continue
			}
			v.StraysSwept++
			if len(v.StrayIDs) < 20 {
				v.StrayIDs = append(v.StrayIDs, f.RestingID)
			}
			continue
		}
		if _, ok := sentinel[f.RestingID]; !ok {
			v.UnexpectedFill++
			continue
		}
		sentinel[f.RestingID]++
		if first < 0 {
			first = at[i]
		}
		last = at[i]
	}
	byWave := map[float64]*waveVerdict{}
	for _, w := range pr.Waves {
		byWave[w] = &waveVerdict{Wave: w}
	}
	for id, n := range sentinel {
		wv := byWave[waveOf[id]]
		if wv == nil {
			wv = &waveVerdict{Wave: waveOf[id]}
			byWave[waveOf[id]] = wv
		}
		wv.Placed++
		switch {
		case n == 0:
			v.Lost++
			wv.Lost++
			wv.LostIDs = append(wv.LostIDs, id)
		default:
			v.Recovered++
			wv.Recovered++
			if n > 1 {
				v.Duplicated++
			}
		}
	}
	for _, w := range pr.Waves {
		sort.Strings(byWave[w].LostIDs)
		v.Waves = append(v.Waves, *byWave[w])
	}
	if first >= 0 {
		v.FirstFillMs = float64(first.Microseconds()) / 1000
		v.LastFillMs = float64(last.Microseconds()) / 1000
	}
	v.OK = v.Lost == 0 && v.Duplicated == 0 && v.UnexpectedFill == 0
	switch {
	case v.OK && v.StraysSwept == 0:
		v.Note = "every acknowledged sentinel survived, exactly once"
	case v.OK:
		v.Note = "every sentinel survived; strays were swept below the sentinel price - resting asks the traffic never consumed, e.g. a sell whose buy was lost or replayed"
	case v.Lost > 0:
		v.Note = "sentinels LOST: acknowledged orders are missing from the restored book - see per-wave counts for how far before T the loss reaches"
	default:
		v.Note = "sentinels duplicated or unexpected fills: the restored book holds orders more than once or the crossing rule misbehaved"
	}
	return v
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}

func writeJSON(path string, v any) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		log.Fatalf("statecheck: %v", err)
	}
	if err := os.WriteFile(path, append(b, '\n'), 0o644); err != nil {
		log.Fatalf("statecheck: %v", err)
	}
}
