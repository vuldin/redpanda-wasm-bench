// Command gateways measures server-side filtering for FIX gateways: how many
// bytes, records and CPU each gateway spends to get its own events, with and
// without the in-broker gateway-demux transform.
//
// Every gateway's events go through one "mega events" topic. Without a
// filter each gateway reads all of it and discards what is not its own, so
// broker egress is N x the topic and most gateway CPU goes on records it
// throws away. With redpanda-wasm-clients' gateway-demux each event is routed
// in the broker to its gateway's own topic (MODE=topics) or to a partition
// keyed by gateway id (MODE=keyed), and each gateway reads only that.
//
//	gateways produce -brokers B -topic events -gateways N -rate R -size S -duration D -out produce.json
//	gateways consume -brokers B -arm firehose|topics|keyed -gateways N -source events|gw-|gw-keyed -duration D -out consume.json
//
// Events are [0x47][gateway:u16][seq:u64][send_nanos:u64][padding] (the guest
// reads the same layout), assigned to gateways round-robin by seq so each
// gateway gets exactly 1/N. Producer and consumer are separate processes so
// each one's CPU is its own, and run on one host so latency is one clock.
package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const (
	magic  = 0x47
	header = 1 + 2 + 8 + 8
)

func encodeEvent(buf []byte, gateway uint16, seq uint64, sendNanos int64) {
	buf[0] = magic
	binary.BigEndian.PutUint16(buf[1:3], gateway)
	binary.BigEndian.PutUint64(buf[3:11], seq)
	binary.BigEndian.PutUint64(buf[11:19], uint64(sendNanos))
}

func decodeEvent(v []byte) (gateway uint16, seq uint64, sendNanos int64, ok bool) {
	if len(v) < header || v[0] != magic {
		return 0, 0, 0, false
	}
	return binary.BigEndian.Uint16(v[1:3]), binary.BigEndian.Uint64(v[3:11]),
		int64(binary.BigEndian.Uint64(v[11:19])), true
}

func cpuSeconds() float64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	tv := func(t syscall.Timeval) float64 { return float64(t.Sec) + float64(t.Usec)/1e6 }
	return tv(ru.Utime) + tv(ru.Stime)
}

func writeJSON(path string, v any) {
	b, _ := json.MarshalIndent(v, "", "  ")
	if path == "" || path == "-" {
		fmt.Println(string(b))
		return
	}
	if err := os.WriteFile(path, append(b, '\n'), 0o644); err != nil {
		log.Fatalf("gateways: write %s: %v", path, err)
	}
}

// fill writes cheap pseudo-random bytes (xorshift seeded by seq).
func fill(b []byte, seq uint64) {
	x := seq*0x9E3779B97F4A7C15 + 1
	for i := 0; i < len(b); i += 8 {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		var w [8]byte
		binary.LittleEndian.PutUint64(w[:], x)
		copy(b[i:], w[:])
	}
}

// ---------------------------------------------------------------- produce

type produceReport struct {
	Topic       string  `json:"topic"`
	Gateways    int     `json:"gateways"`
	Rate        float64 `json:"rate"`
	Achieved    float64 `json:"achieved"`
	Size        int     `json:"size"`
	Compression string  `json:"compression"`
	Sent        int64   `json:"sent"`
	Acked       int64   `json:"acked"`
	Errors      int64   `json:"errors"`
	Bytes       int64   `json:"bytes"`
	CPUSeconds  float64 `json:"cpu_seconds"`
	WallSeconds float64 `json:"wall_seconds"`
	StartUnixMs int64   `json:"start_unix_ms"`
	EndUnixMs   int64   `json:"end_unix_ms"`
	// The demux's lag while sending, summed over brokers, and the verdict:
	// latency is a queue age, not a latency, once the transform falls more
	// than a quarter-second of input behind (lag.go).
	Transform     string   `json:"transform,omitempty"`
	TransformLag  lagStats `json:"transform_lag"`
	LatencyUsable bool     `json:"latency_usable"`
}

func produce(args []string) {
	fs := flag.NewFlagSet("produce", flag.ExitOnError)
	brokers := fs.String("brokers", "localhost:9092", "seed brokers")
	topic := fs.String("topic", "events", "the mega events topic")
	gateways := fs.Int("gateways", 8, "gateway count; events go round-robin across them")
	rate := fs.Float64("rate", 30000, "events per second")
	size := fs.Int("size", 650, "event value size in bytes (>= 19)")
	duration := fs.Duration("duration", 60*time.Second, "how long to produce")
	acks := fs.Int("acks", -1, "-1 (all) or 1")
	compression := fs.String("compression", "none", "none, snappy, lz4 or zstd. none by default: broker egress is then the bytes gateways actually receive")
	out := fs.String("out", "-", "report path")
	warmup := fs.Duration("warmup", 5*time.Second, "the stage marker's warmup: match consume's -warmup")
	label := fs.String("label", "", "the level's tag, carried on the stage marker")
	arm := fs.String("arm", "", "the arm, carried on the stage marker")
	adminURLs := fs.String("admin-urls", "", "comma-separated broker admin URLs: sample -transform's lag from their /public_metrics every second while sending (in this process, not the consumer, whose CPU is the measured gateway cost)")
	transform := fs.String("transform", "", "the demux transform's name, for its lag; empty (firehose) for none")
	metricsListen := fs.String("metrics-listen", "", "serve loadgen_phase_info (warmup, measure, drain) on this address (\":9110\" or \"9110\"), loadgen's port. Off when empty")
	_ = fs.Parse(args)
	if *size < header {
		*size = header
	}
	ack := kgo.AllISRAcks()
	if *acks == 1 {
		ack = kgo.LeaderAck()
	}
	codec := map[string]kgo.CompressionCodec{"none": kgo.NoCompression(), "snappy": kgo.SnappyCompression(),
		"lz4": kgo.Lz4Compression(), "zstd": kgo.ZstdCompression()}[*compression]
	cl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(*brokers, ",")...), kgo.DefaultProduceTopic(*topic),
		kgo.RequiredAcks(ack), kgo.ProducerLinger(0), kgo.MaxBufferedRecords(1_000_000),
		kgo.ProducerBatchCompression(codec))
	if err != nil {
		log.Fatalf("gateways: %v", err)
	}
	defer cl.Close()

	var acked, errs atomic.Int64
	interval := time.Duration(float64(time.Second) / *rate)
	marker := &stageMarker{arm: *arm, label: *label}
	marker.serve(*metricsListen)
	lagCtx, stopLag := context.WithCancel(context.Background())
	defer stopLag()
	finishLag := func() lagStats { return lagStats{} }
	if *transform != "" && *adminURLs != "" {
		finishLag = sampleLag(lagCtx, strings.Split(*adminURLs, ","), *transform, time.Second)
	}
	cpu0, start := cpuSeconds(), time.Now()
	deadline := start.Add(*duration)
	var seq uint64
	stage := ""
	for next := start; time.Now().Before(deadline); next = next.Add(interval) {
		if d := time.Until(next); d > 0 {
			time.Sleep(d)
		}
		if s := stageAt(time.Since(start), *warmup, true); s != stage {
			stage = s
			marker.set(s)
		}
		// Random padding: zeros compress to almost nothing anywhere a codec
		// is in play, and egress would then measure compressed zeros.
		v := make([]byte, *size)
		fill(v[header:], seq)
		encodeEvent(v, uint16(seq%uint64(*gateways)), seq, time.Now().UnixNano())
		cl.Produce(context.Background(), &kgo.Record{Value: v}, func(_ *kgo.Record, err error) {
			if err != nil {
				errs.Add(1)
			} else {
				acked.Add(1)
			}
		})
		seq++
	}
	marker.set(stageAt(time.Since(start), *warmup, false))
	_ = cl.Flush(context.Background())
	wall := time.Since(start).Seconds()
	stopLag()
	lag := finishLag()
	lag.Threshold = lagThreshold(*rate)
	writeJSON(*out, produceReport{
		Topic: *topic, Gateways: *gateways, Rate: *rate, Achieved: float64(seq) / duration.Seconds(),
		Size: *size, Compression: *compression, Sent: int64(seq), Acked: acked.Load(), Errors: errs.Load(), Bytes: int64(seq) * int64(*size),
		CPUSeconds: cpuSeconds() - cpu0, WallSeconds: wall,
		StartUnixMs: start.UnixMilli(), EndUnixMs: time.Now().UnixMilli(),
		Transform: *transform, TransformLag: lag, LatencyUsable: latencyUsable(*transform, lag),
	})
}

// ---------------------------------------------------------------- consume

type gatewayStats struct {
	ID         int   `json:"id"`
	Partitions int   `json:"partitions"`
	Records    int64 `json:"records"`
	Bytes      int64 `json:"bytes"`
	Kept       int64 `json:"kept"`
	Foreign    int64 `json:"foreign"`
	latencies  []int64
}

type consumeReport struct {
	Arm              string             `json:"arm"`
	Gateways         int                `json:"gateways"`
	Source           string             `json:"source"`
	SourcePartitions int                `json:"source_partitions,omitempty"`
	Records          int64              `json:"records"`
	Bytes            int64              `json:"bytes"`
	Kept             int64              `json:"kept"`
	Foreign          int64              `json:"foreign"`
	KeptFraction     float64            `json:"kept_fraction"`
	BytesPerGateway  float64            `json:"bytes_per_gateway"`
	CPUSeconds       float64            `json:"cpu_seconds"`
	CPUMicrosPerKept float64            `json:"cpu_micros_per_kept_event"`
	WallSeconds      float64            `json:"wall_seconds"`
	LatencyMicros    map[string]float64 `json:"latency_micros"`
	PerGateway       []gatewayStats     `json:"per_gateway"`
	StartUnixMs      int64              `json:"start_unix_ms"`
	EndUnixMs        int64              `json:"end_unix_ms"`
}

func quantiles(xs []int64) map[string]float64 {
	if len(xs) == 0 {
		return map[string]float64{}
	}
	sort.Slice(xs, func(i, j int) bool { return xs[i] < xs[j] })
	q := func(p float64) float64 { return float64(xs[int(p*float64(len(xs)-1))]) / 1000 }
	return map[string]float64{"p50": q(0.5), "p99": q(0.99), "p999": q(0.999), "max": q(1), "samples": float64(len(xs))}
}

// assignment is what gateway id reads in each arm: topic and partitions.
func assignment(arm, source string, id, partitions int, sourcePartitions []int32) (string, []int32) {
	switch arm {
	case "topics":
		return fmt.Sprintf("%s%d", source, id), nil // all of its own topic
	case "keyed":
		return source, []int32{keyedPartition(id, partitions)}
	default: // firehose: everything
		return source, sourcePartitions
	}
}

func consume(args []string) {
	fs := flag.NewFlagSet("consume", flag.ExitOnError)
	brokers := fs.String("brokers", "localhost:9092", "seed brokers")
	arm := fs.String("arm", "firehose", "firehose (every gateway reads the whole events topic), topics (gateway i reads <source><i>) or keyed (gateway i reads partition murmur2(i) % P of <source>)")
	source := fs.String("source", "events", "events topic (firehose), topic prefix (topics) or demux topic (keyed)")
	gateways := fs.Int("gateways", 8, "gateway count")
	duration := fs.Duration("duration", 90*time.Second, "give up after this long")
	idle := fs.Duration("idle", 10*time.Second, "stop once no gateway has received anything for this long (after the first record)")
	warmup := fs.Duration("warmup", 5*time.Second, "events sent in the first warmup of the stream are counted but not in latency")
	sampleEvery := fs.Int("sample-every", 10, "keep one latency sample per this many kept events")
	out := fs.String("out", "-", "report path")
	_ = fs.Parse(args)
	seeds := strings.Split(*brokers, ",")

	// The keyed arm needs the demux topic's partition count to find its partition.
	parts := 0
	var all []int32
	if *arm != "topics" {
		admin, err := kgo.NewClient(kgo.SeedBrokers(seeds...))
		if err != nil {
			log.Fatalf("gateways: %v", err)
		}
		parts, err = topicPartitions(admin, *source)
		admin.Close()
		if err != nil {
			log.Fatalf("gateways: partitions of %s: %v", *source, err)
		}
		for p := 0; p < parts; p++ {
			all = append(all, int32(p))
		}
	}

	stats := make([]*gatewayStats, *gateways)
	var lastRecord atomic.Int64
	var firstSend atomic.Int64
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()
	cpu0, start := cpuSeconds(), time.Now()
	for i := 0; i < *gateways; i++ {
		topic, ps := assignment(*arm, *source, i, parts, all)
		st := &gatewayStats{ID: i, Partitions: len(ps)}
		stats[i] = st
		opts := []kgo.Opt{kgo.SeedBrokers(seeds...), kgo.FetchMaxWait(100 * time.Millisecond)}
		if ps == nil {
			opts = append(opts, kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
			st.Partitions = -1 // the whole topic
		} else {
			m := map[int32]kgo.Offset{}
			for _, p := range ps {
				m[p] = kgo.NewOffset().AtStart()
			}
			opts = append(opts, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: m}))
		}
		cl, err := kgo.NewClient(opts...)
		if err != nil {
			log.Fatalf("gateways: %v", err)
		}
		wg.Add(1)
		go func(id int, cl *kgo.Client, st *gatewayStats) {
			defer wg.Done()
			defer cl.Close()
			var n int64
			for ctx.Err() == nil {
				fetches := cl.PollFetches(ctx)
				now := time.Now().UnixNano()
				fetches.EachRecord(func(r *kgo.Record) {
					st.Records++
					st.Bytes += int64(len(r.Value) + len(r.Key))
					g, _, sent, ok := decodeEvent(r.Value)
					if !ok || int(g) != id {
						st.Foreign++
						return
					}
					st.Kept++
					firstSend.CompareAndSwap(0, sent)
					if sent-firstSend.Load() >= warmup.Nanoseconds() {
						if n++; n%int64(*sampleEvery) == 0 {
							st.latencies = append(st.latencies, (now-sent)/1000*1000)
						}
					}
					lastRecord.Store(now)
				})
			}
		}(i, cl, st)
	}
	// Idle watcher: the producer has stopped once nothing has arrived for -idle.
	go func() {
		for ctx.Err() == nil {
			time.Sleep(500 * time.Millisecond)
			if l := lastRecord.Load(); l != 0 && time.Since(time.Unix(0, l)) > *idle {
				cancel()
			}
		}
	}()
	wg.Wait()

	rep := consumeReport{Arm: *arm, Gateways: *gateways, Source: *source, SourcePartitions: parts,
		CPUSeconds: cpuSeconds() - cpu0, WallSeconds: time.Since(start).Seconds(),
		StartUnixMs: start.UnixMilli(), EndUnixMs: time.Now().UnixMilli()}
	var lat []int64
	for _, st := range stats {
		rep.Records += st.Records
		rep.Bytes += st.Bytes
		rep.Kept += st.Kept
		rep.Foreign += st.Foreign
		lat = append(lat, st.latencies...)
		rep.PerGateway = append(rep.PerGateway, *st)
	}
	if rep.Records > 0 {
		rep.KeptFraction = float64(rep.Kept) / float64(rep.Records)
	}
	rep.BytesPerGateway = float64(rep.Bytes) / float64(*gateways)
	if rep.Kept > 0 {
		rep.CPUMicrosPerKept = rep.CPUSeconds * 1e6 / float64(rep.Kept)
	}
	rep.LatencyMicros = quantiles(lat)
	writeJSON(*out, rep)
}

func topicPartitions(cl *kgo.Client, topic string) (int, error) {
	req := kmsg.NewPtrMetadataRequest()
	t := kmsg.NewMetadataRequestTopic()
	t.Topic = kmsg.StringPtr(topic)
	req.Topics = append(req.Topics, t)
	resp, err := req.RequestWith(context.Background(), cl)
	if err != nil {
		return 0, err
	}
	for _, t := range resp.Topics {
		if t.Topic != nil && *t.Topic == topic {
			if t.ErrorCode != 0 {
				return 0, fmt.Errorf("metadata error code %d", t.ErrorCode)
			}
			return len(t.Partitions), nil
		}
	}
	return 0, fmt.Errorf("topic %s not in metadata", topic)
}

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: gateways produce|consume [flags]")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "produce":
		produce(os.Args[2:])
	case "consume":
		consume(os.Args[2:])
	default:
		fmt.Fprintln(os.Stderr, "usage: gateways produce|consume [flags]")
		os.Exit(2)
	}
}
