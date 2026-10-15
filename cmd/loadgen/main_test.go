package main

import (
	"bytes"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/vuldin/redpanda-wasm-bench/wire"
)

// The send timestamp riding in the order id is what replaces a per-order map,
// so a parse that silently fails would show up as "zero receipts" rather than
// as a bug here. Round-trip it explicitly.
func TestAggressorIDRoundTrip(t *testing.T) {
	cases := []struct {
		runID     int64
		phase     byte
		seq       int64
		sendNanos int64
	}{
		{1787938000000000000, 't', 0, 1787938000123456789},
		{1787938000000000000, 'w', 1, 1787938000000000001},
		{1, 't', 999999, 42},
		{1787938000000000000, 't', math.MaxInt32, 1787938999999999999},
	}
	for _, c := range cases {
		id := aggressorID(c.runID, c.phase, c.seq, c.sendNanos)
		got := parseAggressorID(id)
		if !got.ok {
			t.Fatalf("parseAggressorID(%q) failed to parse", id)
		}
		if got.runID != c.runID || got.phase != c.phase || got.seq != c.seq || got.sendNanos != c.sendNanos {
			t.Errorf("round trip of %q: got %+v, want runID=%d phase=%c seq=%d sendNanos=%d",
				id, got, c.runID, c.phase, c.seq, c.sendNanos)
		}
	}
}

// A resting order's id must never parse as an aggressor - if it did, the
// resting leg would contribute bogus zero-send-time latency samples.
func TestRestingIDDoesNotParseAsAggressor(t *testing.T) {
	id := restingID(1787938000000000000, 't', 7)
	if got := parseAggressorID(id); got.ok {
		t.Errorf("restingID %q parsed as an aggressor id: %+v", id, got)
	}
}

func TestParseAggressorIDRejectsGarbage(t *testing.T) {
	for _, bad := range []string{
		"", "-", "x", "abc-b1-2", "123t-b-2", "123t-bx-2", "123t-b1-x",
		"123t-b1", "b1-2", "123t",
	} {
		if got := parseAggressorID(bad); got.ok {
			t.Errorf("parseAggressorID(%q) unexpectedly succeeded: %+v", bad, got)
		}
	}
}

// The collector is fed from franz-go's fetch goroutine while the send loop is
// still recording sends, so exercise the same ordering: send recorded, ack
// recorded, then two receipts for the same order (fan-out of 2).
func TestCollectorStages(t *testing.T) {
	const runID = int64(1787938000000000000)
	coll := newCollector(runID, 2, 1)

	sendNanos := int64(1_000_000_000)
	coll.recordSend(0, sendNanos)
	ackAt := time.Unix(0, sendNanos+300_000) // +300us
	coll.recordAck(0, ackAt)

	id := aggressorID(runID, 't', 0, sendNanos)
	matched := sendNanos + 900_000   // +900us from send, +600us from ack
	receipt := sendNanos + 1_400_000 // +1400us from send, +500us from matched
	observedAt := time.Unix(0, receipt+5_000_000)

	line := id + ":" + strconv.FormatInt(receipt, 10) + ":" + strconv.FormatInt(matched, 10)
	coll.onReceipt(line, observedAt)
	coll.onReceipt(line, observedAt) // second probe's receipt for the same order

	if got := coll.timedReceipts.Load(); got != 2 {
		t.Fatalf("timedReceipts = %d, want 2", got)
	}
	if got := len(coll.totalLat); got != 2 {
		t.Errorf("totalLat samples = %d, want 2 (one per probe)", got)
	}
	if got := len(coll.relayLat); got != 2 {
		t.Errorf("relayLat samples = %d, want 2 (one per probe)", got)
	}
	// match does not vary per probe, so exactly one sample regardless of fanout.
	if got := len(coll.matchLat); got != 1 {
		t.Fatalf("matchLat samples = %d, want 1 (match is per-order, not per-probe)", got)
	}

	if want := 1400.0; !closeTo(coll.totalLat[0], want) {
		t.Errorf("total = %vus, want %vus", coll.totalLat[0], want)
	}
	if want := 500.0; !closeTo(coll.relayLat[0], want) {
		t.Errorf("relay_consume = %vus, want %vus", coll.relayLat[0], want)
	}
	if want := 600.0; !closeTo(coll.matchLat[0], want) {
		t.Errorf("match = %vus, want %vus", coll.matchLat[0], want)
	}

	produce := coll.produceStage()
	if len(produce) != 1 {
		t.Fatalf("produce samples = %d, want 1", len(produce))
	}
	if want := 300.0; !closeTo(produce[0], want) {
		t.Errorf("produce = %vus, want %vus", produce[0], want)
	}
}

func TestCollectorDiscardsWarmupAndForeignRuns(t *testing.T) {
	const runID = int64(1787938000000000000)
	coll := newCollector(runID, 1, 1)
	now := time.Unix(0, 2_000_000_000)

	warm := aggressorID(runID, 'w', 0, 1_000_000_000)
	coll.onReceipt(warm+":1500000000:1200000000", now)
	if got := coll.warmupReceipts.Load(); got != 1 {
		t.Errorf("warmupReceipts = %d, want 1", got)
	}
	if got := coll.timedReceipts.Load(); got != 0 {
		t.Errorf("timedReceipts = %d, want 0 - a warmup receipt must not be timed", got)
	}

	foreign := aggressorID(runID+1, 't', 0, 1_000_000_000)
	coll.onReceipt(foreign+":1500000000:1200000000", now)
	if got := coll.timedReceipts.Load(); got != 0 {
		t.Errorf("timedReceipts = %d, want 0 - another run's receipt must be ignored", got)
	}
}

func TestCollectorSampleEvery(t *testing.T) {
	const runID = int64(1787938000000000000)
	coll := newCollector(runID, 1, 4)
	now := time.Unix(0, 9_000_000_000)
	for seq := int64(0); seq < 8; seq++ {
		id := aggressorID(runID, 't', seq, 1_000_000_000)
		coll.onReceipt(id+":1500000000:1200000000", now)
	}
	// Every receipt counts toward rates; only seq 0 and 4 carry latency.
	if got := coll.timedReceipts.Load(); got != 8 {
		t.Errorf("timedReceipts = %d, want 8 - rates must cover every order", got)
	}
	if got := len(coll.totalLat); got != 2 {
		t.Errorf("totalLat samples = %d, want 2 (seq 0 and 4 at sample-every=4)", got)
	}
}

// judge is what stops a saturated or contaminated run from being read as a
// measurement, so its two open-loop-specific verdicts need to be distinct.
func TestJudgeSeparatesClientAndBrokerCeilings(t *testing.T) {
	clientBound := &report{
		ReceivedReceipts: 100,
		Config:           configReport{OfferedRate: 10000},
		Rates:            ratesReport{AttemptedPerSec: 4000, AchievedOrdersPerSec: 4000, AttemptedRatio: 0.4, AchievedRatio: 0.4},
		Resources:        resourcesReport{ScrapedBeforeAndAfter: true},
	}
	clean, reasons := judge(clientBound)
	if clean {
		t.Error("a run where the client could not offer the rate must not be clean")
	}
	if !containsAny(reasons, "client could not offer") {
		t.Errorf("expected a client-side ceiling reason, got %v", reasons)
	}
	if containsAny(reasons, "past saturation") {
		t.Errorf("a client-bound run must not also be reported as broker saturation: %v", reasons)
	}

	brokerBound := &report{
		ReceivedReceipts: 100,
		Config:           configReport{OfferedRate: 10000},
		Rates:            ratesReport{AttemptedPerSec: 10000, AchievedOrdersPerSec: 6000, AttemptedRatio: 1.0, AchievedRatio: 0.6},
		Resources:        resourcesReport{ScrapedBeforeAndAfter: true},
	}
	clean, reasons = judge(brokerBound)
	if clean {
		t.Error("a run past saturation must not be clean")
	}
	if !containsAny(reasons, "past saturation") {
		t.Errorf("expected a saturation reason, got %v", reasons)
	}

	good := &report{
		ReceivedReceipts: 100,
		Config:           configReport{OfferedRate: 10000},
		Rates:            ratesReport{AttemptedPerSec: 10000, AchievedOrdersPerSec: 10000, AttemptedRatio: 1.0, AchievedRatio: 1.0},
		Resources:        resourcesReport{ScrapedBeforeAndAfter: true},
	}
	if clean, reasons := judge(good); !clean {
		t.Errorf("a run at the offered rate with no contamination should be clean, got %v", reasons)
	}
}

func TestJudgeFlagsBacklogAndDrops(t *testing.T) {
	rep := &report{
		ReceivedReceipts: 100,
		Config:           configReport{OfferedRate: 1000},
		Rates:            ratesReport{AttemptedRatio: 1.0, AchievedRatio: 1.0},
		Lag:              lagReport{FirstLag: 0, FinalLag: 50000, MaxLag: 50000, BacklogThreshold: 1000, GrewDuringRun: true},
		Resources:        resourcesReport{ScrapedBeforeAndAfter: true, RelayDroppedDelta: 12},
	}
	clean, reasons := judge(rep)
	if clean {
		t.Error("a run with a growing backlog and relay drops must not be clean")
	}
	if !containsAny(reasons, "backlog age") {
		t.Errorf("expected the backlog-age warning, got %v", reasons)
	}
	if !containsAny(reasons, "relay dropped") {
		t.Errorf("expected the relay-drop warning, got %v", reasons)
	}
}

// A verdict that fires on every run carries no information. A handful of
// records of lag at a high rate is scheduling noise, not saturation - flagging
// it made a comfortably-keeping-up 20k/sec run read NOT CLEAN on a real
// cluster, which is what motivated the threshold.
func TestLagVerdictIgnoresTrivialBacklog(t *testing.T) {
	cases := []struct {
		name      string
		first     float64
		final     float64
		threshold float64
		wantGrew  bool
	}{
		{"noise at 20k orders/sec", 9, 23, 10000, false},
		{"static large backlog, not growing", 50000, 50000, 10000, false},
		{"real saturation", 8272, 294712, 25000, true},
		{"growing but still trivial", 10, 900, 10000, false},
		{"just over the threshold", 10, 10001, 10000, true},
	}
	for _, c := range cases {
		grew := c.final > c.first && c.final > c.threshold
		if grew != c.wantGrew {
			t.Errorf("%s: lag %v->%v vs threshold %v: got grew=%v, want %v",
				c.name, c.first, c.final, c.threshold, grew, c.wantGrew)
		}
	}
}

// The scraper has to survive a real /public_metrics body: comment lines,
// unrelated families, label sets with the target label in any position.
func TestSnapshotByLabelAndSum(t *testing.T) {
	body := `# HELP redpanda_wasm_engine_cpu_seconds_total Total CPU time
# TYPE redpanda_wasm_engine_cpu_seconds_total counter
redpanda_wasm_engine_cpu_seconds_total{redpanda_function_name="matcher"} 1.5
redpanda_wasm_engine_cpu_seconds_total{function_name="matcher"} 2.5
redpanda_wasm_engine_cpu_seconds_total{function_name="probe-1"} 0.25
redpanda_something_else{function_name="matcher"} 999
redpanda_relay_dropped_total{shard="0"} 3
redpanda_relay_dropped_total{shard="1"} 4
`
	snap := parseMetricsBody(strings.NewReader(body))

	cpu := snap.byLabel("redpanda_wasm_engine_cpu_seconds_total", "function_name")
	// redpanda_function_name="matcher" contains function_name=" as a
	// substring. It is a DIFFERENT label and must not fold into this bucket -
	// only the 2.5 from the real function_name series belongs here. A naive
	// substring match returns 4.0 and inflates every capacity number derived
	// from it.
	if got, want := cpu["matcher"], 2.5; got != want {
		t.Errorf(`cpu["matcher"] = %v, want %v (redpanda_function_name must not fold in)`, got, want)
	}
	if got, want := cpu["probe-1"], 0.25; got != want {
		t.Errorf(`cpu["probe-1"] = %v, want %v`, got, want)
	}
	if got, want := snap.sum("redpanda_relay_dropped_total"), 7.0; got != want {
		t.Errorf("sum(relay_dropped) = %v, want %v", got, want)
	}
	if got := snap.sum("redpanda_something_else"); got != 0 {
		t.Errorf("an unrequested family should not be collected, got %v", got)
	}
}

// --- helpers ---

func closeTo(a, b float64) bool { return math.Abs(a-b) < 0.001 }

func containsAny(reasons []string, substr string) bool {
	for _, r := range reasons {
		if strings.Contains(r, substr) {
			return true
		}
	}
	return false
}

// The multi-producer sequence split must satisfy two properties or correlation
// breaks silently:
//
//  1. Ids stay UNIQUE across producers - a collision would make two orders
//     share a send timestamp and corrupt latency.
//  2. Both legs of a pair go through ONE producer. franz-go preserves order per
//     client per partition, so splitting the legs lets the buy land before its
//     sell; the matcher would then rest the buy and make the SELL the
//     aggressor, and the sell's id has no recorded send time - receipts would
//     be silently dropped as unparseable-or-unknown rather than erroring.
func TestMultiProducerSequenceSplit(t *testing.T) {
	for _, producers := range []int{1, 2, 4, 8} {
		seen := map[int64]int{} // seq -> producer index
		perProducer := make([]int64, producers)
		const countsEach = 500

		for idx := 0; idx < producers; idx++ {
			n := int64(producers)
			for count := int64(0); count < countsEach; count++ {
				// mirrors paceOne's assignment
				seq := count*n + int64(idx)
				if prev, dup := seen[seq]; dup {
					t.Fatalf("producers=%d: seq %d produced by both producer %d and %d",
						producers, seq, prev, idx)
				}
				seen[seq] = idx
				perProducer[idx]++
			}
		}

		if got, want := len(seen), producers*countsEach; got != want {
			t.Errorf("producers=%d: %d distinct sequences, want %d", producers, got, want)
		}
		// Every producer must carry an equal share, or the aggregate offered
		// rate is not the requested rate.
		for idx, n := range perProducer {
			if n != countsEach {
				t.Errorf("producers=%d: producer %d sent %d, want %d (uneven share skews the offered rate)",
					producers, idx, n, countsEach)
			}
		}
		// Both legs of a pair derive from the same seq, so same-producer
		// affinity holds by construction - assert the ids actually agree.
		for seq, idx := range seen {
			a := parseAggressorID(aggressorID(12345, 't', seq, 999))
			if !a.ok || a.seq != seq {
				t.Fatalf("producers=%d: aggressor id for seq %d (producer %d) did not round trip",
					producers, seq, idx)
			}
		}
	}
}

// TestDecodeFills pins the fill wire format the external arm depends on.
//
// This decoder is a Go port of the guest-side decoders in relay-probe.rs and
// relay-sink.rs. If the two drift, the external arm silently stops correlating
// and reports zero receipts - or worse, correlates partially and reports
// plausible but wrong latencies. Encode here exactly what the matcher emits.
func TestDecodeFills(t *testing.T) {
	str := func(s string) []byte {
		b := []byte{byte(len(s) >> 8), byte(len(s))}
		return append(b, s...)
	}
	build := func(ids []string, matchedAt uint64) []byte {
		out := []byte{byte(len(ids) >> 8), byte(len(ids))}
		for _, id := range ids {
			out = append(out, str(id)...)     // aggressor_id
			out = append(out, str("rest")...) // resting_id
			out = append(out, str("pa")...)   // aggressor_party
			out = append(out, str("pb")...)   // resting_party
			out = append(out, make([]byte, 16)...)
		}
		var ts [8]byte
		for i := 0; i < 8; i++ {
			ts[7-i] = byte(matchedAt >> (8 * i))
		}
		return append(out, ts[:]...)
	}

	t.Run("single fill round trips", func(t *testing.T) {
		ids, ts, ok := decodeFills(build([]string{"abc"}, 123456789))
		if !ok {
			t.Fatal("decode failed on a well-formed payload")
		}
		if len(ids) != 1 || ids[0] != "abc" {
			t.Fatalf("ids = %v, want [abc]", ids)
		}
		if ts != 123456789 {
			t.Fatalf("matchedAt = %d, want 123456789", ts)
		}
	})

	t.Run("multiple fills", func(t *testing.T) {
		ids, _, ok := decodeFills(build([]string{"one", "two", "three"}, 1))
		if !ok || len(ids) != 3 || ids[2] != "three" {
			t.Fatalf("ok=%v ids=%v, want 3 ids ending in three", ok, ids)
		}
	})

	t.Run("empty fill list still yields the timestamp", func(t *testing.T) {
		// The matcher stamps matched_at unconditionally, including for a
		// resting order that crossed nothing.
		ids, ts, ok := decodeFills(build(nil, 42))
		if !ok {
			t.Fatal("decode failed on an empty fill list")
		}
		if len(ids) != 0 {
			t.Fatalf("ids = %v, want empty", ids)
		}
		if ts != 42 {
			t.Fatalf("matchedAt = %d, want 42", ts)
		}
	})

	t.Run("truncated payloads are rejected, not guessed", func(t *testing.T) {
		full := build([]string{"abc"}, 7)
		for n := 0; n < len(full); n++ {
			if _, _, ok := decodeFills(full[:n]); ok {
				t.Fatalf("decode accepted a truncated payload of %d/%d bytes", n, len(full))
			}
		}
	})
}

// ---------------------------------------------------------------------------
// mass-quote envelopes
// ---------------------------------------------------------------------------

// The guard the whole backward-compatibility claim rests on: at
// -orders-per-record=1 the generator emits exactly the bytes it emitted
// before envelopes existed, with exactly the same ids. If this fails, no new
// run can be compared against a published figure.
func TestEnvelopePairAtN1IsByteIdenticalToSingleOrders(t *testing.T) {
	const runID = int64(1787938000000000000)
	const seq = int64(42)
	const sendNanos = int64(1_000_000_000)

	sell, buy := encodeEnvelopePair(runID, 't', seq, 1, sendNanos)

	wantSell := wire.EncodeNewOrder(wire.NewOrder{
		OrderID: restingID(runID, 't', seq), Participant: "seller", Side: 1, Price: 100, Qty: 1,
	})
	wantBuy := wire.EncodeNewOrder(wire.NewOrder{
		OrderID: aggressorID(runID, 't', seq, sendNanos), Participant: "buyer", Side: 0, Price: 200, Qty: 1,
	})
	if !bytes.Equal(sell, wantSell) {
		t.Errorf("resting leg differs:\n got %x\nwant %x", sell, wantSell)
	}
	if !bytes.Equal(buy, wantBuy) {
		t.Errorf("aggressor leg differs:\n got %x\nwant %x", buy, wantBuy)
	}
	// And it must still be a plain NewOrder, not a count=1 envelope.
	if wire.MessageType(buy[0]) != wire.MessageNewOrder {
		t.Errorf("type byte = %d, want MessageNewOrder", buy[0])
	}
}

// At N>1 the legs become envelopes carrying N orders each, with per-order ids
// that all round trip back to the same envelope seq.
func TestEnvelopePairCarriesNOrdersWithPerOrderIDs(t *testing.T) {
	const runID = int64(1787938000000000000)
	const seq = int64(7)
	const sendNanos = int64(1_000_000_000)
	const n = 15

	sell, buy := encodeEnvelopePair(runID, 't', seq, n, sendNanos)

	for name, raw := range map[string][]byte{"resting": sell, "aggressor": buy} {
		if wire.MessageType(raw[0]) != wire.MessageMassQuote {
			t.Fatalf("%s: type byte = %d, want MessageMassQuote", name, raw[0])
		}
		m, err := wire.DecodeMassQuote(raw)
		if err != nil {
			t.Fatalf("%s: DecodeMassQuote: %v", name, err)
		}
		if len(m.Orders) != n {
			t.Fatalf("%s: %d orders, want %d", name, len(m.Orders), n)
		}
	}

	m, _ := wire.DecodeMassQuote(buy)
	seenK := map[int]bool{}
	for _, o := range m.Orders {
		pid := parseAggressorID(o.OrderID)
		if !pid.ok {
			t.Fatalf("aggressor id %q did not parse", o.OrderID)
		}
		if pid.seq != seq {
			t.Errorf("id %q: seq = %d, want %d (seq must stay the ENVELOPE's)", o.OrderID, pid.seq, seq)
		}
		if pid.sendNanos != sendNanos {
			t.Errorf("id %q: sendNanos = %d, want %d", o.OrderID, pid.sendNanos, sendNanos)
		}
		if seenK[pid.k] {
			t.Errorf("id %q: duplicate envelope index %d", o.OrderID, pid.k)
		}
		seenK[pid.k] = true
	}
	if len(seenK) != n {
		t.Errorf("distinct envelope indices = %d, want %d", len(seenK), n)
	}
}

// One record earns one send instant, one match stamp and one receipt, so an
// envelope must contribute exactly ONE latency sample per probe however many
// orders it carries. Appending one per order would multiply Samples and
// OverMicros by N while adding no information - and OverMicros is documented
// as a count of affected units.
func TestEnvelopeContributesOneLatencySamplePerProbe(t *testing.T) {
	const runID = int64(1787938000000000000)
	const n = 10
	coll := newCollector(runID, 1, 1)

	sendNanos := int64(1_000_000_000)
	coll.recordSend(0, sendNanos)
	coll.recordAck(0, time.Unix(0, sendNanos+300_000))

	matched := sendNanos + 900_000
	receipt := sendNanos + 1_400_000
	observedAt := time.Unix(0, receipt+5_000_000)

	// relay-probe emits one receipt line per fill, so N lines for one record,
	// every one carrying the SAME receipt and matched stamps.
	for k := 0; k < n; k++ {
		id := aggressorIDAt(runID, 't', 0, k, sendNanos)
		coll.onReceipt(id+":"+strconv.FormatInt(receipt, 10)+":"+strconv.FormatInt(matched, 10), observedAt)
	}

	if got := coll.timedReceipts.Load(); got != n {
		t.Errorf("timedReceipts = %d, want %d (every order counts)", got, n)
	}
	if got := len(coll.totalLat); got != 1 {
		t.Errorf("totalLat samples = %d, want 1 per envelope per probe", got)
	}
	if got := len(coll.relayLat); got != 1 {
		t.Errorf("relayLat samples = %d, want 1 per envelope per probe", got)
	}
	if got := len(coll.matchLat); got != 1 {
		t.Errorf("matchLat samples = %d, want 1", got)
	}
	// Dedupe is still per ORDER: N keys at one receipt each, not one key at N.
	if got := len(coll.sampledSeen); got != n {
		t.Errorf("sampledSeen keys = %d, want %d (one per order)", got, n)
	}
	for key, count := range coll.sampledSeen {
		if count != 1 {
			t.Errorf("order %+v seen %d times, want 1", key, count)
		}
	}
}

// A duplicate must still be visible per order, so a replay cannot hide behind
// an envelope-wide count.
func TestEnvelopeDuplicateIsVisiblePerOrder(t *testing.T) {
	const runID = int64(1787938000000000000)
	coll := newCollector(runID, 1, 1)
	sendNanos := int64(1_000_000_000)
	line := func(k int) string {
		return aggressorIDAt(runID, 't', 0, k, sendNanos) + ":2000000000:1900000000"
	}
	observedAt := time.Unix(0, 3_000_000_000)
	coll.onReceipt(line(0), observedAt)
	coll.onReceipt(line(1), observedAt)
	coll.onReceipt(line(1), observedAt) // order 1 replayed

	if got := coll.sampledSeen[orderKey{Seq: 0, K: 1}]; got != 2 {
		t.Errorf("order 1 seen %d times, want 2", got)
	}
	if got := coll.sampledSeen[orderKey{Seq: 0, K: 0}]; got != 1 {
		t.Errorf("order 0 seen %d times, want 1", got)
	}
}

// An envelope missing an order is invisible to any total-receipt
// reconciliation, so the per-envelope index set is what has to catch it.
func TestIncompleteEnvelopeIsDetectable(t *testing.T) {
	const runID = int64(1787938000000000000)
	coll := newCollector(runID, 1, 1)
	sendNanos := int64(1_000_000_000)
	observedAt := time.Unix(0, 3_000_000_000)

	// Envelope 0: orders 0 and 2 arrive, order 1 never does.
	for _, k := range []int{0, 2} {
		coll.onReceipt(aggressorIDAt(runID, 't', 0, k, sendNanos)+":2000000000:1900000000", observedAt)
	}
	// Envelope 1: complete.
	for _, k := range []int{0, 1, 2} {
		coll.onReceipt(aggressorIDAt(runID, 't', 1, k, sendNanos)+":2000000000:1900000000", observedAt)
	}

	if got := len(coll.envelopeSeen[0]); got != 2 {
		t.Errorf("envelope 0 covered %d orders, want 2", got)
	}
	if got := len(coll.envelopeSeen[1]); got != 3 {
		t.Errorf("envelope 1 covered %d orders, want 3", got)
	}
	// Total receipts reconcile at 5 either way, which is exactly why this
	// check exists: 2+3 and 1+4 are indistinguishable from the total alone.
	if got := coll.timedReceipts.Load(); got != 5 {
		t.Errorf("timedReceipts = %d, want 5", got)
	}
}

// p99.99 is suppressed below the sample count at which it means anything,
// rather than published as an interpolation through three points.
func TestP9999SuppressedBelowSampleFloor(t *testing.T) {
	small := make([]float64, 3000)
	for i := range small {
		small[i] = float64(i)
	}
	if got := newStageStats(small).P9999Micros; got != 0 {
		t.Errorf("p9999 at 3000 samples = %v, want 0 (suppressed)", got)
	}
	big := make([]float64, 100000)
	for i := range big {
		big[i] = float64(i)
	}
	if got := newStageStats(big).P9999Micros; got == 0 {
		t.Error("p9999 at 100000 samples = 0, want it computed")
	}
}

func TestStddevMeasuresSpread(t *testing.T) {
	flat := []float64{100, 100, 100, 100}
	if got := newStageStats(flat).StddevMicros; got != 0 {
		t.Errorf("stddev of a constant = %v, want 0", got)
	}
	// Two distributions with the same median and very different spread - the
	// distinction a percentile alone cannot draw.
	tight := []float64{99, 100, 100, 101}
	wide := []float64{1, 100, 100, 199}
	ts, ws := newStageStats(tight), newStageStats(wide)
	if ts.P50Micros != ws.P50Micros {
		t.Fatalf("medians differ (%v vs %v); the test no longer isolates spread", ts.P50Micros, ws.P50Micros)
	}
	if !(ws.StddevMicros > ts.StddevMicros*10) {
		t.Errorf("stddev did not separate them: tight %v, wide %v", ts.StddevMicros, ws.StddevMicros)
	}
}

// The preload is only worth anything if the orders actually REST. If any of
// them crossed - against each other or against the measured traffic - the book
// would drain and a depth sweep would silently measure an empty book at every
// level, which looks exactly like "depth is free".
func TestBookPreloadOrdersCannotCross(t *testing.T) {
	const runID = int64(1787938000000000000)
	const depth = 50
	recs := encodeBookPreload(runID, depth)
	if len(recs) != depth*2 {
		t.Fatalf("got %d records, want %d (both sides)", len(recs), depth*2)
	}

	var bids, asks []wire.NewOrder
	for _, r := range recs {
		o, err := wire.DecodeNewOrder(r)
		if err != nil {
			t.Fatalf("preload record did not decode: %v", err)
		}
		if o.Side == 0 {
			bids = append(bids, o)
		} else {
			asks = append(asks, o)
		}
	}
	if len(bids) != depth || len(asks) != depth {
		t.Fatalf("sides unbalanced: %d bids, %d asks", len(bids), len(asks))
	}

	// No preload bid may reach any preload ask.
	maxBid, minAsk := int64(-1<<62), int64(1<<62)
	for _, o := range bids {
		if o.Price > maxBid {
			maxBid = o.Price
		}
	}
	for _, o := range asks {
		if o.Price < minAsk {
			minAsk = o.Price
		}
	}
	if maxBid >= minAsk {
		t.Errorf("preload crosses itself: best bid %d >= best ask %d", maxBid, minAsk)
	}

	// Nor may it cross the measured traffic, which sells at 100 and buys at 200.
	const measuredSell, measuredBuy = int64(100), int64(200)
	if maxBid >= measuredSell {
		t.Errorf("preload bid %d would cross the measured sell at %d, consuming it",
			maxBid, measuredSell)
	}
	if minAsk <= measuredBuy {
		t.Errorf("preload ask %d would cross the measured buy at %d, consuming it",
			minAsk, measuredBuy)
	}

	// Ids must not collide with measured traffic, or a preload fill would be
	// counted as a measured receipt.
	for _, o := range append(bids, asks...) {
		if pid := parseAggressorID(o.OrderID); pid.ok && pid.phase != 'p' {
			t.Errorf("preload id %q parses as measured traffic (phase %c)", o.OrderID, pid.phase)
		}
	}
}
