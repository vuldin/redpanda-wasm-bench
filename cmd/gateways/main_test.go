package main

import "testing"

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
