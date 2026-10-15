package wire

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"os"
	"strings"
	"testing"
)

// The fill wire format has two independent implementations: rust/src/wire.rs in
// the redpanda-wasm-clients repository (what the guests encode with) and this
// package (what the load generator decodes with). Each repository's own tests
// pass against its own implementation, which proves nothing about the pair.
//
// This test is what couples them. The fixture is a payload produced by the Rust
// guest, checked in as bytes. Regenerate it from the clients repository - see
// WIRE-FORMAT.md there - and never by hand-editing this file, which would make
// the test agree with a Go bug.
func loadFixture(t *testing.T) []byte {
	t.Helper()
	raw, err := os.ReadFile("testdata/fills-from-rust-guest.hex")
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}
	b, err := hex.DecodeString(strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatalf("decoding fixture hex: %v", err)
	}
	return b
}

// The mass-quote fixture is the input-direction counterpart, and it closes a
// real gap: until it existed only the Rust-to-Go fills direction was pinned,
// so the harness could encode an input the guests could not read and both
// test suites would still pass. The bytes are produced by rust/src/wire.rs -
// REGEN_WIRE_FIXTURES=1 cargo test emit_mass_quote_fixture - and copied here.
func loadMassQuoteFixture(t *testing.T) []byte {
	t.Helper()
	raw, err := os.ReadFile("testdata/mass-quote-from-rust-guest.hex")
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}
	b, err := hex.DecodeString(strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatalf("decoding fixture hex: %v", err)
	}
	return b
}

func TestDecodesRustGuestMassQuote(t *testing.T) {
	m, err := DecodeMassQuote(loadMassQuoteFixture(t))
	if err != nil {
		t.Fatalf("DecodeMassQuote on a real guest payload: %v", err)
	}
	want := []NewOrder{
		{OrderID: "a-1", Participant: "alice", Side: 0, Price: 10150, Qty: 7},
		{OrderID: "a-2", Participant: "alice", Side: 1, Price: -42, Qty: 1},
		{OrderID: "a-3", Participant: "alice", Side: 0, Price: 100, Qty: 3},
	}
	if len(m.Orders) != len(want) {
		t.Fatalf("got %d orders, want %d", len(m.Orders), len(want))
	}
	for i := range want {
		if m.Orders[i] != want[i] {
			t.Errorf("order %d:\n got %+v\nwant %+v", i, m.Orders[i], want[i])
		}
	}
}

// Go must also produce bytes Rust can read, so pin this direction against the
// same fixture: the harness is the encoder in production, and an encoder bug
// is exactly what a decode-only test cannot see.
func TestEncodesMassQuoteByteIdenticalToRustGuest(t *testing.T) {
	got := EncodeMassQuote(MassQuote{Orders: []NewOrder{
		{OrderID: "a-1", Participant: "alice", Side: 0, Price: 10150, Qty: 7},
		{OrderID: "a-2", Participant: "alice", Side: 1, Price: -42, Qty: 1},
		{OrderID: "a-3", Participant: "alice", Side: 0, Price: 100, Qty: 3},
	}})
	if want := loadMassQuoteFixture(t); !bytes.Equal(got, want) {
		t.Errorf("encoder disagrees with the Rust guest:\n got %x\nwant %x", got, want)
	}
}

func TestDecodesRustGuestFills(t *testing.T) {
	buf := loadFixture(t)
	fills, err := DecodeFills(buf)
	if err != nil {
		t.Fatalf("DecodeFills on a real guest payload: %v", err)
	}
	if len(fills) != 2 {
		t.Fatalf("got %d fills, want 2", len(fills))
	}
	want := []Fill{
		{AggressorID: "a-1", RestingID: "r-1", AggressorParty: "alice", RestingParty: "bob", Price: 10150, Qty: 7},
		{AggressorID: "a-2", RestingID: "r-2", AggressorParty: "carol", RestingParty: "dave", Price: -42, Qty: 1},
	}
	for i := range want {
		if fills[i] != want[i] {
			t.Errorf("fill %d:\n got %+v\nwant %+v", i, fills[i], want[i])
		}
	}
}

// The negative price in the fixture is deliberate. Both sides encode price as a
// signed big-endian i64; a decoder that reads it unsigned passes every
// positive-only test and then silently produces 18446744073709551574 in
// production.
func TestPriceIsSigned(t *testing.T) {
	fills, err := DecodeFills(loadFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	if fills[1].Price != -42 {
		t.Errorf("price = %d, want -42 (read as signed i64)", fills[1].Price)
	}
}

// The matcher appends 8 bytes of matched_at_nanos AFTER the last fill, outside
// the count. DecodeFills must stop after `count` fills and ignore them.
//
// This is the single most breakable part of the contract: a decoder that
// validates the buffer was fully consumed rejects every real payload, and one
// that folds the trailing bytes into the last fill corrupts it silently. Both
// mistakes pass a test suite built only from self-encoded round trips.
func TestIgnoresTrailingMatchedAtNanos(t *testing.T) {
	buf := loadFixture(t)

	// The fixture must actually carry the trailing timestamp, or this test is
	// vacuous - guard against a regenerated fixture that dropped it.
	const wantNanos uint64 = 1788406214123456789
	if got := binary.BigEndian.Uint64(buf[len(buf)-8:]); got != wantNanos {
		t.Fatalf("fixture's trailing 8 bytes = %d, want %d - regenerate it with "+
			"the matched_at_nanos append included", got, wantNanos)
	}

	withTS, err := DecodeFills(buf)
	if err != nil {
		t.Fatalf("payload with trailing timestamp must decode: %v", err)
	}
	withoutTS, err := DecodeFills(buf[:len(buf)-8])
	if err != nil {
		t.Fatalf("same payload without the timestamp must decode: %v", err)
	}
	if len(withTS) != len(withoutTS) {
		t.Fatalf("trailing timestamp changed the fill count: %d vs %d",
			len(withTS), len(withoutTS))
	}
	for i := range withTS {
		if withTS[i] != withoutTS[i] {
			t.Errorf("fill %d differs with and without the trailing timestamp:\n"+
				" with: %+v\n  w/o: %+v", i, withTS[i], withoutTS[i])
		}
	}
}

// A resting order that crosses nothing still emits a payload - the relay pushes
// every transformed record - so count=0 is a normal value, not an error.
func TestEmptyFillListIsValid(t *testing.T) {
	fills, err := DecodeFills([]byte{0x00, 0x00})
	if err != nil {
		t.Fatalf("count=0 must decode, got %v", err)
	}
	if len(fills) != 0 {
		t.Errorf("got %d fills, want 0", len(fills))
	}
}

func TestShortBufferRejected(t *testing.T) {
	if _, err := DecodeFills([]byte{0x00}); err == nil {
		t.Error("a 1-byte buffer cannot carry a count and must be rejected")
	}
	// Claims two fills, supplies none.
	if _, err := DecodeFills([]byte{0x00, 0x02}); err == nil {
		t.Error("count=2 with no fill data must be rejected")
	}
}
