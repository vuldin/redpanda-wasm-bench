package wire

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"
)

func TestNewOrderRoundTrip(t *testing.T) {
	want := NewOrder{
		OrderID:     "order-123",
		Participant: "alice",
		Side:        0,
		Price:       10050,
		Qty:         25,
	}
	buf := EncodeNewOrder(want)
	mt, err := DecodeMessageType(buf)
	if err != nil || mt != MessageNewOrder {
		t.Fatalf("expected MessageNewOrder, got %v err=%v", mt, err)
	}
	got, err := DecodeNewOrder(buf)
	if err != nil {
		t.Fatalf("DecodeNewOrder returned error: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip mismatch: got %+v, want %+v", got, want)
	}
}

func TestCancelRoundTrip(t *testing.T) {
	want := Cancel{OrderID: "order-456"}
	buf := EncodeCancel(want)
	mt, err := DecodeMessageType(buf)
	if err != nil || mt != MessageCancel {
		t.Fatalf("expected MessageCancel, got %v err=%v", mt, err)
	}
	got, err := DecodeCancel(buf)
	if err != nil {
		t.Fatalf("DecodeCancel returned error: %v", err)
	}
	if got != want {
		t.Fatalf("round trip mismatch: got %+v, want %+v", got, want)
	}
}

func TestFillsRoundTripIncludingEmpty(t *testing.T) {
	cases := [][]Fill{
		nil,
		{{AggressorID: "a1", RestingID: "r1", AggressorParty: "alice", RestingParty: "bob", Price: 100, Qty: 5}},
		{
			{AggressorID: "a1", RestingID: "r1", AggressorParty: "alice", RestingParty: "bob", Price: 100, Qty: 5},
			{AggressorID: "a1", RestingID: "r2", AggressorParty: "alice", RestingParty: "carol", Price: 101, Qty: 3},
		},
	}
	for _, want := range cases {
		buf := EncodeFills(want)
		got, err := DecodeFills(buf)
		if err != nil {
			t.Fatalf("DecodeFills returned error: %v", err)
		}
		if len(got) != len(want) {
			t.Fatalf("round trip length mismatch: got %d, want %d", len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("round trip mismatch at %d: got %+v, want %+v", i, got[i], want[i])
			}
		}
	}
}

func TestDecodeRejectsShortBuffers(t *testing.T) {
	if _, err := DecodeNewOrder([]byte{byte(MessageNewOrder)}); err != ErrShortBuffer {
		t.Fatalf("expected ErrShortBuffer, got %v", err)
	}
	if _, err := DecodeCancel([]byte{byte(MessageCancel)}); err != ErrShortBuffer {
		t.Fatalf("expected ErrShortBuffer, got %v", err)
	}
	if _, err := DecodeFills([]byte{0}); err != ErrShortBuffer {
		t.Fatalf("expected ErrShortBuffer, got %v", err)
	}
}

func TestDecodeRejectsWrongMessageType(t *testing.T) {
	buf := EncodeCancel(Cancel{OrderID: "x"})
	if _, err := DecodeNewOrder(buf); err != ErrUnknownMessageType {
		t.Fatalf("expected ErrUnknownMessageType, got %v", err)
	}
}

func massOrder(id string, side uint8, price, qty int64) NewOrder {
	return NewOrder{OrderID: id, Participant: "alice", Side: side, Price: price, Qty: qty}
}

func TestMassQuoteRoundTrip(t *testing.T) {
	m := MassQuote{Orders: []NewOrder{
		massOrder("a-1", 0, 200, 1),
		massOrder("a-2", 1, -42, 7),
		massOrder("a-3", 0, 100, 3),
	}}
	got, err := DecodeMassQuote(EncodeMassQuote(m))
	if err != nil {
		t.Fatalf("DecodeMassQuote: %v", err)
	}
	if len(got.Orders) != 3 {
		t.Fatalf("got %d orders, want 3", len(got.Orders))
	}
	for i := range m.Orders {
		if got.Orders[i] != m.Orders[i] {
			t.Errorf("order %d:\n got %+v\nwant %+v", i, got.Orders[i], m.Orders[i])
		}
	}
}

// A resting envelope that crosses nothing is still a legal record, same as
// count=0 on the fills side.
func TestMassQuoteCountZeroIsValid(t *testing.T) {
	buf := EncodeMassQuote(MassQuote{})
	if len(buf) != 3 {
		t.Fatalf("got %d bytes, want 3 (type + count)", len(buf))
	}
	m, err := DecodeMassQuote(buf)
	if err != nil {
		t.Fatalf("DecodeMassQuote: %v", err)
	}
	if len(m.Orders) != 0 {
		t.Errorf("got %d orders, want 0", len(m.Orders))
	}
}

// -payload-bytes pads records to a minimum size, so every decoder has to stop
// at the declared count rather than trying to consume the filler. Getting this
// wrong is invisible in round-trip tests, which never pad.
func TestMassQuoteIgnoresTrailingPadding(t *testing.T) {
	buf := append(EncodeMassQuote(MassQuote{Orders: []NewOrder{massOrder("a-1", 0, 200, 1)}}),
		make([]byte, 64)...)
	m, err := DecodeMassQuote(buf)
	if err != nil {
		t.Fatalf("DecodeMassQuote with padding: %v", err)
	}
	if len(m.Orders) != 1 {
		t.Errorf("got %d orders, want 1", len(m.Orders))
	}
}

// A truncated envelope must fail rather than decode as a shorter one, or a
// partially-delivered record would be processed as if it were complete.
func TestMassQuoteCountLargerThanPayloadRejected(t *testing.T) {
	buf := EncodeMassQuote(MassQuote{Orders: []NewOrder{
		massOrder("a-1", 0, 200, 1),
		massOrder("a-2", 1, 100, 1),
	}})
	binary.BigEndian.PutUint16(buf[1:], 5)
	if _, err := DecodeMassQuote(buf); err != ErrShortBuffer {
		t.Errorf("got %v, want ErrShortBuffer", err)
	}
}

// The invariant the backward-compatibility claim rests on: a count=1 envelope
// carries exactly the bytes EncodeNewOrder emits, minus its type byte. If these
// drift, no single-order figure is comparable to a mass one.
func TestMassQuoteBodyMatchesSingleNewOrder(t *testing.T) {
	o := massOrder("a-1", 0, 200, 1)
	single := EncodeNewOrder(o)
	mass := EncodeMassQuote(MassQuote{Orders: []NewOrder{o}})
	if !bytes.Equal(mass[3:], single[1:]) {
		t.Errorf("bodies differ:\n mass %x\nsingle %x", mass[3:], single[1:])
	}
	if MessageType(single[0]) != MessageNewOrder || MessageType(mass[0]) != MessageMassQuote {
		t.Errorf("type bytes wrong: single %d mass %d", single[0], mass[0])
	}
}

// Type-byte separation is the whole reason a new type was added instead of
// extending MessageNewOrder: each decoder must reject the other's records
// loudly rather than mis-parsing padding as orders.
func TestMassQuoteAndNewOrderDoNotCrossDecode(t *testing.T) {
	mass := EncodeMassQuote(MassQuote{Orders: []NewOrder{massOrder("a-1", 0, 200, 1)}})
	if _, err := DecodeNewOrder(mass); err != ErrUnknownMessageType {
		t.Errorf("DecodeNewOrder(mass quote) = %v, want ErrUnknownMessageType", err)
	}
	single := EncodeNewOrder(massOrder("a-1", 0, 200, 1))
	if _, err := DecodeMassQuote(single); err != ErrUnknownMessageType {
		t.Errorf("DecodeMassQuote(new order) = %v, want ErrUnknownMessageType", err)
	}
}
