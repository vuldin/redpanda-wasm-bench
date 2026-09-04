package wire

import (
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
