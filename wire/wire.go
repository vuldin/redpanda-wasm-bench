// Package wire defines the record formats this project's two entrypoints
// - cmd/wasm-matcher (an in-broker Redpanda WASM transform) and
// cmd/kafka-matcher (a standalone external Kafka client) - both read and
// write. Sharing one encoding between them is the whole point: the same
// bytes on the wire, decoded and processed by the exact same orderbook
// and risk packages, so a latency comparison between the two entrypoints
// is actually apples to apples - the only thing that differs between
// them is how the bytes get from producer to matcher to consumer, not
// what happens once they arrive.
//
// Deliberately not JSON or protobuf: a fixed-layout binary encoding
// keeps both entrypoints free of a serialization library dependency,
// and keeps decode cost itself out of whatever the benchmark is trying
// to measure.
package wire

import (
	"encoding/binary"
	"errors"
)

// MessageType identifies the kind of record in an input message.
type MessageType uint8

const (
	MessageNewOrder MessageType = 0
	MessageCancel   MessageType = 1
)

// ErrShortBuffer is returned by the Decode functions when buf is too
// small to contain a complete, well-formed message.
var ErrShortBuffer = errors.New("wire: buffer too short")

// ErrUnknownMessageType is returned when the leading type byte doesn't
// match a known MessageType.
var ErrUnknownMessageType = errors.New("wire: unknown message type")

// NewOrder is the decoded form of a MessageNewOrder input record.
type NewOrder struct {
	OrderID     string
	Participant string
	Side        uint8 // 0 = buy, 1 = sell - matches orderbook.Side's own encoding
	Price       int64
	Qty         int64
}

// Cancel is the decoded form of a MessageCancel input record.
type Cancel struct {
	OrderID string
}

// Fill is the wire form of a single match, as reported in an output
// record. One output record may carry several Fills (all the fills one
// input order generated), so trades aren't split across a batch flush the
// way transform_processor's own batch-size limiter might otherwise split
// them - see EncodeFills.
type Fill struct {
	AggressorID    string
	RestingID      string
	AggressorParty string
	RestingParty   string
	Price          int64
	Qty            int64
}

// layout: [type:1][len(orderID):2][orderID][len(participant):2][participant][side:1][price:8][qty:8]
func EncodeNewOrder(o NewOrder) []byte {
	buf := make([]byte, 0, 1+2+len(o.OrderID)+2+len(o.Participant)+1+8+8)
	buf = append(buf, byte(MessageNewOrder))
	buf = appendString(buf, o.OrderID)
	buf = appendString(buf, o.Participant)
	buf = append(buf, o.Side)
	buf = appendInt64(buf, o.Price)
	buf = appendInt64(buf, o.Qty)
	return buf
}

// layout: [type:1][len(orderID):2][orderID]
func EncodeCancel(c Cancel) []byte {
	buf := make([]byte, 0, 1+2+len(c.OrderID))
	buf = append(buf, byte(MessageCancel))
	buf = appendString(buf, c.OrderID)
	return buf
}

// DecodeMessageType peeks at the leading type byte without decoding the
// rest of the message, so a caller can dispatch to DecodeNewOrder or
// DecodeCancel.
func DecodeMessageType(buf []byte) (MessageType, error) {
	if len(buf) < 1 {
		return 0, ErrShortBuffer
	}
	return MessageType(buf[0]), nil
}

func DecodeNewOrder(buf []byte) (NewOrder, error) {
	if len(buf) < 1 || MessageType(buf[0]) != MessageNewOrder {
		return NewOrder{}, ErrUnknownMessageType
	}
	rest := buf[1:]
	orderID, rest, err := readString(rest)
	if err != nil {
		return NewOrder{}, err
	}
	participant, rest, err := readString(rest)
	if err != nil {
		return NewOrder{}, err
	}
	if len(rest) < 1+8+8 {
		return NewOrder{}, ErrShortBuffer
	}
	side := rest[0]
	rest = rest[1:]
	price := int64(binary.BigEndian.Uint64(rest))
	rest = rest[8:]
	qty := int64(binary.BigEndian.Uint64(rest))
	return NewOrder{
		OrderID:     orderID,
		Participant: participant,
		Side:        side,
		Price:       price,
		Qty:         qty,
	}, nil
}

func DecodeCancel(buf []byte) (Cancel, error) {
	if len(buf) < 1 || MessageType(buf[0]) != MessageCancel {
		return Cancel{}, ErrUnknownMessageType
	}
	orderID, _, err := readString(buf[1:])
	if err != nil {
		return Cancel{}, err
	}
	return Cancel{OrderID: orderID}, nil
}

// layout: [count:2] then, per fill: [len(aggressorID):2][aggressorID][len(restingID):2][restingID][len(aggressorParty):2][aggressorParty][len(restingParty):2][restingParty][price:8][qty:8]
func EncodeFills(fills []Fill) []byte {
	buf := make([]byte, 2, 64*len(fills)+2)
	binary.BigEndian.PutUint16(buf, uint16(len(fills)))
	for _, f := range fills {
		buf = appendString(buf, f.AggressorID)
		buf = appendString(buf, f.RestingID)
		buf = appendString(buf, f.AggressorParty)
		buf = appendString(buf, f.RestingParty)
		buf = appendInt64(buf, f.Price)
		buf = appendInt64(buf, f.Qty)
	}
	return buf
}

func DecodeFills(buf []byte) ([]Fill, error) {
	if len(buf) < 2 {
		return nil, ErrShortBuffer
	}
	count := binary.BigEndian.Uint16(buf)
	rest := buf[2:]
	fills := make([]Fill, 0, count)
	for i := uint16(0); i < count; i++ {
		var f Fill
		var err error
		f.AggressorID, rest, err = readString(rest)
		if err != nil {
			return nil, err
		}
		f.RestingID, rest, err = readString(rest)
		if err != nil {
			return nil, err
		}
		f.AggressorParty, rest, err = readString(rest)
		if err != nil {
			return nil, err
		}
		f.RestingParty, rest, err = readString(rest)
		if err != nil {
			return nil, err
		}
		if len(rest) < 16 {
			return nil, ErrShortBuffer
		}
		f.Price = int64(binary.BigEndian.Uint64(rest))
		rest = rest[8:]
		f.Qty = int64(binary.BigEndian.Uint64(rest))
		rest = rest[8:]
		fills = append(fills, f)
	}
	return fills, nil
}

func appendString(buf []byte, s string) []byte {
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(s)))
	return append(buf, s...)
}

func appendInt64(buf []byte, v int64) []byte {
	return binary.BigEndian.AppendUint64(buf, uint64(v))
}

func readString(buf []byte) (string, []byte, error) {
	if len(buf) < 2 {
		return "", nil, ErrShortBuffer
	}
	n := int(binary.BigEndian.Uint16(buf))
	buf = buf[2:]
	if len(buf) < n {
		return "", nil, ErrShortBuffer
	}
	return string(buf[:n]), buf[n:], nil
}
