package main

// murmur2 is Kafka's murmur2 (seed 0x9747b28c), returned as the unsigned 32
// bits. The broker routes a transform's keyed write to
// murmur2(key) % partition_count on those unsigned bits
// (src/v/transform/api.cc compute_output_partition) - NOT Kafka clients'
// toPositive(murmur2) % n - so a gateway must use the same rule to find the
// partition its key lands on.
func murmur2(data []byte) uint32 {
	const (
		seed uint32 = 0x9747b28c
		m    uint32 = 0x5bd1e995
		r           = 24
	)
	n := len(data)
	h := seed ^ uint32(n)
	for i := 0; i+4 <= n; i += 4 {
		k := uint32(data[i]) | uint32(data[i+1])<<8 | uint32(data[i+2])<<16 | uint32(data[i+3])<<24
		k *= m
		k ^= k >> r
		k *= m
		h *= m
		h ^= k
	}
	switch tail := data[n&^3:]; len(tail) {
	case 3:
		h ^= uint32(tail[2]) << 16
		fallthrough
	case 2:
		h ^= uint32(tail[1]) << 8
		fallthrough
	case 1:
		h ^= uint32(tail[0])
		h *= m
	}
	h ^= h >> 13
	h *= m
	h ^= h >> 15
	return h
}

// keyedPartition is the partition the broker writes gateway id's events to
// on a P-partition demux topic (the key is the id as decimal text).
func keyedPartition(id, partitions int) int32 {
	return int32(murmur2([]byte(itoa(id))) % uint32(partitions))
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	n := len(b)
	for i > 0 {
		n--
		b[n] = byte('0' + i%10)
		i /= 10
	}
	return string(b[n:])
}
