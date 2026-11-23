// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2026 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package packets

import "sync/atomic"

const (
	// MaxRemainingLength is the largest value encodable in the MQTT fixed
	// header remaining length variable byte integer (1.5.5): 268,435,455.
	MaxRemainingLength = 268435455

	// MaxPacketSize is the largest possible whole MQTT packet on the wire: the
	// fixed header byte (1), the four-byte remaining length encoding and a
	// maximum-length remaining body, i.e. 1 + 4 + 268,435,455 = 268,435,460.
	MaxPacketSize = MaxRemainingLength + 5

	// DefaultSizeBudget is the hard, per-connection size budget applied when no
	// explicit Maximum Packet Size has been negotiated. It matches the MQTT
	// maximum encodable packet and guarantees we never perform an unbounded
	// allocation driven purely by a client supplied remaining length.
	DefaultSizeBudget = MaxPacketSize
)

// SizeBudget is the single, per-connection byte budget shared by every stage
// of the packet lifecycle: fixed header admission, remaining length decoding,
// the read buffer, the write buffer and the final packet consumer. A packet is
// rejected with ErrPacketTooLarge before any allocation sized by untrusted
// input takes place.
type SizeBudget struct {
	// limit is the inclusive maximum whole-packet size (fixed header byte(s),
	// remaining length encoding and the remaining bytes combined).
	limit atomic.Uint32

	// allocated tracks bytes currently held in read/write buffers on behalf of
	// the connection. Reserved before the underlying allocation is performed
	// and released once the bytes have been consumed or written.
	allocated atomic.Int64

	// totalAllocated accumulates every byte reservation ever admitted, even if
	// it has since been released. It backs the allocation byte accounting.
	totalAllocated atomic.Int64

	// totalConsumed accumulates every byte handed to the final packet consumer
	// (inbound decoding and outbound wire writes).
	totalConsumed atomic.Int64

	// rejected counts admissions refused because they would breach the budget.
	rejected atomic.Int64

	// lastAllocated / lastConsumed / lastRejected snapshot the cumulative
	// counters the last time FlushDeltas was called, so per-connection activity
	// can be rolled up into process-wide statistics without double counting.
	lastAllocated atomic.Int64
	lastConsumed  atomic.Int64
	lastRejected  atomic.Int64
}

// NewSizeBudget returns a size budget capped at limit. A zero limit means the
// MQTT maximum encodable packet size is used instead of an unbounded budget.
func NewSizeBudget(limit uint32) *SizeBudget {
	b := &SizeBudget{}
	b.SetLimit(limit)
	return b
}

// SetLimit replaces the inclusive whole-packet size limit. A zero limit falls
// back to the MQTT maximum encodable packet size.
func (b *SizeBudget) SetLimit(limit uint32) {
	if limit == 0 {
		limit = DefaultSizeBudget
	}
	b.limit.Store(limit)
}

// Limit returns the current inclusive whole-packet size limit.
func (b *SizeBudget) Limit() uint32 {
	return b.limit.Load()
}

// FixedHeaderSize returns the whole-packet size (in bytes) implied by a fixed
// header: the header byte itself, the remaining length encoding (lengthBytes)
// and the remaining length payload.
func FixedHeaderSize(remaining, lengthBytes int) int {
	return 1 + lengthBytes + remaining
}

// AdmitFixedHeader validates a decoded fixed header against the shared budget
// before any payload buffer is allocated. wholeSize is the complete on-wire
// packet size as returned by FixedHeaderSize. It returns ErrPacketTooLarge when
// the packet would breach the budget.
func (b *SizeBudget) AdmitFixedHeader(remaining, lengthBytes int) (wholeSize int, err error) {
	if remaining < 0 || lengthBytes < 1 || remaining > MaxRemainingLength {
		return 0, ErrMalformedVariableByteInteger
	}

	wholeSize = FixedHeaderSize(remaining, lengthBytes)
	return wholeSize, b.AdmitSize(wholeSize, 0)
}

// AdmitSize validates a known whole-packet size against the shared budget and,
// optionally, a narrower per-packet limit (e.g. a client advertised Maximum
// Packet Size). A zero extraLimit means only the shared budget applies. This is
// the single admission entry point shared by inbound fixed header handling and
// outbound encoded packets.
func (b *SizeBudget) AdmitSize(wholeSize int, extraLimit uint32) error {
	if wholeSize < 0 || wholeSize > MaxPacketSize {
		return ErrMalformedVariableByteInteger
	}

	limit := b.limit.Load()
	if extraLimit > 0 && extraLimit < limit {
		limit = extraLimit
	}

	if int64(wholeSize) > int64(limit) {
		b.rejected.Add(1)
		return ErrPacketTooLarge // [MQTT-3.2.2-15] [MQTT-3.1.2-24] [MQTT-3.1.2-25]
	}

	return nil
}

// Reserve admits n bytes against the live, per-connection allocation budget.
// It must be called before allocating a read or write buffer sized by network
// input; the reservation is released with Release once the bytes are consumed.
// ErrPacketTooLarge is returned and nothing is reserved if the reservation
// would breach the limit.
func (b *SizeBudget) Reserve(n int) error {
	if n < 0 {
		return ErrMalformedVariableByteInteger
	}

	// The sum is evaluated in 64-bit arithmetic to avoid an overflow before the
	// comparison; admitted reservations can never legitimately exceed the limit.
	if int64(b.allocated.Load())+int64(n) > int64(b.limit.Load()) {
		b.rejected.Add(1)
		return ErrPacketTooLarge
	}

	b.allocated.Add(int64(n))
	b.totalAllocated.Add(int64(n))
	return nil
}

// Release returns n previously reserved bytes to the live budget. Releasing
// more than is currently held is clamped to zero so accounting can never go
// negative due to a call-site mistake.
func (b *SizeBudget) Release(n int) {
	for {
		cur := b.allocated.Load()
		next := cur - int64(n)
		if next < 0 {
			next = 0
		}
		if b.allocated.CompareAndSwap(cur, next) {
			return
		}
	}
}

// AddConsumed records n bytes delivered to the final packet consumer.
func (b *SizeBudget) AddConsumed(n int) {
	if n > 0 {
		b.totalConsumed.Add(int64(n))
	}
}

// Allocated returns the number of bytes currently held by connection buffers.
func (b *SizeBudget) Allocated() int64 { return b.allocated.Load() }

// TotalAllocated returns the cumulative number of bytes ever reserved.
func (b *SizeBudget) TotalAllocated() int64 { return b.totalAllocated.Load() }

// TotalConsumed returns the cumulative number of bytes consumed by packet
// consumers.
func (b *SizeBudget) TotalConsumed() int64 { return b.totalConsumed.Load() }

// Rejected returns the number of admissions refused for breaching the budget.
func (b *SizeBudget) Rejected() int64 { return b.rejected.Load() }

// FlushDeltas returns the bytes allocated, bytes consumed and rejections
// recorded since the previous call. The first call reports everything accrued
// since the budget was created. Callers use these deltas to update aggregate
// server statistics exactly once.
func (b *SizeBudget) FlushDeltas() (allocated, consumed, rejected int64) {
	totalAlloc := b.totalAllocated.Load()
	totalConsumed := b.totalConsumed.Load()
	totalRejected := b.rejected.Load()

	allocated = totalAlloc - b.lastAllocated.Swap(totalAlloc)
	consumed = totalConsumed - b.lastConsumed.Swap(totalConsumed)
	rejected = totalRejected - b.lastRejected.Swap(totalRejected)
	return
}
