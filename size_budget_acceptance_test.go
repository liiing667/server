// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2026 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package mqtt

import (
	"net"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/mochi-mqtt/server/v2/system"
	"github.com/stretchr/testify/require"
)

// TestSizeBudgetAcceptancePrintsAllocationAndConsumedBytes is the acceptance
// print: after a legal packet round trip it reports the bytes reserved by the
// read buffer and the bytes handed to the packet consumer.
func TestSizeBudgetAcceptancePrintsAllocationAndConsumedBytes(t *testing.T) {
	cl, r, _ := newTestClient()
	defer cl.Stop(errClientStop)

	raw := packets.TPacketData[packets.Publish].Get(packets.TPublishBasic).RawBytes
	go func() {
		_, _ = r.Write(raw)
		_ = r.Close()
	}()

	fh := new(packets.FixedHeader)
	require.NoError(t, cl.ReadFixedHeader(fh))

	pk, err := cl.ReadPacket(fh)
	require.NoError(t, err)
	// remaining = 2-byte topic length prefix + topic bytes + payload bytes.
	require.Equal(t, fh.Remaining, 2+len(pk.TopicName)+len(pk.Payload))

	allocated := cl.SizeBudget.TotalAllocated()
	consumed := cl.SizeBudget.TotalConsumed()
	t.Logf("ACCEPTANCE packet budget: remaining=%d allocated_bytes=%d consumed_bytes=%d rejected=%d live=%d",
		fh.Remaining, allocated, consumed, cl.SizeBudget.Rejected(), cl.SizeBudget.Allocated())

	require.Equal(t, int64(fh.Remaining), allocated) // exactly the packet's remaining bytes were reserved
	require.Equal(t, int64(fh.Remaining), consumed)  // and all of them reached the consumer
	require.Equal(t, int64(0), cl.SizeBudget.Allocated())

	// process-wide rollup counters reflect the same numbers.
	require.Equal(t, int64(fh.Remaining), atomic.LoadInt64(&cl.ops.info.PacketBytesAllocated))
	require.Equal(t, int64(fh.Remaining), atomic.LoadInt64(&cl.ops.info.PacketBytesConsumed))
}

// TestSizeBudgetRejectsBeforeUnboundedAllocation proves an oversized packet is
// refused at fixed-header admission, before any payload-sized allocation: the
// heap delta stays far below the attacker-declared remaining length.
func TestSizeBudgetRejectsBeforeUnboundedAllocation(t *testing.T) {
	cl, r, _ := newTestClient()
	defer cl.Stop(errClientStop)
	cl.ops.options.Capabilities.MaximumPacketSize = 64
	cl.SetPacketSizeLimit(0)

	// declare a 1 MiB remaining length via the MQTT variable byte integer.
	huge := 1 << 20
	header := []byte{packets.Publish << 4}
	rem := huge
	for rem > 0 {
		b := byte(rem % 128)
		rem /= 128
		if rem > 0 {
			b |= 0x80
		}
		header = append(header, b)
	}

	var ms1, ms2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&ms1)

	go func() {
		_, _ = r.Write(header)
		// keep the pipe open briefly; the server must never try to read 1 MiB.
		time.Sleep(50 * time.Millisecond)
		_ = r.Close()
	}()

	err := cl.ReadFixedHeader(new(packets.FixedHeader))
	require.ErrorIs(t, err, packets.ErrPacketTooLarge)

	runtime.ReadMemStats(&ms2)
	heapGrew := int64(ms2.HeapAlloc) - int64(ms1.HeapAlloc)
	if heapGrew < 0 {
		heapGrew = 0
	}
	t.Logf("ACCEPTANCE oversized packet: declared=%d budget=%d heap_growth=%d rejected=%d",
		huge, cl.SizeBudget.Limit(), heapGrew, cl.SizeBudget.Rejected())

	// The rejection happened pre-allocation: heap growth must be a small fraction
	// of the declared (and never materialised) payload.
	require.Less(t, heapGrew, int64(huge)/8)
	require.Equal(t, int64(1), cl.SizeBudget.Rejected())
	require.Equal(t, int64(1), atomic.LoadInt64(&cl.ops.info.PacketBytesRejected))
}

// TestSizeBudgetLegalZeroPayload verifies zero-length remaining packets keep
// working and consume zero payload bytes.
func TestSizeBudgetLegalZeroPayload(t *testing.T) {
	for _, version := range []byte{3, 4, 5} {
		cl, r, _ := newTestClient()
		cl.Properties.ProtocolVersion = version

		go func() {
			_, _ = r.Write([]byte{packets.Pingreq << 4, 0x00})
			_ = r.Close()
		}()

		fh := new(packets.FixedHeader)
		require.NoError(t, cl.ReadFixedHeader(fh), "v%d", version)
		require.Equal(t, 0, fh.Remaining)

		_, err := cl.ReadPacket(fh)
		require.NoError(t, err, "v%d", version)
		require.Equal(t, int64(0), cl.SizeBudget.TotalAllocated(), "v%d", version)
		require.Equal(t, int64(0), cl.SizeBudget.TotalConsumed(), "v%d", version)
		cl.Stop(errClientStop)
	}
}

// TestSizeBudgetMaximumBoundaryPacket admits a packet whose whole on-wire size
// is exactly the configured budget, and rejects one byte more.
func TestSizeBudgetMaximumBoundaryPacket(t *testing.T) {
	// TPublishBasic raw size determines a tight exact-fit budget.
	raw := packets.TPacketData[packets.Publish].Get(packets.TPublishBasic).RawBytes

	t.Run("exact_fit_admitted", func(t *testing.T) {
		cl, r, _ := newTestClient()
		defer cl.Stop(errClientStop)
		cl.ops.options.Capabilities.MaximumPacketSize = uint32(len(raw))
		cl.SetPacketSizeLimit(0)

		go func() {
			_, _ = r.Write(raw)
			_ = r.Close()
		}()

		fh := new(packets.FixedHeader)
		require.NoError(t, cl.ReadFixedHeader(fh))
		_, err := cl.ReadPacket(fh)
		require.NoError(t, err)
		require.Equal(t, int64(0), cl.SizeBudget.Rejected())
	})

	t.Run("one_byte_over_rejected", func(t *testing.T) {
		cl, r, _ := newTestClient()
		defer cl.Stop(errClientStop)
		cl.ops.options.Capabilities.MaximumPacketSize = uint32(len(raw) - 1)
		cl.SetPacketSizeLimit(0)

		go func() {
			_, _ = r.Write(raw)
			_ = r.Close()
		}()

		err := cl.ReadFixedHeader(new(packets.FixedHeader))
		require.ErrorIs(t, err, packets.ErrPacketTooLarge)
		require.Equal(t, int64(1), cl.SizeBudget.Rejected())
	})
}

// TestSizeBudgetMQTT3And5ErrorCodesPreserved ensures the same sentinel error
// (0x95 packet too large) is returned regardless of protocol version, and the
// code value is unchanged.
func TestSizeBudgetMQTT3And5ErrorCodesPreserved(t *testing.T) {
	require.Equal(t, byte(0x95), packets.ErrPacketTooLarge.Code)

	for _, version := range []byte{3, 4, 5} {
		cl, r, _ := newTestClient()
		cl.Properties.ProtocolVersion = version
		cl.ops.options.Capabilities.MaximumPacketSize = 2
		cl.SetPacketSizeLimit(0)

		go func() {
			_, _ = r.Write(packets.TPacketData[packets.Publish].Get(packets.TPublishQos1Dup).RawBytes)
			_ = r.Close()
		}()

		err := cl.ReadFixedHeader(new(packets.FixedHeader))
		require.ErrorIs(t, err, packets.ErrPacketTooLarge, "v%d", version)
		code, ok := err.(packets.Code)
		require.True(t, ok, "v%d", version)
		require.Equal(t, packets.ErrPacketTooLarge.Code, code.Code, "v%d", version)
		cl.Stop(errClientStop)
	}
}

// TestSizeBudgetClientAdvertisedLimitNarrowsBudget verifies the negotiated
// client Maximum Packet Size narrows the shared budget below the server value.
func TestSizeBudgetClientAdvertisedLimitNarrowsBudget(t *testing.T) {
	cl, _, _ := newTestClient()
	defer cl.Stop(errClientStop)
	cl.ops.options.Capabilities.MaximumPacketSize = 1000

	cl.SetPacketSizeLimit(300)
	require.Equal(t, uint32(300), cl.SizeBudget.Limit())

	// zero restores the server limit rather than disabling the budget.
	cl.SetPacketSizeLimit(0)
	require.Equal(t, uint32(1000), cl.SizeBudget.Limit())

	// a larger client limit cannot relax the server budget.
	cl.SetPacketSizeLimit(5000)
	require.Equal(t, uint32(1000), cl.SizeBudget.Limit())
}

// TestSizeBudgetReadAndWriteBuffersAreClamped verifies the connection buffers
// cannot pre-allocate past the shared size budget when it is tightly set.
func TestSizeBudgetReadAndWriteBuffersAreClamped(t *testing.T) {
	r, w := net.Pipe()
	cl := newClient(w, &ops{
		info:  new(system.Info()),
		hooks: new(Hooks),
		log:   logger,
		options: &Options{
			ClientNetReadBufferSize:  4096,
			ClientNetWriteBufferSize: 4096,
			Capabilities: &Capabilities{
				MaximumPacketSize:          64,
				MaximumClientWritesPending: 3,
				TopicAliasMaximum:          10000,
			},
		},
	})
	defer cl.Stop(errClientStop)

	require.Equal(t, 64, cl.Net.bconn.Size()) // bufio.Reader reports its buffer length

	cl.Net.outbuf = cl.newWriteBuffer()
	require.NotNil(t, cl.Net.outbuf)
	require.Greater(t, cl.Net.outbuf.Cap(), 0)
	require.LessOrEqual(t, cl.Net.outbuf.Cap(), 64)
	_ = r.Close()
}

// TestSizeBudgetWriteFailureIsAttributable ensures a failed queued outbound
// write is counted and no longer silently swallowed: WriteLoop records the
// failure and rolls back the qos>0 in-flight entry.
func TestSizeBudgetWriteFailureIsAttributable(t *testing.T) {
	cl, r, _ := newTestClient()
	defer cl.Stop(errClientStop)

	pk := *packets.TPacketData[packets.Publish].Get(packets.TPublishQos1).Packet
	cl.State.Inflight.Set(pk)
	cl.State.Inflight.DecreaseSendQuota()
	cl.State.outbound <- &pk
	atomic.AddInt32(&cl.State.outboundQty, 1)

	require.NoError(t, r.Close()) // break the underlying connection

	require.Eventually(t, func() bool {
		return cl.WriteFailures() == 1
	}, time.Second, 5*time.Millisecond)

	// the qos 1 in-flight entry was rolled back and quota restored.
	_, ok := cl.State.Inflight.Get(pk.PacketID)
	require.False(t, ok)
	require.Equal(t, int32(1), atomic.LoadInt32(&cl.State.Inflight.sendQuota))
	require.Equal(t, int64(1), atomic.LoadInt64(&cl.ops.info.MessagesDropped))
}
