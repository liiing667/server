// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2026 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package packets

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewSizeBudgetDefaultsToMQTTMaximum(t *testing.T) {
	b := NewSizeBudget(0)
	require.Equal(t, uint32(DefaultSizeBudget), b.Limit())
	require.Equal(t, uint32(MaxPacketSize), b.Limit())
}

func TestSizeBudgetSetLimit(t *testing.T) {
	b := NewSizeBudget(128)
	require.Equal(t, uint32(128), b.Limit())

	b.SetLimit(0) // zero restores the safe MQTT maximum, never an unbounded budget
	require.Equal(t, uint32(DefaultSizeBudget), b.Limit())

	b.SetLimit(64)
	require.Equal(t, uint32(64), b.Limit())
}

func TestFixedHeaderSize(t *testing.T) {
	// header byte (1) + remaining length encoding (bu) + remaining bytes.
	require.Equal(t, 2, FixedHeaderSize(0, 1))    // zero payload packet
	require.Equal(t, 20, FixedHeaderSize(18, 1))  // 18 remaining + 1 length byte + header byte
	require.Equal(t, 270, FixedHeaderSize(267, 2))
}

func TestSizeBudgetAdmitFixedHeaderBoundary(t *testing.T) {
	// Limit of 20 admits a whole packet of exactly 20 bytes but not 21.
	b := NewSizeBudget(20)

	whole, err := b.AdmitFixedHeader(18, 1) // 1 + 1 + 18 = 20
	require.NoError(t, err)
	require.Equal(t, 20, whole)

	// legal zero payload packet is always admitted.
	whole, err = b.AdmitFixedHeader(0, 1)
	require.NoError(t, err)
	require.Equal(t, 2, whole)

	_, err = b.AdmitFixedHeader(19, 1) // 1 + 1 + 19 = 21
	require.ErrorIs(t, err, ErrPacketTooLarge)
	require.Equal(t, int64(1), b.Rejected())

	// malformed inputs.
	_, err = b.AdmitFixedHeader(-1, 1)
	require.Error(t, err)
	_, err = b.AdmitFixedHeader(0, 0)
	require.Error(t, err)
	_, err = b.AdmitFixedHeader(MaxRemainingLength+1, 4)
	require.Error(t, err)
}

func TestSizeBudgetReserveAndRelease(t *testing.T) {
	b := NewSizeBudget(10)

	require.NoError(t, b.Reserve(4))
	require.Equal(t, int64(4), b.Allocated())
	require.NoError(t, b.Reserve(6)) // exactly at the limit
	require.Equal(t, int64(10), b.Allocated())

	err := b.Reserve(1) // breaches the live budget
	require.ErrorIs(t, err, ErrPacketTooLarge)
	require.Equal(t, int64(1), b.Rejected())
	require.Equal(t, int64(10), b.Allocated()) // nothing reserved on failure

	b.Release(6)
	require.Equal(t, int64(4), b.Allocated())
	require.NoError(t, b.Reserve(1))

	// over-release clamps at zero.
	b.Release(999)
	require.Equal(t, int64(0), b.Allocated())

	// cumulative allocation accounting retains history.
	require.Equal(t, int64(11), b.TotalAllocated())
}

func TestSizeBudgetConsumed(t *testing.T) {
	b := NewSizeBudget(100)
	b.AddConsumed(30)
	b.AddConsumed(25)
	b.AddConsumed(-5) // negatives are ignored
	require.Equal(t, int64(55), b.TotalConsumed())
}

func TestSizeBudgetAdmitSizeExtraLimit(t *testing.T) {
	b := NewSizeBudget(100)

	require.NoError(t, b.AdmitSize(50, 0))   // only shared budget
	require.NoError(t, b.AdmitSize(40, 40)) // narrower client limit wins

	err := b.AdmitSize(41, 40) // client limit breached even though shared budget allows it
	require.ErrorIs(t, err, ErrPacketTooLarge)

	err = b.AdmitSize(101, 0) // shared budget breached
	require.ErrorIs(t, err, ErrPacketTooLarge)

	// a larger extra limit must not relax the shared budget.
	require.ErrorIs(t, b.AdmitSize(101, 200), ErrPacketTooLarge)
}

func TestSizeBudgetFlushDeltas(t *testing.T) {
	b := NewSizeBudget(1000)

	require.NoError(t, b.Reserve(100))
	b.AddConsumed(60)

	allocated, consumed, rejected := b.FlushDeltas()
	require.Equal(t, int64(100), allocated)
	require.Equal(t, int64(60), consumed)
	require.Equal(t, int64(0), rejected)

	// second flush only reports the new activity.
	b.Release(100)
	require.NoError(t, b.Reserve(40))
	b.AddConsumed(40)
	_ = b.AdmitSize(2000, 0) // one rejection

	allocated, consumed, rejected = b.FlushDeltas()
	require.Equal(t, int64(40), allocated)
	require.Equal(t, int64(40), consumed)
	require.Equal(t, int64(1), rejected)

	// an idle flush reports zeros.
	allocated, consumed, rejected = b.FlushDeltas()
	require.Equal(t, int64(0), allocated)
	require.Equal(t, int64(0), consumed)
	require.Equal(t, int64(0), rejected)
}

func TestSizeBudgetMaxRemainingLengthBoundary(t *testing.T) {
	// A maximum encodable packet (remaining 268435455, 4 length bytes + header)
	// is admitted by the default budget.
	b := NewSizeBudget(0)
	whole, err := b.AdmitFixedHeader(MaxRemainingLength, 4)
	require.NoError(t, err)
	require.Equal(t, MaxRemainingLength+5, whole)
}
