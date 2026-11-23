// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2023 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package mqtt

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mochi-mqtt/server/v2/packets"
)

// uid34714AcceptanceConn accepts all writes, so shared deliveries can be
// observed without a reader on the other side.
type uid34714AcceptanceConn struct{}

func (c *uid34714AcceptanceConn) Read([]byte) (int, error)    { return 0, io.EOF }
func (c *uid34714AcceptanceConn) Write(p []byte) (int, error) { return len(p), nil }
func (c *uid34714AcceptanceConn) Close() error                { return nil }
func (c *uid34714AcceptanceConn) LocalAddr() net.Addr         { return &net.TCPAddr{} }
func (c *uid34714AcceptanceConn) RemoteAddr() net.Addr        { return &net.TCPAddr{} }
func (c *uid34714AcceptanceConn) SetDeadline(time.Time) error {
	return nil
}
func (c *uid34714AcceptanceConn) SetReadDeadline(time.Time) error {
	return nil
}
func (c *uid34714AcceptanceConn) SetWriteDeadline(time.Time) error {
	return nil
}

type uid34714DeliveryCounter struct {
	HookBase
	mu     sync.Mutex
	counts map[string]int
}

func (h *uid34714DeliveryCounter) ID() string { return "uid34714-delivery-counter" }
func (h *uid34714DeliveryCounter) Provides(b byte) bool {
	return b == OnPacketSent
}
func (h *uid34714DeliveryCounter) OnPacketSent(cl *Client, pk packets.Packet, _ []byte) {
	if pk.FixedHeader.Type != packets.Publish {
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if h.counts == nil {
		h.counts = map[string]int{}
	}
	h.counts[cl.ID]++
}

type uid34714AllowACL struct{ HookBase }

func (h *uid34714AllowACL) ID() string { return "uid34714-allow-acl" }
func (h *uid34714AllowACL) Provides(b byte) bool {
	return b == OnACLCheck
}
func (h *uid34714AllowACL) OnACLCheck(cl *Client, topic string, write bool) bool {
	return true
}

// TestSharedSelectionAcceptanceReport prints the final selected member and the
// delivery count for every share group. It covers several groups matching one
// publish, one group with several matching filters, a single-member group, and
// one member which belongs to two groups.
func TestSharedSelectionAcceptanceReport(t *testing.T) {
	caps := NewDefaultServerCapabilities()
	s := New(&Options{
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		Capabilities: caps,
	})

	counter := new(uid34714DeliveryCounter)
	if err := s.AddHook(counter, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.AddHook(new(uid34714AllowACL), nil); err != nil {
		t.Fatal(err)
	}

	members := map[string][]string{}
	addClient := func(id string) {
		cl := s.NewClient(new(uid34714AcceptanceConn), "test", id, false)
		cl.Properties.ProtocolVersion = 5
		s.Clients.Add(cl)
		go cl.WriteLoop()
		t.Cleanup(func() { cl.Stop(nil) })
	}
	subscribe := func(id, filter string, qos byte) {
		if !s.Topics.Subscribe(id, packets.Subscription{Filter: filter, Qos: qos}) {
			t.Fatalf("subscribe %s to %s", id, filter)
		}
		members[SharedGroupName(filter)] = append(members[SharedGroupName(filter)], id)
	}

	for _, id := range []string{"alpha-1", "alpha-2", "beta-1", "gamma-1", "gamma-2", "same-member"} {
		addClient(id)
	}

	subscribe("alpha-1", "$share/alpha/a/b", 1)
	subscribe("alpha-2", "$share/alpha/a/+", 2) // same group, another matching filter
	subscribe("beta-1", "$share/beta/a/b", 0)   // single-member group
	subscribe("gamma-1", "$share/gamma/a/b", 0)
	subscribe("gamma-2", "$share/gamma/a/+", 0) // same group, second member/filter
	subscribe("same-member", "$share/delta/a/b", 0)
	subscribe("same-member", "$share/epsilon/a/+", 0) // another group, same member

	subs := s.Topics.Subscribers("a/b")
	subs.SelectShared()
	selections := append([]SharedSelection(nil), subs.SharedSelections...)
	if len(selections) != 5 {
		t.Fatalf("selections=%d want 5", len(selections))
	}

	s.publishToSubscribers(packets.Packet{
		TopicName:   "a/b",
		Payload:     []byte("x"),
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 0},
	})

	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt64(&s.Info.MessagesSent) < int64(len(selections)) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	for _, group := range []string{"alpha", "beta", "gamma", "delta", "epsilon"} {
		selected := ""
		deliveries := 0
		for _, selection := range selections {
			if selection.Group == group {
				selected = selection.Client
				deliveries++
			}
		}
		fmt.Printf("group=%s members=%v selected=%s deliveries=%d\n", group, members[group], selected, deliveries)
		if deliveries != 1 {
			t.Fatalf("group %s deliveries=%d want 1", group, deliveries)
		}
	}

	total := atomic.LoadInt64(&s.Info.MessagesSent)
	counter.mu.Lock()
	ids := make([]string, 0, len(counter.counts))
	for id := range counter.counts {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	counts := make([]string, 0, len(ids))
	for _, id := range ids {
		counts = append(counts, fmt.Sprintf("%s:%d", id, counter.counts[id]))
	}
	counter.mu.Unlock()
	fmt.Printf("total_deliveries=%d member_deliveries=%s\n", total, counts)
	if total != int64(len(selections)) {
		t.Fatalf("total deliveries=%d want %d", total, len(selections))
	}
}
