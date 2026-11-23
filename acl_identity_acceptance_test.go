// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2023 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package mqtt

import (
	"io"
	"log/slog"
	"net"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mochi-mqtt/server/v2/packets"
)

var uid34711Logger = slog.New(slog.NewTextHandler(io.Discard, nil))

// uid34711AcceptanceConn accepts everything written to it, so deliveries can be
// observed without a reader on the other end.
type uid34711AcceptanceConn struct {
	mu     sync.Mutex
	writes [][]byte
}

func (c *uid34711AcceptanceConn) Read([]byte) (int, error) { return 0, io.EOF }
func (c *uid34711AcceptanceConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	c.writes = append(c.writes, append([]byte(nil), p...))
	c.mu.Unlock()
	return len(p), nil
}
func (c *uid34711AcceptanceConn) Close() error                     { return nil }
func (c *uid34711AcceptanceConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *uid34711AcceptanceConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (c *uid34711AcceptanceConn) SetDeadline(time.Time) error      { return nil }
func (c *uid34711AcceptanceConn) SetReadDeadline(time.Time) error  { return nil }
func (c *uid34711AcceptanceConn) SetWriteDeadline(time.Time) error { return nil }
func (c *uid34711AcceptanceConn) writeCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.writes)
}

type uid34711ACLCall struct {
	client string
	topic  string
	write  bool
	allow  bool
}

// uid34711RecordingACL records every decision so the acceptance test can
// compare what each subscriber received with what the ACL actually reported.
// Reads are allowed for public topics, and for secret topics only to the
// explicitly allowed identities. Writes follow the same rule.
type uid34711RecordingACL struct {
	HookBase
	mu    sync.Mutex
	calls []uid34711ACLCall
}

func (h *uid34711RecordingACL) ID() string { return "uid34711-recording-acl" }
func (h *uid34711RecordingACL) Provides(b byte) bool {
	return b == OnACLCheck
}
func (h *uid34711RecordingACL) OnACLCheck(cl *Client, topic string, write bool) bool {
	allow := true
	if strings.Contains(topic, "secret") {
		allow = cl.ID == "zzz-alice" || cl.ID == "carol"
	}

	h.mu.Lock()
	h.calls = append(h.calls, uid34711ACLCall{client: cl.ID, topic: topic, write: write, allow: allow})
	h.mu.Unlock()
	return allow
}
func (h *uid34711RecordingACL) snapshot() []uid34711ACLCall {
	h.mu.Lock()
	defer h.mu.Unlock()
	calls := make([]uid34711ACLCall, len(h.calls))
	copy(calls, h.calls)
	return calls
}

type uid34711RejectPublish struct{ HookBase }

func (h *uid34711RejectPublish) ID() string { return "uid34711-reject-publish" }
func (h *uid34711RejectPublish) Provides(b byte) bool {
	return b == OnPublish
}
func (h *uid34711RejectPublish) OnPublish(cl *Client, pk packets.Packet) (packets.Packet, error) {
	return pk, packets.ErrNotAuthorized
}

func uid34711Client(t *testing.T, s *Server, id string, conn *uid34711AcceptanceConn) *Client {
	t.Helper()
	cl := s.NewClient(conn, "acceptance", id, false)
	cl.Properties.ProtocolVersion = 5
	cl.State.Inflight.ResetReceiveQuota(10)
	s.Clients.Add(cl)
	return cl
}

func uid34711Drain(cl *Client) []packets.Packet {
	var out []packets.Packet
	for {
		select {
		case pk := <-cl.State.outbound:
			out = append(out, *pk)
		default:
			return out
		}
	}
}

func uid34711Topics(pks []packets.Packet) []string {
	topics := make([]string, 0, len(pks))
	for _, pk := range pks {
		topics = append(topics, pk.TopicName)
	}
	sort.Strings(topics)
	return topics
}

// TestACLIdentityAcceptance checks, per subscriber, that everything received
// was reported as allowed by the ACL under that same subscriber identity, and
// that a refusal never removes a retained message.
func TestACLIdentityAcceptance(t *testing.T) {
	s := New(&Options{Logger: uid34711Logger})
	defer s.Close()
	acl := new(uid34711RecordingACL)
	if err := s.AddHook(acl, nil); err != nil {
		t.Fatal(err)
	}

	// One public and one restricted retained message. A parent filter matches
	// both, so the read decision must be made per actual topic.
	if r := s.Topics.RetainMessage(packets.Packet{
		TopicName:   "site/public/a",
		Payload:     []byte("public"),
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Retain: true},
	}); r != 1 {
		t.Fatalf("retain public: %d", r)
	}
	if r := s.Topics.RetainMessage(packets.Packet{
		TopicName:   "site/secret/b",
		Payload:     []byte("secret"),
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Retain: true},
	}); r != 1 {
		t.Fatalf("retain secret: %d", r)
	}

	readerConn := new(uid34711AcceptanceConn)
	reader := uid34711Client(t, s, "reader", readerConn)
	s.publishRetainedToClient(reader, packets.Subscription{Filter: "site/#"}, false)
	readerTopics := uid34711Topics(uid34711Drain(reader))
	if len(readerTopics) != 1 || readerTopics[0] != "site/public/a" {
		t.Fatalf("retained parent filter delivered %v, want [site/public/a]", readerTopics)
	}
	if got := len(s.Topics.Messages("site/secret/b")); got != 1 {
		t.Fatalf("denied retained message was removed: %d left", got)
	}

	// Several filters must still be honoured, each one applying the same
	// per-topic read check.
	multiConn := new(uid34711AcceptanceConn)
	multi := uid34711Client(t, s, "reader2", multiConn)
	s.publishRetainedToClient(multi, packets.Subscription{Filter: "site/public/+"}, false)
	first := uid34711Topics(uid34711Drain(multi))
	s.publishRetainedToClient(multi, packets.Subscription{Filter: "site/#"}, false)
	second := uid34711Topics(uid34711Drain(multi))
	if len(first) != 1 || first[0] != "site/public/a" {
		t.Fatalf("first filter delivered %v", first)
	}
	if len(second) != 1 || second[0] != "site/public/a" {
		t.Fatalf("second filter delivered %v", second)
	}

	// A shared group must not choose a member which cannot read the real
	// topic: the candidate is skipped and another member gets the message.
	deniedConn := new(uid34711AcceptanceConn)
	allowedConn := new(uid34711AcceptanceConn)
	carolConn := new(uid34711AcceptanceConn)
	denied := uid34711Client(t, s, "aaa-bob", deniedConn) // sorts first
	allowed := uid34711Client(t, s, "zzz-alice", allowedConn)
	carol := uid34711Client(t, s, "carol", carolConn)
	s.Topics.Subscribe(denied.ID, packets.Subscription{Filter: "$share/g/secret/#", Qos: 0})
	s.Topics.Subscribe(allowed.ID, packets.Subscription{Filter: "$share/g/secret/#", Qos: 0})
	s.Topics.Subscribe(carol.ID, packets.Subscription{Filter: "$share/h/secret/#", Qos: 0})
	s.publishToSubscribers(packets.Packet{
		TopicName:   "secret/x",
		Payload:     []byte("secret"),
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 0},
	})

	deniedTopics := uid34711Topics(uid34711Drain(denied))
	allowedTopics := uid34711Topics(uid34711Drain(allowed))
	carolTopics := uid34711Topics(uid34711Drain(carol))
	if len(deniedTopics) != 0 {
		t.Fatalf("restricted shared member received %v", deniedTopics)
	}
	if len(allowedTopics) != 1 || allowedTopics[0] != "secret/x" {
		t.Fatalf("allowed shared member received %v", allowedTopics)
	}
	if len(carolTopics) != 1 || carolTopics[0] != "secret/x" {
		t.Fatalf("other shared group received %v", carolTopics)
	}

	// A refused empty retained publish is a refusal, not a delete.
	publishConn := new(uid34711AcceptanceConn)
	publisher := uid34711Client(t, s, "publisher", publishConn)
	if err := s.processPublish(publisher, packets.Packet{
		TopicName:   "site/secret/b",
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Retain: true, Qos: 0},
	}); err != nil {
		t.Fatalf("denied publish returned %v", err)
	}
	if got := len(s.Topics.Messages("site/secret/b")); got != 1 {
		t.Fatalf("denied retained delete removed retained message: %d left", got)
	}

	if err := s.AddHook(new(uid34711RejectPublish), nil); err != nil {
		t.Fatal(err)
	}
	if err := s.processPublish(publisher, packets.Packet{
		TopicName:   "site/public/a",
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Retain: true, Qos: 0},
	}); err != nil {
		t.Fatalf("refused publish returned %v", err)
	}
	if got := len(s.Topics.Messages("site/public/a")); got != 1 {
		t.Fatalf("refused retained delete removed retained message: %d left", got)
	}

	// Retained admission uses the real topic too. A publish whose topic is
	// supplied by an alias must be authorized at the resolved topic, and a
	// denied alias must not be remembered.
	aliasClient := uid34711Client(t, s, "alias", new(uid34711AcceptanceConn))
	aliasClient.State.TopicAliases.Inbound.Set(1, "site/secret/alias")
	if err := s.processPublish(aliasClient, packets.Packet{
		Properties:  packets.Properties{TopicAliasFlag: true, TopicAlias: 1},
		Payload:     []byte("hidden"),
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Retain: true},
	}); err != nil {
		t.Fatalf("alias publish returned %v", err)
	}
	if got := len(s.Topics.Messages("site/secret/alias")); got != 0 {
		t.Fatalf("denied alias retained %d message(s)", got)
	}
	if err := s.processPublish(aliasClient, packets.Packet{
		TopicName:   "site/secret/new",
		Properties:  packets.Properties{TopicAliasFlag: true, TopicAlias: 2},
		Payload:     []byte("hidden"),
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Retain: true},
	}); err != nil {
		t.Fatalf("alias publish returned %v", err)
	}
	if _, ok := aliasClient.State.TopicAliases.Inbound.Get(2); ok {
		t.Fatal("denied publish registered an alias")
	}
	var checkedAliasTopic, checkedNewTopic bool
	for _, c := range acl.snapshot() {
		if c.client == "alias" && c.write && !c.allow {
			checkedAliasTopic = checkedAliasTopic || c.topic == "site/secret/alias"
			checkedNewTopic = checkedNewTopic || c.topic == "site/secret/new"
		}
	}
	if !checkedAliasTopic || !checkedNewTopic {
		t.Fatalf("write ACL did not see the resolved topic: alias=%v new=%v", checkedAliasTopic, checkedNewTopic)
	}

	// A retry is a delivery under the current identity. A queued publish for
	// a topic this identity cannot read must be dropped, not resent.
	retryConn := new(uid34711AcceptanceConn)
	retry := uid34711Client(t, s, "retry", retryConn)
	retry.State.Inflight.Set(packets.Packet{
		PacketID:    1,
		TopicName:   "site/secret/retry",
		Payload:     []byte("secret"),
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 0},
	})
	if err := retry.ResendInflightMessages(true); err != nil {
		t.Fatal(err)
	}
	if retry.State.Inflight.Len() != 0 {
		t.Fatal("denied retry remained inflight")
	}
	if retryConn.writeCount() != 0 {
		t.Fatal("denied retry was written")
	}

	// An authorized retry still goes out.
	retryOKConn := new(uid34711AcceptanceConn)
	retryOK := uid34711Client(t, s, "retry-ok", retryOKConn)
	retryOK.State.Inflight.Set(packets.Packet{
		PacketID:    2,
		TopicName:   "site/public/retry",
		Payload:     []byte("public"),
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 0},
	})
	if err := retryOK.ResendInflightMessages(true); err != nil {
		t.Fatal(err)
	}
	if retryOKConn.writeCount() != 1 {
		t.Fatalf("allowed retry wrote %d packets", retryOKConn.writeCount())
	}

	// Per-subscriber report: every received topic must have an allow decision
	// recorded for that same subscriber.
	calls := acl.snapshot()
	allowedPairs := map[string]bool{}
	for _, c := range calls {
		if !c.write && c.allow {
			allowedPairs[c.client+"\x00"+c.topic] = true
		}
	}
	delivered := map[string][]string{
		"reader":    readerTopics,
		"reader2":   append(first, second...),
		"zzz-alice": allowedTopics,
		"carol":     carolTopics,
	}
	for client, topics := range delivered {
		for _, topic := range topics {
			if !allowedPairs[client+"\x00"+topic] {
				t.Fatalf("client %s received %s without an allow decision", client, topic)
			}
		}
	}

	for _, client := range []string{"reader", "reader2", "aaa-bob", "zzz-alice", "carol", "retry", "retry-ok", "alias"} {
		got := make([]string, 0)
		for _, c := range calls {
			if c.client != client {
				continue
			}
			verdict := "deny"
			if c.allow {
				verdict = "allow"
			}
			mode := "read"
			if c.write {
				mode = "write"
			}
			got = append(got, mode+" "+c.topic+":"+verdict)
		}
		sort.Strings(got)
		t.Logf("acl[%s]=%v", client, got)
	}
}
