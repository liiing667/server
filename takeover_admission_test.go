package mqtt

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"

	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/stretchr/testify/require"
)

// uid34709AuthHook admits only the named owner. It deliberately does not use
// the server's allow-all test hook, because the point of these tests is that a
// failed authentication must not participate in session takeover.
type uid34709AuthHook struct {
	HookBase
}

func (h *uid34709AuthHook) ID() string { return "takeover-auth" }

func (h *uid34709AuthHook) Provides(b byte) bool {
	return b == OnConnectAuthenticate || b == OnACLCheck
}

func (h *uid34709AuthHook) OnConnectAuthenticate(cl *Client, pk packets.Packet) bool {
	return string(pk.Connect.Username) == "owner"
}

func (h *uid34709AuthHook) OnACLCheck(cl *Client, topic string, write bool) bool {
	return true
}

func uid34709ConnectBytes(t *testing.T, username string, clean bool) []byte {
	t.Helper()
	pk := *packets.TPacketData[packets.Connect].Get(packets.TConnectUserPass).Packet
	pk.Connect.Username = []byte(username)
	pk.Connect.Password = []byte("password")
	pk.Connect.Clean = clean
	buf := new(bytes.Buffer)
	require.NoError(t, pk.ConnectEncode(buf))
	return buf.Bytes()
}

func uid34709ReadChunk(t *testing.T, c net.Conn, what string) []byte {
	t.Helper()
	require.NoError(t, c.SetReadDeadline(time.Now().Add(3*time.Second)))
	buf := make([]byte, 4096)
	n, err := c.Read(buf)
	require.NoError(t, err, what)
	return append([]byte(nil), buf[:n]...)
}

func uid34709ReadAll(c net.Conn) chan []byte {
	out := make(chan []byte, 1)
	go func() {
		b, _ := io.ReadAll(c)
		out <- b
	}()
	return out
}

// TestTakeoverAdmissionFailedLoginKeepsExistingSession is the security
// regression: a duplicate CONNECT for an online client id with bad credentials
// is refused, leaves the original session registered and lets it continue to
// receive publishes.
func TestTakeoverAdmissionFailedLoginKeepsExistingSession(t *testing.T) {
	s := New(&Options{Logger: logger})
	require.NoError(t, s.AddHook(new(uid34709AuthHook), nil))
	defer s.Close()

	clientID := packets.TPacketData[packets.Connect].Get(packets.TConnectUserPass).Packet.Connect.ClientIdentifier

	r1, w1 := net.Pipe()
	o1 := make(chan error, 1)
	go func() { o1 <- s.EstablishConnection("tcp", r1) }()
	go func() { _, _ = w1.Write(uid34709ConnectBytes(t, "owner", false)) }()

	// Consume the CONNACK, then wait until the session is published.
	_ = uid34709ReadChunk(t, w1, "owner connack")
	require.Eventually(t, func() bool {
		_, ok := s.Clients.Get(clientID)
		return ok
	}, 3*time.Second, 10*time.Millisecond)

	owner, ok := s.Clients.Get(clientID)
	require.True(t, ok)
	sub := packets.Subscription{Filter: "takeover/online", Qos: 0}
	owner.State.Subscriptions.Add(sub.Filter, sub)
	require.True(t, s.Topics.Subscribe(owner.ID, sub))

	// Attacker uses the same client id but cannot authenticate.
	r2, w2 := net.Pipe()
	o2 := make(chan error, 1)
	go func() { o2 <- s.EstablishConnection("tcp", r2) }()
	go func() { _, _ = w2.Write(uid34709ConnectBytes(t, "attacker", false)) }()
	attackerAck := uid34709ReadAll(w2)

	select {
	case err := <-o2:
		require.ErrorIs(t, err, packets.ErrBadUsernameOrPassword)
	case <-time.After(3 * time.Second):
		t.Fatal("failed takeover did not finish")
	}
	require.Equal(t,
		packets.TPacketData[packets.Connack].Get(packets.TConnackBadUsernamePasswordNoSession).RawBytes,
		<-attackerAck,
	)

	// The failed login must not stop, mark, or replace the owner.
	require.False(t, owner.Closed(), "failed login closed the existing connection")
	require.False(t, owner.IsTakenOver(), "failed login marked the existing session taken over")
	current, ok := s.Clients.Get(clientID)
	require.True(t, ok)
	require.Same(t, owner, current)

	// Acceptance: the original connection still receives messages.
	sender, _, _ := newTestClient()
	sender.Net.Inline = true
	sender.ID = "takeover-sender"
	s.Clients.Add(sender)
	require.NoError(t, s.InjectPacket(sender, packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish},
		TopicName:   sub.Filter,
		Payload:     []byte("still-online"),
	}))

	require.Contains(t, string(uid34709ReadChunk(t, w1, "message on existing connection")), "still-online")

	_ = w1.Close()
	_ = w2.Close()
	<-o1
}

// TestTakeoverAdmissionPreservesSessionPresentAndCleanStart verifies the
// normal MQTT 3.1.1 takeover cases remain unchanged: a resumable session
// reports Session Present, while a clean start reports no session and clears
// the previous subscriptions.
func TestTakeoverAdmissionPreservesSessionPresentAndCleanStart(t *testing.T) {
	for _, cleanStart := range []bool{false, true} {
		t.Run(map[bool]string{false: "resume", true: "clean"}[cleanStart], func(t *testing.T) {
			s := New(&Options{Logger: logger})
			require.NoError(t, s.AddHook(new(uid34709AuthHook), nil))
			defer s.Close()

			clientID := packets.TPacketData[packets.Connect].Get(packets.TConnectUserPass).Packet.Connect.ClientIdentifier

			r1, w1 := net.Pipe()
			o1 := make(chan error, 1)
			go func() { o1 <- s.EstablishConnection("tcp", r1) }()
			go func() { _, _ = w1.Write(uid34709ConnectBytes(t, "owner", false)) }()
			_ = uid34709ReadChunk(t, w1, "first connack")
			oldWire := uid34709ReadAll(w1)

			require.Eventually(t, func() bool {
				_, ok := s.Clients.Get(clientID)
				return ok
			}, 3*time.Second, 10*time.Millisecond)
			old, ok := s.Clients.Get(clientID)
			require.True(t, ok)
			sub := packets.Subscription{Filter: "takeover/old", Qos: 0}
			old.State.Subscriptions.Add(sub.Filter, sub)
			require.True(t, s.Topics.Subscribe(old.ID, sub))

			r2, w2 := net.Pipe()
			o2 := make(chan error, 1)
			go func() { o2 <- s.EstablishConnection("tcp", r2) }()
			go func() { _, _ = w2.Write(uid34709ConnectBytes(t, "owner", cleanStart)) }()
			ack := uid34709ReadChunk(t, w2, "takeover connack")
			require.GreaterOrEqual(t, len(ack), 3)
			require.Equal(t, !cleanStart, ack[2] == 1, "session present must follow clean start")

			require.Eventually(t, func() bool {
				return old.IsTakenOver() && old.Closed()
			}, 3*time.Second, 10*time.Millisecond)
			require.Empty(t, old.State.Subscriptions.GetAll())

			current, ok := s.Clients.Get(clientID)
			require.True(t, ok)
			require.NotSame(t, old, current)
			if cleanStart {
				require.Empty(t, current.State.Subscriptions.GetAll())
				require.Empty(t, s.Topics.Subscribers(sub.Filter).Subscriptions)
			} else {
				require.NotEmpty(t, current.State.Subscriptions.GetAll())
				require.NotEmpty(t, s.Topics.Subscribers(sub.Filter).Subscriptions)
			}

			_ = w1.Close()
			_ = w2.Close()
			<-o1
			<-o2
			<-oldWire
		})
	}
}
