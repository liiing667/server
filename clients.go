// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2023 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package mqtt

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/xid"

	"github.com/mochi-mqtt/server/v2/packets"
)

const (
	defaultKeepalive             uint16 = 10 // the default connection keepalive value in seconds.
	defaultClientProtocolVersion byte   = 4  // the default mqtt protocol version of connecting clients (if somehow unspecified).
	minimumKeepalive             uint16 = 5  // the minimum recommended keepalive - values under with display a warning.
)

var (
	ErrMinimumKeepalive = errors.New("client keepalive is below minimum recommended value and may exhibit connection instability")
)

// ReadFn is the function signature for the function used for reading and processing new packets.
type ReadFn func(*Client, packets.Packet) error

// Clients contains a map of the clients known by the broker.
type Clients struct {
	internal map[string]*Client // clients known by the broker, keyed on client id.
	sync.RWMutex
}

// NewClients returns an instance of Clients.
func NewClients() *Clients {
	return &Clients{
		internal: make(map[string]*Client),
	}
}

// Add adds a new client to the clients map, keyed on client id.
func (cl *Clients) Add(val *Client) {
	cl.Lock()
	defer cl.Unlock()
	cl.internal[val.ID] = val
}

// GetAll returns all the clients.
func (cl *Clients) GetAll() map[string]*Client {
	cl.RLock()
	defer cl.RUnlock()
	m := map[string]*Client{}
	for k, v := range cl.internal {
		m[k] = v
	}
	return m
}

// Get returns the value of a client if it exists.
func (cl *Clients) Get(id string) (*Client, bool) {
	cl.RLock()
	defer cl.RUnlock()
	val, ok := cl.internal[id]
	return val, ok
}

// Len returns the length of the clients map.
func (cl *Clients) Len() int {
	cl.RLock()
	defer cl.RUnlock()
	val := len(cl.internal)
	return val
}

// Delete removes a client from the internal map.
func (cl *Clients) Delete(id string) {
	cl.Lock()
	defer cl.Unlock()
	delete(cl.internal, id)
}

// GetByListener returns clients matching a listener id.
func (cl *Clients) GetByListener(id string) []*Client {
	cl.RLock()
	defer cl.RUnlock()
	clients := make([]*Client, 0, cl.Len())
	for _, client := range cl.internal {
		if client.Net.Listener == id && !client.Closed() {
			clients = append(clients, client)
		}
	}
	return clients
}

// Client contains information about a client known by the broker.
type Client struct {
	Properties ClientProperties    // client properties
	State      ClientState         // the operational state of the client.
	Net        ClientConnection    // network connection state of the client
	SizeBudget *packets.SizeBudget // shared byte budget for fixed header admission, remaining length, read/write buffers and the packet consumer
	ID         string              // the client id.
	ops        *ops                // ops provides a reference to server ops.
	sync.RWMutex                   // mutex
}

// ClientConnection contains the connection transport and metadata for the client.
type ClientConnection struct {
	Conn     net.Conn      // the net.Conn used to establish the connection
	bconn    *bufio.Reader // a buffered net.Conn for reading packets
	outbuf   *bytes.Buffer // a buffer for writing packets
	Remote   string        // the remote address of the client
	Listener string        // listener id of the client
	Inline   bool          // if true, the client is the built-in 'inline' embedded client
}

// ClientProperties contains the properties which define the client behaviour.
type ClientProperties struct {
	Props           packets.Properties
	Will            Will
	Username        []byte
	ProtocolVersion byte
	Clean           bool
}

// Will contains the last will and testament details for a client connection.
type Will struct {
	Payload           []byte                 // -
	User              []packets.UserProperty // -
	TopicName         string                 // -
	Flag              uint32                 // 0,1
	WillDelayInterval uint32                 // -
	Qos               byte                   // -
	Retain            bool                   // -
}

// ClientState tracks the state of the client.
type ClientState struct {
	TopicAliases    TopicAliases         // a map of topic aliases
	stopCause       atomic.Value         // reason for stopping
	Inflight        *Inflight            // a map of in-flight qos messages
	Subscriptions   *Subscriptions       // a map of the subscription filters a client maintains
	disconnected    int64                // the time the client disconnected in unix time, for calculating expiry
	outbound        chan *packets.Packet // queue for pending outbound packets
	endOnce         sync.Once            // only end once
	isTakenOver     atomic.Bool          // used to identify orphaned clients
	packetID        uint32               // the current highest packetID
	open            context.Context      // indicate that the client is open for packet exchange
	cancelOpen      context.CancelFunc   // cancel function for open context
	outboundQty     int32                // number of messages currently in the outbound queue
	outbufReserved  int64                // size-budget bytes currently held by the coalescing outbound write buffer
	writeFailures   int64                // number of outbound packets that failed to be written to the connection
	Keepalive       uint16               // the number of seconds the connection can wait
	ServerKeepalive bool                 // keepalive was set by the server
}

// newClient returns a new instance of Client. This is almost exclusively used by Server
// for creating new clients, but it lives here because it's not dependent.
func newClient(c net.Conn, o *ops) *Client {
	ctx, cancel := context.WithCancel(context.Background())
	cl := &Client{
		State: ClientState{
			Inflight:      NewInflights(),
			Subscriptions: NewSubscriptions(),
			TopicAliases:  NewTopicAliases(o.options.Capabilities.TopicAliasMaximum),
			open:          ctx,
			cancelOpen:    cancel,
			Keepalive:     defaultKeepalive,
			outbound:      make(chan *packets.Packet, o.options.Capabilities.MaximumClientWritesPending),
		},
		Properties: ClientProperties{
			ProtocolVersion: defaultClientProtocolVersion, // default protocol version
		},
		SizeBudget: packets.NewSizeBudget(budgetLimit(o)),
		ops: o,
	}

	if c != nil {
		readBufferSize := o.options.ClientNetReadBufferSize
		if max := int(cl.SizeBudget.Limit()); readBufferSize > max {
			readBufferSize = max // the read buffer itself is bounded by the shared size budget
		}
		if readBufferSize < 1 {
			readBufferSize = 1
		}
		cl.Net = ClientConnection{
			Conn:   c,
			bconn:  bufio.NewReaderSize(c, readBufferSize),
			Remote: c.RemoteAddr().String(),
		}
	}

	return cl
}

// budgetLimit resolves the shared whole-packet size budget for a new connection.
// A configured server capability takes precedence; otherwise the MQTT maximum
// encodable packet size is used so client input can never drive an unbounded
// allocation.
func budgetLimit(o *ops) uint32 {
	if o != nil && o.options != nil && o.options.Capabilities != nil &&
		o.options.Capabilities.MaximumPacketSize > 0 {
		return o.options.Capabilities.MaximumPacketSize
	}
	return packets.DefaultSizeBudget
}

// SetPacketSizeLimit narrows (or restores) the shared size budget from the
// negotiated Maximum Packet Size. The effective limit is the smaller of the
// server capability and the client advertised limit, so fixed header admission,
// remaining length, read/write buffers and the packet consumer all share one
// budget. A zero value restores the server configured default.
func (cl *Client) SetPacketSizeLimit(clientLimit uint32) {
	limit := budgetLimit(cl.ops)
	if clientLimit > 0 && clientLimit < limit {
		limit = clientLimit
	}
	cl.SizeBudget.SetLimit(limit)
}

// WriteFailures returns the number of queued outbound packets which could not be
// written to the underlying connection.
func (cl *Client) WriteFailures() int64 {
	return atomic.LoadInt64(&cl.State.writeFailures)
}

// flushBudgetDeltas rolls the per-connection size-budget counters (allocated
// bytes, consumed bytes and rejections) into the process-wide system info once.
func (cl *Client) flushBudgetDeltas() {
	allocated, consumed, rejected := cl.SizeBudget.FlushDeltas()
	if allocated != 0 {
		atomic.AddInt64(&cl.ops.info.PacketBytesAllocated, allocated)
	}
	if consumed != 0 {
		atomic.AddInt64(&cl.ops.info.PacketBytesConsumed, consumed)
	}
	if rejected != 0 {
		atomic.AddInt64(&cl.ops.info.PacketBytesRejected, rejected)
	}
}

// WriteLoop ranges over pending outbound messages and writes them to the client connection.
func (cl *Client) WriteLoop() {
	for {
		select {
		case pk := <-cl.State.outbound:
			if err := cl.WritePacket(*pk); err != nil {
				// The message failed after it left the broker's control surface:
				// record precisely which packet failed (packet id, type and topic)
				// instead of silently swallowing the error. For qos > 0 messages the
				// in-flight entry must be rolled back so the quota is not leaked.
				atomic.AddInt64(&cl.State.writeFailures, 1)
				if pk.FixedHeader.Type == packets.Publish {
					cl.ops.hooks.OnPublishDropped(cl, *pk)
				}
				cl.ops.log.Error("failed writing outbound packet",
					"error", err,
					"client", cl.ID,
					"packet_id", pk.PacketID,
					"packet_type", packets.PacketNames[pk.FixedHeader.Type],
					"topic", pk.TopicName,
					"qos", pk.FixedHeader.Qos,
				)

				if pk.FixedHeader.Type == packets.Publish && pk.FixedHeader.Qos > 0 && pk.PacketID > 0 {
					if ok := cl.State.Inflight.Delete(pk.PacketID); ok {
						cl.ops.hooks.OnQosDropped(cl, *pk)
						atomic.AddInt64(&cl.ops.info.Inflight, -1)
						atomic.AddInt64(&cl.ops.info.MessagesDropped, 1)
					}
					cl.State.Inflight.IncreaseSendQuota()
				}
			}
			atomic.AddInt32(&cl.State.outboundQty, -1)
		case <-cl.State.open.Done():
			return
		}
	}
}

// ParseConnect parses the connect parameters and properties for a client.
func (cl *Client) ParseConnect(lid string, pk packets.Packet) {
	cl.Net.Listener = lid

	cl.Properties.ProtocolVersion = pk.ProtocolVersion
	cl.Properties.Username = pk.Connect.Username
	cl.Properties.Clean = pk.Connect.Clean
	cl.Properties.Props = pk.Properties.Copy(false)

	if cl.Properties.Props.ReceiveMaximum > cl.ops.options.Capabilities.MaximumInflight { // 3.3.4 Non-normative
		cl.Properties.Props.ReceiveMaximum = cl.ops.options.Capabilities.MaximumInflight
	}

	if pk.Connect.Keepalive <= minimumKeepalive {
		cl.ops.log.Warn(
			ErrMinimumKeepalive.Error(),
			"client", cl.ID,
			"keepalive", pk.Connect.Keepalive,
			"recommended", minimumKeepalive,
		)
	}

	cl.State.Keepalive = pk.Connect.Keepalive                                              // [MQTT-3.2.2-22]
	cl.State.Inflight.ResetReceiveQuota(int32(cl.ops.options.Capabilities.ReceiveMaximum)) // server receive max per client
	cl.State.Inflight.ResetSendQuota(int32(cl.Properties.Props.ReceiveMaximum))            // client receive max
	cl.State.TopicAliases.Outbound = NewOutboundTopicAliases(cl.Properties.Props.TopicAliasMaximum)

	// Narrow the shared size budget to the smaller of the server capability and
	// the client advertised Maximum Packet Size, so every lifecycle stage stays
	// on one budget for the lifetime of the connection.
	cl.SetPacketSizeLimit(pk.Properties.MaximumPacketSize)

	cl.ID = pk.Connect.ClientIdentifier
	if cl.ID == "" {
		cl.ID = xid.New().String() // [MQTT-3.1.3-6] [MQTT-3.1.3-7]
		cl.Properties.Props.AssignedClientID = cl.ID
	}

	if pk.Connect.WillFlag {
		cl.Properties.Will = Will{
			Qos:               pk.Connect.WillQos,
			Retain:            pk.Connect.WillRetain,
			Payload:           pk.Connect.WillPayload,
			TopicName:         pk.Connect.WillTopic,
			WillDelayInterval: pk.Connect.WillProperties.WillDelayInterval,
			User:              pk.Connect.WillProperties.User,
		}
		if pk.Properties.SessionExpiryIntervalFlag &&
			pk.Properties.SessionExpiryInterval < pk.Connect.WillProperties.WillDelayInterval {
			cl.Properties.Will.WillDelayInterval = pk.Properties.SessionExpiryInterval
		}
		if pk.Connect.WillFlag {
			cl.Properties.Will.Flag = 1 // atomic for checking
		}
	}
}

// refreshDeadline refreshes the read/write deadline for the net.Conn connection.
func (cl *Client) refreshDeadline(keepalive uint16) {
	var expiry time.Time // nil time can be used to disable deadline if keepalive = 0
	if keepalive > 0 {
		expiry = time.Now().Add(time.Duration(keepalive+(keepalive/2)) * time.Second) // [MQTT-3.1.2-22]
	}

	if cl.Net.Conn != nil {
		_ = cl.Net.Conn.SetDeadline(expiry) // [MQTT-3.1.2-22]
	}
}

// NextPacketID returns the next available (unused) packet id for the client.
// If no unused packet ids are available, an error is returned and the client
// should be disconnected.
func (cl *Client) NextPacketID() (i uint32, err error) {
	cl.Lock()
	defer cl.Unlock()

	i = atomic.LoadUint32(&cl.State.packetID)
	started := i
	overflowed := false
	for {
		if overflowed && i == started {
			return 0, packets.ErrQuotaExceeded
		}

		if i >= cl.ops.options.Capabilities.maximumPacketID {
			overflowed = true
			i = 0
			continue
		}

		i++

		if _, ok := cl.State.Inflight.Get(uint16(i)); !ok {
			atomic.StoreUint32(&cl.State.packetID, i)
			return i, nil
		}
	}
}

// ResendInflightMessages attempts to resend any pending inflight messages to connected clients.
func (cl *Client) ResendInflightMessages(force bool) error {
	if cl.State.Inflight.Len() == 0 {
		return nil
	}

	for _, tk := range cl.State.Inflight.GetAll(false) {
		if tk.FixedHeader.Type == packets.Publish {
			tk.FixedHeader.Dup = true // [MQTT-3.3.1-1] [MQTT-3.3.1-3]
		}

		cl.ops.hooks.OnQosPublish(cl, tk, tk.Created, 0)
		err := cl.WritePacket(tk)
		if err != nil {
			return err
		}

		if tk.FixedHeader.Type == packets.Puback || tk.FixedHeader.Type == packets.Pubcomp {
			if ok := cl.State.Inflight.Delete(tk.PacketID); ok {
				cl.ops.hooks.OnQosComplete(cl, tk)
				atomic.AddInt64(&cl.ops.info.Inflight, -1)
			}
		}
	}

	return nil
}

// ClearInflights deletes all inflight messages for the client, e.g. for a disconnected user with a clean session.
func (cl *Client) ClearInflights() {
	for _, tk := range cl.State.Inflight.GetAll(false) {
		if ok := cl.State.Inflight.Delete(tk.PacketID); ok {
			cl.ops.hooks.OnQosDropped(cl, tk)
			atomic.AddInt64(&cl.ops.info.Inflight, -1)
		}
	}
}

// ClearExpiredInflights deletes any inflight messages which have expired.
func (cl *Client) ClearExpiredInflights(now, maximumExpiry int64) []uint16 {
	deleted := []uint16{}
	for _, tk := range cl.State.Inflight.GetAll(false) {
		expired := tk.ProtocolVersion == 5 && tk.Expiry > 0 && tk.Expiry < now // [MQTT-3.3.2-5]

		// If the maximum message expiry interval is set (greater than 0), and the message
		// retention period exceeds the maximum expiry, the message will be forcibly removed.
		enforced := maximumExpiry > 0 && now-tk.Created > maximumExpiry

		if expired || enforced {
			if ok := cl.State.Inflight.Delete(tk.PacketID); ok {
				cl.ops.hooks.OnQosDropped(cl, tk)
				atomic.AddInt64(&cl.ops.info.Inflight, -1)
				deleted = append(deleted, tk.PacketID)
			}
		}
	}

	return deleted
}

// Read reads incoming packets from the connected client and transforms them into
// packets to be handled by the packetHandler.
func (cl *Client) Read(packetHandler ReadFn) error {
	var err error

	for {
		if cl.Closed() {
			return nil
		}

		cl.refreshDeadline(cl.State.Keepalive)
		fh := new(packets.FixedHeader)
		err = cl.ReadFixedHeader(fh)
		if err != nil {
			return err
		}

		pk, err := cl.ReadPacket(fh)
		if err != nil {
			return err
		}

		err = packetHandler(cl, pk) // Process inbound packet.
		if err != nil {
			return err
		}
	}
}

// Stop instructs the client to shut down all processing goroutines and disconnect.
func (cl *Client) Stop(err error) {
	cl.State.endOnce.Do(func() {

		if cl.Net.Conn != nil {
			_ = cl.Net.Conn.Close() // omit close error
		}

		if err != nil {
			cl.State.stopCause.Store(err)
		}

		if cl.State.cancelOpen != nil {
			cl.State.cancelOpen()
		}

		atomic.StoreInt64(&cl.State.disconnected, time.Now().Unix())
	})
}

// StopCause returns the reason the client connection was stopped, if any.
func (cl *Client) StopCause() error {
	if cl.State.stopCause.Load() == nil {
		return nil
	}
	return cl.State.stopCause.Load().(error)
}

// StopTime returns the the time the client disconnected in unix time, else zero.
func (cl *Client) StopTime() int64 {
	return atomic.LoadInt64(&cl.State.disconnected)
}

// Closed returns true if client connection is closed.
func (cl *Client) Closed() bool {
	return cl.State.open == nil || cl.State.open.Err() != nil
}

func (cl *Client) IsTakenOver() bool {
	return cl.State.isTakenOver.Load()
}

// ReadFixedHeader reads in the values of the next packet's fixed header.
func (cl *Client) ReadFixedHeader(fh *packets.FixedHeader) error {
	if cl.Net.bconn == nil {
		return ErrConnectionClosed
	}

	b, err := cl.Net.bconn.ReadByte()
	if err != nil {
		return err
	}

	err = fh.Decode(b)
	if err != nil {
		return err
	}

	var bu int
	fh.Remaining, bu, err = packets.DecodeLength(cl.Net.bconn)
	if err != nil {
		return err
	}

	// Admit the packet against the shared whole-packet size budget before any
	// payload buffer is allocated. This is the unbounded-allocation guard: the
	// remaining length above is untrusted client input.
	if _, err = cl.SizeBudget.AdmitFixedHeader(fh.Remaining, bu); err != nil {
		cl.flushBudgetDeltas()
		return err // [MQTT-3.2.2-15]
	}

	atomic.AddInt64(&cl.ops.info.BytesReceived, int64(bu+1))
	return nil
}

// ReadPacket reads the remaining buffer into an MQTT packet.
func (cl *Client) ReadPacket(fh *packets.FixedHeader) (pk packets.Packet, err error) {
	atomic.AddInt64(&cl.ops.info.PacketsReceived, 1)

	pk.ProtocolVersion = cl.Properties.ProtocolVersion // inherit client protocol version for decoding
	pk.FixedHeader = *fh

	// Reserve the remaining bytes against the shared budget before allocating.
	// ReadFixedHeader has already admitted the whole packet, so this only fails
	// if concurrent buffer usage exhausted the per-connection allowance.
	remaining := pk.FixedHeader.Remaining
	if err = cl.SizeBudget.Reserve(remaining); err != nil {
		cl.flushBudgetDeltas()
		return pk, err
	}
	defer cl.SizeBudget.Release(remaining)

	p := make([]byte, remaining)
	n, err := io.ReadFull(cl.Net.bconn, p)
	if err != nil {
		return pk, err
	}

	atomic.AddInt64(&cl.ops.info.BytesReceived, int64(n))

	// Decode directly from the reserved buffer. Decoders only slice the bytes for
	// payloads/strings, so the packet keeps a reference to this single allocation
	// rather than copying it into a second, equally large buffer.
	px := p[:n]
	switch pk.FixedHeader.Type {
	case packets.Connect:
		err = pk.ConnectDecode(px)
	case packets.Disconnect:
		err = pk.DisconnectDecode(px)
	case packets.Connack:
		err = pk.ConnackDecode(px)
	case packets.Publish:
		err = pk.PublishDecode(px)
		if err == nil {
			atomic.AddInt64(&cl.ops.info.MessagesReceived, 1)
		}
	case packets.Puback:
		err = pk.PubackDecode(px)
	case packets.Pubrec:
		err = pk.PubrecDecode(px)
	case packets.Pubrel:
		err = pk.PubrelDecode(px)
	case packets.Pubcomp:
		err = pk.PubcompDecode(px)
	case packets.Subscribe:
		err = pk.SubscribeDecode(px)
	case packets.Suback:
		err = pk.SubackDecode(px)
	case packets.Unsubscribe:
		err = pk.UnsubscribeDecode(px)
	case packets.Unsuback:
		err = pk.UnsubackDecode(px)
	case packets.Pingreq:
	case packets.Pingresp:
	case packets.Auth:
		err = pk.AuthDecode(px)
	default:
		err = fmt.Errorf("invalid packet type; %v", pk.FixedHeader.Type)
	}

	if err != nil {
		return pk, err
	}

	// The bytes have reached the final packet consumer: account for them once.
	cl.SizeBudget.AddConsumed(n)
	cl.flushBudgetDeltas()

	pk, err = cl.ops.hooks.OnPacketRead(cl, pk)
	return
}

// WritePacket encodes and writes a packet to the client.
func (cl *Client) WritePacket(pk packets.Packet) error {
	if cl.Closed() {
		return ErrConnectionClosed
	}

	if cl.Net.Conn == nil {
		return nil
	}

	if pk.Expiry > 0 {
		expiry := pk.Expiry - time.Now().Unix()
		if expiry < 1 {
			expiry = 1
		}
		pk.Properties.MessageExpiryInterval = uint32(expiry) // [MQTT-3.3.2-6]
	}

	pk.ProtocolVersion = cl.Properties.ProtocolVersion
	if pk.Mods.MaxSize == 0 { // NB we use this statement to embed client packet sizes in tests
		pk.Mods.MaxSize = cl.Properties.Props.MaximumPacketSize
	}

	if cl.Properties.Props.RequestProblemInfoFlag && cl.Properties.Props.RequestProblemInfo == 0x0 {
		pk.Mods.DisallowProblemInfo = true // [MQTT-3.1.2-29] strict, no problem info on any packet if set
	}

	if pk.FixedHeader.Type != packets.Connack || cl.Properties.Props.RequestResponseInfo == 0x1 || cl.ops.options.Capabilities.Compatibilities.AlwaysReturnResponseInfo {
		pk.Mods.AllowResponseInfo = true // [MQTT-3.1.2-28] we need to know which properties we can encode
	}

	pk = cl.ops.hooks.OnPacketEncode(cl, pk)

	var err error
	buf := new(bytes.Buffer)
	switch pk.FixedHeader.Type {
	case packets.Connect:
		err = pk.ConnectEncode(buf)
	case packets.Connack:
		err = pk.ConnackEncode(buf)
	case packets.Publish:
		err = pk.PublishEncode(buf)
	case packets.Puback:
		err = pk.PubackEncode(buf)
	case packets.Pubrec:
		err = pk.PubrecEncode(buf)
	case packets.Pubrel:
		err = pk.PubrelEncode(buf)
	case packets.Pubcomp:
		err = pk.PubcompEncode(buf)
	case packets.Subscribe:
		err = pk.SubscribeEncode(buf)
	case packets.Suback:
		err = pk.SubackEncode(buf)
	case packets.Unsubscribe:
		err = pk.UnsubscribeEncode(buf)
	case packets.Unsuback:
		err = pk.UnsubackEncode(buf)
	case packets.Pingreq:
		err = pk.PingreqEncode(buf)
	case packets.Pingresp:
		err = pk.PingrespEncode(buf)
	case packets.Disconnect:
		err = pk.DisconnectEncode(buf)
	case packets.Auth:
		err = pk.AuthEncode(buf)
	default:
		err = fmt.Errorf("%w: %v", packets.ErrNoValidPacketAvailable, pk.FixedHeader.Type)
	}
	if err != nil {
		return err
	}

	// The encoded packet is admitted against the same shared size budget used on
	// the read path, additionally narrowed by any client advertised Maximum
	// Packet Size. This keeps fixed header admission, remaining length, read
	// buffer, write buffer and packet consumer on one budget.
	if err = cl.SizeBudget.AdmitSize(buf.Len(), pk.Mods.MaxSize); err != nil {
		cl.flushBudgetDeltas()
		return err // [MQTT-3.1.2-24] [MQTT-3.1.2-25]
	}

	n, err := func() (int64, error) {
		cl.Lock()
		defer cl.Unlock()
		if len(cl.State.outbound) == 0 {
			if cl.Net.outbuf == nil {
				// Direct write: reserve only for the duration of the syscall, so
				// the live budget reflects bytes actually in flight.
				if err := cl.SizeBudget.Reserve(buf.Len()); err != nil {
					return 0, err
				}
				written, werr := buf.WriteTo(cl.Net.Conn)
				cl.SizeBudget.Release(buf.Len())
				if werr == nil {
					cl.SizeBudget.AddConsumed(int(written))
				}
				return written, werr
			}

			// first write to buffer, then flush buffer
			if err := cl.chargeWriteBuffer(buf.Len()); err != nil {
				return 0, err
			}
			n, _ := cl.Net.outbuf.Write(buf.Bytes()) // will always be successful
			flushed, err := cl.flushOutbuf()
			if err == nil {
				cl.SizeBudget.AddConsumed(flushed)
			}
			return int64(n), err
		}

		// there are more writes in the queue
		if cl.Net.outbuf == nil {
			if buf.Len() >= cl.ops.options.ClientNetWriteBufferSize {
				if err := cl.SizeBudget.Reserve(buf.Len()); err != nil {
					return 0, err
				}
				written, werr := buf.WriteTo(cl.Net.Conn)
				cl.SizeBudget.Release(buf.Len())
				if werr == nil {
					cl.SizeBudget.AddConsumed(int(written))
				}
				return written, werr
			}
			cl.Net.outbuf = cl.newWriteBuffer()
		}

		if err := cl.chargeWriteBuffer(buf.Len()); err != nil {
			return 0, err
		}
		n, _ := cl.Net.outbuf.Write(buf.Bytes()) // will always be successful
		if cl.Net.outbuf.Len() < cl.ops.options.ClientNetWriteBufferSize {
			return int64(n), nil
		}

		flushed, err := cl.flushOutbuf()
		if err == nil {
			cl.SizeBudget.AddConsumed(flushed)
		}
		return int64(n), err
	}()
	if err != nil {
		cl.flushBudgetDeltas()
		return err
	}

	atomic.AddInt64(&cl.ops.info.BytesSent, n)
	atomic.AddInt64(&cl.ops.info.PacketsSent, 1)
	if pk.FixedHeader.Type == packets.Publish {
		atomic.AddInt64(&cl.ops.info.MessagesSent, 1)
	}
	cl.flushBudgetDeltas()

	cl.ops.hooks.OnPacketSent(cl, pk, buf.Bytes())

	return err
}

// newWriteBuffer allocates the coalescing outbound write buffer. Its initial
// capacity is clamped to the shared size budget so a tiny configured packet
// limit cannot be undermined by an oversized pre-allocation.
func (cl *Client) newWriteBuffer() *bytes.Buffer {
	size := cl.ops.options.ClientNetWriteBufferSize
	if max := int(cl.SizeBudget.Limit()); size > max {
		size = max
	}
	if size < 1 {
		size = 1
	}
	return bytes.NewBuffer(make([]byte, 0, size))
}

// chargeWriteBuffer reserves n bytes for data about to be appended to the
// coalescing outbound buffer. The reservation stays live until the buffer is
// flushed to the connection, so concurrent buffered packets cannot exceed the
// shared size budget.
func (cl *Client) chargeWriteBuffer(n int) error {
	if err := cl.SizeBudget.Reserve(n); err != nil {
		return err
	}
	atomic.AddInt64(&cl.State.outbufReserved, int64(n))
	return nil
}

// flushOutbuf writes the coalescing outbound buffer to the connection and
// returns the number of bytes flushed. On success the buffer is released.
func (cl *Client) flushOutbuf() (flushed int, err error) {
	if cl.Net.outbuf == nil {
		return 0, nil
	}

	flushed = cl.Net.outbuf.Len()
	_, err = cl.Net.outbuf.WriteTo(cl.Net.Conn)
	if err == nil {
		cl.Net.outbuf = nil
	}
	// bytes.Buffer.WriteTo drains the whole buffer on success (and on a partial
	// write error the connection is unusable anyway), so release the full
	// reservation associated with the coalescing buffer.
	if reserved := atomic.SwapInt64(&cl.State.outbufReserved, 0); reserved > 0 {
		cl.SizeBudget.Release(int(reserved))
	}
	return flushed, err
}
