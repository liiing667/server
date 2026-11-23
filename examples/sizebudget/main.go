// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2026 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

// This acceptance demo drives a real broker over an in-process net.Pipe and
// prints the shared size-budget accounting: the bytes reserved by read/write
// buffers (allocated) and the bytes handed to the packet consumer (consumed),
// plus the number of packets rejected before any unbounded allocation.
package main

import (
	"bytes"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/packets"
)

func main() {
	capabilities := mqtt.NewDefaultServerCapabilities()
	capabilities.MaximumPacketSize = 128 // tight shared whole-packet budget

	server := mqtt.New(&mqtt.Options{Capabilities: capabilities})
	if err := server.AddHook(new(auth.AllowHook), nil); err != nil {
		panic(err)
	}

	clientConn, serverConn := net.Pipe()
	go func() {
		if err := server.EstablishConnection("pipe", serverConn); err != nil {
			fmt.Println("server connection ended:", err)
		}
	}()

	// 1. MQTT 5 CONNECT (zero will, zero payload) so the negotiated budget is live.
	connect := encodeConnect()
	if _, err := clientConn.Write(connect); err != nil {
		panic(err)
	}
	readConnack(clientConn)

	// 2. A legal, in-budget PUBLISH with a small payload.
	legal := encodePublish("a/b/c", []byte("hello mochi"))
	if _, err := clientConn.Write(legal); err != nil {
		panic(err)
	}

	// 3. An oversized PUBLISH declaring a remaining length beyond the budget.
	// The broker must reject it at fixed-header admission without allocating the
	// declared payload.
	oversized := encodeOversizedFixedHeader(capabilities.MaximumPacketSize)
	if _, err := clientConn.Write(oversized); err != nil {
		panic(err)
	}

	// Give the broker a moment to process the three packets.
	time.Sleep(100 * time.Millisecond)

	fmt.Println("=== shared packet size budget acceptance ===")
	fmt.Printf("budget_limit_bytes=%d\n", capabilities.MaximumPacketSize)
	fmt.Printf("allocated_bytes=%d\n", atomic.LoadInt64(&server.Info.PacketBytesAllocated))
	fmt.Printf("consumed_bytes=%d\n", atomic.LoadInt64(&server.Info.PacketBytesConsumed))
	fmt.Printf("rejected_packets=%d\n", atomic.LoadInt64(&server.Info.PacketBytesRejected))

	_ = clientConn.Close()
	_ = server.Close()
}

func encodeConnect() []byte {
	pk := packets.Packet{
		ProtocolVersion: 5,
		FixedHeader:     packets.FixedHeader{Type: packets.Connect},
		Connect: packets.ConnectParams{
			ProtocolName:     []byte("MQTT"),
			ClientIdentifier: "size-budget-demo",
			Clean:            true,
			Keepalive:        10,
		},
	}
	var buf bytes.Buffer
	if err := pk.ConnectEncode(&buf); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func encodePublish(topic string, payload []byte) []byte {
	pk := packets.Packet{
		ProtocolVersion: 5,
		FixedHeader:     packets.FixedHeader{Type: packets.Publish},
		TopicName:       topic,
		Payload:         payload,
	}
	var buf bytes.Buffer
	if err := pk.PublishEncode(&buf); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func encodeOversizedFixedHeader(limit uint32) []byte {
	// declare a remaining length guaranteed to exceed the whole-packet budget.
	remaining := int(limit) * 4
	out := []byte{packets.Publish << 4}
	rem := remaining
	for {
		b := byte(rem % 128)
		rem /= 128
		if rem > 0 {
			b |= 0x80
		}
		out = append(out, b)
		if rem == 0 {
			break
		}
	}
	return out
}

func readConnack(c net.Conn) {
	header := make([]byte, 1)
	if _, err := c.Read(header); err != nil {
		panic(err)
	}

	// remaining length is a variable byte integer (single byte for a connack).
	lengthByte := make([]byte, 1)
	if _, err := c.Read(lengthByte); err != nil {
		panic(err)
	}
	remaining := int(lengthByte[0])
	if remaining > 0 {
		body := make([]byte, remaining)
		if _, err := c.Read(body); err != nil {
			panic(err)
		}
	}
}
