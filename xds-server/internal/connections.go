package xdsserver

import (
	"context"
	"sort"
	"sync"

	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/stats"
)

type clientConnections struct {
	NodeID               string `json:"node_id"`
	ActiveTCPConnections int    `json:"xds_tcp_connections_active"`
	ActiveStreams        int    `json:"active_ads_streams"`
	OpenedStreams        uint64 `json:"ads_streams_opened"`
	ClosedStreams        uint64 `json:"ads_streams_closed"`
	Requests             uint64 `json:"discovery_requests"`
	ACKs                 uint64 `json:"acks"`
	NACKs                uint64 `json:"nacks"`
}

type connectionSnapshot struct {
	TCPConnectionsActive int                 `json:"xds_tcp_connections_active"`
	TCPConnectionsOpened uint64              `json:"xds_tcp_connections_opened"`
	TCPConnectionsClosed uint64              `json:"xds_tcp_connections_closed"`
	Clients              []clientConnections `json:"clients"`
}

type connectionTracker struct {
	mu sync.Mutex

	streams        map[int64]string
	streamPeers    map[int64]string
	clients        map[string]*clientConnections
	remoteNode     map[string]string
	activeByRemote map[string]int
	tcpActive      int
	tcpOpened      uint64
	tcpClosed      uint64
}

func newConnectionTracker() *connectionTracker {
	return &connectionTracker{
		streams:        make(map[int64]string),
		streamPeers:    make(map[int64]string),
		clients:        make(map[string]*clientConnections),
		remoteNode:     make(map[string]string),
		activeByRemote: make(map[string]int),
	}
}

func (t *connectionTracker) client(nodeID string) *clientConnections {
	client := t.clients[nodeID]
	if client == nil {
		client = &clientConnections{NodeID: nodeID}
		t.clients[nodeID] = client
	}
	return client
}

func (t *connectionTracker) trackStream(streamID int64, remote string) {
	t.mu.Lock()
	t.streamPeers[streamID] = remote
	t.mu.Unlock()
}

func (t *connectionTracker) request(streamID int64, node *core.Node, nonce string, rejected bool) string {
	t.mu.Lock()
	defer t.mu.Unlock()
	nodeID := t.streams[streamID]
	if node != nil && node.GetId() != "" {
		nodeID = node.GetId()
	}
	if nodeID == "" {
		return ""
	}
	client := t.client(nodeID)
	if remote := t.streamPeers[streamID]; remote != "" && t.remoteNode[remote] == "" {
		t.remoteNode[remote] = nodeID
		client.ActiveTCPConnections += t.activeByRemote[remote]
	}
	if _, exists := t.streams[streamID]; !exists {
		t.streams[streamID] = nodeID
		client.OpenedStreams++
		client.ActiveStreams++
	}
	client.Requests++
	if nonce != "" {
		if rejected {
			client.NACKs++
		} else {
			client.ACKs++
		}
	}
	return nodeID
}

func (t *connectionTracker) closeStream(streamID int64, node *core.Node) string {
	t.mu.Lock()
	defer t.mu.Unlock()
	nodeID, tracked := t.streams[streamID]
	if !tracked {
		return ""
	}
	if nodeID == "" && node != nil {
		nodeID = node.GetId()
	}
	delete(t.streams, streamID)
	delete(t.streamPeers, streamID)
	if nodeID != "" {
		client := t.client(nodeID)
		if client.ActiveStreams > 0 {
			client.ActiveStreams--
		}
		client.ClosedStreams++
	}
	return nodeID
}

func (t *connectionTracker) snapshot() connectionSnapshot {
	t.mu.Lock()
	defer t.mu.Unlock()
	result := connectionSnapshot{
		TCPConnectionsActive: t.tcpActive,
		TCPConnectionsOpened: t.tcpOpened,
		TCPConnectionsClosed: t.tcpClosed,
		Clients:              make([]clientConnections, 0, len(t.clients)),
	}
	for _, client := range t.clients {
		result.Clients = append(result.Clients, *client)
	}
	sort.Slice(result.Clients, func(i, j int) bool { return result.Clients[i].NodeID < result.Clients[j].NodeID })
	return result
}

func (t *connectionTracker) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}

func (*connectionTracker) HandleRPC(context.Context, stats.RPCStats) {}

type remoteAddressContextKey struct{}

func (t *connectionTracker) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	remote := ""
	if info.RemoteAddr != nil {
		remote = info.RemoteAddr.String()
	}
	return context.WithValue(ctx, remoteAddressContextKey{}, remote)
}

func (t *connectionTracker) HandleConn(ctx context.Context, event stats.ConnStats) {
	remote, _ := ctx.Value(remoteAddressContextKey{}).(string)
	t.mu.Lock()
	switch event.(type) {
	case *stats.ConnBegin:
		t.tcpActive++
		t.tcpOpened++
		t.activeByRemote[remote]++
		if nodeID := t.remoteNode[remote]; nodeID != "" {
			t.client(nodeID).ActiveTCPConnections++
		}
	case *stats.ConnEnd:
		if t.tcpActive > 0 {
			t.tcpActive--
		}
		t.tcpClosed++
		if t.activeByRemote[remote] <= 1 {
			delete(t.activeByRemote, remote)
		} else {
			t.activeByRemote[remote]--
		}
		if nodeID := t.remoteNode[remote]; nodeID != "" {
			client := t.client(nodeID)
			if client.ActiveTCPConnections > 0 {
				client.ActiveTCPConnections--
			}
			delete(t.remoteNode, remote)
		}
	}
	t.mu.Unlock()
}

func (t *connectionTracker) discoveryCallbacks() *discoveryCallbacks {
	return &discoveryCallbacks{tracker: t}
}

type discoveryCallbacks struct {
	tracker *connectionTracker
}

func (c *discoveryCallbacks) onOpen(streamID int64, ctx context.Context) {
	remote := ""
	if peerInfo, ok := peer.FromContext(ctx); ok && peerInfo.Addr != nil {
		remote = peerInfo.Addr.String()
	}
	c.tracker.trackStream(streamID, remote)
}

func (c *discoveryCallbacks) onRequest(streamID int64, node *core.Node, typeURL, version, nonce string, rejected bool) {
	nodeID := c.tracker.request(streamID, node, nonce, rejected)
	logDiscoveryRequest(nodeID, typeURL, version, nonce, rejected)
}

func (c *discoveryCallbacks) onClose(streamID int64, node *core.Node) {
	logStreamClosed(c.tracker.closeStream(streamID, node))
}

var _ stats.Handler = (*connectionTracker)(nil)
