package xdsserver

import (
	"context"
	"log/slog"

	cluster "github.com/envoyproxy/go-control-plane/envoy/service/cluster/v3"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	endpoint "github.com/envoyproxy/go-control-plane/envoy/service/endpoint/v3"
	listener "github.com/envoyproxy/go-control-plane/envoy/service/listener/v3"
	route "github.com/envoyproxy/go-control-plane/envoy/service/route/v3"
	"github.com/envoyproxy/go-control-plane/pkg/server/v3"
	"google.golang.org/grpc"
)

func newDiscoveryServer(state *resourceStore, tracker *connectionTracker) server.Server {
	callbacks := tracker.discoveryCallbacks()
	return server.NewServer(context.Background(), &state.muxCache, server.CallbackFuncs{
		StreamOpenFunc: func(ctx context.Context, streamID int64, typeURL string) error {
			callbacks.onOpen(streamID, ctx)
			slog.Info("xds_stream_open", "stream_id", streamID, "type_url", typeURL, "mode", "sotw")
			return nil
		},
		DeltaStreamOpenFunc: func(ctx context.Context, streamID int64, typeURL string) error {
			callbacks.onOpen(streamID, ctx)
			slog.Info("xds_stream_open", "stream_id", streamID, "type_url", typeURL, "mode", "delta")
			return nil
		},
		StreamRequestFunc: func(streamID int64, req *discovery.DiscoveryRequest) error {
			callbacks.onRequest(streamID, req.GetNode(), req.GetTypeUrl(), req.GetVersionInfo(), req.GetResponseNonce(), req.GetErrorDetail() != nil)
			return nil
		},
		StreamClosedFunc: callbacks.onClose,
		StreamDeltaRequestFunc: func(streamID int64, req *discovery.DeltaDiscoveryRequest) error {
			callbacks.onRequest(streamID, req.GetNode(), req.GetTypeUrl(), "", req.GetResponseNonce(), req.GetErrorDetail() != nil)
			return nil
		},
		DeltaStreamClosedFunc: callbacks.onClose,
	})
}

func logDiscoveryRequest(nodeID, typeURL, version, nonce string, rejected bool) {
	slog.Info("xds_discovery_request", "node_id", nodeID, "type_url", typeURL, "version_info", version, "response_nonce", nonce, "nack", rejected)
}

func logStreamClosed(nodeID string) {
	slog.Info("xds_stream_closed", "node_id", nodeID)
}

func registerDiscoveryServices(grpcServer *grpc.Server, discoveryServer server.Server) {
	discovery.RegisterAggregatedDiscoveryServiceServer(grpcServer, discoveryServer)
	endpoint.RegisterEndpointDiscoveryServiceServer(grpcServer, discoveryServer)
	cluster.RegisterClusterDiscoveryServiceServer(grpcServer, discoveryServer)
	listener.RegisterListenerDiscoveryServiceServer(grpcServer, discoveryServer)
	route.RegisterRouteDiscoveryServiceServer(grpcServer, discoveryServer)
}
