package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

type connectionStats struct {
	active atomic.Int64
	opened atomic.Uint64
	closed atomic.Uint64
}

func (*connectionStats) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context { return ctx }
func (*connectionStats) HandleRPC(context.Context, stats.RPCStats)                       {}
func (*connectionStats) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}
func (s *connectionStats) HandleConn(_ context.Context, event stats.ConnStats) {
	switch event.(type) {
	case *stats.ConnBegin:
		s.active.Add(1)
		s.opened.Add(1)
		slog.Info("responder_connection_open")
	case *stats.ConnEnd:
		s.active.Add(-1)
		s.closed.Add(1)
		slog.Info("responder_connection_closed")
	}
}

func main() {
	listenAddress := flag.String("listen", "127.0.0.1:50051", "responder listen address")
	name := flag.String("name", "responder", "log name for this responder process")
	flag.Parse()
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("responder", *name))
	listener, err := net.Listen("tcp", *listenAddress)
	if err != nil {
		slog.Error("responder_start_failed", "error", err)
		os.Exit(1)
	}

	connections := &connectionStats{}
	server := grpc.NewServer(
		grpc.StatsHandler(connections),
		grpc.UnaryInterceptor(logUnaryRPC),
	)
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(server, healthServer)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			slog.Error("responder_serve_failed", "error", err)
			stop()
		}
	}()
	slog.Info("responder_ready", "address", listener.Addr().String())
	<-ctx.Done()
	server.GracefulStop()
	slog.Info("responder_summary", "connections_opened", connections.opened.Load(), "connections_closed", connections.closed.Load(), "connections_active", connections.active.Load())
}

func logUnaryRPC(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	started := time.Now()
	response, err := handler(ctx, request)
	code := codes.OK
	if err != nil {
		code = status.Code(err)
	}
	slog.Info("responder_rpc", "method", info.FullMethod, "code", code.String(), "duration_ms", float64(time.Since(started).Microseconds())/1000)
	return response, err
}

var _ stats.Handler = (*connectionStats)(nil)
var _ healthpb.HealthServer = (*health.Server)(nil)
