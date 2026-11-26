package xdsserver

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// Run starts the xDS gRPC server and its snapshot admin HTTP server, then
// gracefully shuts both down when the process receives SIGINT or SIGTERM.
func Run() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	state := newResourceStore()
	connections := newConnectionTracker()
	xdsServer := newDiscoveryServer(state, connections)
	grpcServer := grpc.NewServer(grpc.StatsHandler(connections))
	registerDiscoveryServices(grpcServer, xdsServer)
	reflection.Register(grpcServer)

	grpcListener, err := net.Listen("tcp", "127.0.0.1:18000")
	if err != nil {
		slog.Error("xds_listener_failed", "error", err)
		os.Exit(1)
	}
	adminServer := &http.Server{Addr: "127.0.0.1:8080", Handler: newHTTPHandler(state, connections), ReadHeaderTimeout: 5 * time.Second}

	slog.Info("starting up xds server", "grpc_address", grpcListener.Addr().String(), "admin_address", adminServer.Addr)
	go func() {
		if err := grpcServer.Serve(grpcListener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			slog.Error("xds_server_stopped", "error", err)
			stop()
		}
	}()
	go func() {
		if err := adminServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("admin_server_stopped", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	grpcServer.GracefulStop()
	if err := adminServer.Shutdown(context.Background()); err != nil {
		slog.Error("admin_shutdown_failed", "error", err)
	}
	slog.Info("xds_connection_summary", "connections", connections.snapshot())
}
