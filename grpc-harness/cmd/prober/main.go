package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/health/grpc_health_v1"
	grpcpeer "google.golang.org/grpc/peer"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	_ "google.golang.org/grpc/xds"
)

type profile struct {
	Service           string `json:"service"`
	XDSServer         string `json:"xds_server"`
	Duration          string `json:"duration"`
	Processes         int    `json:"processes"`
	WorkersPerProcess int    `json:"workers_per_process"`
	RequestsPerSecond int    `json:"requests_per_second_per_process"`
	RequestTimeout    string `json:"request_timeout"`
}

type clientConnectionStats struct {
	Active   int            `json:"active"`
	Opened   uint64         `json:"opened"`
	Closed   uint64         `json:"closed"`
	ByRemote map[string]int `json:"active_by_remote_address"`
}

type probeSummary struct {
	Event                  string                `json:"event"`
	NodeID                 string                `json:"node_id"`
	Service                string                `json:"service"`
	Duration               string                `json:"duration"`
	Workers                int                   `json:"workers"`
	RequestsPerSecondLimit int                   `json:"requests_per_second_limit"`
	Requests               uint64                `json:"requests"`
	Succeeded              uint64                `json:"succeeded"`
	Failed                 uint64                `json:"failed"`
	ErrorsByCode           map[string]uint64     `json:"errors_by_code"`
	LatencyMilliseconds    map[string]float64    `json:"latency_ms"`
	ClientConnections      clientConnectionStats `json:"client_connections"`
}

type probeResults struct {
	mu        sync.Mutex
	succeeded uint64
	failed    uint64
	errors    map[string]uint64
	latencies []time.Duration
}

type clientStats struct {
	mu      sync.Mutex
	active  int
	opened  uint64
	closed  uint64
	remotes map[string]int
}

type remoteAddressKey struct{}

func main() {
	profilePath := flag.String("profile", "examples/load-profile.json", "JSON load profile")
	nodeID := flag.String("node-id", "", "unique xDS node ID (set automatically when profile launches processes)")
	child := flag.Bool("child", false, "internal flag used by the profile process launcher")
	debug := flag.Bool("debug", false, "log unary requests, data-plane connections, and grpc-go xDS updates")
	flag.Parse()
	setLogger(*debug, "")
	contents, err := os.ReadFile(*profilePath)
	if err != nil {
		fatal("read profile", err)
	}
	var load profile
	if err := json.Unmarshal(contents, &load); err != nil {
		fatal("parse profile", err)
	}
	if !*child {
		if load.Processes < 1 {
			load.Processes = 1
		}
		slog.Info("prober_starting", "profile", *profilePath, "processes", load.Processes, "service", load.Service, "debug", *debug)
		if err := runProcesses(*profilePath, load, *debug); err != nil {
			fatal("run prober processes", err)
		}
		return
	}
	if *nodeID == "" {
		*nodeID = defaultNodeID()
	}
	if err := runProbe(load, *nodeID, *debug); err != nil {
		fatal("run probe", err)
	}
}

func runProcesses(profilePath string, load profile, debug bool) error {
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	count := load.Processes
	commands := make([]*exec.Cmd, 0, count)
	for i := 0; i < count; i++ {
		nodeID := defaultNodeID()
		if count > 1 {
			nodeID = fmt.Sprintf("prober-%02d", i+1)
		}
		bootstrap, err := bootstrapConfig(load, nodeID)
		if err != nil {
			return err
		}
		command := exec.Command(executable, "-child", "-profile", profilePath, "-node-id", nodeID)
		if debug {
			command.Args = append(command.Args, "-debug")
		}
		command.Env = withBootstrapEnv(os.Environ(), bootstrap)
		command.Stdout = os.Stdout
		command.Stderr = os.Stderr
		commands = append(commands, command)
	}
	started := make([]*exec.Cmd, 0, len(commands))
	for _, command := range commands {
		if err := command.Start(); err != nil {
			for _, running := range started {
				_ = running.Process.Kill()
				_ = running.Wait()
			}
			return err
		}
		slog.Info("prober_process_started", "node_id", commandNodeID(command))
		started = append(started, command)
	}
	var wait sync.WaitGroup
	var failures atomic.Int64
	for _, command := range commands {
		wait.Add(1)
		go func(command *exec.Cmd) {
			defer wait.Done()
			if err := command.Wait(); err != nil {
				failures.Add(1)
				slog.Error("prober_process_failed", "node_id", commandNodeID(command), "error", err)
			}
		}(command)
	}
	wait.Wait()
	slog.Info("prober_process_summary", "processes", count, "failed_processes", failures.Load())
	if failures.Load() != 0 {
		return fmt.Errorf("%d prober processes failed", failures.Load())
	}
	return nil
}

func runProbe(load profile, nodeID string, debug bool) error {
	if load.Service == "" {
		load.Service = "probe.demo"
	}
	if load.XDSServer == "" {
		load.XDSServer = "127.0.0.1:18000"
	}
	if load.Processes < 1 {
		load.Processes = 1
	}
	if load.WorkersPerProcess < 1 {
		load.WorkersPerProcess = 1
	}
	duration, err := time.ParseDuration(load.Duration)
	if err != nil || duration <= 0 {
		return fmt.Errorf("duration must be a positive Go duration")
	}
	requestTimeout := 2 * time.Second
	if load.RequestTimeout != "" {
		requestTimeout, err = time.ParseDuration(load.RequestTimeout)
		if err != nil || requestTimeout <= 0 {
			return fmt.Errorf("request_timeout must be a positive Go duration")
		}
	}
	if load.RequestsPerSecond < 0 || load.RequestsPerSecond > 1_000_000_000 {
		return fmt.Errorf("requests_per_second_per_process must be between 0 and 1000000000")
	}
	if os.Getenv("GRPC_XDS_BOOTSTRAP_CONFIG") == "" && os.Getenv("GRPC_XDS_BOOTSTRAP") == "" {
		return fmt.Errorf("xDS bootstrap config is missing; start the prober through its profile launcher")
	}
	setLogger(debug, nodeID)
	slog.Info("probe_started", "service", load.Service, "xds_server", load.XDSServer, "duration", duration.String(), "workers", load.WorkersPerProcess, "requests_per_second_limit", load.RequestsPerSecond)

	connections := &clientStats{remotes: make(map[string]int)}
	conn, err := grpc.NewClient("xds:///"+load.Service,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(connections),
	)
	if err != nil {
		return err
	}
	defer conn.Close()
	client := grpc_health_v1.NewHealthClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	results := &probeResults{errors: make(map[string]uint64)}
	var ticker *time.Ticker
	var tick <-chan time.Time
	if load.RequestsPerSecond > 0 {
		ticker = time.NewTicker(time.Second / time.Duration(load.RequestsPerSecond))
		tick = ticker.C
		defer ticker.Stop()
	}
	var workers sync.WaitGroup
	for i := 0; i < load.WorkersPerProcess; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				if tick != nil {
					select {
					case <-ctx.Done():
						return
					case <-tick:
					}
				} else {
					select {
					case <-ctx.Done():
						return
					default:
					}
				}
				callCtx, callCancel := context.WithTimeout(ctx, requestTimeout)
				started := time.Now()
				var remotePeer grpcpeer.Peer
				response, callErr := client.Check(callCtx, &grpc_health_v1.HealthCheckRequest{}, grpc.Peer(&remotePeer))
				latency := time.Since(started)
				callCancel()
				if debug {
					responder := "unknown"
					if remotePeer.Addr != nil {
						responder = remotePeer.Addr.String()
					}
					slog.Debug("probe_request", "method", "/grpc.health.v1.Health/Check", "service", load.Service, "responder", responder, "response_status", response.GetStatus().String(), "error_code", status.Code(callErr).String(), "error", errorString(callErr), "latency", latency.String())
				}
				results.record(latency, response, callErr)
			}
		}()
	}
	workers.Wait()
	conn.Close()
	summary := results.summary(load, nodeID, duration, connections.snapshot())
	encoded, err := json.Marshal(summary)
	if err != nil {
		return err
	}
	_, err = os.Stdout.Write(append(encoded, '\n'))
	return err
}

func bootstrapConfig(load profile, nodeID string) ([]byte, error) {
	if load.XDSServer == "" {
		load.XDSServer = "127.0.0.1:18000"
	}
	return json.Marshal(map[string]any{
		"xds_servers": []any{map[string]any{
			"server_uri":      load.XDSServer,
			"channel_creds":   []any{map[string]any{"type": "insecure"}},
			"server_features": []string{"xds_v3"},
		}},
		"node": map[string]string{"id": nodeID, "cluster": "local-probers"},
	})
}

func withBootstrapEnv(environment []string, bootstrap []byte) []string {
	filtered := make([]string, 0, len(environment)+1)
	for _, variable := range environment {
		if strings.HasPrefix(variable, "GRPC_XDS_BOOTSTRAP=") || strings.HasPrefix(variable, "GRPC_XDS_BOOTSTRAP_CONFIG=") {
			continue
		}
		filtered = append(filtered, variable)
	}
	return append(filtered, "GRPC_XDS_BOOTSTRAP_CONFIG="+string(bootstrap))
}

func setLogger(debug bool, nodeID string) {
	level := slog.LevelInfo
	if debug {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	if nodeID != "" {
		logger = logger.With("node_id", nodeID)
	}
	slog.SetDefault(logger)
	if debug {
		grpclog.SetLoggerV2(grpcSlogLogger{verbosity: 2})
	}
}

type grpcSlogLogger struct {
	verbosity int
}

func (l grpcSlogLogger) Info(args ...any) {
	l.log(slog.LevelInfo, fmt.Sprint(args...))
}

func (l grpcSlogLogger) Infoln(args ...any) {
	l.log(slog.LevelInfo, strings.TrimSpace(fmt.Sprintln(args...)))
}

func (l grpcSlogLogger) Infof(format string, args ...any) {
	l.log(slog.LevelInfo, fmt.Sprintf(format, args...))
}

func (l grpcSlogLogger) Warning(args ...any) {
	l.log(slog.LevelWarn, fmt.Sprint(args...))
}

func (l grpcSlogLogger) Warningln(args ...any) {
	l.log(slog.LevelWarn, strings.TrimSpace(fmt.Sprintln(args...)))
}

func (l grpcSlogLogger) Warningf(format string, args ...any) {
	l.log(slog.LevelWarn, fmt.Sprintf(format, args...))
}

func (l grpcSlogLogger) Error(args ...any) {
	l.log(slog.LevelError, fmt.Sprint(args...))
}

func (l grpcSlogLogger) Errorln(args ...any) {
	l.log(slog.LevelError, strings.TrimSpace(fmt.Sprintln(args...)))
}

func (l grpcSlogLogger) Errorf(format string, args ...any) {
	l.log(slog.LevelError, fmt.Sprintf(format, args...))
}

func (l grpcSlogLogger) Fatal(args ...any) {
	l.log(slog.LevelError, fmt.Sprint(args...))
	os.Exit(1)
}

func (l grpcSlogLogger) Fatalln(args ...any) {
	l.log(slog.LevelError, strings.TrimSpace(fmt.Sprintln(args...)))
	os.Exit(1)
}

func (l grpcSlogLogger) Fatalf(format string, args ...any) {
	l.log(slog.LevelError, fmt.Sprintf(format, args...))
	os.Exit(1)
}

func (l grpcSlogLogger) V(level int) bool {
	return level <= l.verbosity
}

func (grpcSlogLogger) log(level slog.Level, message string) {
	slog.Log(context.Background(), level, message, "source", "grpc-go")
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func commandNodeID(command *exec.Cmd) string {
	for i := 0; i+1 < len(command.Args); i++ {
		if command.Args[i] == "-node-id" {
			return command.Args[i+1]
		}
	}
	return "unknown"
}

func (r *probeResults) record(latency time.Duration, response *grpc_health_v1.HealthCheckResponse, callErr error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if callErr == nil && response.GetStatus() == grpc_health_v1.HealthCheckResponse_SERVING {
		r.succeeded++
	} else {
		r.failed++
		code := "NOT_SERVING"
		if callErr != nil {
			code = status.Code(callErr).String()
		}
		r.errors[code]++
	}
	r.latencies = append(r.latencies, latency)
}

func (r *probeResults) summary(load profile, nodeID string, duration time.Duration, connections clientConnectionStats) probeSummary {
	r.mu.Lock()
	defer r.mu.Unlock()
	sort.Slice(r.latencies, func(i, j int) bool { return r.latencies[i] < r.latencies[j] })
	quantile := func(q float64) float64 {
		if len(r.latencies) == 0 {
			return 0
		}
		index := int(float64(len(r.latencies)-1) * q)
		return float64(r.latencies[index].Microseconds()) / 1000
	}
	return probeSummary{
		Event: "probe_summary", NodeID: nodeID, Service: load.Service, Duration: duration.String(),
		Workers: load.WorkersPerProcess, RequestsPerSecondLimit: load.RequestsPerSecond,
		Requests: r.succeeded + r.failed, Succeeded: r.succeeded, Failed: r.failed,
		ErrorsByCode:        r.errors,
		LatencyMilliseconds: map[string]float64{"p50": quantile(.50), "p95": quantile(.95), "p99": quantile(.99)},
		ClientConnections:   connections,
	}
}

func (s *clientStats) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context { return ctx }
func (*clientStats) HandleRPC(context.Context, stats.RPCStats)                         {}
func (s *clientStats) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	remote := "unknown"
	if info.RemoteAddr != nil {
		remote = info.RemoteAddr.String()
	}
	return context.WithValue(ctx, remoteAddressKey{}, remote)
}
func (s *clientStats) HandleConn(ctx context.Context, event stats.ConnStats) {
	remote, _ := ctx.Value(remoteAddressKey{}).(string)
	s.mu.Lock()
	connectionEvent := ""
	switch event.(type) {
	case *stats.ConnBegin:
		connectionEvent = "open"
		s.active++
		s.opened++
		s.remotes[remote]++
	case *stats.ConnEnd:
		connectionEvent = "close"
		if s.active > 0 {
			s.active--
		}
		s.closed++
		if s.remotes[remote] <= 1 {
			delete(s.remotes, remote)
		} else {
			s.remotes[remote]--
		}
	}
	active, opened, closed := s.active, s.opened, s.closed
	s.mu.Unlock()
	if connectionEvent != "" {
		slog.Info("probe_connection", "event", connectionEvent, "remote_address", remote, "active", active, "opened", opened, "closed", closed)
	}
}

func (s *clientStats) snapshot() clientConnectionStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	remotes := make(map[string]int, len(s.remotes))
	for remote, count := range s.remotes {
		remotes[remote] = count
	}
	return clientConnectionStats{Active: s.active, Opened: s.opened, Closed: s.closed, ByRemote: remotes}
}

func defaultNodeID() string {
	host, _ := os.Hostname()
	return strings.ToLower(host) + "-" + strconv.Itoa(os.Getpid())
}

func fatal(operation string, err error) {
	slog.Error("prober_failed", "operation", operation, "error", err)
	os.Exit(1)
}

var _ stats.Handler = (*clientStats)(nil)
