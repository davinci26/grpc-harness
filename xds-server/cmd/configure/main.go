package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	cluster "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpoint "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	listener "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	route "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	router "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/router/v3"
	hcm "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type resourceToPublish struct {
	kind string
	name string
	msg  proto.Message
}

func main() {
	adminURL := flag.String("admin", "http://127.0.0.1:8080", "xDS admin API base URL")
	serviceName := flag.String("service", "probe.demo", "xDS target name used by probe clients")
	routeCount := flag.Int("routes", 100, "number of route entries to publish")
	secondaryServiceName := flag.String("secondary-service", "", "optional additional service resource set, not used by the prober (for example unused.demo)")
	resourceKind := flag.String("resource", "all", "resource to publish: all, listeners, routes, clusters, or endpoints")
	connectTimeout := flag.Duration("connect-timeout", 2*time.Second, "cluster connection timeout")
	addresses := flag.String("responders", "127.0.0.1:50051,127.0.0.1:50052", "comma-separated responder host:port endpoints")
	flag.Parse()
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if *routeCount < 1 {
		fatal("routes must be at least 1")
	}
	if *secondaryServiceName == *serviceName && *secondaryServiceName != "" {
		fatal("secondary-service must differ from service")
	}
	if *connectTimeout <= 0 {
		fatal("connect-timeout must be positive")
	}
	if *resourceKind != "all" && *resourceKind != "listeners" && *resourceKind != "routes" && *resourceKind != "clusters" && *resourceKind != "endpoints" {
		fatal("resource must be all, listeners, routes, clusters, or endpoints")
	}
	parsedAddresses, err := parseAddresses(*addresses)
	if err != nil {
		fatal(err.Error())
	}
	resources, err := makeResourceSet(*serviceName, "probe-routes", "probe-cluster", *routeCount, *connectTimeout, parsedAddresses)
	if err != nil {
		fatal(err.Error())
	}
	if *secondaryServiceName != "" {
		secondaryResources, err := makeResourceSet(*secondaryServiceName, "unused-routes", "unused-cluster", *routeCount, *connectTimeout, parsedAddresses)
		if err != nil {
			fatal(err.Error())
		}
		resources = append(resources, secondaryResources...)
	}
	client := &http.Client{Timeout: 10 * time.Second}
	for _, entry := range resources {
		if *resourceKind != "all" && entry.kind != *resourceKind {
			continue
		}
		if err := putResource(client, *adminURL, entry.kind, entry.name, entry.msg); err != nil {
			fatal(fmt.Sprintf("publish %s %s: %v", entry.kind, entry.name, err))
		}
		slog.Info("resource_published", "kind", entry.kind, "name", entry.name)
	}
	slog.Info("example_resources_ready", "service", *serviceName, "routes", *routeCount, "secondary_service", *secondaryServiceName, "responders", parsedAddresses)
}

func makeResourceSet(serviceName, routeName, clusterName string, routeCount int, connectTimeout time.Duration, addresses []string) ([]resourceToPublish, error) {
	listenerResource, err := makeListener(serviceName, routeName)
	if err != nil {
		return nil, err
	}
	return []resourceToPublish{
		{"endpoints", clusterName, makeEndpoints(clusterName, addresses)},
		{"clusters", clusterName, makeCluster(clusterName, connectTimeout)},
		{"routes", routeName, makeRoutes(routeName, serviceName, clusterName, routeCount)},
		{"listeners", serviceName, listenerResource},
	}, nil
}

func fatal(message string) {
	slog.Error("configure_failed", "error", message)
	os.Exit(1)
}

func parseAddresses(value string) ([]string, error) {
	var addresses []string
	for _, item := range strings.Split(value, ",") {
		address := strings.TrimSpace(item)
		host, port, err := net.SplitHostPort(address)
		if err != nil || host == "" {
			return nil, fmt.Errorf("invalid responder address %q", address)
		}
		portNumber, err := strconv.ParseUint(port, 10, 32)
		if err != nil || portNumber == 0 {
			return nil, fmt.Errorf("invalid responder port in %q", address)
		}
		addresses = append(addresses, address)
	}
	if len(addresses) == 0 {
		return nil, fmt.Errorf("at least one responder address is required")
	}
	return addresses, nil
}

func putResource(client *http.Client, adminURL, kind, name string, msg proto.Message) error {
	body, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(msg)
	if err != nil {
		return err
	}
	request, err := http.NewRequest(http.MethodPut, strings.TrimRight(adminURL, "/")+"/v1/"+kind+"/"+name, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4<<10))
		message := strings.TrimSpace(string(body))
		if message == "" {
			return fmt.Errorf("admin API returned %s", response.Status)
		}
		return fmt.Errorf("admin API returned %s: %s", response.Status, message)
	}
	return nil
}

func makeListener(serviceName, routeName string) (*listener.Listener, error) {
	routerConfig, err := anypb.New(&router.Router{})
	if err != nil {
		return nil, fmt.Errorf("marshal router filter: %w", err)
	}
	hcmConfig := &hcm.HttpConnectionManager{
		RouteSpecifier: &hcm.HttpConnectionManager_Rds{Rds: &hcm.Rds{
			ConfigSource:    &core.ConfigSource{ConfigSourceSpecifier: &core.ConfigSource_Ads{Ads: &core.AggregatedConfigSource{}}},
			RouteConfigName: routeName,
		}},
		HttpFilters: []*hcm.HttpFilter{{Name: "envoy.filters.http.router", ConfigType: &hcm.HttpFilter_TypedConfig{TypedConfig: routerConfig}}},
	}
	hcmAny, err := anypb.New(hcmConfig)
	if err != nil {
		return nil, fmt.Errorf("marshal HTTP connection manager: %w", err)
	}
	return &listener.Listener{
		Name:        serviceName,
		ApiListener: &listener.ApiListener{ApiListener: hcmAny},
		FilterChains: []*listener.FilterChain{{Filters: []*listener.Filter{{
			Name:       "envoy.filters.network.http_connection_manager",
			ConfigType: &listener.Filter_TypedConfig{TypedConfig: hcmAny},
		}}}},
	}, nil
}

func makeRoutes(routeName, domain, clusterName string, count int) *route.RouteConfiguration {
	routes := make([]*route.Route, 0, count)
	for i := 0; i < count-1; i++ {
		routes = append(routes, routeToCluster("/benchmark/unmatched/"+strconv.Itoa(i), clusterName))
	}
	routes = append(routes, &route.Route{
		Match:  &route.RouteMatch{PathSpecifier: &route.RouteMatch_Path{Path: "/grpc.health.v1.Health/Check"}},
		Action: &route.Route_Route{Route: &route.RouteAction{ClusterSpecifier: &route.RouteAction_Cluster{Cluster: clusterName}}},
	})
	return &route.RouteConfiguration{Name: routeName, VirtualHosts: []*route.VirtualHost{{Domains: []string{domain}, Routes: routes}}}
}

func routeToCluster(prefix, clusterName string) *route.Route {
	return &route.Route{
		Match:  &route.RouteMatch{PathSpecifier: &route.RouteMatch_Prefix{Prefix: prefix}},
		Action: &route.Route_Route{Route: &route.RouteAction{ClusterSpecifier: &route.RouteAction_Cluster{Cluster: clusterName}}},
	}
}

func makeCluster(name string, connectTimeout time.Duration) *cluster.Cluster {
	return &cluster.Cluster{
		Name:                 name,
		ConnectTimeout:       durationpb.New(connectTimeout),
		ClusterDiscoveryType: &cluster.Cluster_Type{Type: cluster.Cluster_EDS},
		EdsClusterConfig: &cluster.Cluster_EdsClusterConfig{
			EdsConfig: &core.ConfigSource{
				ConfigSourceSpecifier: &core.ConfigSource_Ads{Ads: &core.AggregatedConfigSource{}},
				ResourceApiVersion:    core.ApiVersion_V3,
			},
			ServiceName: name,
		},
		LbPolicy: cluster.Cluster_ROUND_ROBIN,
	}
}

func makeEndpoints(clusterName string, addresses []string) *endpoint.ClusterLoadAssignment {
	endpoints := make([]*endpoint.LbEndpoint, 0, len(addresses))
	for _, address := range addresses {
		host, port, _ := net.SplitHostPort(address)
		portNumber, _ := strconv.ParseUint(port, 10, 32)
		endpoints = append(endpoints, &endpoint.LbEndpoint{HostIdentifier: &endpoint.LbEndpoint_Endpoint{Endpoint: &endpoint.Endpoint{
			Address: &core.Address{Address: &core.Address_SocketAddress{SocketAddress: &core.SocketAddress{
				Protocol:      core.SocketAddress_TCP,
				Address:       host,
				PortSpecifier: &core.SocketAddress_PortValue{PortValue: uint32(portNumber)},
			}}},
		}}})
	}
	return &endpoint.ClusterLoadAssignment{ClusterName: clusterName, Endpoints: []*endpoint.LocalityLbEndpoints{{
		Locality:            &core.Locality{Region: "local", Zone: "local", SubZone: "default"},
		LoadBalancingWeight: wrapperspb.UInt32(1),
		LbEndpoints:         endpoints,
	}}}
}
