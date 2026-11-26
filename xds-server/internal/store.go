package xdsserver

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"

	cluster "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpoint "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	listener "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	route "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	"github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"google.golang.org/protobuf/proto"
)

type resourceStore struct {
	mu sync.RWMutex

	listeners map[string]*listener.Listener
	routes    map[string]*route.RouteConfiguration
	clusters  map[string]*cluster.Cluster
	endpoints map[string]*endpoint.ClusterLoadAssignment

	servicesCache  cache.SnapshotCache
	endpointsCache cache.SnapshotCache
	muxCache       cache.MuxCache
	versions       map[resource.Type]uint64
}

type sharedNodeHasher struct{}

func (sharedNodeHasher) ID(_ *core.Node) string { return "" }

func newResourceStore() *resourceStore {
	services := cache.NewSnapshotCache(false, sharedNodeHasher{}, nil)
	endpoints := cache.NewSnapshotCache(false, sharedNodeHasher{}, nil)
	return &resourceStore{
		listeners:      make(map[string]*listener.Listener),
		routes:         make(map[string]*route.RouteConfiguration),
		clusters:       make(map[string]*cluster.Cluster),
		endpoints:      make(map[string]*endpoint.ClusterLoadAssignment),
		servicesCache:  services,
		endpointsCache: endpoints,
		versions: map[resource.Type]uint64{
			resource.ListenerType: 1,
			resource.RouteType:    1,
			resource.ClusterType:  1,
			resource.EndpointType: 1,
		},
		muxCache: cache.MuxCache{
			Classify:      func(req *cache.Request) string { return cacheName(req.TypeUrl) },
			ClassifyDelta: func(req *cache.DeltaRequest) string { return cacheName(req.TypeUrl) },
			Caches:        map[string]cache.Cache{"services": services, "endpoints": endpoints},
		},
	}
}

func cacheName(typeURL string) string {
	if typeURL == resource.EndpointType {
		return "endpoints"
	}
	switch typeURL {
	case resource.ListenerType, resource.RouteType, resource.ClusterType:
		return "services"
	default:
		return ""
	}
}

func (s *resourceStore) put(ctx context.Context, kind, name string, value proto.Message) error {
	if name == "" {
		return fmt.Errorf("resource name is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch kind {
	case "listeners":
		v, ok := value.(*listener.Listener)
		if !ok {
			return fmt.Errorf("invalid listener")
		}
		s.listeners[name] = v
	case "routes":
		v, ok := value.(*route.RouteConfiguration)
		if !ok {
			return fmt.Errorf("invalid route")
		}
		s.routes[name] = v
	case "clusters":
		v, ok := value.(*cluster.Cluster)
		if !ok {
			return fmt.Errorf("invalid cluster")
		}
		s.clusters[name] = v
	case "endpoints":
		v, ok := value.(*endpoint.ClusterLoadAssignment)
		if !ok {
			return fmt.Errorf("invalid endpoint assignment")
		}
		s.endpoints[name] = v
	default:
		return fmt.Errorf("unknown resource type %q", kind)
	}
	return s.publishLocked(ctx, kind)
}

func (s *resourceStore) remove(ctx context.Context, kind, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch kind {
	case "listeners":
		delete(s.listeners, name)
	case "routes":
		delete(s.routes, name)
	case "clusters":
		delete(s.clusters, name)
	case "endpoints":
		delete(s.endpoints, name)
	default:
		return fmt.Errorf("unknown resource type %q", kind)
	}
	return s.publishLocked(ctx, kind)
}

func (s *resourceStore) publishLocked(ctx context.Context, kind string) error {
	changedType := resourceType(kind)
	s.versions[changedType]++
	version := fmt.Sprintf("%d", s.versions[changedType])
	if changedType == resource.EndpointType {
		endpointSnapshot, err := cache.NewSnapshot(version, map[resource.Type][]types.Resource{
			resource.EndpointType: resources(s.endpoints),
		})
		if err != nil {
			return fmt.Errorf("build endpoint snapshot: %w", err)
		}
		if err := s.endpointsCache.SetSnapshot(ctx, "", endpointSnapshot); err != nil {
			return fmt.Errorf("publish endpoint snapshot: %w", err)
		}
	} else {
		serviceSnapshot, err := cache.NewSnapshot(version, map[resource.Type][]types.Resource{
			resource.ListenerType: resources(s.listeners),
			resource.RouteType:    resources(s.routes),
			resource.ClusterType:  resources(s.clusters),
		})
		if err != nil {
			return fmt.Errorf("build service snapshot: %w", err)
		}
		serviceSnapshot.Resources[types.Listener].Version = fmt.Sprint(s.versions[resource.ListenerType])
		serviceSnapshot.Resources[types.Route].Version = fmt.Sprint(s.versions[resource.RouteType])
		serviceSnapshot.Resources[types.Cluster].Version = fmt.Sprint(s.versions[resource.ClusterType])
		if err := s.servicesCache.SetSnapshot(ctx, "", serviceSnapshot); err != nil {
			return fmt.Errorf("publish service snapshot: %w", err)
		}
	}
	slog.Info("snapshot_published",
		"type_url", changedType,
		"version", version,
		"listeners", len(s.listeners),
		"routes", len(s.routes),
		"clusters", len(s.clusters),
		"endpoints", len(s.endpoints),
	)
	return nil
}

func resourceType(kind string) resource.Type {
	switch kind {
	case "listeners":
		return resource.ListenerType
	case "routes":
		return resource.RouteType
	case "clusters":
		return resource.ClusterType
	case "endpoints":
		return resource.EndpointType
	default:
		return ""
	}
}

func resources[T proto.Message](values map[string]T) []types.Resource {
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]types.Resource, 0, len(names))
	for _, name := range names {
		out = append(out, values[name])
	}
	return out
}

func (s *resourceStore) snapshotResources(kind string) ([]proto.Message, error) {
	var snapshotCache cache.SnapshotCache
	var typeURL string
	switch kind {
	case "listeners":
		snapshotCache, typeURL = s.servicesCache, resource.ListenerType
	case "routes":
		snapshotCache, typeURL = s.servicesCache, resource.RouteType
	case "clusters":
		snapshotCache, typeURL = s.servicesCache, resource.ClusterType
	case "endpoints":
		snapshotCache, typeURL = s.endpointsCache, resource.EndpointType
	default:
		return nil, fmt.Errorf("unknown resource type %q", kind)
	}

	snapshot, err := snapshotCache.GetSnapshot("")
	if err != nil {
		return []proto.Message{}, nil
	}
	resourcesByName := snapshot.GetResources(typeURL)
	names := make([]string, 0, len(resourcesByName))
	for name := range resourcesByName {
		names = append(names, name)
	}
	sort.Strings(names)
	values := make([]proto.Message, 0, len(names))
	for _, name := range names {
		values = append(values, resourcesByName[name])
	}
	return values, nil
}

func (s *resourceStore) snapshotVersions() map[string]string {
	versions := make(map[string]string, 4)
	if snapshot, err := s.servicesCache.GetSnapshot(""); err == nil {
		versions[string(resource.ListenerType)] = snapshot.GetVersion(string(resource.ListenerType))
		versions[string(resource.RouteType)] = snapshot.GetVersion(string(resource.RouteType))
		versions[string(resource.ClusterType)] = snapshot.GetVersion(string(resource.ClusterType))
	}
	if snapshot, err := s.endpointsCache.GetSnapshot(""); err == nil {
		versions[string(resource.EndpointType)] = snapshot.GetVersion(string(resource.EndpointType))
	}
	return versions
}
