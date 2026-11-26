package xdsserver

import (
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	cluster "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	endpoint "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	listener "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	route "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	_ "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/router/v3"
	_ "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	"github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

//go:embed web/index.html
var adminPage embed.FS

func newHTTPHandler(state *resourceStore, connections *connectionTracker) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		page, err := adminPage.ReadFile("web/index.html")
		if err != nil {
			http.Error(w, "read admin page: "+err.Error(), http.StatusInternalServerError)
			return
		}
		_, _ = w.Write(page)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("GET /snapshots", func(w http.ResponseWriter, _ *http.Request) { writeSnapshot(w, state, "all") })
	mux.HandleFunc("GET /snapshots/", func(w http.ResponseWriter, r *http.Request) {
		writeSnapshot(w, state, strings.TrimPrefix(r.URL.Path, "/snapshots/"))
	})
	mux.HandleFunc("GET /connections", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(connections.snapshot())
	})
	mux.HandleFunc("GET /versions", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(state.snapshotVersions())
	})
	mux.HandleFunc("/v1/", func(w http.ResponseWriter, r *http.Request) { updateResource(w, r, state) })
	return mux
}

func updateResource(w http.ResponseWriter, r *http.Request, state *resourceStore) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 3 || parts[0] != "v1" || parts[2] == "" {
		http.Error(w, "use /v1/{listeners|routes|clusters|endpoints}/{name}", http.StatusNotFound)
		return
	}
	kind, name := parts[1], parts[2]
	switch r.Method {
	case http.MethodPut:
		msg, err := newResource(kind)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 32<<20))
		if err != nil {
			http.Error(w, "read request body: "+err.Error(), http.StatusBadRequest)
			return
		}
		if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(body, msg); err != nil {
			http.Error(w, "decode resource: "+err.Error(), http.StatusBadRequest)
			return
		}
		if resourceName(msg) != name {
			http.Error(w, fmt.Sprintf("path name %q does not match resource name %q", name, resourceName(msg)), http.StatusBadRequest)
			return
		}
		if err := state.put(r.Context(), kind, name, msg); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		slog.Info("resource_updated", "kind", kind, "name", name)
		w.WriteHeader(http.StatusNoContent)
	case http.MethodDelete:
		if err := state.remove(r.Context(), kind, name); err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		slog.Info("resource_removed", "kind", kind, "name", name)
		w.WriteHeader(http.StatusNoContent)
	default:
		w.Header().Set("Allow", "PUT, DELETE")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func newResource(kind string) (proto.Message, error) {
	switch kind {
	case "listeners":
		return &listener.Listener{}, nil
	case "routes":
		return &route.RouteConfiguration{}, nil
	case "clusters":
		return &cluster.Cluster{}, nil
	case "endpoints":
		return &endpoint.ClusterLoadAssignment{}, nil
	default:
		return nil, fmt.Errorf("unknown resource type %q", kind)
	}
}

func resourceName(msg proto.Message) string {
	switch value := msg.(type) {
	case *listener.Listener:
		return value.GetName()
	case *route.RouteConfiguration:
		return value.GetName()
	case *cluster.Cluster:
		return value.GetName()
	case *endpoint.ClusterLoadAssignment:
		return value.GetClusterName()
	default:
		return ""
	}
}

func writeSnapshot(w http.ResponseWriter, state *resourceStore, kind string) {
	kinds := []string{kind}
	if kind == "all" {
		kinds = []string{"listeners", "routes", "clusters", "endpoints"}
	}
	response := make(map[string][]json.RawMessage, len(kinds))
	for _, resourceKind := range kinds {
		messages, err := state.snapshotResources(resourceKind)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		for _, msg := range messages {
			encoded, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(msg)
			if err != nil {
				http.Error(w, "encode snapshot: "+err.Error(), http.StatusInternalServerError)
				return
			}
			response[typeURL(resourceKind)] = append(response[typeURL(resourceKind)], encoded)
		}
		if response[typeURL(resourceKind)] == nil {
			response[typeURL(resourceKind)] = []json.RawMessage{}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

func typeURL(kind string) string {
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
		return kind
	}
}
