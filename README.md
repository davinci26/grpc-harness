# Local gRPC xDS harness

A small local environment for exploring how grpc-go clients consume xDS configuration and connect to gRPC backends. It runs as separate Go processes; containers and service-discovery informers are not required.

The harness is intended for experiments such as:

- observing when a grpc-go client creates connections to responder endpoints
- changing listeners, routes, clusters, or endpoints independently and observing the xDS updates
- increasing route counts and watching the client and control-plane behavior
- counting xDS connections and ADS streams per probe process

## Projects

This repository contains two independent Go modules:

| Directory | Contents |
| --- | --- |
| [`xds-server/`](xds-server/) | In-memory xDS control plane, snapshot admin UI/API, and `configure` command |
| [`grpc-harness/`](grpc-harness/) | grpc-go prober, health-check responder, and example load profile |

Both modules declare Go 1.26. The grpc-harness module uses grpc-go v1.84.0 from [`grpc-harness/third_party/grpc-go`](grpc-harness/third_party/grpc-go), so grpc-go source can be edited locally. Its other dependencies remain Go module dependencies.

## Quick start

Run each command in its own terminal from the repository root.

**Terminal 1: start the xDS server**

```sh
cd xds-server && go run ./cmd/xds-server
```

The server listens on `127.0.0.1:18000` for xDS and `127.0.0.1:8080` for its admin UI and API. Open [http://127.0.0.1:8080/](http://127.0.0.1:8080/) to inspect and edit the current resources.

**Terminals 2 and 3: start two responders**

```sh
cd grpc-harness && go run ./cmd/responder -listen 127.0.0.1:50051 -name responder-a
```

```sh
cd grpc-harness && go run ./cmd/responder -listen 127.0.0.1:50052 -name responder-b
```

Responders expose the standard gRPC health service. The example configuration below sends health-check requests to them.

**Terminal 4: publish an initial resource set**

```sh
cd xds-server && go run ./cmd/configure -routes 2
```

The configure command publishes endpoint assignments, an EDS cluster, a route configuration, and a client listener for `probe.demo`. By default, it points the cluster at both responders on ports 50051 and 50052.

**Terminal 5: run the example probe profile**

```sh
cd grpc-harness && go run ./cmd/prober -profile examples/load-profile.json
```

The prober starts the number of separate processes configured by the profile. Each process creates one grpc-go xDS client channel and runs unary `grpc.health.v1.Health/Check` calls for the configured service. The checked-in profile targets `probe.demo` for 10 minutes, with one process, one worker, and one request per second.

The profile launcher generates an xDS bootstrap configuration for each process and sets `GRPC_XDS_BOOTSTRAP_CONFIG` for it. Run the prober through the profile launcher to get this bootstrap automatically.

## Configure a run

Edit [`grpc-harness/examples/load-profile.json`](grpc-harness/examples/load-profile.json) to change the probe workload:

| Field | Meaning |
| --- | --- |
| `service` | xDS target name; defaults to `probe.demo` |
| `xds_server` | xDS gRPC address; defaults to `127.0.0.1:18000` |
| `duration` | How long each probe process runs, as a Go duration |
| `processes` | Number of independent operating-system probe processes |
| `workers_per_process` | Number of concurrent unary RPC workers in each process |
| `requests_per_second_per_process` | Aggregate request rate limit for each process; `0` runs without a rate limit |
| `request_timeout` | Per-RPC timeout; defaults to `2s` |

Each process logs a final JSON summary with request totals, success/failure counts, error codes, latency percentiles, and data-plane connections grouped by responder address. Add `-debug` to log individual probe requests, their responder and errors, client connection open/close events, and grpc-go xDS resource updates:

```sh
cd grpc-harness && go run ./cmd/prober -profile examples/load-profile.json -debug
```

The prober and responder also log process and connection lifecycle events at the default info level. Per-request probe logs are debug-level.

## Configure resources

The web UI provides simplified forms for creating and editing listeners, routes, clusters, and endpoint sets. Parent resources select existing child resources. Create resources in this order:

1. Endpoints: endpoint-set name and IP/port pairs
2. Clusters: load-balancing policy and endpoint set
3. Routes: optional header match rules and cluster
4. Listeners: listener name and route

## Inspect snapshots and connections

The admin UI is at [http://127.0.0.1:8080/](http://127.0.0.1:8080/). The following read-only endpoints are also available:

| Endpoint | Returns |
| --- | --- |
| `GET /healthz` | Server health (`ok`) |
| `GET /snapshots` | Current listeners, routes, clusters, and endpoints |
| `GET /snapshots/listeners` | Current listeners |
| `GET /snapshots/routes` | Current routes |
| `GET /snapshots/clusters` | Current clusters |
| `GET /snapshots/endpoints` | Current endpoint assignments |
| `GET /versions` | Current resource-type versions |
| `GET /connections` | Active and total xDS TCP connections, ADS streams, requests, ACKs, and NACKs by client node ID |

The xDS server keeps listeners, routes, and clusters in one snapshot cache and endpoint assignments in a separate cache. A mux selects the cache for each resource type. Updating one type advances that type's version while publishing a complete snapshot for its cache.

## Resource API

The admin API accepts Envoy protobuf JSON. The resource's name in the request body must match the `{name}` in the URL.

```text
PUT    /v1/listeners/{name}
PUT    /v1/routes/{name}
PUT    /v1/clusters/{name}
PUT    /v1/endpoints/{name}
DELETE /v1/{listeners|routes|clusters|endpoints}/{name}
```

The UI and configure command use this API. For example, publish an endpoint assignment with JSON encoded for `envoy.config.endpoint.v3.ClusterLoadAssignment`:

```sh
curl -X PUT http://127.0.0.1:8080/v1/endpoints/probe-cluster \
  -H 'Content-Type: application/json' \
  -d '{"cluster_name":"probe-cluster","endpoints":[]}'
```

## Project layout

```text
xds-server/
  cmd/xds-server/    xDS server executable
  cmd/configure/     example resource publisher
  internal/          xDS server, snapshot store, admin HTTP API, embedded UI

grpc-harness/
  cmd/prober/        multi-process grpc-go xDS client and load generator
  cmd/responder/     gRPC health responder
  examples/          example load profile
  third_party/grpc-go/  editable grpc-go source used by the harness
```
