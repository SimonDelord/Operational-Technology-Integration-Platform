# Operational Technology Integration Platform

OpenShift manifests, POC services, and design documentation for integrated mining fleet demos (trucks, crushers, Kafka orchestration, live map).

## Documentation

- **[Mining fleet overview](docs/mining-fleet/README.md)** — architecture, deployment order, and cross-fleet integration
- **[Demo pods index](docs/mining-fleet/DEMO-PODS.md)** — workloads deployed on OpenShift for the fleet demo

## Repository layout

| Path | Contents |
|------|----------|
| [`docs/mining-fleet/`](docs/mining-fleet/) | Fleet design docs (truck, crusher, water spray, integration, live map) |
| [`poc/`](poc/) | Python POCs: truck MQTT agents, crusher PLC/historian, fleet-integration bridges, live map server |
| [`openshift/`](openshift/) | Namespaces, BuildConfigs, Deployments, Kafka topic manifests |

Shared Modbus and S3 CSV examples used by some patterns live in the separate [alleo-work](https://github.com/SimonDelord/alleo-work) repository (`poc/modbus`, `poc/csv`).

## Quick start

1. Read [docs/mining-fleet/README.md](docs/mining-fleet/README.md) for deployment order.
2. Apply OpenShift manifests under `openshift/mining-fleet-kafka/`, then `openshift/truck-fleet/`, `openshift/fleet-integration/`, `openshift/crusher-fleet/`, and optionally `openshift/fleet-live-map/`.
3. Build container images from this repo (`main` branch) via the included BuildConfigs after pushing changes.
