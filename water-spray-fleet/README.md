# Water spray fleet

Environmental dust control (`water-spray-fleet`) — field PLCs for north and south spray zones, commanded by site events rather than high-frequency telemetry.

Detailed design docs will follow: OpenShift namespace layout, Modbus actuator patterns, phase-1 bench testing, and Kafka → Modbus bridge integration (phase 2).

---

## Deployment

**Planned — not yet in repo.** There is no [`openshift/water-spray-fleet/`](../openshift/water-spray-fleet/) folder yet. When manifests are added, the expected apply order will be:

1. `01-namespace.yaml` — `water-spray-fleet` namespace
2. ConfigMaps / Secrets — Modbus register maps, spray zone env
3. North and south spray PLC Deployments (Modbus TCP servers)
4. BuildConfigs + `oc start-build` for any bridge/controller images
5. Phase 2: Kafka → Modbus bridge and optional `spray-controller` Deployment

Phase 2 integration will consume `fleet.trucks.events` and `fleet.crushers.events` from the [`mining-fleet-kafka`](../openshift/mining-fleet-kafka/) cluster (see [fleet-integration](../fleet-integration/README.md)). Deploy Kafka and fleet-integration **before** wiring spray control.

Parent overview: [../README.md](../README.md) · [Deployment order](../README.md#deployment-order-recommended)
