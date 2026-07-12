# dcre-agt

AGT (Collections Agent): the only long-running DCRE service. Quarkus 3.33 LTS / Java 25. Watches the exchange directories, registers file arrivals (SHA-256 identity, R-31 filename tokens, same-key-different-hash quarantine), launches pipeline stages as ephemeral Kubernetes Jobs with write-ahead intents, records externally observed outcomes (R-33: agt_ops is the sole termination authority), and drives the Collections DAG level-triggered from its ledgers. Survives its own death: DB lease singleton + reconciliation from ledgers, proven by the kill-test.

## Ledgers (agt_ops, Liquibase-owned)

`file_arrival` (arrival registry + quarantine) - `launch_intent` (write-ahead, UNIQUE(arrival,stage) = non-overlap, UNIQUE(job_name) = deterministic names) - `stage_outcome` (UNIQUE(intent) = idempotent observation) - `agt_lease` (CAS singleton).

## M1 state

DAG `CRR -> CTV -> [CDE, CIR]; CDE -> CRW` runs busybox stub Jobs. The business verdict travels via `/exchange/outcomes/<job>` (SYNTHETIC-CONTRACT seam; M2 services replace it). Absence of an outcome is never success (arbiter clause). TECH_FAILED launches no successors.

## Run

Local: `./gradlew quarkusDev` against the compose stack (`dcre-infra`). Cluster: `./gradlew build -x test && docker build -f src/main/docker/Dockerfile.jvm -t dcre-agt:m1 . && kind load docker-image dcre-agt:m1 --name dcre-dev && kubectl apply -f k8s/`. Config via env: `AGT_DB_URL`, `DCRE_EXCHANGE_ROOT`, `AGT_NAMESPACE`, `OTEL_EXPORTER_OTLP_ENDPOINT` (12FactorApp; clean clone runs with defaults).

## Tests

`./gradlew test`: Testcontainers CockroachDB v26.2.3 (ledger constraints, lease CAS/takeover, arrival dup/quarantine) + pure DAG-logic tests. E2E + chaos runs live in the sprint runbook (dcre-infra).

Metrics: `agt_lease_held`, `agt_file_arrivals_total{status}`, `agt_launch_intents_total{status}`, `agt_stage_outcomes_total{outcome}` on the `dcre-agt` Grafana dashboard.
