# dcre-agt

Collections Agent: the only long-running service in the DCRE Collections pipeline.

## What it does

AGT watches the per-client inbound exchange drop zones, registers stable file arrivals into a durable ledger (SHA-256 content identity, R-31 filename tokens, same-key-different-hash quarantine), and drives the request DAGs (DC Collections, ENDO Payments, and M10 Mandates `MRR -> MRV -> MAS -> MIT -> [MIR, MRW]`, R-36) level-triggered from that ledger: minting each pipeline stage as a write-ahead, deterministically-named Kubernetes Job, then observing its externally reported termination facts as the sole authority for stage completion (R-33). Response routes are token-picked: `fint-resp` launches the per-token reader (IXR/SXR/PXR), `fint-resp-man` picks the matching mandate leg reader the same way (MIX/MSX/MPX, SCRUM-91). It also launches the clock-driven executors (CRW process-date, PRG payment-status, HCS holiday-calendar-sync, MRG mandates-report) on interval windows, reconciles Jobs against intents after any restart, and bounds/relaunches same-identity Jobs that die mid-run (OrphanSweeper) instead of leaving an arrival stuck.

## Architecture and principles

- **SOLID**: each control loop is a single-responsibility `@ApplicationScoped` bean with one job: `DirectoryWatcher` (arrival discovery), `ArrivalService` (claim/hash/dedup), `DagEngine` (pure DAG decision logic, unit-testable via `computeLaunches()`/`terminalState()`), `JobLauncher` (write-ahead Job creation), `OutcomeWatcher` (termination observation), `Reconciler` (post-restart/reap recovery), `OrphanRelauncher` (bounded same-identity relaunch), `LeaseService` (single-writer CAS lease), `CrwScheduler`/`PrgScheduler`/`HcsScheduler`/`MrgScheduler` (clock windows), `MetricsService` (ledger-derived gauges).
- **12FactorApp Alignment** (https://12factor.net/): every override point is `${ENV_VAR:default}` in `application.yml` with a working dev default committed (clean clone runs with no `.env`); config never hardcodes URLs/images/secrets; the process is stateless (all state lives in `agt_ops` and the K8s API, never in memory alone: `LeaseService`/`Reconciler` rebuild everything from the ledgers on restart); CockroachDB, Kubernetes, and the OTLP collector are attached backing services reached only via env-configured endpoints.
- **Layer-first packages**: `config` (SmallRye `@ConfigMapping`), `domain` (enums/records: `Stage`, `Outcome`, `ArrivalStatus`, `FileArrival`, `LaunchIntent`), `repo` (JDBC access to the three ledgers), `service` (control loops).
- **Idempotent restart semantics**: `launch_intent` is write-ahead (intent row inserted before the Job), so a crash between intent-insert and Job-create is safe; Job names embed the full 128-bit arrival UUID (`dcre-<stage>-<uuid-no-dashes>`), so a 409 Conflict on re-create is treated as already-launched; `stage_outcome` inserts are `UNIQUE(intent_id, attempt)`, so re-observation is a no-op; the `Reconciler` (every 5s) is status-driven, never blind-recreates a `LAUNCHED` intent, and resolves reaped-before-observed Jobs from the durable `/exchange/outcomes/<job>` seam before falling back to a bounded relaunch.
- **OrphanSweeper** (`Reconciler` + `OrphanRelauncher`): a `LAUNCHED` intent whose current attempt ended TECH-class (dead pod, no live Job, no readable outcome after the reap grace window) gets a same-identity Job recreate up to `agt.orphan-max-attempts` (default 3) with `agt.orphan-backoff-seconds` (default 60) between attempts; exhausting the budget records a terminal `TECH_EXHAUSTED` outcome and fails the arrival (`DAG_FAILED`) rather than leaving a silent zombie. Business outcomes are never relaunched; clock intents self-heal at the next window boundary instead.
- **Fail-closed defaults**: unparseable filenames, unknown fint-resp tokens (either response route), and missing stage images all quarantine/refuse rather than guess; `BUSINESS_FILE_REJECTED`/`BUSINESS_FILE_FATAL` route to the route family's responder only (CIR, or MIR on `onhost-req-man`: whole-file NACK); a tech-failed responder keeps the arrival open instead of closing it without a response ever reaching OnHost.

## Prerequisites

- Java 25
- Docker (for `./gradlew test`, which runs against a real Testcontainers CockroachDB, and for building/loading images)
- A running `dcre-dev` kind cluster + CockroachDB + exchange volume, provisioned by `dcre-infra` (see Quickstart)

## Quickstart

Clean clone runs with no `.env`: every setting in `application.yml` has a working dev default.

```bash
# 1. bring up the dev cluster + CRDB + exchange hostPath (from dcre-infra)
../../../../../infra/dcre-infra/scripts/kind-up.sh

# 2. run AGT locally against that stack
./gradlew quarkusDev
```

AGT resolves `agt.exchange-root` (default `../../../../../infra/dcre-infra/exchange`) relative to its own working directory, so `quarkusDev` from this module directory lines up with the `dcre-infra` checkout as a sibling under `env/repo`.

## Configuration

12FactorApp Alignment (https://12factor.net/): all values below are `${ENV_VAR:default}` in `src/main/resources/application.yml`; override via real environment variables, never by editing the file.

| Variable | Default | Purpose |
|---|---|---|
| `AGT_DB_URL` | `jdbc:postgresql://localhost:26257/agt_ops?sslmode=disable` | JDBC URL for AGT's own ledger DB (`agt_ops`) |
| `AGT_DB_USER` | `root` | `agt_ops` datasource username |
| `AGT_DB_PASSWORD` | (empty) | `agt_ops` datasource password |
| `DCRE_EXCHANGE_ROOT` | `../../../../../infra/dcre-infra/exchange` | Root of the exchange directory tree (R-30 contract) |
| `AGT_NAMESPACE` | `dcre` | AGT's own CONTROL namespace only (K8s client, deployment, crdb/lgtm shared infra); stage Jobs launch into the flow namespaces below (SCRUM-70) |
| `AGT_NAMESPACE_COL` | `dcre-col` | Flow namespace for Collections stage Jobs (`col-*`: onhost-req, CRW window, HCS, collections-client PRG/fint-resp) |
| `AGT_NAMESPACE_PAY` | `dcre-pay` | Flow namespace for Payments stage Jobs (`pay-*`: onhost-req-endo, pay-client PRG/fint-resp) |
| `AGT_NAMESPACE_MAN` | `dcre-man` | Flow namespace for Mandates stage Jobs (`man-*`: onhost-req-man, fint-resp-man, MRG windows) |
| `AGT_PAY_CLIENTS` | `FNBRF01` | INTERIM (R-42) comma-separated client tokens on the pay flow, until the R-14 client table lands; trimmed + uppercased on read |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4317` | OTLP endpoint for traces/metrics export |
| `AGT_LAUNCH_ENABLED` | `true` | Gate for K8s Job creation; observation stays on independently |
| `AGT_CRR_IMAGE` / `AGT_CTV_IMAGE` / `AGT_CIR_IMAGE` / `AGT_CDE_IMAGE` / `AGT_CRW_IMAGE` | (empty) | Stage images; a missing image fails the launch fast (no stub fallback) |
| `AGT_IXR_IMAGE` | `dcre-ixr:m4` | fint-resp ISR reader image |
| `AGT_SXR_IMAGE` | `dcre-sxr:m4` | fint-resp SBSR reader image |
| `AGT_PXR_IMAGE` | `dcre-pxr:m4` | fint-resp PBSR reader image |
| `AGT_PRG_IMAGE` | `dcre-prg:m4` | PRG clock-window executor image |
| `AGT_AIS_IMAGE` | `dcre-ais:m5` | AIS endorsements stage image (ENDO route) |
| `AGT_HCS_IMAGE` | `dcre-hcs:m6` | HCS holiday-calendar-sync clock executor image |
| `AGT_MRR_IMAGE` / `AGT_MRV_IMAGE` / `AGT_MAS_IMAGE` / `AGT_MIT_IMAGE` / `AGT_MIR_IMAGE` / `AGT_MRW_IMAGE` / `AGT_MIX_IMAGE` / `AGT_MSX_IMAGE` / `AGT_MPX_IMAGE` / `AGT_MRG_IMAGE` | (empty) | M10 mandates stage images (SCRUM-79); absent/empty = launch-disabled until the 2.3 release line |
| `AGT_MAN_CLIENTS` | `FNBCC01,FNBCC02,FNBRF01` | INTERIM comma-separated mandate-capable client tokens (MRG windows launch only for these), until the R-14 client table lands; trimmed + uppercased on read |
| `AGT_CRW_INTERVAL_SECONDS` | `60` | CRW Process-Date Executor window length |
| `AGT_PRG_INTERVAL_SECONDS` | `60` | PRG clock-window length |
| `AGT_HCS_INTERVAL_HOURS` | `6` | HCS holiday-sync re-sync cadence |
| `AGT_MRG_INTERVAL_SECONDS` | `60` | MRG mandates-report clock-window length |
| `AGT_MRG_SUSPEND_INTERVAL_SECONDS` | `60` | MRG suspension-sweep clock-window length (SCRUM-91); mandate expiry is a view predicate and has no sweep |
| `AGT_SERVICE_DB_URL` | `jdbc:postgresql://crdb.dcre.svc.cluster.local:26257/dcre_col?sslmode=disable` | JDBC URL handed to launched stage Jobs for `dcre_col` (FQDN: stage pods run in the flow namespaces, where the short `crdb` name does not resolve) |
| `AGT_MAN_SERVICE_DB_URL` | `jdbc:postgresql://crdb.dcre.svc.cluster.local:26257/dcre_man?sslmode=disable` | JDBC URL for `dcre_man`; the DB URL follows the stage's flow family. Handed to the M10 mandates stage Jobs (`MRR`..`MRG`) as their primary DB, AND to every CTV stage pod as `DCRE_CTV_MANDATES_DB_URL` for CTV's second, read-only projection datasource: CTV stays on `dcre_col` primarily, so it reuses this knob rather than a second URL to keep in step. CTV fails at startup in-cluster if that variable is unset, so this value is load-bearing on the collections flow too |
| `AGT_CTV_MANDATE_SOURCE` | `legacy` | Which mandate store CTV's DC-flow gate reads, handed to every CTV stage pod as `DCRE_CTV_MANDATE_SOURCE` (SCRUM-91). `legacy` reads the `mandate` table in `dcre_col`; `projection` reads `man_ctv_view` in `dcre_man`. The token is carried verbatim and never interpreted here: CTV owns the vocabulary, so a new mode needs no AGT change. The default matches CTV's own yml default, so the seam is inert until it is set; nothing else injects it, so without it an in-cluster CTV is frozen on `legacy` |
| `AGT_STAGE_MEMORY_LIMIT` | `768Mi` | Stage-pod memory limit |
| `AGT_STAGE_DEADLINE_SECONDS` | `900` | Stage Job `activeDeadlineSeconds` |
| `AGT_ORPHAN_MAX_ATTEMPTS` | `3` | OrphanSweeper: bounded same-identity relaunch attempts |
| `AGT_ORPHAN_BACKOFF_SECONDS` | `60` | OrphanSweeper: minimum seconds between relaunch attempts |
| `HOSTNAME` | `local-agt` | DB lease holder id (must be unique per running instance) |

Per-client inbound exchange layout (clients `FNBCC01`, `FNBCC02`, `FNBRF01`, each with `onhost-req`/`onhost-req-endo`/`fint-resp`/`onhost-req-man`/`fint-resp-man` channels) is data in `agt.exchange.clients.*`, not an env var; edit `application.yml` to add a client.

## Testing

```bash
./gradlew test
```

Runs against a real `cockroachdb/cockroach:v26.2.3` Testcontainer (ledger constraints, lease CAS/takeover, arrival dedup/quarantine, OrphanSweeper relaunch/exhaustion, infrastructure-vs-job failure classification) plus pure DAG-logic unit tests (`DagEngineTest`, `ClockJobNameTest`). Broader e2e and chaos (kill/resume) runs live in the sprint runbook in `dcre-infra`.

`ConfigFailureClassificationTest` covers the `TECH_CONFIG_FAILED` class: a Failed Job whose pod exited **78** (`EX_CONFIG`, platform-batch's reserved code for a failure before the runner phase) is an infrastructure startup failure, not a job outcome. It is still retried, but on `agt.infra-max-attempts` instead of the 3-attempt `agt.orphan-max-attempts` budget, so a cfg restart or a Vault re-seed cannot turn a defect-free arrival into a terminal `DAG_FAILED` in minutes. The ceiling stays bounded: exhaustion still mints `TECH_EXHAUSTED` and fails the DAG (`OrphanSweepTest`, `StaleHeartbeatSweepTest`).

## Local cluster deployment

```bash
./gradlew build -x test
docker build -f src/main/docker/Dockerfile.jvm.prod -t dcre-agt:<tag> .
kind load docker-image dcre-agt:<tag> --name dcre-dev
kubectl apply -f k8s/
```

`Dockerfile.jvm.prod` is the Alpine (`eclipse-temurin:25-jre-alpine`) production image (engineering standing rule: prefer Alpine over the Quarkus-generated `Dockerfile.jvm`, which is UBI9-based and kept only as the generated default). `k8s/10-agt-deployment.yml` runs AGT itself as a `Deployment` (`replicas: 1`, `strategy: Recreate`) with a readiness probe on `/q/health/ready`; AGT in turn mints each pipeline stage as a short-lived `Job` (never a `Deployment`) via `JobLauncher`, mounting the same `dcre-exchange` PVC and passing identifying params as program arguments.

Ledger-derived metrics on the `dcre-agt` Grafana dashboard: `agt_lease_held`, `agt_file_arrivals_total{status}`, `agt_launch_intents_total{status}`, `agt_stage_outcomes_total{outcome}`.

## Related repositories

- https://github.com/sean-huni/dcre-crr
- https://github.com/sean-huni/dcre-ctv
- https://github.com/sean-huni/dcre-cde
- https://github.com/sean-huni/dcre-cir
- https://github.com/sean-huni/dcre-crw
- https://github.com/sean-huni/dcre-ixr
- https://github.com/sean-huni/dcre-sxr
- https://github.com/sean-huni/dcre-pxr
- https://github.com/sean-huni/dcre-prg
- https://github.com/sean-huni/dcre-ais
- https://github.com/sean-huni/dcre-hcs
- https://github.com/sean-huni/dcre-platform-model
- https://github.com/sean-huni/dcre-platform-files
- https://github.com/sean-huni/dcre-platform-batch
- https://github.com/sean-huni/dcre-platform-persistence
- https://github.com/sean-huni/dcre-infra
- https://github.com/sean-huni/dcre-fixture-toolkit
- https://github.com/sean-huni/dcre-design-register
- https://github.com/sean-huni/dcre-rpt
