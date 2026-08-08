# dcre-agt

Collections Agent: the only long-running service in the DCRE Collections pipeline.

## What it does

AGT watches the per-client inbound exchange drop zones, registers stable file arrivals into a durable ledger (SHA-256 content identity, R-31 filename tokens, same-key-different-hash quarantine), and drives the three request DAGs level-triggered from that ledger: minting each pipeline stage as a write-ahead, deterministically-named Kubernetes Job, then observing its externally reported termination facts as the sole authority for stage completion (R-33). It also launches the clock-driven executors (CRW process-date, CRG/PRG/MRG report windows, HCS holiday-calendar-sync) on interval windows, reconciles Jobs against intents after any restart, and bounds/relaunches same-identity Jobs that die mid-run (OrphanSweeper) instead of leaving an arrival stuck.

## The roster IS the diagrams

THE DIAGRAMS ARE THE SPECIFICATION (`design-register/docs/diagrams`, R-49). AGT launches exactly the 28 stage services on the six sheets plus the cross-family HCS, and nothing else. `StageRosterTest` compares `Stage` to the sheets as a SET, never a size: collections once held 9 of 9 required services with four misnamed, and a count check reported 9/9 and passed.

```
collections   CRR CTV CDE CRW CIR   CIX CSX CPX   CRG     -> dcre_col
payments      PRR PTV PAI PRW PIR   PIX PSX PPX   PRG     -> dcre_pay
mandates      MRR MRV MAS MIT MIR MRW   MIX MSX MPX MRG   -> dcre_man
cross-family  HCS                                         -> dcre_col
```

**PRG IS THE PAYMENTS REPORT GENERATOR.** Before the 2026-08-08 cutover the same token named the COLLECTIONS one, which is now CRG. The token did not move, it changed MEANING, so a find-and-replace over this repository produces a build that compiles and is semantically inverted. Read every occurrence in context.

Each family's request DAG is on its own sheet, and no stage appears in two of them:

- DC Collections: `CRR -> CTV -> {CDE -> CRW, CIR}`
- ENDO Payments: `PRR -> PTV -> PAI -> {PRW, PIR}`
- Mandates: `MRR -> MRV -> MAS -> MIT -> {MIR, MRW}`

Response routes are token-picked, and the FLOW selects the family's reader: `fint-resp` carries both collections and payments replies (both families send pain.008 and the reply has no family marker), so it launches CIX/CSX/CPX or PIX/PSX/PPX by the reading client's flow; `fint-resp-man` picks MIX/MSX/MPX.

### The timing rule

Owner, 2026-08-08: "CRW TxList are processed on the collection-day, but for payments Tx's processed immediately."

Collections waits: CRW is a clock-driven Process-Date Executor, CDE estimates the collection day, and a DC arrival stays `DAG_RUNNING` until `crw_emission_owed` says nothing is owed. **Payments does not wait at all**: there is no CDE analogue on the payments sheet, PRW is a real DAG stage that runs the moment PAI accepts, and `RouteDags.ENDO` carries `Emission.NONE`. PRW forks from CRW and both emit `pain.008`, which makes the collection-day wait easy to inherit silently, so it is asserted in both directions in `EmissionGatedTerminalTest` rather than described.

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
| `AGT_NAMESPACE_COL` | `dcre-col` | Flow namespace for Collections stage Jobs (`col-*`: onhost-req, CRW window, HCS, collections-client CRG/fint-resp) |
| `AGT_NAMESPACE_PAY` | `dcre-pay` | Flow namespace for Payments stage Jobs (`pay-*`: onhost-req-endo, pay-client PRG/fint-resp) |
| `AGT_NAMESPACE_MAN` | `dcre-man` | Flow namespace for Mandates stage Jobs (`man-*`: onhost-req-man, fint-resp-man, MRG windows) |
| `AGT_PAY_CLIENTS` | `FNBRF01` | INTERIM (R-42) comma-separated client tokens on the pay flow, until the R-14 client table lands; trimmed + uppercased on read |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4317` | OTLP endpoint for traces/metrics export |
| `AGT_LAUNCH_ENABLED` | `true` | Gate for K8s Job creation; observation stays on independently |
| `AGT_CRR_IMAGE` / `AGT_CTV_IMAGE` / `AGT_CDE_IMAGE` / `AGT_CRW_IMAGE` / `AGT_CIR_IMAGE` / `AGT_CIX_IMAGE` / `AGT_CSX_IMAGE` / `AGT_CPX_IMAGE` / `AGT_CRG_IMAGE` | (empty) | The nine COLLECTIONS stage images. `CIX`/`CSX`/`CPX` are the three fint-resp leg readers (ISR/SBSR/PBSR), formerly `IXR`/`SXR`/`PXR`; **`CRG` is the collections report generator**, formerly called `PRG` |
| `AGT_PRR_IMAGE` / `AGT_PTV_IMAGE` / `AGT_PAI_IMAGE` / `AGT_PRW_IMAGE` / `AGT_PIR_IMAGE` / `AGT_PIX_IMAGE` / `AGT_PSX_IMAGE` / `AGT_PPX_IMAGE` / `AGT_PRG_IMAGE` | (empty) | The nine PAYMENTS stage images. `PAI` is the Account Init Service, formerly `AIS` on the collections family; **`AGT_PRG_IMAGE` CHANGED MEANING at the 2026-08-08 cutover: it named the collections report generator and now names the payments one** |
| `AGT_MRR_IMAGE` / `AGT_MRV_IMAGE` / `AGT_MAS_IMAGE` / `AGT_MIT_IMAGE` / `AGT_MIR_IMAGE` / `AGT_MRW_IMAGE` / `AGT_MIX_IMAGE` / `AGT_MSX_IMAGE` / `AGT_MPX_IMAGE` / `AGT_MRG_IMAGE` | (empty) | The ten MANDATES stage images |
| `AGT_HCS_IMAGE` | (empty) | HCS holiday-calendar-sync clock executor image (cross-family; on no sheet) |
| `AGT_MAN_CLIENTS` | `FNBCC01,FNBCC02,FNBRF01` | INTERIM comma-separated mandate-capable client tokens (MRG windows launch only for these), until the R-14 client table lands; trimmed + uppercased on read |
| `AGT_CRW_INTERVAL_SECONDS` | `60` | CRW Process-Date Executor window length (COLLECTIONS ONLY: this is the collection-day clock, and payments has no analogue) |
| `AGT_CRG_INTERVAL_SECONDS` | `60` | CRG collections-report clock-window length |
| `AGT_PRG_INTERVAL_SECONDS` | `60` | PRG payments-report clock-window length. Its own knob, not shared with CRG: collections transaction lists are processed ON the collection day and payments transactions IMMEDIATELY, so the two cadences have no reason to move together |
| `AGT_HCS_INTERVAL_HOURS` | `6` | HCS holiday-sync re-sync cadence |
| `AGT_MRG_INTERVAL_SECONDS` | `60` | MRG mandates-report clock-window length |
| `AGT_MRG_SUSPEND_INTERVAL_SECONDS` | `60` | MRG suspension-sweep clock-window length (SCRUM-91); mandate expiry is a view predicate and has no sweep |
| `AGT_COLLECTIONS_DB_URL` | `jdbc:postgresql://localhost:26257/dcre_col?sslmode=disable` | AGT's own read-only window into `dcre_col` (`prg_report_due`, `prg_sla_pending`, `crw_emission_owed`) |
| `AGT_PAYMENTS_DB_URL` | `jdbc:postgresql://localhost:26257/dcre_pay?sslmode=disable` | AGT's own read-only window into `dcre_pay`. **A separate READ SEAM, not a rename**: both databases publish views called `prg_report_due` and `prg_sla_pending`, so a single datasource leaves one family's IMMEDIATE reports permanently undiscovered, with no exception and no log line |
| `AGT_SERVICE_DB_URL` | `jdbc:postgresql://crdb.dcre.svc.cluster.local:26257/dcre_col?sslmode=disable` | JDBC URL handed to launched COLLECTIONS stage Jobs for `dcre_col`, and to HCS (FQDN: stage pods run in the flow namespaces, where the short `crdb` name does not resolve) |
| `AGT_PAY_SERVICE_DB_URL` | `jdbc:postgresql://crdb.dcre.svc.cluster.local:26257/dcre_pay?sslmode=disable` | JDBC URL handed to launched PAYMENTS stage Jobs. Without it every payments stage receives the collections URL and builds the payments schema inside `dcre_col` without erroring, because the write is perfectly valid against the wrong database (`StageDatabases` resolves the three families with no default arm) |
| `AGT_MAN_SERVICE_DB_URL` | `jdbc:postgresql://crdb.dcre.svc.cluster.local:26257/dcre_man?sslmode=disable` | JDBC URL for `dcre_man`; the DB URL follows the stage's flow family. Handed to the M10 mandates stage Jobs (`MRR`..`MRG`) as their primary DB, AND to every CTV stage pod as `DCRE_CTV_MANDATES_DB_URL` for CTV's second, read-only projection datasource: CTV stays on `dcre_col` primarily, so it reuses this knob rather than a second URL to keep in step. CTV fails at startup in-cluster if that variable is unset, so this value is load-bearing on the collections flow too |
| `AGT_CTV_MANDATE_SOURCE` | `projection` | Which mandate store CTV's DC-flow gate reads, handed to every CTV stage pod as `DCRE_CTV_MANDATE_SOURCE` (SCRUM-107). The vocabulary is `projection` only: it reads `man_ctv_view` in `dcre_man`, and the `dcre_col.mandate` table the retired `legacy` value selected has been dropped. CTV FAILS CLOSED on `legacy`, so handing that value to a stage pod stops the pod from starting rather than degrading to a different gate. The token is carried verbatim and never interpreted here: CTV owns the vocabulary |
| `AGT_REPORT_STALL_SCANS` | `20` | Bounded-attempt guard on the IMMEDIATE report trigger: consecutive scans that may see the same (flow, client, parent) still due before AGT WARNs. It makes a silent loop VISIBLE; it never retries, widens or falls back |
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

Collections: `dcre-crr` `dcre-ctv` `dcre-cde` `dcre-crw` `dcre-cir` `dcre-cix` `dcre-csx` `dcre-cpx` `dcre-crg`
Payments: `dcre-prr` `dcre-ptv` `dcre-pai` `dcre-prw` `dcre-pir` `dcre-pix` `dcre-psx` `dcre-ppx` `dcre-prg`
Mandates: `dcre-mrr` `dcre-mrv` `dcre-mas` `dcre-mit` `dcre-mir` `dcre-mrw` `dcre-mix` `dcre-msx` `dcre-mpx` `dcre-mrg`

The pre-cutover repositories (`dcre-ixr`, `dcre-sxr`, `dcre-pxr`, `dcre-ais`) are archived per R-48 and their images are no longer built.

- https://github.com/sean-huni/dcre-hcs
- https://github.com/sean-huni/dcre-platform-model
- https://github.com/sean-huni/dcre-platform-files
- https://github.com/sean-huni/dcre-platform-batch
- https://github.com/sean-huni/dcre-platform-persistence
- https://github.com/sean-huni/dcre-infra
- https://github.com/sean-huni/dcre-fixture-toolkit
- https://github.com/sean-huni/dcre-design-register
- https://github.com/sean-huni/dcre-rpt
