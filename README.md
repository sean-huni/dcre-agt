# dcre-agt

The DCRE orchestrator. AGT is the only long-running service in the platform: everything
else is a short-lived Kubernetes `Job` that AGT mints, watches and reaps.

AGT watches the per-client inbound exchange drop zones, claims each stable file arrival
into a durable ledger (SHA-256 content identity, R-31 filename tokens, same-key-different-hash
quarantine), and drives the three request DAGs level-triggered from that ledger. Each pipeline
stage becomes a write-ahead, deterministically-named Kubernetes `Job`; AGT then observes the
externally reported termination facts as the sole authority for stage completion (R-33). It
also launches the clock-driven executors (CRW process-date, CRG/PRG/MRG report windows, the
MRG suspension sweep, HCS holiday-calendar sync), reconciles Jobs against intents after any
restart, and relaunches same-identity Jobs that die mid-run instead of leaving an arrival stuck.

**AGT is the only component that decides which database and which namespace each stage runs
against.** That decision is two separate exhaustive switches, and the section
[How stage routing works](#how-stage-routing-works) is the one to read before changing anything.

Two entry points, depending on why you are here. To CHANGE AGT, read
[How stage routing works](#how-stage-routing-works). To MAKE AGT DO SOMETHING, read
[Exercise it end to end](#exercise-it-end-to-end): building and deploying AGT is not sufficient,
because a deployed AGT with no arrival file and no stage images processes nothing and reports
nothing.

State lives in AGT's own `agt_ops` database and in the Kubernetes API. Nothing lives only in
memory: `LeaseService` and `Reconciler` rebuild everything from the ledgers on restart.

---

## The v1 roster and the five databases

THE DIAGRAMS ARE THE SPECIFICATION (repository `dcre-design-register`, `docs/diagrams`, R-49). AGT
launches exactly the 28 stage services on the six sheets plus the cross-family HCS, and
nothing else. `Stage` is a transcription of them.

The register is checked out on this machine as `env/repo/be/java/spring/dcre/design-register`,
under the directory name `design-register` rather than the repository name, so searching the
filesystem for `dcre-design-register` finds nothing. The six sheets are
`dcre-{collections,payments,mandates}-{req,res}.png`.

```
family        stages                                            database    namespace
------------  ------------------------------------------------  ----------  ---------
collections   CRR CTV CDE CRW CIR   CIX CSX CPX   CRG            dcre_col    dcre-col
payments      PRR PTV PAI PRW PIR   PIX PSX PPX   PRG            dcre_pay    dcre-pay
mandates      MRR MRV MAS MIT MIR MRW   MIX MSX MPX   MRG        dcre_man    dcre-man
cross-family  HCS                                               dcre_hcs    dcre-col
```

**HCS is the row to read twice.** It is hosted in the *collections namespace* and it writes
its *own database*. Those are two different answers to two different questions, and until
2026-08-08 one enum gave both. See [How stage routing works](#how-stage-routing-works).

The fifth database is `agt_ops`, AGT's own ledger. It is deliberately not a family: it is not
sharded by flow, and every stage pod receives it under its own name (`DCRE_AGTOPS_DB_URL`) for
the platform-batch heartbeat writer.

| Database | Owner | AGT's relationship to it |
|---|---|---|
| `agt_ops` | AGT | Read/write. Its own ledgers: `file_arrival`, `launch_intent`, `stage_outcome`, `duplicate_delivery`, and `agt_lease`. |
| `dcre_col` | collections services | Read-only, via a second datasource, on published views only. Also handed to collections stage pods. |
| `dcre_pay` | payments services | Read-only, via a third datasource, on published views only. Also handed to payments stage pods. |
| `dcre_man` | mandates services | AGT never opens a datasource here. It only hands the URL to pods. |
| `dcre_hcs` | `dcre-hcs` | AGT never opens a datasource here. It only hands the URL to pods. |

**`PRG` IS THE PAYMENTS REPORT GENERATOR.** Before the 2026-08-08 cutover the same token named
the COLLECTIONS one, which is now `CRG`. The token did not move, it changed MEANING, so a
find-and-replace over this repository produces a build that compiles and is semantically
inverted. Read every occurrence in context. Pinned by
`StageRosterTest.prgIsThePaymentsGeneratorAndCrgIsTheCollectionsOne`.

Each family's request DAG is on its own sheet, and no stage appears in two of them:

- DC Collections: `CRR -> CTV -> {CDE -> CRW, CIR}`
- ENDO Payments: `PRR -> PTV -> PAI -> {PRW, PIR}`
- Mandates: `MRR -> MRV -> MAS -> MIT -> {MIR, MRW}`

Response routes are token-picked, and the FLOW selects the family's reader. `fint-resp`
carries both collections and payments replies (both families send pain.008 and the reply has
no family marker), so it launches CIX/CSX/CPX or PIX/PSX/PPX by the reading client's flow;
`fint-resp-man` picks MIX/MSX/MPX.

### The timing rule

Owner, 2026-08-08: "CRW TxList are processed on the collection-day, but for payments Tx's
processed immediately."

Collections waits: CRW is a clock-driven Process-Date Executor, CDE estimates the collection
day, and a DC arrival stays `DAG_RUNNING` until `crw_emission_owed` says nothing is owed.
**Payments does not wait at all.** There is no CDE analogue on the payments sheet, PRW is a
real DAG stage that runs the moment PAI accepts, and `RouteDags.ENDO` carries `Emission.NONE`.
PRW forks from CRW and both emit `pain.008`, which makes the collection-day wait easy to
inherit silently, so it is asserted in both directions in `service/EmissionGatedTerminalTest`
rather than described.

---

## How stage routing works

This is the thing people get wrong, so it is written out rather than left to the code.

There are **two separate exhaustive switches over `Stage`**, and the separation is deliberate:

| Question | Answered by | Returns |
|---|---|---|
| Which DATABASE does this stage's pod write? | `service/StageDatabases.dbFamily(Stage)` | `domain/DbFamily` (COL, PAY, MAN, HCS) |
| Which NAMESPACE does this stage's Job land in? | `service/StageNamespaces.namespaceFamilyOf(Stage)` | `domain/Flow` (COL, PAY, MAN) |

They were one switch until 2026-08-08. While the two answers agreed the conflation was
invisible. **HCS is where they diverge**: it is hosted in `dcre-col` and it owns `dcre_hcs`.
One enum answering two questions gives the wrong answer to one of them the moment they
diverge, silently, because both answers are well-formed and a write to the wrong database is
perfectly valid SQL.

### Neither switch has a default arm, and that is the feature

Adding a constant to `Stage` **breaks the build** in `StageDatabases`, `StageNamespaces` and
`StageImages` alike, until somebody names its database, its namespace and its image. There is
no "everything else" arm anywhere in the routing path. `JobLauncher.LAUNCHABLE` is a fourth
tripwire: it lists every launchable stage explicitly, and `StageRosterTest` asserts it equals
all of `Stage`, so a new constant fails that assertion until it is listed deliberately. It used
to be an `EnumSet.complementOf`, which failed OPEN: every stage added afterwards became
launchable by default and nothing said so.

This has been proved in both directions by one short-lived constant. `ACS` was added on
2026-08-08 and broke every switch until its database was named. It was retired on 2026-08-09
and **removing it broke them again**, so the deletion could not be half-done. A `Map` lookup or
an `EnumSet.complementOf` would have absorbed both silently.

Treat a compile error here as the design working. Do not add a default arm to make it go away.

### The two fail-closed guards on top

1. **`StageDatabases.requireAddresses`.** The family's configured URL must address the database
   that family owns, compared by parsing the database segment out of the JDBC URL. Pointing
   `AGT_PAY_SERVICE_DB_URL` at `dcre_col` is a one-variable typo that otherwise reads as a
   working deployment. It fails at Job-BUILD time, before a pod exists, and the message names
   both sides. This is the AGT-side twin of `shared/hcs`'s `FamilyGuard`, which compares
   `current_database()` before any DDL.
2. **`StageNamespaces.requireCorrectNamespace`.** The namespace a Job is going into must be the
   one its stage is hosted in. The namespace comes from the durable intent row, so this also
   catches a reconciled re-create that would rebuild a pod into the wrong family.

Between them, a Job cannot be built into the wrong namespace and a pod cannot be handed the
wrong database.

### Route to flow

`FlowNamespaces.flowForRoute` maps an inbound route to a flow, and **its default arm throws**
(SCRUM-107). It used to read `default -> Flow.COL`, which meant an unrecognised route resolved
to collections: observed live, an unknown route produced a CRR Job in `dcre-col`. A catch-all
that returns the happy path cannot tell "collections" from "I have never heard of this route".

| Route | Flow |
|---|---|
| `onhost-req` | COL |
| `onhost-req-endo` | PAY |
| `onhost-req-man`, `fint-resp-man` | MAN |
| `fint-resp` | PAY if the reading client is in `AGT_PAY_CLIENTS`, else COL |
| anything else | `IllegalArgumentException` |

`FlowNamespaces.flowForNamespace` is the inverse, used when rebuilding a Job spec from a
durable intent row. The legacy control namespace (`dcre`) resolves to COL, because everything
that predates flow namespaces was collections. An unknown namespace throws.

### Tests that pin the routing

| Test | Pins |
|---|---|
| `service/StageRosterTest` | `Stage` equals the diagrams' roster as a SET, every stage has exactly one database family and exactly one namespace family, `LAUNCHABLE` equals all of `Stage`, and PRG/CRG are not inverted |
| `service/StageDatabaseGuardTest` | The URL-addresses-the-right-database guard |
| `service/NamespaceRoutingTest` | Namespace resolution, legacy relaunch, and that no pod carries a retired account seam |
| `service/RouteRegistryConsistencyTest` | Route registry agreement across `RouteDags` / `DirectoryWatcher` / `FlowNamespaces` |
| `AgtCtvMandateSourceDefaultTest` | The `@WithDefault` and the yml default for `agt.ctv-mandate-source` agree (two homes for one fact) |

`StageRosterTest` compares against the sheets as a SET, never a size. Collections once held
9 of 9 required services with four of them misnamed, and a count check reported 9/9 and passed.

---

## Setup

Everything below is verified on macOS 26.5.2 (Darwin 25.5.0, arm64) on 2026-08-09. Nothing here
is macOS-specific except the Homebrew commands.

| Tool | Version needed | Check it | Get it |
|---|---|---|---|
| JDK | **25** (Temurin) | `java -version` | SDKMAN, see below |
| Gradle | none installed; the wrapper is **9.3.1** | `./gradlew --version` | Ships in the repo |
| Docker | any recent engine, running | `docker info` | Docker Desktop or colima |
| kubectl | any 1.3x | `kubectl version --client` | `brew install kubectl` |
| kind | any recent | `kind get clusters` | `brew install kind` |
| Python 3 | for the manifest-arch check only | `python3 --version` | Preinstalled on macOS |

### JDK 25 via SDKMAN

`.sdkmanrc` pins `java=25-tem` and is the single source of truth for the JDK. The build does
**not** declare a Gradle toolchain block; it declares `sourceCompatibility` and
`targetCompatibility` as `JavaVersion.VERSION_25`, so whatever JDK Gradle runs on must already
be 25.

```bash
curl -s "https://get.sdkman.io" | bash          # once per machine
source "$HOME/.sdkman/bin/sdkman-init.sh"
cd <this repo>
sdk env install                                  # installs exactly what .sdkmanrc pins
sdk env                                          # activates it in this shell
java -version                                    # expect: openjdk version "25"
```

If `sdk env` does nothing automatically when you `cd` here, set `sdkman_auto_env=true` in
`~/.sdkman/etc/config`.

### Docker is required for the tests

`./gradlew test` boots a real `cockroachdb/cockroach:v26.2.3` Testcontainer. Without a running
Docker engine the suite cannot start. This is not optional and there is no mock fallback.

### The dev cluster

AGT needs a CockroachDB with the five databases, a `dcre-exchange` PVC, and the `dcre`,
`dcre-col`, `dcre-pay`, `dcre-man` namespaces. All of that is provisioned by `dcre-infra`,
which is a sibling checkout under `env/repo/infra`:

```bash
../../../../../infra/dcre-infra/scripts/kind-up.sh
```

Verify it landed:

```bash
kind get clusters                                # expect: dcre-dev
kubectl config current-context                   # expect: kind-dcre-dev
kubectl get ns | grep dcre                       # expect: dcre, dcre-col, dcre-pay, dcre-man
kubectl exec -n dcre crdb-0 -- ./cockroach sql --insecure --database=defaultdb \
  -e "SHOW DATABASES;"                           # expect all five, see Known gaps
```

`dcre-infra/scripts/verify-databases.sh` is the authoritative check and expects exactly
`agt_ops dcre_col dcre_man dcre_pay dcre_hcs`.

---

## Build

```bash
./gradlew clean build
```

Run on this working tree at commit `SCRUM-107-feat-three-family-topology` on 2026-08-09, with
Docker running and no `.env` present. The runner's own closing lines, plus the captured exit
code:

```
BUILD SUCCESSFUL in 3m 38s
14 actionable tasks: 14 executed
gradle_exit=0
elapsed_seconds=219
```

Both numbers came off a warm Gradle cache and an already-pulled CockroachDB image. A first-ever
clone that must resolve the Quarkus BOM and pull `cockroachdb/cockroach:v26.2.3` will take longer,
and how much longer is not measured here.

`clean build` runs the full test suite, so budget roughly **4 minutes**. Most of that is the
suite, not compilation.

### What it produces

| Path | What it is |
|---|---|
| `build/quarkus-app/quarkus-run.jar` | The fast-jar launcher. This is what the container runs. |
| `build/quarkus-app/app/agt-2.0.1.jar` | The application classes. Version comes from `build.gradle`. |
| `build/quarkus-app/lib/` | Dependency jars |
| `build/libs/agt-2.0.1.jar` | The plain Gradle jar. **Not** what the container runs. |

Note `build/quarkus-app/` is the Quarkus fast-jar layout. Copying `build/libs/agt-2.0.1.jar`
into an image on its own produces something that will not start.

### Tests only

```bash
./gradlew test                    # whole suite
./gradlew test --tests '*StageRosterTest'
./gradlew test --tests 'za.co.fnb.dcre.agt.service.*'
```

**Current suite: 241 tests in 35 test classes, 0 failures, 0 errors, 0 skipped.** Derived by
aggregating `build/test-results/test/TEST-*.xml` after the run above, not by reading a report
summary. There are 36 `.java` files under `src/test/java`; `CrdbTestResource.java` is a
`QuarkusTestResourceLifecycleManager` helper, not a test class, which is the difference between
36 and 35.

The suite mixes pure unit tests (`DagEngineTest`, `ClockJobNameTest`, `FlowNamespacesTest`,
the routing guards) with `@QuarkusTest` classes across several `TestProfile`s, each booting a
Quarkus context plus a CockroachDB container. `build.gradle` sets `maxHeapSize = '4g'` on the
`test` task for exactly this reason: Gradle's 512m default kills the worker with a message
that names no cause.

Expect noise in the log that is not failure:

- `Failed to export LogsRequestMarshaler ... Connection refused: localhost/127.0.0.1:4317` is
  the OTLP exporter with no collector running locally. Harmless.
- `QUARANTINED ...`, `report-stall ...`, `sla stage=FINT ...` lines are tests asserting the
  fail-closed paths, printed by the code under test.

---

## Run locally

```bash
./gradlew quarkusDev
```

AGT then listens on `http://localhost:8080`, with health at `/q/health/live` and
`/q/health/ready`.

### Clean-clone rule

**A fresh clone runs with no `.env` at all.** Every setting in
`src/main/resources/application.yml` is written as `${ENV_VAR:working-default}`, so the
committed defaults are a working dev configuration (12FactorApp Alignment,
https://12factor.net/). There is no `.env` and no `.env.example` in the repository; `.env` is
gitignored and is purely the override point.

Precedence, highest first: real environment variable, then `.env`, then the yml default.

Two things a clean clone still needs from outside the repo, and neither is a `.env` matter:

1. **A reachable CockroachDB.** `quarkus.liquibase.migrate-at-start` is `true` and the default
   `AGT_DB_URL` is `jdbc:postgresql://localhost:26257/agt_ops?sslmode=disable`. With nothing on
   26257 the process starts and then fails its migration. Run `kind-up.sh` first, or
   `kubectl port-forward -n dcre svc/crdb 26257:26257`.
2. **The exchange tree.** `agt.exchange-root` defaults to the relative path
   `../../../../../infra/dcre-infra/exchange`, which from this module resolves to
   `env/repo/infra/dcre-infra/exchange`. Verified 2026-08-09 with
   `realpath ../../../../../infra/dcre-infra/exchange` (exit 0). A relative default breaks
   silently if the module moves, and often by creating a wrong directory tree rather than
   erroring, so an unexplained new `exchange/` directory means this default no longer resolves.

### Running against a port-forwarded cluster

```bash
kubectl port-forward -n dcre svc/crdb 26257:26257 &
AGT_LAUNCH_ENABLED=false ./gradlew quarkusDev
```

`AGT_LAUNCH_ENABLED=false` stops AGT minting Jobs against whatever cluster your kubeconfig
points at. Set it whenever you run locally against a shared cluster. The two gates are fully
independent: `AGT_LAUNCH_ENABLED` gates Job creation and every clock scheduler,
`AGT_OBSERVE_ENABLED` gates `OutcomeWatcher` alone, and all four combinations are reachable.

The `%test` profile in `application.yml` disables the scheduler entirely, sets
`agt.launch-enabled: false`, uses a random HTTP port (8081 collides with kubectl
port-forwards), and points `exchange-root` at `build/test-exchange`.

---

## Exercise it end to end

Everything above gets AGT *running*. This section gets it *working*. A running AGT with no
arrivals and no stage images does nothing at all, and by design says nothing about it.

### 1. The exchange directory tree

AGT never invents a path. Every directory it touches is derived from `agt.exchange-root` plus the
per-client layout under `agt.exchange.clients.*` in `application.yml`.

```
<exchange-root>/
  <client>/                          fnbcc01, fnbcc02, fnbrf01  (LOWERCASE on disk)
    onhost-req/       in/  error/  archive/{inflight,duplicates}     <- AGT watches in/
    onhost-req-endo/  in/  error/  archive/{inflight,duplicates}     <- AGT watches in/
    onhost-req-man/   in/  error/  archive/{inflight,duplicates}     <- AGT watches in/
    fint-resp/        in/  error/  archive/{inflight,duplicates}     <- AGT watches in/
    fint-resp-man/    in/  error/  archive/{inflight,duplicates}     <- AGT watches in/
    onhost-resp/      out/ error/  archive/     outbound; AGT never watches these
    onhost-resp-man/  out/ error/  archive/     outbound
    fint-req/         out/ error/  archive/     outbound
    fint-req-man/     out/ error/  archive/     outbound
  outcomes/                          one file per Kubernetes Job name
  reference/account/<version>/       immutable versioned account-reference artifact
  chaos/                             fault-injection triggers
  .staging-drop/                     scratch dir for atomic drops
```

**The client directories are lowercase and the client TOKENS are uppercase, and both are load
bearing.** `application.yml` keys the map by the uppercase token (`FNBCC01`) and gives lowercase
directory paths as the values. The uppercase key becomes `file_arrival.client_token` and must
equal the `FNB...` token in the filename; the lowercase name is only the directory. Inbound
channels use `in/`, outbound channels use `out/`; AGT scans `in/` only, and there are ten channels
per client although AGT watches five of them.

Verified against the provisioned tree on 2026-08-09 with
`find <infra>/exchange/fnbcc01 -maxdepth 2 -type d` and `ls -A <infra>/exchange` (both exit 0).
That tree is provisioned by `dcre-infra`; in cluster the same tree is the `dcre-exchange` PVC
mounted at `/exchange`.

`<exchange-root>/outcomes/<jobName>` is the business-verdict seam and the thing to list first in
any diagnosis. Each file holds **one line: a single `domain/Outcome` enum name**, written by the
stage pod, read by `OutcomeWatcher`. Legal values are `BUSINESS_ACCEPTED`, `BUSINESS_PARTIAL`,
`BUSINESS_FILE_REJECTED`, `BUSINESS_FILE_FATAL`, `TECH_FAILED`, `TECH_EXHAUSTED`,
`TECH_CONFIG_FAILED`. A missing file means "not finished yet", and is retried on the next tick. A
present but unparseable file is `TECH_FAILED` with a WARN.

### 2. The arrival filename convention (the R-31 tokens)

Parsed in exactly one place, `service/ArrivalService` (the token block around lines 80-96), and it
is **lenient**:

```
<CLIENT>_<MsgId>[.<anything>]
```

- The extension is everything after the **last** `.` and is stripped before tokenising. AGT never
  inspects it; `.txt` and `.xml` both occur in practice.
- Split the stem on `_`. There must be at least one `_`, and token 0 must `startsWith("FNB")`.
  That is the entire syntactic rule: no length check, no whitelist, no character class.
- Token 0 is the filename client. **Everything after the first `_` is the MsgId**, underscores
  included, so the response-leg suffixes `_ISR` / `_SBSR` / `_PBSR` are part of the logical key and
  make three distinct logical files rather than three conflicting re-sends of one.
- The **directory** client is authoritative for the ledger, not the filename. A filename token that
  disagrees with the drop zone is quarantined as `CLIENT_PATH_MISMATCH` rather than believed.

Real examples, and where they come from:

| Filename | Origin |
|---|---|
| `FNBRF01_DCRERF2026072707365303.txt` | committed fixture, `dcre-infra` `fixtures/ctv-gate/` |
| `FNBRF01_DCRERF2026071120010002.txt` | committed fixture, `dcre-infra` `fixtures/warmup/`, dropped by `env-reset.sh` |
| `FNBCC02_<msgId>_ISR.xml` | `src/test/java/za/co/fnb/dcre/agt/DirectoryWatcherTest.java` |

The MsgId's internal shape is a **toolkit convention, not an AGT rule**. The generator builds it as
`<senderId><fileType><yyyyMMddHHmmss><vv>`, so `DCRERF2026072707365303` reads as sender `DCRE`,
file type `RF`, cut timestamp `20260727073653`, layout version `03`. The toolkit's own source
marks the token order and separator PROVISIONAL. AGT enforces none of it and will happily claim
`FNBCC01_ANYTHING.txt`.

Two rules that are not in the filename and bite anyway:

- `DirectoryWatcher` skips names starting with `.` and ending in `.tmp`, and requires the file size
  to be **stable across two ticks** (2s apart) before claiming. Write to a temp name and rename in,
  or accept a couple of seconds of latency.
- Re-dropping identical bytes on the same route is a content-hash duplicate and is deliberately
  ignored (a row lands in `duplicate_delivery`, nothing else happens). To replay, cut a fresh file
  with a new timestamp rather than copying the old one back.

### 3. Getting a file to drop

Three routes, cheapest first.

**Pre-cut fixtures.** `dcre-infra` carries committed books under `fixtures/`: `warmup/` (one tiny
file per route, which is what `scripts/env-reset.sh` drops to force every service's Liquibase to
build its tables), `ctv-gate/` (a book plus a per-row expectation manifest, plus its own README
with the exact re-cut command), and `mandate/`. Drop one with an atomic rename:

```bash
X=../../../../../infra/dcre-infra/exchange
cp $X/../fixtures/ctv-gate/<book>.txt $X/.staging-drop/
mv $X/.staging-drop/<book>.txt $X/fnbrf01/onhost-req/in/
```

**Cut a new one.** The generator is the `dcre-fixture-toolkit` repository, checked out on this
machine as `env/repo/be/python/dcre/fnb_dcre_ctv_toolkit` (directory name differs from the
repository name, verified 2026-08-09 by its README title and its `origin` remote). It is
stdlib-only Python 3 with every knob a CLI flag, and produces the fixed-width OnHost copybook plus
a sidecar manifest declaring the expected verdict per detail row, with an independent verifier
oracle alongside it. `fixtures/ctv-gate/README.md` in `dcre-infra` carries a working, copy-pasteable
cut command. Two things that README warns about and are easy to lose: pass a **fresh
`--timestamp` on every re-cut**, both to dodge AGT's content-hash dedupe and because CTV pins
`AS OF SYSTEM TIME` and replaying an old arrival dies on the CockroachDB GC threshold; and keep
`--unique-amounts` on, because R-41 content hashing otherwise rejects duplicate transactions.

**Response legs.** `_ISR` / `_SBSR` / `_PBSR` replies are not hand-written. `dcre-infra`'s
`scripts/fint-sim.sh` is the Fintegrate simulator: it polls the outbound `fint-req/out` zone and
writes the matching reply trio into `fint-resp/in` with an atomic rename, which is what produces
response-route arrivals.

### 4. Getting a stage image

**This is the step that silently decides whether anything happens.** All 29 `AGT_<STAGE>_IMAGE`
knobs default to empty, empty means launch-disabled, and the only log line about it is at DEBUG
level, so at the default log level an unset knob produces no Job, no intent row and no message.

There is **no aggregate build**: no root Gradle project and no build-all script across the stage
services. Each is its own repository with its own Dockerfile, built one at a time:

```bash
cd <dcre>/collections/crr
./gradlew bootJar
docker build -t dcre-crr:1.0.0 .
kind load docker-image --name dcre-dev dcre-crr:1.0.0
```

If `kind load docker-image` fails with a `ctr: content digest ... not found` error, which it does
against Docker Desktop's containerd image store, use the archive form instead:

```bash
docker save --platform "linux/$(docker version --format '{{.Server.Arch}}')" dcre-crr:1.0.0 \
  | kind load image-archive /dev/stdin --name dcre-dev
```

Then point AGT at the whole fleet at once with `dcre-infra`'s `scripts/switch-version.sh <version>`,
which is the other half of the cross-repo contract described under
[The 29 stage images](#the-29-stage-images): it `kubectl set env`s
`AGT_<STAGE>_IMAGE=dcre-<stage>:<version>` for all 29 stages, sets the AGT image itself, and waits
on the rollout. It refuses pre-cutover version lines, refuses retired stage names, and asserts the
roster shape, so a mismatched roster fails loudly rather than half-applying.

**Known incomplete, checked 2026-08-09.** Only 22 of the stage services have a `Dockerfile` at all
(`find <dcre> -name Dockerfile -not -path '*/build/*'`, exit 0): all 9 collections, all 10
mandates, `shared/hcs`, `shared/rpt`, and `payments/pai`. The other **eight payments services
(`prr`, `ptv`, `prw`, `pir`, `pix`, `psx`, `ppx`, `prg`) have no Dockerfile**, so their images
cannot be built by the recipe above. How those eight are meant to be built is **not established
here**. Until it is, the payments family cannot be exercised end to end from this document.

### 5. What a working run looks like

Watch these four places. They are independent, and disagreement between them is the diagnosis.

**The AGT log**, at INFO, in this order:

```
Arrival FNBCC01_PROBE2026080900000001.txt claimed as <uuid>_FNBCC01_PROBE2026080900000001.txt
Launched col-crr-<32hex> in dcre-col (uid <uid>)
Outcome col-crr-<32hex> = BUSINESS_ACCEPTED (<type>/<reason>, exit=0)
```

`kubectl -n dcre logs deploy/dcre-agt -f` and grep for `Arrival|Launched|Outcome|QUARANTINED`.

**The exchange tree.** The file leaves `in/` and reappears under
`archive/inflight/<arrivalUuid>_<name>`, and one file per stage appears in `outcomes/<jobName>`.
A file in `error/<arrivalUuid>_<name>` is a quarantine; one in `archive/duplicates/` is an ignored
re-delivery.

**Kubernetes.** Job names are deterministic: `<col-|pay-|man->` + lowercase stage + `-` + the
arrival UUID with dashes stripped, for example `col-ctv-8c77113c6c71455f8d69ede318e65ed6`. Every
Job carries `dcre/managed-by=agt` and `dcre/stage=<STAGE>`, and arrival-scoped Jobs also carry
`dcre/arrival=<uuid>`.

```bash
kubectl get jobs -A -l dcre/managed-by=agt --sort-by=.metadata.creationTimestamp
kubectl -n dcre-col get jobs -l dcre/arrival=<arrival-uuid>
kubectl -n dcre-col logs job/col-ctv-<32hex>
```

Note `ttlSecondsAfterFinished` on the Job spec: **a finished Job disappears after five minutes.**
An empty `kubectl get jobs` is not evidence that nothing ran. `outcomes/` and `agt_ops` are the
durable record; Kubernetes is not.

**The `agt_ops` ledger**, which is the authority:

```sql
-- did my file get claimed, and what happened to it?
SELECT id, status, quarantine_reason, route_id, client_token, msg_id_token, claimed_path, arrived_at
FROM   file_arrival WHERE physical_filename = '<the file name>';

-- the stage timeline for one arrival
SELECT li.stage, li.job_name, li.namespace, li.status, li.attempt, li.heartbeat_at,
       so.outcome, so.exit_code, so.k8s_condition, so.observed_at
FROM   launch_intent li LEFT JOIN stage_outcome so ON so.intent_id = li.id
WHERE  li.arrival_id = '<arrival-uuid>' ORDER BY li.created_at;

-- launched but never observed
SELECT job_name, namespace, stage, attempt, heartbeat_at FROM launch_intent
WHERE  status = 'LAUNCHED' AND id NOT IN (SELECT intent_id FROM stage_outcome);
```

Reach it with `kubectl exec -n dcre crdb-0 -- ./cockroach sql --insecure --database=agt_ops`.

**The status vocabularies**, because two of them look alike and are not:

| Column | Values |
|---|---|
| `file_arrival.status` | `CLAIMED`, `QUARANTINED`, `DAG_RUNNING`, `DAG_COMPLETE`, `DAG_FAILED`. Terminal: the last three. Note `DAG_COMPLETE`, not `DAG_COMPLETED`. |
| `launch_intent.status` | `INTENDED`, `LAUNCHED`, `ABANDONED` |
| `stage_outcome.outcome` | the seven `Outcome` values listed in step 1 |
| `file_arrival.quarantine_reason` | `UNPARSEABLE_FILENAME`, `CLIENT_PATH_MISMATCH`, `SAME_KEY_DIFFERENT_HASH`, `CLAIM_RETRY` |

An arrival reaching `DAG_COMPLETE` is success. A DC-flow arrival sitting at `DAG_RUNNING` for days
is not necessarily stuck: the collection-day emission gate holds it there deliberately, which is
exactly why `agt_dag_running_oldest_age_seconds` exists.

### 6. The smallest run that proves the machinery, with no cluster

This isolates AGT from Kubernetes and from the shared cluster entirely, and it is how the
configuration claims in this README were verified on 2026-08-09. It exercises discovery, claiming,
hashing, filename parsing and the ledger, and stops short of launching.

```bash
docker run -d --name agt-probe-crdb -p 36257:26257 cockroachdb/cockroach:v26.2.3 \
  start-single-node --insecure --store=type=mem,size=0.5
sleep 20
docker exec agt-probe-crdb ./cockroach sql --insecure --database=defaultdb \
  -e "CREATE DATABASE agt_ops; CREATE DATABASE dcre_col; CREATE DATABASE dcre_pay;"

mkdir -p /tmp/xroot/fnbcc01/onhost-req/{in,error,archive}
echo body > /tmp/xroot/fnbcc01/onhost-req/in/FNBCC01_PROBE2026080900000001.txt

AGT_EXCHANGE_ROOT=/tmp/xroot \
AGT_DB_URL='jdbc:postgresql://localhost:36257/agt_ops?sslmode=disable' \
AGT_COLLECTIONS_DB_URL='jdbc:postgresql://localhost:36257/dcre_col?sslmode=disable' \
AGT_PAYMENTS_DB_URL='jdbc:postgresql://localhost:36257/dcre_pay?sslmode=disable' \
AGT_LAUNCH_ENABLED=false AGT_OBSERVE_ENABLED=false KUBECONFIG=/dev/null \
  java -jar build/quarkus-app/quarkus-run.jar
```

Within a few seconds the log says `Arrival FNBCC01_... claimed as <uuid>_FNBCC01_...`, the file has
moved to `/tmp/xroot/fnbcc01/onhost-req/archive/inflight/`, and
`SELECT physical_filename, client_token, msg_id_token, status FROM file_arrival` returns one
`DAG_RUNNING` row with `client_token = FNBCC01` and `msg_id_token = PROBE2026080900000001`. Drop a
file named without an `FNB` token to see the other half: `QUARANTINED ...: filename lacks R-31
tokens` and the file in `error/` with reason `UNPARSEABLE_FILENAME`.

`AGT_LAUNCH_ENABLED=false` and `KUBECONFIG=/dev/null` together are what keep this off whatever
cluster your kubeconfig points at. Do not omit them: the dev cluster is shared.
`docker rm -f agt-probe-crdb` when done.

---

## Configuration: what AGT reads

12FactorApp Alignment (https://12factor.net/): every value below is `${ENV_VAR:default}` in
`src/main/resources/application.yml`. Override via real environment variables, never by editing
the file.

### This list is not the whole universe, and cannot be

`application.yml` names **74** distinct environment variables, and those are the ones documented
below. **That is a count of what is documented, not a closed set of what AGT reads.** Do not
treat the absence of a name from these tables as proof that AGT ignores it.

SmallRye Config makes **every** configuration property settable by a derived environment-variable
name, whether or not any yml line mentions it
(https://smallrye.io/smallrye-config/Main/config/environment-variables/). The derivation is
mechanical: take the property name, uppercase it, and replace every `.` and `-` with `_`.

| Property | Derived environment variable |
|---|---|
| `agt.exchange-root` | `AGT_EXCHANGE_ROOT` |
| `agt.holder-id` | `AGT_HOLDER_ID` |
| `agt.observe-enabled` | `AGT_OBSERVE_ENABLED` |
| `dcre.agt.report-scan-seconds` | `DCRE_AGT_REPORT_SCAN_SECONDS` |
| `quarkus.datasource.jdbc.url` | `QUARKUS_DATASOURCE_JDBC_URL` |

Apply it to every property in `AgtConfig`, `AgtImages` and `AgtExchangeConfig`, and to every
`quarkus.*` property Quarkus itself defines.

**The environment beats the yml line, including when the yml line names a DIFFERENT variable.**
The environment config source outranks `application.yml`, so a `${SOMENAME:default}` placeholder
is only consulted once the property's own derived name has been found absent. Two documented
variables are shadowed this way, and both matter:

| Documented variable | Silently outranked by | Consequence |
|---|---|---|
| `DCRE_EXCHANGE_ROOT` | `AGT_EXCHANGE_ROOT` | An `AGT_EXCHANGE_ROOT` set anywhere wins outright. Someone debugging "why is `DCRE_EXCHANGE_ROOT` ignored" has no reason to look for a name the document never mentions. |
| `HOSTNAME` | `AGT_HOLDER_ID` | **`AGT_HOLDER_ID` is where the lease-uniqueness invariant actually lives.** Setting `HOSTNAME` uniquely per instance does not guarantee unique holders if anything sets `AGT_HOLDER_ID`. |

Both were proved by execution on 2026-08-09, not argued. AGT was booted against a throwaway
single-node CockroachDB (`cockroachdb/cockroach:v26.2.3` on port 36257, no cluster involved) with
`HOSTNAME=hostname-should-lose`, `AGT_HOLDER_ID=marker-holder-probe`, and two populated exchange
trees, one named by `DCRE_EXCHANGE_ROOT` and one by `AGT_EXCHANGE_ROOT`:

```
SELECT holder FROM agt_lease;   ->  marker-holder-probe        (HOSTNAME lost)
the only file AGT saw was the one under AGT_EXCHANGE_ROOT      (DCRE_EXCHANGE_ROOT lost)
  grep -c PROBEAGTROOT  <log> -> 1     grep -c PROBEDCREROOT <log> -> 0 (exit 1)
```

The practical rule: when a value is not what you expect, derive the property's own name with the
recipe above and check whether that variable is set, before believing the yml line.

### AGT's own datasources

| Variable | Default | Required | Purpose |
|---|---|---|---|
| `AGT_DB_URL` | `jdbc:postgresql://localhost:26257/agt_ops?sslmode=disable` | yes | AGT's own ledger database. Liquibase migrates it at start. |
| `AGT_DB_USER` | `root` | yes | `agt_ops` datasource user |
| `AGT_DB_PASSWORD` | (empty) | no | `agt_ops` datasource password |
| `AGT_COLLECTIONS_DB_URL` | `jdbc:postgresql://localhost:26257/dcre_col?sslmode=disable` | yes in cluster | Read-only window into `dcre_col` (`prg_report_due`, `prg_sla_pending`, `crw_emission_owed`). Absent in-cluster, readiness stays DOWN forever with "Connection to localhost:26257 refused". |
| `AGT_COLLECTIONS_DB_USER` | `root` | no | |
| `AGT_COLLECTIONS_DB_PASSWORD` | (empty) | no | |
| `AGT_PAYMENTS_DB_URL` | `jdbc:postgresql://localhost:26257/dcre_pay?sslmode=disable` | yes in cluster | Read-only window into `dcre_pay`. **A separate READ SEAM, not a rename**: both databases publish views called `prg_report_due` and `prg_sla_pending`, so a single datasource leaves one family's IMMEDIATE reports permanently undiscovered, with no exception and no log line. |
| `AGT_PAYMENTS_DB_USER` | `root` | no | |
| `AGT_PAYMENTS_DB_PASSWORD` | (empty) | no | |

AGT opens **no** datasource against `dcre_man` or `dcre_hcs`. It only hands those URLs to pods.

### Namespaces and identity

| Variable | Default | Required | Purpose |
|---|---|---|---|
| `AGT_NAMESPACE` | `dcre` | yes | AGT's own CONTROL namespace only: the Kubernetes client, AGT's Deployment, and the shared `crdb`/`lgtm` infrastructure. Stage Jobs do **not** land here. |
| `AGT_NAMESPACE_COL` | `dcre-col` | no | Flow namespace for collections stage Jobs (`col-*`), and for HCS |
| `AGT_NAMESPACE_PAY` | `dcre-pay` | no | Flow namespace for payments stage Jobs (`pay-*`) |
| `AGT_NAMESPACE_MAN` | `dcre-man` | no | Flow namespace for mandates stage Jobs (`man-*`) |
| `AGT_HOLDER_ID` | none; falls through to `HOSTNAME` | no | **The lease holder id, and the variable the uniqueness invariant belongs to.** It outranks `HOSTNAME` (see the precedence rule above). Normally left unset. |
| `HOSTNAME` | `local-agt` | no | The fallback holder id, used only when `AGT_HOLDER_ID` is unset. In Kubernetes the pod name supplies it automatically. |
| `DCRE_EXCHANGE_ROOT` | `../../../../../infra/dcre-infra/exchange` | no | Root of the exchange directory tree (R-30). `/exchange` in cluster. Outranked by `AGT_EXCHANGE_ROOT`. |

**The holder id must be unique per running instance**, whichever of the two supplies it. Two
instances resolving to one holder id both believe they hold the single-writer lease, and both act.
`LeaseService` reads exactly one value, `agt.holder-id`, so the invariant is on the resolved
property and not on either variable by itself. Setting `HOSTNAME` per pod is not sufficient if
anything also sets `AGT_HOLDER_ID`.

### Gates

| Variable | Default | Required | Purpose |
|---|---|---|---|
| `AGT_LAUNCH_ENABLED` | `true` | no | Gate for Kubernetes Job creation |
| `AGT_OBSERVE_ENABLED` | `true` | no | Gate for termination observation. Independent of the above: observation stays on when launching is paused. Has no line in `application.yml`; it binds from the `@WithDefault` on `AgtConfig.observeEnabled()`. |

### Client rosters

| Variable | Default | Required | Purpose |
|---|---|---|---|
| `AGT_PAY_CLIENTS` | `FNBRF01` | no | INTERIM (R-42): comma-separated client tokens whose `fint-resp` arrivals and clock jobs ride the pay flow, until the R-14 client reference table lands. Trimmed and uppercased on read. |
| `AGT_MAN_CLIENTS` | `FNBCC01,FNBCC02,FNBRF01` | no | INTERIM (R-42 analog for M10): mandate-capable client tokens. MRG windows launch only for these. Trimmed and uppercased on read. |

The per-client inbound exchange layout (clients `FNBCC01`, `FNBCC02`, `FNBRF01`, each with
`onhost-req` / `onhost-req-endo` / `fint-resp` / `onhost-req-man` / `fint-resp-man`) is
structured data under `agt.exchange.clients.*` in `application.yml`, bound by
`config/AgtExchangeConfig`. Adding a client is done by editing that file. The precedence rule
above still applies to the individual leaf paths, which carry derived names of the shape
`AGT_EXCHANGE_CLIENTS_<CLIENT>_<CHANNEL>_IN`, so a stray one of those redirects a single drop
zone. The tree these paths must match is drawn in
[Exercise it end to end](#1-the-exchange-directory-tree).

### The stage database URLs (handed to pods, never opened by AGT)

| Variable | Default | Required | Purpose |
|---|---|---|---|
**None of these six is required in cluster.** Every one already defaults to the correct in-cluster
FQDN, which is the whole point of the defaults. The variables that ARE required in cluster are the
three of AGT's OWN datasources in the first table, because those default to `localhost:26257`
(`application.yml:6`, `:14`, `:24`): `AGT_DB_URL`, `AGT_COLLECTIONS_DB_URL`, `AGT_PAYMENTS_DB_URL`.
Those three, and only those three, are what `k8s/10-agt-deployment.yml` needs to set among the
database knobs.

| Variable | Default (`application.yml` line) | Required in cluster | Purpose |
|---|---|---|---|
| `AGT_SERVICE_DB_URL` | `jdbc:postgresql://crdb.dcre.svc.cluster.local:26257/dcre_col?sslmode=disable` (`:140`) | no | The URL every COLLECTIONS stage pod gets as `DCRE_DB_URL`. Also the `DCRE_COL_DB_URL` handed to the MRG suspension sweep. **No longer handed to HCS.** |
| `AGT_PAY_SERVICE_DB_URL` | `...:26257/dcre_pay?sslmode=disable` (`:145`) | no | Every PAYMENTS stage pod's `DCRE_DB_URL`. |
| `AGT_MAN_SERVICE_DB_URL` | `...:26257/dcre_man?sslmode=disable` (`:148`) | no | Every MANDATES stage pod's `DCRE_DB_URL`, **and** every CTV pod's `DCRE_CTV_MANDATES_DB_URL`. One knob feeds both deliberately, so the two cannot drift. |
| `AGT_HCS_SERVICE_DB_URL` | `...:26257/dcre_hcs?sslmode=disable` (`:155`) | no | The HCS pod's `DCRE_DB_URL`, **and** every CDE pod's `DCRE_CDE_HOLIDAYS_DB_URL`. `shared/hcs` carries a `FamilyGuard` on `current_database()`, so with the pre-2026-08-08 routing every HCS pod dies on startup. |
| `AGT_AGTOPS_DB_URL` | `...:26257/agt_ops?sslmode=disable` (`:170`) | no | Handed to **every** stage pod so the platform-batch heartbeat writer can reach `agt_ops`. Identical for all flows. |
| `AGT_AGTOPS_DB_USER` | `root` (`:171`) | no | The heartbeat writer's user |

The committed manifest bears this out. Verified 2026-08-09 by extracting `- name:` from
`k8s/10-agt-deployment.yml` (37 names, 29 of them image knobs, 8 others): it sets all three of the
genuinely required knobs, sets `AGT_PAY_SERVICE_DB_URL` and `AGT_HCS_SERVICE_DB_URL` redundantly,
and sets `AGT_SERVICE_DB_URL`, `AGT_MAN_SERVICE_DB_URL` and `AGT_AGTOPS_DB_URL` **not at all**,
while AGT deploys and runs. All six defaults use the FQDN
`crdb.dcre.svc.cluster.local`, not the short name `crdb`, because stage pods run in the flow
namespaces where `crdb` does not resolve.

**Pointing one of these at another family's database is refused, not absorbed.**
`StageDatabases.requireAddresses` parses the database segment out of the configured URL and throws
at Job-BUILD time, before a pod exists, naming both sides. That guard is why the historical
failure it was written for cannot recur: before `AGT_PAY_SERVICE_DB_URL` existed, every payments
stage received the collections URL and would have built the payments schema inside `dcre_col`
without erroring, because the write is perfectly valid SQL against the wrong database. Today the
default is already `dcre_pay` and the guard refuses the misconfiguration on top of that.

### The 29 stage images

One knob per stage, all defaulting to **empty**, which means **launch-disabled**.

| Family | Variables |
|---|---|
| collections | `AGT_CRR_IMAGE` `AGT_CTV_IMAGE` `AGT_CDE_IMAGE` `AGT_CRW_IMAGE` `AGT_CIR_IMAGE` `AGT_CIX_IMAGE` `AGT_CSX_IMAGE` `AGT_CPX_IMAGE` `AGT_CRG_IMAGE` |
| payments | `AGT_PRR_IMAGE` `AGT_PTV_IMAGE` `AGT_PAI_IMAGE` `AGT_PRW_IMAGE` `AGT_PIR_IMAGE` `AGT_PIX_IMAGE` `AGT_PSX_IMAGE` `AGT_PPX_IMAGE` `AGT_PRG_IMAGE` |
| mandates | `AGT_MRR_IMAGE` `AGT_MRV_IMAGE` `AGT_MAS_IMAGE` `AGT_MIT_IMAGE` `AGT_MIR_IMAGE` `AGT_MRW_IMAGE` `AGT_MIX_IMAGE` `AGT_MSX_IMAGE` `AGT_MPX_IMAGE` `AGT_MRG_IMAGE` |
| cross-family | `AGT_HCS_IMAGE` |

`CIX`/`CSX`/`CPX` are the three `fint-resp` leg readers (ISR/SBSR/PBSR), formerly
`IXR`/`SXR`/`PXR`. `PAI` is the Account Init Service, formerly `AIS` on the collections family.
`AGT_CRG_IMAGE` is the collections report generator; **`AGT_PRG_IMAGE` changed meaning at the
2026-08-08 cutover** and now names the payments one.

The names are a cross-repo contract with `dcre-infra/scripts/switch-version.sh`, which sets
`AGT_<STAGE>_IMAGE=dcre-<stage>:<version>` for every stage in the roster. Renaming a knob here
makes that export silently inert. Verified 2026-08-09: 29 knobs in `application.yml` and 29 in
`k8s/10-agt-deployment.yml`.

**An unset image is launch-disabled SILENTLY, by design (SCRUM-33).** A clock scheduler skips
its windows and a DAG launch fails fast. There is no stub fallback, and the only log line about it
is at DEBUG, so at the default log level the symptom is total absence. **This is the single most
likely reason a freshly deployed AGT appears to do nothing**, because all 29 knobs default to
empty. See [Getting a stage image](#4-getting-a-stage-image) for how to fill them, and
[Troubleshooting](#troubleshooting) for how to confirm which one is missing.

### Cadences and window lengths

| Variable | Default | Purpose |
|---|---|---|
| `AGT_CRW_INTERVAL_SECONDS` | `60` | CRW Process-Date Executor window. COLLECTIONS ONLY: this is the collection-day clock and payments has no analogue. |
| `AGT_CRG_INTERVAL_SECONDS` | `60` | CRG collections-report window |
| `AGT_PRG_INTERVAL_SECONDS` | `60` | PRG payments-report window. Its own knob, not shared with CRG: collections wait for the collection day and payments do not, so the two cadences have no reason to move together. |
| `AGT_MRG_INTERVAL_SECONDS` | `60` | MRG mandates-report window |
| `AGT_MRG_SUSPEND_INTERVAL_SECONDS` | `60` | MRG suspension-sweep window (SCRUM-91). Mandate expiry is a view predicate and has no sweep. |
| `AGT_HCS_INTERVAL_HOURS` | `6` | HCS holiday re-sync cadence, so `public_holiday` re-syncs from Nager.Date four times a day |
| `AGT_REPORT_SCAN_SECONDS` | `15` | `*_report_due` scan cadence. Binds `dcre.agt.report-scan-seconds`, outside the `agt` prefix. |
| `AGT_SLA_SCAN_SECONDS` | `300` | `*_sla_pending` scan cadence. Binds `dcre.agt.sla-scan-seconds`. |

### Recovery, limits and thresholds

| Variable | Default | Purpose |
|---|---|---|
| `AGT_ORPHAN_MAX_ATTEMPTS` | `3` | Bounded same-identity relaunch attempts for a died arrival Job |
| `AGT_INFRA_MAX_ATTEMPTS` | `10` | Separate, larger ceiling for `TECH_CONFIG_FAILED` (pod exit **78**, `EX_CONFIG`, a platform-batch failure before the runner phase). Infrastructure, not a job outcome, so a config restart cannot turn a defect-free arrival into a terminal `DAG_FAILED` in minutes. Still bounded: exhaustion mints `TECH_EXHAUSTED`. |
| `AGT_ORPHAN_BACKOFF_SECONDS` | `60` | Minimum seconds between relaunch attempts of one intent |
| `AGT_HEARTBEAT_TTL_SECONDS` | `45` | Wedged-but-alive detection. A `LAUNCHED` intent whose `heartbeat_at` fell behind is relaunched. Stage pods beat every 10s, so 45s is about 4 missed beats. |
| `AGT_SELF_LIVENESS_TTL_SECONDS` | `15` | AGT self-liveness. `/q/health/live` goes DOWN if the reconciler has not ticked within this window; Kubernetes restarts the pod and the lease CAS re-acquires. 3x the 5s reconcile interval. |
| `AGT_STAGE_MEMORY_REQUEST` | `512Mi` | Stage-pod memory request |
| `AGT_STAGE_MEMORY_LIMIT` | `768Mi` | Stage-pod memory limit. Large-copybook runs (300k tx) need more: CTV OOMed at 768Mi across partition workers on 2026-07-14. |
| `AGT_STAGE_DEADLINE_SECONDS` | `900` | Stage Job `activeDeadlineSeconds`. Same 300k-tx caveat. |
| `AGT_SLA_AMBER_HOURS` | `20` | Fintegrate SLA amber warn threshold |
| `AGT_SLA_RED_HOURS` | `24` | Fintegrate SLA red breach threshold; the Grafana alert on the red gauge drives the ops runbook |
| `AGT_REPORT_STALL_SCANS` | `20` | Consecutive scans that may see the same (flow, client, parent) still due before AGT WARNs. It makes a silent loop VISIBLE. It never retries, widens or falls back. |

### Other

| Variable | Default | Purpose |
|---|---|---|
| `AGT_CTV_MANDATE_SOURCE` | `projection` | Which mandate store CTV's DC-flow gate reads. Vocabulary is `projection` only: `man_ctv_view` in `dcre_man`. CTV **fails closed** on the retired `legacy` value (throws at bean creation), and the `dcre_col.mandate` table it selected has been dropped. AGT carries the token verbatim and never interprets it. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4317` | OTLP endpoint for traces, metrics and logs |

---

## Configuration: what AGT injects into stage Jobs

**This is a different list from the one above and conflating the two is the defect class that
has already shipped here.** AGT documented `pay-service-db-url` as "handed to every payments
stage pod as `DCRE_DB_URL`" while eight of the nine payments services were reading
`DCRE_PAY_DB_URL`, which nothing set. Both sides were green: five tests pinned the consumer
side of the name and nothing pinned AGT's, so the only symptom was a pod falling back to its
committed localhost default, which in a cluster is the pod itself.

These variable names are a **hand-maintained cross-repo wire contract**. The consumers are in
different git repositories and are not on AGT's classpath, so no test here can read what they
declare. `StageRosterTest.everyStagePodReadsItsDatabaseFromOneEnvNameSharedByAllFamilies` pins
the literal where it is made, which is the most this repo can assert on its own, and
`NamespaceRoutingTest` asserts that no pod ever carries `DCRE_PAY_DB_URL`. The durable fix is
to generate both sides from one schema, and it is not done.

**That drift is repaired as of 2026-08-09**, checked against the committed HEAD of each of the nine
payments repositories: all nine read `DCRE_DB_URL`. The mechanism that allowed it is not repaired;
see Known gaps, item 4. Anything this README says about another repository is a snapshot with a
date on it, because those repositories move independently of this file. Re-check before relying on
one.

### On every stage pod, always

Set in both `JobLauncher.serviceJob` (DAG stages) and `JobLauncher.clockJob` (clock windows).

| Variable | Value | Source |
|---|---|---|
| `JOB_NAME` | the Kubernetes Job name | Computed. The pod's heartbeat writer keys on it. |
| `DCRE_DB_URL` | the stage's family database URL | `StageDatabases.urlFor(stage)` |
| `DCRE_EXCHANGE_ROOT` | `/exchange` | Hard-coded, matching the `dcre-exchange` PVC mount |
| `DCRE_AGTOPS_DB_URL` | `agt_ops` URL | `AGT_AGTOPS_DB_URL` |
| `DCRE_AGTOPS_DB_USER` | `root` | `AGT_AGTOPS_DB_USER` |

### Stage-keyed second datasource seams

Set by `JobLauncher.stageEnv`, on DAG service Jobs only. Each is a read-only window from one
stage into another bounded context's published views. **Both fail CLOSED at the consumer**:
`cde` and `ctv` detect `KUBERNETES_SERVICE_HOST` and refuse to start rather than fall back to
their committed localhost default, naming the exact variable. AGT is the only thing that sets
them, so a missing arm here is a crash-looping pod, not a wrong answer.

| Stage | Variable | Points at | Fed by |
|---|---|---|---|
| `CTV` | `DCRE_CTV_MANDATES_DB_URL` | `dcre_man`, the `man_ctv_view` projection | `AGT_MAN_SERVICE_DB_URL` |
| `CTV` | `DCRE_CTV_MANDATE_SOURCE` | not a URL: selects which store the gate reads | `AGT_CTV_MANDATE_SOURCE` |
| `CDE` | `DCRE_CDE_HOLIDAYS_DB_URL` | `dcre_hcs`, the holiday calendar | `AGT_HCS_SERVICE_DB_URL` |

The `stageEnv` switch **does** have a default arm, and that asymmetry with the routing switches
is deliberate. "This stage opens no second datasource" is the correct answer for 27 of the 29
stages, and a stage that needs one and is forgotten fails closed at the consumer with the
variable named. In the routing switches a default arm gave a well-formed WRONG answer that
nothing could observe. Same shape, opposite risk.

Per-role `SELECT` grants on the PUBLISHED VIEWS only, never the base tables, are required for
these seams and are an infra task. AGT does not and must not apply them.

### Launch-scoped env, carried on the durable intent row

`MrgSuspendScheduler` attaches these to one specific launch rather than to `Stage.MRG`, so the
MRG report windows on the same stage do not carry them. They travel as durable launch args
prefixed `env:` and are split back out by `JobLauncher.splitEnvArgs`, so the intent row alone
rebuilds the identical Job on a reconciled re-create.

| Variable | Value | Why launch-scoped |
|---|---|---|
| `DCRE_MRG_JOB_NAME` | `mrgSuspendJob` | Selects the Batch job. Absent, MRG runs its `mrgJob` report default. |
| `DCRE_COL_DB_URL` | `AGT_SERVICE_DB_URL` (`dcre_col`) | The consecutive-failed-collections signal is the only thing on this leg living in the collections DB, and a single-DB view cannot span it. Absent, every window dies with "Connection to localhost:26257 refused". |

### Retired on 2026-08-09, deliberately not replaced

A fifth knob `AGT_ACS_SERVICE_DB_URL` and four account seams
(`DCRE_CTV_ACCOUNTS_DB_URL`, `DCRE_MRV_ACCOUNTS_DB_URL`, `DCRE_PTV_ACCOUNTS_DB_URL`,
`DCRE_MIT_ACCOUNTS_DB_URL`) pointed at a shared `dcre_acs` database, alongside an `ACS` stage,
an `AGT_ACS_IMAGE` knob and an `AcsScheduler` minting a census on a timer. All of it is gone.

`shared/acs` had no authoritative source, no accountable owner, no ingestion of its own and no
freshness contract, so it was a shared integration database wearing the costume of a bounded
context. The account reference now travels as ONE immutable versioned artifact and each context
materialises its own projection into its own database, so there is no cross-context account
read to inject and `ctv` reads `dcre_col.account` over its primary datasource.

Removing the stage rather than disabling it is the point: an unset image is launch-disabled
silently, so a census with no service behind it would simply never run and nothing would say
why. Tripwires: `NamespaceRoutingTest.noPodCarriesARetiredAccountSeam`,
`StageRosterTest.noStageCarriesTheRetiredAccountContext`, and
`StageRosterTest.noDatabaseFamilyCarriesTheRetiredAccountContext`.

Verified 2026-08-09: every remaining occurrence of the string `ACS`/`acs` under `src/main`,
`src/main/resources` and `k8s/` is a comment recording the retirement. No `Stage.ACS`, no
`DbFamily.ACS`, no `AcsScheduler`, no `AGT_ACS_*` binding.

---

## Control loops

Every loop is a single-responsibility `@ApplicationScoped` bean. **Every loop that WRITES
gates on `lease.holdsLease()`**, so exactly one AGT instance acts even if several are running.
The read-only observers do not gate, deliberately: they emit gauges and warnings and would be
useless if only the leader reported. Verified 2026-08-09 by grepping `holdsLease` across
`service/`; the ungated ones are `SlaMonitor`, `LatentDirAuditor` and `MetricsService`, and
`MetricsService` calls it only to publish `agt_lease_held` as a gauge, never as a gate.

| Bean | Cadence | Responsibility |
|---|---|---|
| `LeaseService` | 5s | Single-writer CAS lease renew/acquire. Gates nothing; it *is* the gate. |
| `DirectoryWatcher` | 2s | Arrival discovery across the inbound channels |
| `ArrivalService` | on demand | Claim, hash, dedupe, quarantine |
| `DagEngine` | 2s | Pure DAG decision logic. Unit-testable via `computeLaunches()` / `terminalState()`. |
| `JobLauncher` | on demand | Write-ahead Job creation |
| `OutcomeWatcher` | 3s | Termination observation. Gated by `AGT_OBSERVE_ENABLED`. |
| `Reconciler` | 5s | Post-restart and post-reap recovery, orphan and stale-heartbeat sweeps |
| `OrphanRelauncher` | on demand | Bounded same-identity relaunch |
| `CrwScheduler` | 10s tick | CRW process-date windows. Gates directly. |
| `CrgScheduler` | 10s tick | CRG collections-report windows, COL clients only. Gates via `ReportWindows`. |
| `PrgScheduler` | 10s tick | PRG payments-report windows, PAY clients only. Gates via `ReportWindows`. |
| `MrgScheduler` | 10s tick | MRG mandates-report windows, `AGT_MAN_CLIENTS` only. Gates via `ReportWindows`. |
| `MrgSuspendScheduler` | 10s tick | MRG suspension sweep. Gates directly. |
| `HcsScheduler` | 10s tick | HCS holiday sync, one window per `AGT_HCS_INTERVAL_HOURS`. Gates directly. |
| `ReportTrigger` | `AGT_REPORT_SCAN_SECONDS` | IMMEDIATE report trigger from the `*_report_due` views |
| `SlaMonitor` | `AGT_SLA_SCAN_SECONDS` | Fintegrate SLA amber/red gauges from the `*_sla_pending` views. **Ungated observer.** A missing view logs a WARN for that family only and never kills the scheduler. |
| `LatentDirAuditor` | 300s | Reports files sitting in directories AGT does not own. **Ungated observer.** |
| `MetricsService` | 10s | Ledger-derived gauges. **Ungated observer.** |

The 10s tick on the clock schedulers is not the window length. The tick computes the current
window number from the epoch (`window = epochSeconds / intervalSeconds`), so every AGT
incarnation derives the same run key and the clock-intent unique key dedupes. Level-triggered,
not edge-triggered: a restart mid-window relaunches nothing.

### Idempotency and restart semantics

- `launch_intent` is write-ahead: the intent row is inserted before the Job is created, so a
  crash between the two is safe.
- Job names are deterministic and embed the full 128-bit arrival UUID
  (`JobLauncher.jobName` builds `<flow-prefix><stage-lowercase>-<uuid-without-dashes>`, for
  example `col-crr-4f3c...`), so a 409 Conflict on re-create means "already launched", not an
  error. Clock Jobs use `clockJobName`, which substitutes a deterministic window run key for
  the UUID. The flow prefix (`col-`, `pay-`, `man-`) replaced the old `dcre-` literal and is
  shorter, so the 63-character Kubernetes name limit only got safer.
- `stage_outcome` inserts are `UNIQUE(intent_id, attempt)`, so re-observation is a no-op.
- The `Reconciler` is status-driven and never blind-recreates a `LAUNCHED` intent. It resolves
  reaped-before-observed Jobs from the durable `<exchange-root>/outcomes/<jobName>` file seam
  before falling back to a bounded relaunch.
- Business outcomes are never relaunched. Clock intents self-heal at the next window boundary.

### Metrics

Eight metric names, in **three different prefixes**, which anyone writing a dashboard query needs
to know before they filter on `agt_`. Enumerated 2026-08-09 with
`command grep -rn --include='*.java' -oE '"(agt|dcre)_[a-z_]+"' src/main/java | sort -u` (exit 0),
discarding the four `dcre_col`/`dcre_pay`/`dcre_man`/`dcre_hcs` database-name literals in
`domain/DbFamily.java` that the same expression matches.

| Metric | Tags | Published by | Notes |
|---|---|---|---|
| `agt_lease_held` | none | `MetricsService.java:38` | 1 on the leader, 0 elsewhere |
| `agt_file_arrivals_total` | `status` | `MetricsService.java:44` | `file_arrival` grouped by status |
| `agt_dag_running_oldest_age_seconds` | `scope` | `MetricsService.java:51` | AGE, not count. The emission gate makes `DAG_RUNNING` legitimately long-lived, so a count cannot distinguish "warehoused" from "stranded". This is the number worth alerting on. |
| `agt_launch_intents_total` | `status` | `MetricsService.java:52` | |
| `agt_stage_outcomes_total` | `outcome` | `MetricsService.java:53` | |
| `dcre_sla_pending_amber` | `flow`, `client` | `SlaMonitor.java:45` | Fintegrate SLA amber, per flow AND client: a client can ride both flows and a merged gauge cannot say which side is breaching |
| `dcre_sla_pending_red` | `flow`, `client` | `SlaMonitor.java:46` | **This is the gauge the ops runbook alerts on** (`AGT_SLA_RED_HOURS`) |
| `dcre_agt_latent_dir_files_total` | `client`, `dir` | `LatentDirAuditor.java:33` | Counter, not a gauge. Files sitting in directories AGT does not own. |

Despite the `agt_` prefix on five of them, all eight are ordinary Micrometer meters exported over
OTLP to `OTEL_EXPORTER_OTLP_ENDPOINT`. There is no `dcre-agt` Grafana dashboard in any repository;
see Known gaps.

---

## Deploy

### Build the image

```bash
./gradlew clean build                      # produces build/quarkus-app/
docker build -f src/main/docker/Dockerfile.jvm.prod -t dcre-agt:<tag> .
```

`Dockerfile.jvm.prod` is the Alpine production image (`eclipse-temurin:25-jre-alpine`), used in
preference to the Quarkus-generated `Dockerfile.jvm`, which is UBI9-based and kept only as the
generated default. `src/main/docker/` also holds `Dockerfile.native`, `Dockerfile.native-micro`
and `Dockerfile.legacy-jar`; none is in use.

**Always `clean` before building an image.** `.dockerignore` allowlists `build/quarkus-app/*`
wholesale, so a build without `clean` copies whatever stale jars are still sitting there.
Verified 2026-08-09: `dcre-agt:2.0.1` contains three application jars, including
`agt-1.0.0-SNAPSHOT.jar` and `dcre-agt-1.0.0-SNAPSHOT.jar` from earlier versions. This is a
defect in `.dockerignore`, not a usage note, and it is recorded as Known gap 8: the instruction
above is a workaround that depends on the operator remembering it.

### Architecture

The image must match the cluster's node architecture.

- **Local kind on Apple Silicon**: the host is `arm64` and so is the kind node, so a plain
  `docker build` is correct. Confirm with `uname -m`.
- **A real linux/amd64 cluster**: build with `--platform linux/amd64` and **verify the pushed
  manifest before rollout**. An arm64 image on amd64 nodes fails as `ErrImagePull` minutes
  after the release already looks complete, because old pods keep serving and the rollout only
  reports a timeout.

```bash
docker build --platform linux/amd64 -f src/main/docker/Dockerfile.jvm.prod -t <img>:<tag> .
docker push <img>:<tag>
docker manifest inspect <img>:<tag> | python3 -c "import json,sys; \
  [print(m['platform']['os'], m['platform']['architecture']) for m in json.load(sys.stdin).get('manifests',[])]"
```

Do not read the push's success off a pipeline that ends in `head` or `tail`: that reports the
consumer's exit status, not the push's. Verify out of band with `docker manifest inspect`.

### Roll it out

```bash
kind load docker-image dcre-agt:<tag> --name dcre-dev
kubectl apply -f k8s/
kubectl -n dcre rollout status deploy/dcre-agt
```

`k8s/10-agt-deployment.yml` is the only manifest in this repo. It runs AGT as a `Deployment`
with `replicas: 1` and `strategy: Recreate`, a `readinessProbe` on `/q/health/ready`, a
`startupProbe` on `/q/health/live` budgeting up to 5 minutes for a cold Liquibase migration on
CockroachDB, and then a `livenessProbe` on the same path. While the startup probe is still
failing Kubernetes runs neither of the other two, so a slow migration cannot be
liveness-killed into a crashloop.

The `ServiceAccount` `dcre-agt` and its RBAC are **not** in this repo. They live in
`dcre-infra/k8s/base/01-rbac.yml`, which as of 2026-08-09 grants, per namespace:

| Namespace | Resource | Verbs |
|---|---|---|
| `dcre`, `dcre-col`, `dcre-pay`, `dcre-man` | `batch` `jobs` | create, get, list, watch, delete |
| `dcre`, `dcre-col`, `dcre-pay`, `dcre-man` | `pods` | get, list, watch |
| `dcre` only | `configmaps`, `secrets` | get, list, watch |

Four `Role` plus `RoleBinding` pairs, each binding back to the single `dcre-agt` ServiceAccount
in `dcre`. There is no `ClusterRole`. Adding a fifth flow namespace means adding a fifth pair
there, not just a knob here.

The manifest sets its own copies of the image knobs and the database URLs. It carries the
image tag `dcre-agt:m7` as committed, which is not the tag anyone deploys; the tag is chosen at
`kubectl set image` or `kubectl apply` time. That is a wart, not a convention.

---

## Troubleshooting

Start from the same principle every time: **check the RUNNING thing, not the repository.** A
long-running Deployment froze its configuration when it started, and the repo has moved on.

```bash
kubectl -n dcre get deploy dcre-agt -o jsonpath='{.spec.template.spec.containers[0].image}'
kubectl -n dcre get deploy dcre-agt -o json | python3 -c \
  "import json,sys; [print(e['name'],'=',e.get('value')) for e in \
   json.load(sys.stdin)['spec']['template']['spec']['containers'][0].get('env',[])]"
kubectl -n dcre logs deploy/dcre-agt --tail=200
```

Verified 2026-08-09: the deployed `dcre-agt` had **27 env vars, 21 of them image knobs**, while
`k8s/10-agt-deployment.yml` in this repo declares **37, of which 29 are image knobs**. The
running instance had no `AGT_PAYMENTS_DB_URL`, no `AGT_PAY_SERVICE_DB_URL` and no
`AGT_HCS_SERVICE_DB_URL`, and it had an `AGT_CTV_MANDATE_SOURCE` the manifest does not carry.
Reading the manifest would have described a service that was not running.

### An image tag does not identify an artifact

The tag is typed by hand at `docker build -t`; the jar name comes from `version` in
`build.gradle`. Nothing connects them.

```bash
docker run --rm --entrypoint sh dcre-agt:<tag> -c 'ls /deployments/app/'
```

Verified 2026-08-09: `dcre-agt:2.3.0`, the image the cluster was actually running, contains
`agt-2.0.1.jar`. So does `dcre-agt:2.1.0`. Do not infer the code in an image from its tag.

### A stage pod will not start, or writes the wrong database

Read the pod's actual environment rather than the config you believe was applied:

```bash
kubectl -n dcre-pay get pods -l dcre/managed-by=agt
kubectl -n dcre-pay get job <job> -o jsonpath='{.spec.template.spec.containers[0].env}' | python3 -m json.tool
kubectl -n dcre-pay logs job/<job>
```

| Symptom | Likely cause |
|---|---|
| `Connection to localhost:26257 refused` in a stage pod | AGT did not set that pod's URL variable, so the pod fell back to its committed localhost default, which in a cluster is the pod itself. Check which variable the consumer reads: the name is a cross-repo contract. |
| That symptom in a **payments** pod | Historically this was the `DCRE_PAY_DB_URL` drift, repaired in all nine payments repositories on 2026-08-09. If it recurs, the pod is reading a variable name AGT does not set. Confirm with `kubectl -n dcre-pay get job <job> -o jsonpath='{.spec.template.spec.containers[0].env}'`: AGT sets `DCRE_DB_URL` and never `DCRE_PAY_DB_URL`. Check the consumer's committed `application.yml`, not this README. |
| `cde` or `ctv` refuses to start, naming a variable | The `JobLauncher.stageEnv` arm is missing, or the knob feeding it is unset. Both fail closed on purpose. |
| HCS pod dies immediately on startup | `AGT_HCS_SERVICE_DB_URL` is unset or points at `dcre_col`. `shared/hcs` compares `current_database()` against `dcre_hcs` before any DDL and refuses. |
| A payments schema appearing inside `dcre_col` | Historical only, and doubly prevented now: `AGT_PAY_SERVICE_DB_URL` defaults to the `dcre_pay` FQDN, and `StageDatabases.requireAddresses` refuses a cross-family URL at Job-build time before a pod exists. If you see this on a live cluster, the pod predates both. |
| `IllegalStateException: the PAY family owns database 'dcre_pay' but its configured url addresses '...'` | The guard working. Fix the `AGT_*_SERVICE_DB_URL`, not the guard. |
| `IllegalStateException: stage X is hosted in the Y namespace but its Job targets namespace '...'` | The namespace guard working. The stage and the namespace disagree about the family. |

### A stage never runs, and nothing says why

**An unset image is launch-disabled SILENTLY, by design (SCRUM-33).** There is no stub
fallback and no warning. A clock scheduler with an empty image knob returns from its tick
before minting a window, so the symptom is total absence: no Job, no intent row, no log line.

```bash
kubectl -n dcre get deploy dcre-agt -o json | python3 -c \
  "import json,sys; env=json.load(sys.stdin)['spec']['template']['spec']['containers'][0].get('env',[]); \
   ks=[e['name'] for e in env if e['name'].endswith('_IMAGE')]; \
   print(len(ks),'image knobs set'); print('\n'.join(sorted(ks)))"
```

Compare that list against the 29 in the table above. A missing one is your answer. Note that
this failure mode is the reason `ACS` was **removed** rather than left with an empty knob: a
stage with no service behind it would simply never run and nothing would report it.

### A report parent is due forever

`report-stall stage=... scans=N` in the AGT log means the same (flow, client, parent) has been
seen due for `AGT_REPORT_STALL_SCANS` consecutive scans and its report has not settled it.
Nothing is retrying and nothing has failed. The usual cause is that the client token in the
report-due view is not the token the generator's read path selects on. AGT never retries,
widens or falls back here; the WARN exists purely to make silence visible.

### AGT itself is wedged

`/q/health/live` reports DOWN when the reconciler has not ticked within
`AGT_SELF_LIVENESS_TTL_SECONDS`, and Kubernetes restarts the pod. The lease CAS then
re-acquires the single-writer role on the new incarnation.

```bash
kubectl -n dcre exec deploy/dcre-agt -- \
  sh -c 'wget -qO- http://localhost:8080/q/health/live'
```

Look at `lastTickAt` in the response. `"never"` on a pod that has been up for minutes means the
scheduler never fired at all, which is a different problem from a wedged tick.

### Readiness never passes in cluster

Almost always a missing datasource URL. The current code opens three datasources (`agt_ops` as
`<default>`, plus `collections` and `payments`) and the Quarkus readiness check covers every
one of them by name. `AGT_COLLECTIONS_DB_URL` was absent from the manifest until SCRUM-107, and
every in-cluster AGT fell back to the localhost default, so the pod ran, the readiness probe
never passed, and the rollout timed out.

```bash
kubectl -n dcre exec deploy/dcre-agt -- sh -c 'wget -qO- http://localhost:8080/q/health/ready'
```

The response names each datasource, which tells you which one is down. This is also the fastest
way to confirm which build is running: queried 2026-08-09, the deployed pod listed only
`collections` and `<default>`, with no `payments`, because it predates the payments read seam.
Three names means current code; two means an older image regardless of its tag.

### Tests will not run

`Test process encountered an unexpected problem` from Gradle usually means
`OutOfMemoryError: Java heap space` in the test worker. `build.gradle` already sets
`maxHeapSize = '4g'`; if you have reduced it, put it back. If Docker is not running, the
CockroachDB Testcontainer cannot start and the suite fails at the first `@QuarkusTest`.

---

## Known gaps

Documented because they are not true yet, rather than described as if they were.

1. **`dcre_hcs` does not exist on the current dev cluster.** Verified 2026-08-09:
   `SHOW DATABASES` on `crdb-0` returned `agt_ops`, `dcre_col`, `dcre_man`, `dcre_pay` and the
   CockroachDB system databases, with no `dcre_hcs`. `dcre-infra/k8s/base/02-crdb.yml` does
   create it and `verify-databases.sh` expects it, so the design is right and the running
   cluster predates the 2026-08-08 ruling. The cluster needs re-provisioning before HCS or CDE
   can work. Until then, applying `k8s/10-agt-deployment.yml` gives AGT an
   `AGT_HCS_SERVICE_DB_URL` pointing at a database that is not there.

2. **The images are not built with Paketo buildpacks.** The house rule is Paketo with
   `BP_JVM_VERSION=25` rather than a hand-written prod JVM Dockerfile. This repo has no
   container-image extension in `build.gradle` at all (verified: `grep -n 'container-image\|buildpack'
   build.gradle` exits 1), and `bootBuildImage` is a Spring Boot Gradle plugin task that does not
   exist in a Quarkus build. Adopting it means adding `quarkus-container-image-buildpack` and
   retiring `Dockerfile.jvm.prod`. Not done. Until it is, the Dockerfile path documented above
   is what actually works.

3. **The `DCRE_PAY_DB_URL` drift is repaired, and the mirror that produced it is not.** Re-checked
   2026-08-09 against the **committed HEAD** of each of the nine payments repositories (each is its
   own git repository; the parent `payments/` directory is not one, so a `git show` from there
   silently resolves against an unrelated repository and returns nothing). All nine now read
   `${DCRE_DB_URL:...}` on the `url:` line, and the eight that used to read `DCRE_PAY_DB_URL` now
   name it only in an explanatory comment. Nothing in AGT or `dcre-infra` sets `DCRE_PAY_DB_URL`.
   This entry is kept because the gap is the mechanism, not the instance: see item 4.

4. **The stage-pod env var names remain a hand-maintained cross-repo mirror.** Item 3 was the
   second drift on this exact seam, and the repair was hand-applied on nine repositories, so a
   third is a matter of time. No test in this repository can read what a consumer in another
   repository declares. The durable fix is to generate both sides from one schema. Not done, and
   recorded as a follow-up rather than mitigated.

5. **No `.env.example`.** The clean-clone rule holds (the yml defaults work), but the house
   convention also asks for a committed `.env.example` skeleton and there is none.

6. **The committed manifest image tag is meaningless.** `k8s/10-agt-deployment.yml` says
   `dcre-agt:m7`, which nobody deploys. The tag comes from the release process, not the file.

7. **RBAC is not in this repo.** The `dcre-agt` ServiceAccount and the cross-namespace Job
   permissions live in `dcre-infra`, so `kubectl apply -f k8s/` on a cluster that has not been
   provisioned by `dcre-infra` produces a Deployment that cannot create anything.

8. **`.dockerignore` lets stale jars into the image, so an image build is not reproducible.**
   `.dockerignore` is `*` followed by an allowlist, and the last entry allowlists
   `build/quarkus-app/*` **wholesale**. A build without `clean` leaves earlier versions' jars in
   that directory and they are copied straight into the image. Verified 2026-08-09:
   `docker run --rm --entrypoint sh dcre-agt:2.0.1 -c 'ls /deployments/app/'` (exit 0) returns
   three application jars, `agt-1.0.0-SNAPSHOT.jar`, `agt-2.0.1.jar` and
   `dcre-agt-1.0.0-SNAPSHOT.jar`. The workaround under [Deploy](#build-the-image) is to always
   `clean` first, but that is an operator remembering a step, not a fix: the correct repair is to
   allowlist the specific paths the runtime needs, or to have the image build depend on a clean
   output directory. Not done.

9. **There is no `dcre-agt` Grafana dashboard anywhere.** Searched 2026-08-09 across `dcre-infra`
   for `agt_lease_held`: exit 1, looked and found nothing, with a positive control on the same
   tree returning matches at exit 0. `dcre-infra` does carry `scripts/grafana-dashboards.sh`, but
   it provisions two OTHER dashboards over the Grafana API, `dcre-client-stats` and
   `dcre-internal-stats`, and neither queries an AGT metric. So the eight metrics in
   [Metrics](#metrics) are exported and nothing charts them, including
   `dcre_sla_pending_red`, which the ops runbook is supposed to be alerted from. Whether a
   dashboard has been created by hand in the running Grafana was not checked; either way nothing
   in any repository can recreate it.

---

## Related repositories

All DCRE repositories are **private** under https://github.com/sean-huni. Verified 2026-08-09
with `gh api repos/sean-huni/<name> --jq '.private'`: all 38 exist and every one returned
`true`. An unauthenticated `curl` of any of them returns 404, and that is privacy, not a broken
link. The control for that check: https://github.com/sean-huni returns 200 and an invented
repository name under the same account returns 404.

Because they are private, the plain names are given rather than links that would 404 for a
reader who is not signed in.

| Group | Repositories |
|---|---|
| This service | `dcre-agt` |
| Collections | `dcre-crr` `dcre-ctv` `dcre-cde` `dcre-crw` `dcre-cir` `dcre-cix` `dcre-csx` `dcre-cpx` `dcre-crg` |
| Payments | `dcre-prr` `dcre-ptv` `dcre-pai` `dcre-prw` `dcre-pir` `dcre-pix` `dcre-psx` `dcre-ppx` `dcre-prg` |
| Mandates | `dcre-mrr` `dcre-mrv` `dcre-mas` `dcre-mit` `dcre-mir` `dcre-mrw` `dcre-mix` `dcre-msx` `dcre-mpx` `dcre-mrg` |
| Cross-family | `dcre-hcs` `dcre-rpt` |
| Shared platform | `dcre-platform-model` `dcre-platform-files` `dcre-platform-batch` `dcre-platform-persistence` |
| Infrastructure and docs | `dcre-infra` `dcre-fixture-toolkit` `dcre-design-register` |

The last three are the ones a new engineer needs by name rather than by group. `dcre-infra`
provisions the cluster, the five databases and the exchange tree, and owns `switch-version.sh`,
`env-reset.sh` and `fint-sim.sh`. `dcre-fixture-toolkit` is the Python generator that cuts the
arrival files, and is checked out here as `env/repo/be/python/dcre/fnb_dcre_ctv_toolkit`.
`dcre-design-register` holds the six sheets that are the specification, and is checked out here as
`env/repo/be/java/spring/dcre/design-register`. Both of those directory names differ from the
repository name, so searching the filesystem for the repository name finds nothing. See
[Exercise it end to end](#exercise-it-end-to-end).

The pre-cutover repositories (`dcre-ixr`, `dcre-sxr`, `dcre-pxr`, `dcre-ais`) are archived per
R-48 and their images are no longer built.

### External references

- 12FactorApp Alignment: https://12factor.net/
- Database per Service: https://microservices.io/patterns/data/database-per-service.html
- SDKMAN: https://sdkman.io/
- kind: https://kind.sigs.k8s.io/

Each of the four returned HTTP 200 on 2026-08-09 via
`curl -s -o /dev/null -w "%{http_code}" -L <url>`.

---

## Verified facts

Every number in this README, with the command that produced it. Re-run these rather than
trusting the table; all were run on 2026-08-09.

**Two rules this table follows, because both were learned the hard way.** First, **no line number
from another repository ever appears in this document.** A Known gap once pinned two sibling files
by `file:line`; it was correct when written and stale within a day, and the stale line numbers then
pointed at comments, which reads as an error rather than as age. Describe the defect, name the
file, let the reader find the line. Second, **every claim about a sibling repository or the running
cluster carries the date it was checked**, because both move independently of this README and a
dateless claim about them cannot be aged by a reader.

| Fact | Value | Command |
|---|---|---|
| Build result | `BUILD SUCCESSFUL in 3m 38s`, `gradle_exit=0` (warm cache) | `./gradlew clean build` |
| Test count | 241 tests, 35 classes, 0 failures/errors/skipped | aggregate `build/test-results/test/TEST-*.xml` |
| Test source files | 36 (35 test classes + `CrdbTestResource`) | `find src/test/java -name '*.java' \| wc -l` |
| Main source files | 44 | `find src/main/java -name '*.java' \| wc -l` |
| Liquibase changelogs | 9 files (8 changesets + master) | `find src/main/resources/db/changelog -name '*.xml' \| wc -l` |
| Gradle wrapper | 9.3.1 | `./gradlew --version` |
| JDK | 25 (Temurin 25+36-LTS) | `java -version` |
| Quarkus platform | 3.33.2.1 | `gradle.properties` |
| Application version | 2.0.1 | `build.gradle` |
| Stage constants | 29 | `domain/Stage.java` |
| Image knobs | 29 in `application.yml`, 29 in the manifest | `grep -c '\-image: \${AGT_' src/main/resources/application.yml` |
| Env vars NAMED in `application.yml` | 74. **Not a closed set**: any property is also settable by its derived name | regex over `${VAR:` in `application.yml` |
| `AGT_HOLDER_ID` outranks `HOSTNAME` | proved | booted AGT with both set against a throwaway CRDB; `SELECT holder FROM agt_lease` returned the `AGT_HOLDER_ID` value |
| `AGT_EXCHANGE_ROOT` outranks `DCRE_EXCHANGE_ROOT` | proved | same run, two populated trees; only the `AGT_EXCHANGE_ROOT` one was scanned |
| Metrics published | 8 names in 3 prefixes | `command grep -rn --include='*.java' -oE '"(agt\|dcre)_[a-z_]+"' src/main/java \| sort -u` |
| `agt_ops` tables | 5 | `command grep -rhoE 'tableName="[a-z_]+"' src/main/resources/db/changelog/ \| sort -u` |
| Stage services with a Dockerfile | 22 of 30; 8 payments services have none | `find <dcre> -name Dockerfile -not -path '*/build/*'` |
| `dcre-agt` Grafana dashboard | none in any repository | `command grep -rl "agt_lease_held" <dcre-infra>` exit 1, with a positive control at exit 0 |
| Stale jars in the image | 3 application jars in `dcre-agt:2.0.1` | `docker run --rm --entrypoint sh dcre-agt:2.0.1 -c 'ls /deployments/app/'` |
| CockroachDB test image | `cockroachdb/cockroach:v26.2.3` | `src/test/java/za/co/fnb/dcre/agt/CrdbTestResource.java` |
| Exchange root default | resolves to `env/repo/infra/dcre-infra/exchange` | `realpath ../../../../../infra/dcre-infra/exchange` |
| Deployed image (dev) | `dcre-agt:2.3.0`, containing `agt-2.0.1.jar` | `kubectl get deploy`, `docker run --entrypoint sh` |
| Databases on the dev cluster | 4 of 5; `dcre_hcs` absent | `cockroach sql --database=defaultdb -e "SHOW DATABASES;"` |
| Manifest env vars | 37 total, 29 of them image knobs | regex over `- name: <UPPER>` in `k8s/10-agt-deployment.yml` |
| Running deployment env vars | 27 total, 21 image knobs | `kubectl get deploy dcre-agt -o json` |
| Payments services reading `DCRE_DB_URL` | 9 of 9, as of 2026-08-09 | `git -C <each payments repo> show HEAD:src/main/resources/application.yml`, then grep the `url:` line. Each service is its OWN repository; a `git show` from the parent directory resolves elsewhere and returns nothing. |
| Loops that gate on the lease | all writers; `SlaMonitor`, `LatentDirAuditor`, `MetricsService` do not | `grep -c holdsLease` per file in `service/` |
| DCRE repositories | 38, all private | `gh api repos/sean-huni/<name> --jq '.private'` |
