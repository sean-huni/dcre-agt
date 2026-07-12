I verified the surrounding facts that decide two of the sharpest questions (does the outcome-file survive TTL reap, and how many AGT instances actually race). Findings below are the finalized, consolidated review.

## Verified environment facts (these scope the race analysis)

- **The exchange volume is `ReadWriteMany`, hostPath-backed, and shared by AGT and every Job pod** (`PVC dcre-exchange (RWX)`; deployment mounts it at `/exchange`; jobs write `/exchange/outcomes/$JOB_NAME`). **The outcome file therefore survives `ttlSecondsAfterFinished` Job reaping.** This is what makes F1 both dangerous (present-but-unread → recreate) and *fixable* (AGT can read it after reap).
- **AGT is `replicas: 1`, `strategy: Recreate`.** So the "two live replicas double-launch" scenario is largely prevented at the orchestration layer, and same-identity double-launch is additionally blocked by DB unique constraints + name-idempotent create. The **real** races are therefore (a) the four scheduled loops racing *within one process* on shared threads, and (b) a single AGT racing *its own crash/restart* against the ledgers + TTL. I weight severities accordingly.
- Master changelog wires in `001-agtops-core.xml`; `holderId = ${HOSTNAME:local-agt}`; `LaunchIntent.status` is a free `String`.

## Invariant scorecard

| Invariant | Verdict |
|---|---|
| Killed orchestrator resumes without duplicate launches | ❌ **broken** — reconciler recreates reaped/`LAUNCHED` intents (F1) |
| Deterministic Job names | ❌ 32-bit truncation collides (F2) |
| SHA-256 identity; same-content re-delivery = no-op | ❌ dedup keyed on filename, not content (F3); identity computed pre-claim (F5) |
| Create-level idempotency via 409 | ✅ correct (409 swallowed) — but useless once names collide (F2) |
| K8s pinning (Never / backoff 0 / TTL) | ✅ pinned; interacts fatally with observation (F1) |
| Outcome file is the seam; absence ≠ success | ⚠️ honored on happy path; transient read → false TECH_FAILED (F8); file ignored after reap (F1) |
| TECH_FAILED never launches successors | ✅ in `computeLaunches`; but responder-fail mis-terminates arrival (F6) |
| Ledger monotonicity under lease expiry | ❌ blind UPDATEs regress state (F7) |
| agt_ops records observed K8s termination facts | ❌ exit code/condition fabricated (F9) |

---

# Findings

## BLOCKERS

### F1 — Reconciler re-runs completed stages after TTL reap → duplicate side effects (breaks "resume without duplicate launches" + R-05)
The reconciler decides "recreate" from `intentsWithoutOutcome()` + *live-Job presence*, and **does not inspect `intent.status()`**:
```java
for (LaunchIntent intent : repo.intentsWithoutOutcome())
    if (!liveNames.contains(intent.jobName()))
        launcher.createJob(...);   // recreates LAUNCHED-and-reaped intents too
```
Reachable paths (all plausible on a 1-replica Deployment):
- **(a)** A `LAUNCHED` job completes, writes its outcome file, and is reaped after `ttlSecondsAfterFinished=300` **before AGT observes it** — because AGT crashed/was rescheduling, was not lease holder, or `launchEnabled=false` for a maintenance pause > 5 min (`OutcomeWatcher` is gated on `launchEnabled`, so pausing launches also pauses observation). `OutcomeWatcher` sees `job == null` → `continue`; reconciler sees no outcome + no live Job → **recreates**. For CRW that is **re-emitted payment XML**.
- **(b)** `createJob()` succeeds but `markIntentLaunched()` throws; intent stays `INTENDED`, `OutcomeWatcher` skips it (`status != LAUNCHED`), the job runs, gets reaped, reconciler recreates.

Because `insertOutcome` is `intent_id`-keyed with no attempt/Job-UID, the ledger can't even tell two executions occurred.

**Fix (viable precisely because the outcome file survives reap — verified):** reconcile *by status*, and read the durable seam instead of blind-recreating:
- `INTENDED` + no live Job → `createJob` (genuinely unconfirmed create — the only safe recreate).
- `INTENDED` + live Job → promote to `LAUNCHED`.
- `LAUNCHED` + live terminal Job → record outcome.
- **`LAUNCHED` + no live Job → never recreate.** Read `/exchange/outcomes/<job_name>`: present+valid → record that verdict; absent after a bounded grace → record `TECH_FAILED`.
- Decouple the flags: `agt.observe-enabled` must stay on even when `agt.launch-enabled` is off. Persist the Job UID on create (see F9) so a later same-name object is never mistaken for the original.

### F2 — 32-bit Job-name truncation collides → engine wedge + cross-arrival contamination
```java
"dcre-" + stage.name().toLowerCase() + "-" + arrivalId.toString().substring(0, 8) // 32 bits
```
`job_name` is globally `UNIQUE`. By the birthday bound collisions become likely near ~65k arrivals/stage — days-to-weeks at bank volumes. On a colliding *different* arrival, `insertIntent`'s `ON CONFLICT (arrival_id, stage)` does **not** cover `uq_intent_job_name` → raw `SQLException` → `IllegalStateException` escapes `DagEngine.tick()`; that arrival re-throws every tick and **blocks every arrival after it in the loop** (permanent liveness failure). Had the DB allowed it, two arrivals would share one Job + one outcome file → wrong verdict, and could read a **stale accumulated outcome file** from the earlier run (F18).
**Fix:** use the full UUID: `"dcre-" + stage.name().toLowerCase() + "-" + arrivalId.toString().replace("-","")` → 41 chars (≤ 63, DNS-1123-safe). Also wrap the per-arrival body in try/catch so one poisoned arrival cannot wedge the loop (see F10).

### F3 — Same content re-delivered under a different filename is processed twice (idempotency flaw; dedup keyed on filename, not content)
The no-op dedup is `ON CONFLICT (route_id, physical_filename, payload_sha256)`, and `sameKeyDifferentHashExists` only fires when the hash *differs*. So `FNBCC01_MSG123_A.dat` and `FNBCC01_MSG123_REDELIVERY.dat` with **identical bytes** collide on neither guard → the second is a fresh `CLAIMED` arrival → **a second DAG runs for the same logical file** (double debit). R-16's "same-hash re-delivery is a no-op" is defeated by a filename change (MFT retransmits with timestamped names, operator re-drops). There is **no DB backstop on the logical key.**
**Fix:** make same-content a no-op independent of filename (dedup on `(route_id, payload_sha256)` or on the logical key `(route_id, client_token, msg_id_token)` for non-quarantined rows), and enforce it with a DB unique/partial index so the database is the arbiter (R-04 posture), not a pre-check.

## HIGH

### F4 — Arrival-claim crash windows orphan the arrival and can delete the only payload copy
Order is `insertArrival(CLAIMED)` → `move(→inflight)` → `updateArrivalClaimedPath`.
- **insert ok, move throws/crash:** row `CLAIMED`, `claimed_path=NULL`, file still in `onhost-req`. Next tick re-registers → same `(route,name,hash)` → `insertArrival` returns empty → the duplicate branch runs **`Files.deleteIfExists(file)` and deletes the only payload**, leaving a `CLAIMED` arrival with no file. CRR is then launched against a null input.
- **move ok, update throws/crash:** file in `inflight`, `claimed_path=NULL` forever; watcher won't re-see it.

**Fix:** make the physical claim the first durable, idempotent act — generate the arrival UUID in app code, `ATOMIC_MOVE` to `inflight/<uuid>_<name>`, then `INSERT` the row already carrying `claimed_path`; or insert `CLAIMING` → move → CAS to `CLAIMED`. Never `deleteIfExists` a source that backs an incomplete claim. Add startup reconciliation for `claimed_path IS NULL` rows and inflight files without rows.

### F5 — SHA-256 is computed before the claim, and the move is not atomic (identity may not match claimed bytes)
`sha256(file)` runs on the file *in `onhost-req`* before `move(...)`, and `move` uses `REPLACE_EXISTING` (not `ATOMIC_MOVE`). A producer that resumes writing after the 2-tick size check, or an overlapping watcher tick (F10), makes the stored hash describe **different bytes than the claimed payload** — corrupting the very identity the whole registry rests on. The quarantine move (also `REPLACE_EXISTING`) can overwrite prior evidence in `error/`.
**Fix:** claim first (`ATOMIC_MOVE`, same filesystem — holds for this PVC), then hash the *claimed* inflight file; write that hash into the row. Quarantine to a unique name so evidence isn't clobbered.

### F6 — A tech-failed CIR still closes the arrival as DAG_FAILED → lost OnHost NACK
```java
if (outcomes.getOrDefault(Stage.CIR, null) != null && anyMatch(BUSINESS_FILE_FATAL)) → DAG_FAILED
```
The guard is "CIR outcome is *non-null*," not "CIR is *business-done*." If CIR (the OnHost responder) `TECH_FAILED`, the arrival is marked terminally `DAG_FAILED` and leaves `DAG_RUNNING` — **the NACK was never emitted, and nothing relaunches CIR.** The file-fatal file silently vanishes with OnHost never told.
**Fix:** require `isBusinessDone(outcomes.get(CIR))` before returning `DAG_FAILED`/`DAG_COMPLETE`; otherwise keep the arrival open / move it to `NEEDS_ATTENTION` (F16) so the responder retries or alerts.

### F7 — Blind status UPDATEs + check-then-act lease → terminal-state regression
Every transition is an unconditional write:
```java
repo.updateArrivalStatus(arrival.id(), ArrivalStatus.DAG_RUNNING);   // no WHERE guard
```
A tick reads a `CLAIMED`/`DAG_RUNNING` arrival, stalls past the 30 s lease (GC pause, slow DB/K8s), another actor advances the arrival to `DAG_COMPLETE`, then the stale tick resumes and **writes `DAG_RUNNING` over a terminal state.** `holdsLease()` is checked once at tick start, never re-checked before the write.
**Fix:** make transitions compare-and-set and monotonic — `CLAIMED→DAG_RUNNING WHERE status='CLAIMED'`; terminal only `WHERE status='DAG_RUNNING'`; never terminal→running. Re-check `holdsLease()` immediately before each side-effecting write; longer term add a lease epoch/fencing token stamped into writes.

### F8 — A transient outcome-file read error is frozen into an irrevocable false TECH_FAILED
```java
try { return Outcome.valueOf(Files.readString(f).strip()); }
catch (Exception e) { return Outcome.TECH_FAILED; }
```
This conflates *file present but unparseable* (genuinely TECH) with *file not yet visible / IO hiccup* on the shared PVC (remount, NFS/hostPath lag). Because `insertOutcome` is first-write-wins, one blip on a job that actually **succeeded permanently records TECH_FAILED and destroys the real `BUSINESS_ACCEPTED`**, with no successors and no recovery.
**Fix:** for a *Complete* Job, only finalize `TECH_FAILED` when the file is provably absent after a bounded grace/retry window; treat `IOException`/not-yet-readable as "retry next tick"; treat only `IllegalArgumentException` (bad content) as arbiter-TECH_FAILED. Write outcome files atomically (F18).

### F9 — K8s termination facts are fabricated, not observed (R-33) + fragile fabric8 read
```java
repo.insertOutcome(intent.id(), outcome, failed ? 1 : 0, failed ? "Failed" : "Complete");
```
The exit code is synthesized from a boolean and the condition string is hardcoded. R-33 makes agt_ops "the sole authority for … externally observed K8s Job terminal state **+ exit code**." Also, terminality is read from `.status.succeeded/.failed` **counters** rather than Job **conditions**, which misclassifies cases like `activeDeadlineSeconds` expiry (`Failed`/`DeadlineExceeded`), and no Job **UID** is captured, so a same-name recreate (F1) can't be distinguished from the original.
**Fix:** derive terminality from `job.status.conditions[type in {Complete,Failed} && status=True]`; persist the real condition type + reason; read the controlled Pod's `state.terminated.exitCode`; store the Job `metadata.uid` on the intent at create/409 time and verify it before recording an outcome. Widen `k8s_condition` for reasons like `DeadlineExceeded`/`BackoffLimitExceeded`.

## MEDIUM

### F10 — Scheduled loops overlap (default `PROCEED`) + shared `HashMap`, no per-item isolation
`register()` (hash `readAllBytes` + DB + move) runs synchronously inside the watcher tick, so a large file makes the tick exceed its 2 s period; Quarkus default `concurrentExecution = PROCEED` then fires the next tick concurrently, and both mutate the plain `HashMap lastSizes` (`put`/`remove`/`keySet().removeIf`) → data race / `ConcurrentModificationException` / corrupted stability tracking. `IllegalStateException` from any single file/arrival also aborts the whole `forEach` tick.
**Fix:** add `@Scheduled(concurrentExecution = SKIP)` to all four loops (they are singleton, level-triggered), make `lastSizes` a `ConcurrentHashMap`, and wrap per-file/per-arrival/per-intent bodies in try/catch so one bad item can't starve the rest.

### F11 — Same-key/different-hash quarantine is check-then-act across two connections, not DB-enforced
`sameKeyDifferentHashExists` (SELECT) and `insertArrival` (INSERT) run on separate connections. Two overlapping ticks (F10) or a split-brain window (F22) let two different-hash arrivals with the same logical key both pass the SELECT and both insert as `CLAIMED` — **neither quarantines**, and nothing in the schema backstops it.
**Fix:** perform check+insert+quarantine in one transaction, and add a partial unique/exclusion constraint on `(route_id, client_token, msg_id_token)` for non-quarantined rows (this is the same DB backstop F3 needs).

### F12 — `BUSINESS_PARTIAL` unconditionally cascades to CDE/CRW (violates A-16 fail-closed default)
`computeLaunches` treats `BUSINESS_PARTIAL` identically to `BUSINESS_ACCEPTED`. Per R-19/A-16 the default until the OnHost contract is closed is `ALL_OR_NOTHING`, fail-closed — **suppress CDE/CRW on any item failure.** The code bakes in the unsafe default (proceed on partial → debit from a file OnHost may consider failed).
**Fix:** gate the partial→successor edge behind a per-client `acceptance_mode`; default to suppressing CDE/CRW on `BUSINESS_PARTIAL` while still allowing CIR.

### F13 — Malformed/non-FNB filenames are accepted (fail-open)
When the stem doesn't start with `FNB`, tokens are null; `sameKeyDifferentHashExists` returns `false`, and the file is accepted as a normal `CLAIMED` arrival with no logical key. R-31 says every inbound OnHost filename carries InitgPty+MsgId, so a file lacking them is malformed and must fail closed.
**Fix:** if required tokens can't be parsed, insert `QUARANTINED` with reason `UNPARSEABLE_FILENAME`, move to `error/`, and don't launch CRR.

### F14 — Producer-ready protocol is size-stability only (no rename/marker)
Accepting any non-`.tmp` file whose size is equal across two 2 s ticks lets a writer that stalls for one 4 s window be claimed and hashed as a **partial file**, and that partial hash becomes the durable identity (F5).
**Fix:** require stage-then-rename from a temp name or a ready-marker/manifest as the primary readiness contract; keep size-stability as a secondary guard.

### F15 — No CockroachDB serialization-retry (SQLSTATE 40001) anywhere (SPEC-DAG §6b unmet)
Every repo/lease method throws `IllegalStateException` on any `SQLException`. Under CRDB serializable, `40001` retries are normal under contention. For ledger writes this merely aborts an idempotent tick, but for `LeaseService.tryAcquire`/`renewOrAcquire` a run of aborted renewals can let the 30 s lease **lapse and flap** (needless standby takeover, loops no-op).
**Fix:** wrap agt_ops statements in a 40001-aware retry-with-backoff helper; prioritize the lease path.

### F16 — TECH_FAILED has no relaunch/terminal/alert path
Once `TECH_FAILED` is recorded, the intent has an outcome (reconciler ignores it) and `computeLaunches` returns nothing → the arrival sits in `DAG_RUNNING` **forever**, invisibly (e.g., tech-failed CRW after CIR already ACKed = OnHost ACKed, no debit, no alert). The code comment defers this to "the reconciler's/operator's call," but **no such path or operator surface exists.**
**Fix:** add a `DAG_STALLED`/`NEEDS_ATTENTION` state + metric when any stage is `TECH_FAILED` with the DAG non-terminal; define the R-12 carry-over/relaunch policy even if manual in M1.

## LOW

### F17 — SHA-256 loads the whole file into memory
`md.digest(Files.readAllBytes(file))` risks OOM on the long-running AGT for large collection files and blocks the scheduler thread (feeds F10).
**Fix:** stream via `DigestInputStream`/`MessageDigest.update` in fixed buffers.

### F18 — Outcome files are non-atomic, un-scoped, and never GC'd
`echo BUSINESS_ACCEPTED > /exchange/outcomes/$JOB_NAME` is a non-atomic write (partial-read hazard once F1's "read after reap" fix lands), the files accumulate forever, and a name reuse (F2) could read a **stale** prior file as a new verdict.
**Fix:** write `*.tmp` then rename; scope the filename by Job UID/attempt; GC after the outcome row is recorded.

### F19 — DDL has no CHECK constraints on enum-like columns
`status`, `stage`, `outcome`, `quarantine_reason` are free strings. A bad value makes `ArrivalStatus/Stage/Outcome.valueOf(...)` throw inside a scheduled loop → wedge (same failure class as F2/F10).
**Fix:** add `CHECK (... IN (...))` (or reference tables) and handle unknown values defensively so one bad row can't kill a tick.

### F20 — Post-ACK file-fatal silently flips to DAG_FAILED (ACK-vs-submission seam)
On the accepted fork both CDE and CIR launch; if CDE later returns `BUSINESS_FILE_FATAL`, `terminalState` sets `DAG_FAILED` although CIR already sent an "accepted" initial response. Latent in M1 (stubs never emit FILE_FATAL) but mirrors the spec's ACK-vs-downstream seam.
**Fix:** define post-ACK fatal as a reportable exception surfaced via PRG, not a silent `DAG_FAILED`.

### F21 — `register()` Result ignored; CRR launched without a verified claimed_path
`DirectoryWatcher` discards the `Result`, and `DagEngine` launches CRR for `CLAIMED` arrivals without checking `claimed_path` is set — combining with F4 to launch stages against missing inputs.
**Fix:** guard the CRR launch on non-null `claimed_path`; act on/log the register result.

### F22 — Lease is a contention-reducer, not a fence; holder id defaults can collide
The CAS SQL is correct, and with `replicas:1`+`Recreate`+DB-unique constraints+name-idempotent create, two-live-instance double-launch is genuinely low-risk — so this is LOW, not the headline. But `holderId` defaults to `${HOSTNAME:local-agt}`, so two **local/dev** processes both become `local-agt` and both "hold" the lease → double everything; and the reconciler's recreate rides on a check-then-act lease (F1/F7).
**Fix:** require a unique `holderId` (fail fast if it's the shared default with >1 expected instance); re-check the lease before side effects; add the epoch/fence from F7.

### F23 — NACK/fork routing is untested end-to-end
The M1 stub only emits `BUSINESS_ACCEPTED` or `exit 1` (TECH). The `BUSINESS_FILE_FATAL`/`BUSINESS_PARTIAL` branches — the core SPEC-DAG §3 CIR-only fork routing — are **unexercised by e2e** (chaos test does `fail-CTV` = TECH only).
**Fix:** give the stub a chaos hook to emit FILE_FATAL/PARTIAL and add e2e assertions for CIR-only routing and partial suppression (F12).

---

## Non-findings I verified (so the next reviewer can skip them)
- `previous != size` is **safe** — `size` is primitive `long`, so the `Long` unboxes to a numeric compare (after the `previous == null` check).
- `INSERT … ON CONFLICT DO NOTHING RETURNING` correctly yields empty on conflict and is concurrency-safe on CRDB → same-identity double-launch is genuinely prevented at the DB.
- `createJob` 409 handling is correct (swallow `AlreadyExists` = idempotent; rethrow others for reconciler retry). fabric8 `.get()` returning `null` when absent is handled correctly.
- Lease CAS predicate (`expires_at < now() OR holder = excluded.holder`) is correct; TTL 30 s / renew 5 s gives adequate headroom.
- Write-ahead ordering (intent INSERT before Job create) is correct; the crash-between window is handled *for INTENDED intents* — the defect is that F1 also mishandles LAUNCHED.
- All three idempotency-backing unique constraints (`uq_arrival_identity`, `uq_intent_arrival_stage`, `uq_outcome_intent`) exist; master changelog includes the core changeset; `PersistentVolumeClaim`/label/`resource().create()` fabric8 calls are API-valid.

---

## Verdict: **NEEDS-FIXES**

The bounded-context split, write-ahead-intent pattern, and level-triggered re-derivation are sound, and same-identity double-launch is genuinely prevented by the DB constraints. But the two headline invariants are defeated: **F1** (the reconciler re-runs completed/reaped stages — directly violating "a killed orchestrator must resume without duplicate launches" and R-05; worst for side-effecting CRW) and **F2** (32-bit Job-name truncation collides, wedging the engine and contaminating outcomes), with **F3** (same-content re-delivery under a new name double-processes) close behind. The HIGH set — **F4/F5** (payload deletion + identity computed pre-claim), **F6** (lost NACK on responder tech-failure), **F7** (terminal-state regression), **F8** (irrevocable false TECH_FAILED), **F9** (fabricated termination facts) — each independently breaks a stated invariant.

**Ship gate:** resolve **F1–F9** (F1/F2/F3 are non-negotiable), and land **F10, F11, F12, F15** before this drives real side-effecting stages in M2. F13–F23 are follow-ups that should be tracked but need not block once the blockers/highs are closed.