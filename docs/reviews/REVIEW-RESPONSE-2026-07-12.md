# Response to Fugu-Ultra AGT review (2026-07-12)

Verdict was NEEDS-FIXES. Ship gate (F1-F9 + F10/F11/F12/F15) fully closed this sprint; follow-ups tracked.

| # | Sev | Disposition |
|---|---|---|
| F1 | BLK | FIXED: Reconciler is status-driven; LAUNCHED intents are NEVER recreated; reaped Jobs resolve from the durable outcome seam, absent-after-grace => TECH_FAILED ("VanishedNoSeam"); INTENDED+live promotes to LAUNCHED. Observation decoupled from launching (agt.observe-enabled) |
| F2 | BLK | FIXED: job names carry the full 128-bit arrival UUID (41 chars) |
| F3 | BLK | FIXED: content-level dedup (sameContentExists + partial unique index uq_arrival_content) makes same-bytes-any-name a no-op; duplicates archived, never deleted |
| F4 | HIGH | FIXED: claim-first (uuid ATOMIC_MOVE to inflight) before hash + insert; row carries claimed_path at insert; no delete path for unclaimed payloads |
| F5 | HIGH | FIXED: hash computed on the CLAIMED file; all moves ATOMIC_MOVE; quarantine/duplicate names uuid-prefixed (evidence never clobbered) |
| F6 | HIGH | FIXED: terminal verdicts require CIR business-done; tech-failed responder keeps the arrival open (test: techFailedResponderBlocksTerminalVerdict) |
| F7 | HIGH | FIXED: transitionArrival is CAS + monotonic (WHERE status=from); lease re-checked before engine side effects. Full fencing epoch deferred (follow-up) |
| F8 | HIGH | FIXED: absent/IO-unreadable seam => retry (empty), invalid content => TECH_FAILED; grace cutoff owned by the reconciler |
| F9 | HIGH | FIXED: terminality from Job conditions (type/status), real condition+reason persisted, best-effort pod exit code, Job UID stored at create/409 and verified before recording |
| F10 | MED | FIXED: all five loops SKIP concurrent execution; ConcurrentHashMap; per-item try/catch in watcher/engine/observer/reconciler |
| F11 | MED | FIXED: uq_arrival_logical_key partial unique index; insert-conflict fallback classifies duplicate-vs-quarantine |
| F12 | MED | FIXED: BUSINESS_PARTIAL fail-closed (CIR only, CDE/CRW suppressed) per A-16 default; completes via CIR |
| F13 | MED | FIXED: unparseable filename => QUARANTINED(UNPARSEABLE_FILENAME), fail closed |
| F15 | MED | FIXED (lease path): 40001 retry-with-backoff around the CAS; ledger-wide retry helper is a follow-up |
| F14 | MED | Follow-up (M2): rename/ready-marker as primary readiness; size-stability stays secondary |
| F16 | MED | Follow-up (M2): NEEDS_ATTENTION state + metric for TECH_FAILED stalls; R-12 relaunch policy |
| F17 | LOW | FIXED: streaming digest |
| F18 | LOW | FIXED (stub writes tmp+rename); GC of outcome files is a follow-up |
| F19 | LOW | Follow-up: CHECK constraints on enum-like columns |
| F20 | LOW | Tracked in register (post-ACK fatal is the CIR ACK-semantics seam, Fugu collections F11) |
| F21 | LOW | FIXED: CRR launch guarded on claimed_path non-null |
| F22 | LOW | Partially fixed (lease re-check); unique-holder fail-fast + fencing epoch follow-up |
| F23 | LOW | FIXED: chaos outcome override hook; FILE_FATAL routing e2e-proven on kind (CIR-only, DAG_FAILED after responder done) |

Live evidence after the fold: FATALTEST run shows CRR ACCEPTED -> CTV FILE_FATAL -> CIR only -> DAG_FAILED, full-UUID job names, real conditions (Complete/CompletionsReached). Test suite 14/14 green on Testcontainers CockroachDB.
