package za.co.fnb.dcre.agt.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

import java.util.Set;

@ConfigMapping(prefix = "agt")
public interface AgtConfig {

    /** Root of the exchange directory tree (R-30 contract). */
    String exchangeRoot();

    String namespace();

    /** SCRUM-70 flow namespaces: agt.namespace above stays AGT's own/control
     *  namespace; stage Jobs launch into the flow namespace of the route
     *  family (spec 2026-07-21 section 1 item 3). */
    @WithDefault("dcre-col")
    String namespaceCol();

    @WithDefault("dcre-pay")
    String namespacePay();

    /** Dormant until the M10 mandates program. */
    @WithDefault("dcre-man")
    String namespaceMan();

    /** INTERIM (R-42): client tokens whose fint-resp arrivals and clock jobs
     *  ride the pay flow, until the R-14 client reference table lands.
     *  Membership semantics (m4): consumers normalize (trim + uppercase) on read. */
    @WithDefault("FNBRF01")
    Set<String> payClients();

    /** Unique holder id for the DB lease (pod hostname). */
    String holderId();

    /** Gate for K8s launches; disabled in unit tests. */
    @WithDefault("true")
    boolean launchEnabled();

    /** Observation stays on even when launching is paused (Fugu F1a). */
    @WithDefault("true")
    boolean observeEnabled();

    /** Stage images moved to {@link AgtImages} with the v1 topology (29 knobs, same
     *  {@code agt} prefix, so {@code AGT_<STAGE>_IMAGE} bindings are unchanged). */

    /** INTERIM (R-42 analog for M10): client tokens that are mandate-capable;
     *  MRG windows launch only for these, until the R-14 client reference
     *  table lands. Normalized (trim + uppercase) on read like pay-clients. */
    @WithDefault("FNBCC01,FNBCC02,FNBRF01")
    Set<String> manClients();

    /** CRW Process-Date Executor window length (R-37); dev default 60s. */
    @WithDefault("60")
    long crwIntervalSeconds();

    /** CRG COLLECTIONS-report clock-window length (R-28); dev default 60s.
     *  This is the knob the pre-cutover {@code prg-interval-seconds} actually
     *  controlled, under the name the diagrams give that generator. */
    @WithDefault("60")
    long crgIntervalSeconds();

    /** PRG PAYMENTS-report clock-window length; dev default 60s. Its own knob, not
     *  a shared one: payments transactions are processed IMMEDIATELY while
     *  collections wait for the collection day, so the two report cadences have no
     *  reason to move together and must be tunable apart. */
    @WithDefault("60")
    long prgIntervalSeconds();

    /** HCS holiday-sync clock-window length in hours (R-38); Nager re-sync cadence. */
    @WithDefault("6")
    int hcsIntervalHours();

    /** MRG mandates-report clock-window length (M10/SCRUM-79); dev default 60s. */
    @WithDefault("60")
    long mrgIntervalSeconds();

    /** MRG suspend-sweep clock-window length (SCRUM-91): ACCP mandates with too
     *  many consecutive failed collections get a SUSPENDED override (MS03). Its
     *  own knob so the sweep is paced independently of the MRG report windows;
     *  dev default 60s. There is no expiry knob any more: the auth window is a
     *  predicate of mandate_effective_status, so nothing sweeps it. */
    @WithDefault("60")
    long mrgSuspendIntervalSeconds();

    /** JDBC url the service Jobs use for dcre_col (in-cluster).
     *  SCRUM-70: FQDN, because stage pods run in the flow namespaces where the
     *  short service name `crdb` does not resolve. */
    @WithDefault("jdbc:postgresql://crdb.dcre.svc.cluster.local:26257/dcre_col?sslmode=disable")
    String serviceDbUrl();

    /** JDBC url the PAYMENTS stage Jobs use for dcre_pay (v1 topology). Same FQDN
     *  treatment as the man url and for the same reason: stage pods run in the flow
     *  namespaces where the short service name {@code crdb} does not resolve.
     *
     *  <p>Before this knob existed there was no {@code dcre_pay} URL anywhere in
     *  AGT, so every payments stage received the collections one and would have
     *  built the payments schema inside {@code dcre_col} without erroring. See
     *  {@link za.co.fnb.dcre.agt.service.StageDatabases}. */
    @WithDefault("jdbc:postgresql://crdb.dcre.svc.cluster.local:26257/dcre_pay?sslmode=disable")
    String payServiceDbUrl();

    /** JDBC url the M10 mandates stage Jobs use for dcre_man (B2, SCRUM-79
     *  review): the man services own their schema in dcre_man; handing them
     *  the dcre_col URL would silently build it there. */
    @WithDefault("jdbc:postgresql://crdb.dcre.svc.cluster.local:26257/dcre_man?sslmode=disable")
    String manServiceDbUrl();

    /** JDBC url the HCS holiday-sync Job uses for dcre_hcs (owner ruling 2026-08-08).
     *
     *  <p>HCS used to receive {@link #serviceDbUrl()}, because the enum that said
     *  which NAMESPACE a stage runs in was also the enum that said which DATABASE it
     *  writes, and HCS runs in the collections namespace. The owner ruled holiday data
     *  in dcre_col a "Violation of the 12FactorApp" (https://12factor.net/) and moved
     *  the calendar to its own context. shared/hcs now carries a FamilyGuard comparing
     *  current_database() against dcre_hcs BEFORE any DDL, so with the old routing
     *  every HCS pod AGT launched died on startup rather than quietly re-contaminating
     *  the collections database. This knob is the fix. */
    @WithDefault("jdbc:postgresql://crdb.dcre.svc.cluster.local:26257/dcre_hcs?sslmode=disable")
    String hcsServiceDbUrl();

    /** Which mandate store CTV's DC-flow gate reads (SCRUM-107).
     *  Handed to every CTV stage pod as DCRE_CTV_MANDATE_SOURCE. Values are ctv's
     *  MandateSource enum, parsed there and never interpreted here: AGT only carries
     *  the token.
     *
     *  <p>The default is `projection`, and it must stay in step with BOTH ctv's
     *  application.yml default and agt's own application.yml. This annotation held
     *  `legacy` for one commit after the yml flipped, which is a two-homes-for-one-fact
     *  hazard rather than a cosmetic mismatch: ctv now FAILS CLOSED on `legacy`
     *  (MandateSource.from throws at bean creation), so whichever home wins, handing
     *  `legacy` to a stage pod stops every CTV pod from starting. `legacy` is not a
     *  fallback any more; the dcre_col.mandate table it selected has been dropped.
     *  AgtCtvMandateSourceDefaultTest pins the two homes together. */
    @WithDefault("projection")
    String ctvMandateSource();

    /** JDBC url every launched stage Job gets so the platform-batch heartbeat
     *  writer (M12/SCRUM-88, R-47, T2) can reach agt_ops from the stage pod.
     *  FQDN for the same reason as manServiceDbUrl: stage pods run in the flow
     *  namespaces where the short `crdb` name does not resolve. Identical for
     *  every flow (agt_ops is not sharded by flow), so it is not stage-keyed. */
    @WithDefault("jdbc:postgresql://crdb.dcre.svc.cluster.local:26257/agt_ops?sslmode=disable")
    String agtopsDbUrl();

    /** agt_ops datasource user handed to the stage pod's heartbeat writer (dev
     *  cluster = root). The password stays the blank datasource default; passing
     *  the URL + user is sufficient for dev and forward-compat with a tenant role. */
    @WithDefault("root")
    String agtopsDbUser();

    /** Stage-pod memory request. Default matches the pre-load-test sizing;
     *  large-copybook runs (300k tx) need more (found live 2026-07-14: CTV OOM
     *  at 768Mi across partition workers). */
    @WithDefault("512Mi")
    String stageMemoryRequest();

    /** Stage-pod memory limit (see stageMemoryRequest). */
    @WithDefault("768Mi")
    String stageMemoryLimit();

    /** Stage Job activeDeadlineSeconds. Default matches pre-load-test sizing;
     *  300k-tx runs need more (AIS DeadlineExceeded live 2026-07-14). */
    @WithDefault("900")
    long stageDeadlineSeconds();

    /** SLA amber threshold: hours a tx may sit Fintegrate-visible without a
     *  terminal status before the amber gauge/WARN fires (SCRUM-55 Task 13). */
    @WithDefault("20")
    int slaAmberHours();

    /** SLA red threshold: breach hours; the Grafana alert on the red gauge
     *  drives the email-to-Fintegrate ops runbook. */
    @WithDefault("24")
    int slaRedHours();

    /** Bounded-attempt guard on the IMMEDIATE report trigger: how many consecutive
     *  scans may see the SAME (flow, client, parent) still due before AGT says so.
     *
     *  <p>A report parent that the generator cannot satisfy stays in its
     *  {@code *_report_due} view forever. Nothing throws, nothing is ledgered, and
     *  the deterministic window key makes every later scan an idempotent no-op, so
     *  the failure mode is total silence: AGT re-observes the parent indefinitely
     *  and the client never receives a terminal status. This threshold turns that
     *  into a WARN naming the stage, client, window and flow. It does NOT retry,
     *  widen anything or fall back: a silent loop is made VISIBLE, never quieter.
     *  env AGT_REPORT_STALL_SCANS. */
    @WithDefault("20")
    int reportStallScans();

    /** OrphanSweeper: bounded same-identity relaunch attempts for died arrival Jobs. */
    @WithDefault("3")
    int orphanMaxAttempts();

    /** OrphanSweeper ceiling for TECH_CONFIG_FAILED (pod exit 78: a platform-batch
     *  pre-runner startup failure). Infrastructure, not a job outcome, so it gets
     *  its own, larger budget: the first relaunch is immediate and later ones are
     *  spaced by orphan-backoff-seconds, so 10 covers ~9 minutes of config-plane
     *  outage (a cfg rolling restart or a Vault re-seed is minutes, not seconds),
     *  roughly 3x a realistic recovery with margin. Deliberately BOUNDED: on
     *  exhaustion the intent still mints TECH_EXHAUSTED and the arrival still goes
     *  DAG_FAILED, never a silent unbounded wedge. env AGT_INFRA_MAX_ATTEMPTS. */
    @WithDefault("10")
    int infraMaxAttempts();

    /** Minimum seconds between relaunch attempts of one intent. */
    @WithDefault("60")
    long orphanBackoffSeconds();

    /** Stale-heartbeat TTL (M12/SCRUM-86, R-47): a LAUNCHED arrival intent whose
     *  heartbeat_at fell behind this many seconds is a wedged-but-alive orphan
     *  (k8s Job still Running, step hung, no Failed condition). The stage pod
     *  heartbeats every 10s (platform-batch HeartbeatWriter, T2), so the default
     *  45s is ~4 missed beats before AGT relaunches. env AGT_HEARTBEAT_TTL_SECONDS.
     *  NOTE: the plan labels this dcre.agt.heartbeat-ttl-seconds; the actual
     *  binding is agt.heartbeat-ttl-seconds (this ConfigMapping prefix), which is
     *  what the specified env var AGT_HEARTBEAT_TTL_SECONDS maps to and matches
     *  every other injected AgtConfig knob (orphan-max-attempts, ...). */
    @WithDefault("45")
    long heartbeatTtlSeconds();

    /** AGT self-liveness TTL (M12/SCRUM-87, R-47): the reconciler liveness probe
     *  reports DOWN when the reconcile scheduler has not ticked within this many
     *  seconds; k8s then restarts the wedged AGT pod. Default 15s = 3x the 5s
     *  reconcile interval. env AGT_SELF_LIVENESS_TTL_SECONDS (binds
     *  agt.self-liveness-ttl-seconds; see heartbeatTtlSeconds note on the prefix). */
    @WithDefault("15")
    long selfLivenessTtlSeconds();
}
