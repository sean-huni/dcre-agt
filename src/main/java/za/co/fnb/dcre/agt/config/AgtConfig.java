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

    /** Service images per stage (SCRUM-33: no stub fallback; a missing image
     *  fails the launch fast). */
    java.util.Optional<String> crrImage();

    java.util.Optional<String> ctvImage();

    java.util.Optional<String> cirImage();

    java.util.Optional<String> cdeImage();

    java.util.Optional<String> crwImage();

    /** M4 fint-resp reader images (single-stage response DAGs). */
    java.util.Optional<String> ixrImage();

    java.util.Optional<String> sxrImage();

    java.util.Optional<String> pxrImage();

    /** M4 PRG clock-window executor image (R-28). */
    java.util.Optional<String> prgImage();

    /** M5 AIS endorsements stage image (ENDO route, R-36). */
    java.util.Optional<String> aisImage();

    /** M6 HCS holiday-calendar-sync clock executor image (R-38). */
    java.util.Optional<String> hcsImage();

    /** M10 mandates stage images (SCRUM-79). Absent/empty = launch-disabled:
     *  the MRG scheduler skips its windows and a DAG launch fails fast
     *  (SCRUM-33 semantics, no stub fallback). */
    java.util.Optional<String> mrrImage();

    java.util.Optional<String> mrvImage();

    java.util.Optional<String> mafImage();

    java.util.Optional<String> misImage();

    java.util.Optional<String> mirImage();

    java.util.Optional<String> mrwImage();

    /** SCRUM-91 response-leg images: one reader per pain.012 leg (ISR/SBSR/PBSR),
     *  replacing the merged mar-image and the msr-image projection writer. */
    java.util.Optional<String> mixImage();

    java.util.Optional<String> msxImage();

    java.util.Optional<String> mpxImage();

    java.util.Optional<String> mrgImage();

    /** INTERIM (R-42 analog for M10): client tokens that are mandate-capable;
     *  MRG windows launch only for these, until the R-14 client reference
     *  table lands. Normalized (trim + uppercase) on read like pay-clients. */
    @WithDefault("FNBCC01,FNBCC02,FNBRF01")
    Set<String> manClients();

    /** CRW Process-Date Executor window length (R-37); dev default 60s. */
    @WithDefault("60")
    long crwIntervalSeconds();

    /** PRG clock-window length (R-28); dev default 60s. */
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

    /** JDBC url the M10 mandates stage Jobs use for dcre_man (B2, SCRUM-79
     *  review): the man services own their schema in dcre_man; handing them
     *  the dcre_col URL would silently build it there. */
    @WithDefault("jdbc:postgresql://crdb.dcre.svc.cluster.local:26257/dcre_man?sslmode=disable")
    String manServiceDbUrl();

    /** Which mandate store CTV's DC-flow gate reads (SCRUM-91, Task 11 Step 8).
     *  Handed to every CTV stage pod as DCRE_CTV_MANDATE_SOURCE, so the gate is
     *  switchable from AGT instead of being frozen at ctv's yml default: nothing
     *  else injects it, so an in-cluster CTV always ran `legacy`, which reads
     *  `FROM mandate` in dcre_col, a table only env-reset.sh --seed creates.
     *  Values are ctv's MandateSource enum (legacy|projection), parsed there and
     *  never interpreted here: AGT only carries the token, so a new mode needs no
     *  AGT change. The default is `legacy`, matching ctv's own application.yml
     *  default, so wiring the seam changes nothing until it is set. */
    @WithDefault("legacy")
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
