package za.co.fnb.dcre.agt.service;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.domain.FileArrival;
import za.co.fnb.dcre.agt.domain.Flow;
import za.co.fnb.dcre.agt.domain.Outcome;
import za.co.fnb.dcre.agt.domain.Stage;
import za.co.fnb.dcre.agt.repo.ArrivalRepo;
import za.co.fnb.dcre.agt.repo.IntentRepo;
import za.co.fnb.dcre.agt.repo.OutcomeRepo;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Write-ahead launcher (SPEC-DAG section 3): intent row first, then the K8s
 * Job create. Deterministic Job names give create-level idempotency (409 =
 * already launched). Job spec pinning per SPEC-DAG section 6.
 */
@ApplicationScoped
public class JobLauncher {

    private static final Logger LOG = Logger.getLogger(JobLauncher.class);

    public static final String LABEL_MANAGED_BY = "dcre/managed-by";
    public static final String LABEL_STAGE = "dcre/stage";
    public static final String LABEL_ARRIVAL = "dcre/arrival";

    /** Env var AGT sets on an MRG clock pod to select the Batch job it runs
     *  (mrgSuspendJob); absent = MRG's mrgJob report default, so the report
     *  windows are never overridden (SCRUM-91, replacing DCRE_MSR_JOB_NAME). */
    public static final String MRG_JOB_ENV = "DCRE_MRG_JOB_NAME";

    /** Env var carrying the dcre_col JDBC url to a pod whose SECOND, read-only
     *  datasource reads the collections DB. Launch-scoped, NOT stage-keyed like
     *  DCRE_DB_URL: only the MRG suspension sweep crosses into dcre_col (the
     *  consecutive-failed-collections signal a single-DB view cannot span), so
     *  the MRG report windows on the same stage must not carry it. Absent, the
     *  pod falls back to MRG's localhost dev default and every window dies with
     *  "Connection to localhost:26257 refused" (found live 2026-07-26). */
    public static final String COL_DB_URL_ENV = "DCRE_COL_DB_URL";

    /** Env var carrying the dcre_man JDBC url to CTV's SECOND, read-only
     *  datasource, which reads the man_ctv_view projection for the mandate gate
     *  (SCRUM-78). STAGE-keyed, not launch-scoped like COL_DB_URL_ENV: CTV is a
     *  DAG stage with no per-launch env seam, and every CTV pod runs the gate,
     *  so the url belongs to the stage exactly as DCRE_DB_URL does. CTV stays a
     *  COLLECTIONS stage in StageDatabases: its PRIMARY datasource is dcre_col and
     *  this is a second, read-only seam. Absent, the
     *  pod falls back to ctv's localhost dev default and the projection gate
     *  cannot reach dcre_man at all (found 2026-07-26). */
    public static final String CTV_MANDATES_DB_URL_ENV = "DCRE_CTV_MANDATES_DB_URL";

    /** Env var selecting WHICH mandate store CTV's DC-flow gate reads (ctv
     *  MandateSource). STAGE-keyed for the same reason as CTV_MANDATES_DB_URL_ENV:
     *  every CTV pod runs the gate and there is no per-launch env seam.
     *
     *  <p>SCRUM-107: the vocabulary is now `projection` only. Absent, the pod takes
     *  ctv's yml default, which is also `projection`, so the seam is inert in the
     *  safe direction. Setting the retired `legacy` value does NOT fall back: ctv
     *  throws at bean creation, so the pod never starts, and the dcre_col.mandate
     *  table that value used to read has been dropped. The token is carried verbatim
     *  from agt.ctv-mandate-source and never interpreted here; ctv owns the
     *  vocabulary. */
    public static final String CTV_MANDATE_SOURCE_ENV = "DCRE_CTV_MANDATE_SOURCE";

    /** Reserved durable-arg prefix carrying a pod env var rather than a Spring
     *  Batch program arg (SCRUM-78). Encoding sweep env into the durable launch
     *  args means the intent row alone rebuilds the same Job on a reconciled
     *  re-create (createJob -> clockJob), exactly as serviceJob derives
     *  DCRE_FLOW_DC from the durable arrival route; the program args the pod
     *  receives stay clean (client, window). No real Batch arg starts with this token. */
    static final String ENV_ARG_PREFIX = "env:";

    /** Boundary stages that read the claimed payload file: each family's flat-file
     *  reader (CRR, PRR, MRR) and each family's three pain.002/pain.012 leg readers. */
    static final java.util.Set<Stage> BOUNDARY_READERS = java.util.EnumSet.of(
            Stage.CRR, Stage.CIX, Stage.CSX, Stage.CPX,
            Stage.PRR, Stage.PIX, Stage.PSX, Stage.PPX,
            Stage.MRR, Stage.MIX, Stage.MSX, Stage.MPX);

    /**
     * Stages AGT may launch: the whole v1 roster, listed EXPLICITLY.
     *
     * <p>It used to be {@code EnumSet.complementOf(EnumSet.of(<the retired ones>))},
     * which fails OPEN: every stage added afterwards was launchable by default and
     * nothing anywhere said so, so a renamed stage silently became launchable again
     * (infra report item 3). Compare {@link StageImages}, an exhaustive switch, which
     * fails CLOSED because adding a constant breaks the build until somebody decides
     * what it does.
     *
     * <p>Every stage is launchable today, so this set is behaviourally redundant, and
     * it is written out anyway: {@code StageRosterTest} asserts it equals
     * {@code EnumSet.allOf(Stage.class)}, so the next stage added to the enum fails
     * that assertion until it is listed here deliberately. A complement would have
     * absorbed it silently. Enforced at launch by {@link #requireLaunchable}.
     */
    static final java.util.Set<Stage> LAUNCHABLE = java.util.Collections.unmodifiableSet(
            java.util.EnumSet.of(
                    Stage.CRR, Stage.CTV, Stage.CDE, Stage.CRW, Stage.CIR,
                    Stage.CIX, Stage.CSX, Stage.CPX, Stage.CRG,
                    Stage.PRR, Stage.PTV, Stage.PAI, Stage.PRW, Stage.PIR,
                    Stage.PIX, Stage.PSX, Stage.PPX, Stage.PRG,
                    Stage.MRR, Stage.MRV, Stage.MAS, Stage.MIT, Stage.MIR, Stage.MRW,
                    Stage.MIX, Stage.MSX, Stage.MPX, Stage.MRG,
                    Stage.HCS));

    /** Whole-file responder stages: carry the A-45 arrival identity params and
     *  the rejecting validator's outcome.hint. One per family: CIR, PIR, MIR. */
    static final java.util.Set<Stage> RESPONDERS =
            java.util.EnumSet.of(Stage.CIR, Stage.PIR, Stage.MIR);

    @Inject
    IntentRepo intentRepo;

    @Inject
    ArrivalRepo arrivalRepo;

    @Inject
    OutcomeRepo outcomeRepo;

    @Inject
    AgtConfig config;

    @Inject
    FlowNamespaces flowNamespaces;

    @Inject
    KubernetesClient k8s;

    @Inject
    StageImages stageImages;

    @Inject
    StageDatabases stageDatabases;

    /** Fail-closed launch gate: a stage outside {@link #LAUNCHABLE} is a bug, never
     *  a fallback. Checked before the write-ahead intent, so a rejected launch leaves
     *  no row behind to reconcile. */
    private static Stage requireLaunchable(final Stage stage) {
        if (!LAUNCHABLE.contains(stage)) {
            throw new IllegalStateException("stage " + stage + " is not launchable:"
                    + " add it to JobLauncher.LAUNCHABLE deliberately or stop launching it");
        }
        return stage;
    }

    /** Full 128-bit arrival identity in the name (Fugu F2). SCRUM-70: the
     *  resolved flow prefix (col-/pay-/man-, 4 chars) replaces the dcre-
     *  literal: 40 chars, DNS-1123 safe, still well under the 63-char limit. */
    public static String jobName(Flow flow, Stage stage, UUID arrivalId) {
        return flow.jobPrefix() + stage.name().toLowerCase() + "-" + arrivalId.toString().replace("-", "");
    }

    /** Launch stage for arrival; no-op when an intent already exists (non-overlap).
     *  Single arrival fetch (m3): flow resolution and the Job spec share it. */
    public void launch(UUID arrivalId, Stage stage) {
        requireLaunchable(stage);
        FileArrival arrival = arrivalOf(arrivalId, stage.name());
        Flow flow = flowNamespaces.flowFor(arrival);
        String name = jobName(flow, stage, arrivalId);
        String namespace = flowNamespaces.namespaceOf(flow);
        Optional<UUID> intent = intentRepo.insertIntent(arrivalId, stage, name, namespace);
        if (intent.isEmpty()) {
            return; // already intended/launched by us or a predecessor incarnation
        }
        createFromSpec(intent.get(), serviceJob(name, namespace, stage, arrival, stageImages.required(stage)));
    }

    /** Create (or re-create after crash) the Job for an existing intent. The
     *  namespace comes from the intent row (durable), never re-resolved. Durable
     *  launch args (clock windows AND SCRUM-90 arrival-scoped IMMEDIATE reports)
     *  rebuild the SAME Job from the intent row; a DAG-stage intent has none and
     *  rebuilds from its arrival. Driven by args presence, not arrivalId nullness:
     *  a report intent carries BOTH an arrival_id and durable report args, and
     *  must recreate as the report Job, never as an arrival stage Job. */
    public void createJob(UUID intentId, UUID arrivalId, Stage stage, String name, String namespace) {
        requireLaunchable(stage);
        java.util.Optional<String> durable = intentRepo.intentLaunchArgs(intentId).filter(a -> !a.isBlank());
        if (durable.isPresent()) {
            createFromSpec(intentId, clockJob(name, namespace, stage, stageImages.required(stage),
                    java.util.List.of(durable.get().split("\\n"))));
            return;
        }
        if (arrivalId == null) {
            throw new IllegalStateException(
                    "intent " + intentId + " has neither durable launch args nor an arrival");
        }
        createFromSpec(intentId, serviceJob(name, namespace, stage,
                arrivalOf(arrivalId, name), stageImages.required(stage)));
    }

    private FileArrival arrivalOf(UUID arrivalId, String context) {
        return arrivalRepo.arrivalById(arrivalId).orElseThrow(() -> new IllegalStateException(
                "arrival " + arrivalId + " not found: cannot build Job for " + context));
    }

    private void createFromSpec(UUID intentId, Job job) {
        String name = job.getMetadata().getName();
        String namespace = job.getMetadata().getNamespace();
        if (!config.launchEnabled()) {
            LOG.debugf("launch disabled; intent %s stays INTENDED", name);
            return;
        }
        String uid;
        try {
            Job created = k8s.batch().v1().jobs().inNamespace(namespace).resource(job).create();
            uid = created.getMetadata() != null ? created.getMetadata().getUid() : null;
            LOG.infof("Launched %s in %s (uid %s)", name, namespace, uid);
        } catch (KubernetesClientException e) {
            if (e.getCode() != 409) {
                throw e; // reconciler retries later; intent stays INTENDED
            }
            Job existing = k8s.batch().v1().jobs().inNamespace(namespace).withName(name).get();
            uid = existing != null && existing.getMetadata() != null ? existing.getMetadata().getUid() : null;
            LOG.infof("Job %s already exists (409, uid %s): treating as launched", name, uid);
        }
        intentRepo.markIntentLaunched(intentId, uid);
    }

    /**
     * DB URL for a stage's Job env, per FAMILY: dcre_col, dcre_pay or dcre_man.
     *
     * <p>Resolved by {@link StageDatabases}, an exhaustive switch with no default
     * arm. The previous form here was a two-way ternary with a collections default,
     * so adding the payments family without touching it would have handed every
     * payments stage the collections database and never errored.
     */
    private String dbUrlFor(Stage stage) {
        return stageDatabases.urlFor(stage);
    }

    /**
     * Clock Job name from (flow, stage, runKey). Lowercase BEFORE sanitizing
     * (M5: a sanitize-first bug collapsed FNBRF01/FNBCC01 to one name), and a
     * LEADING stage token in the run key is stripped once (M6: HcsScheduler's
     * runKey "hcs-w<n>" produced "dcre-hcs-hcs-w<n>"): the name owns the
     * prefix, the run key never contributes it. SCRUM-70: the flow prefix
     * replaces the dcre- literal.
     */
    public static String clockJobName(Flow flow, Stage stage, String runKey) {
        String svc = stage.name().toLowerCase();
        String key = runKey.toLowerCase().replaceAll("[^a-z0-9-]", "-");
        if (key.startsWith(svc + "-")) {
            key = key.substring(svc.length() + 1);
        }
        return flow.jobPrefix() + svc + "-" + key;
    }

    /** Clock-triggered launch (R-37 CRW; the CRG and PRG report windows): identity (stage, runKey). */
    public void launchClock(Flow flow, Stage stage, String runKey, java.util.List<String> args) {
        launchClock(flow, stage, runKey, args, java.util.Map.of());
    }

    /**
     * Clock launch that also injects pod env vars (the MRG suspension sweep
     * selects its Batch job via DCRE_MRG_JOB_NAME). The env is folded into the
     * durable launch args (ENV_ARG_PREFIX) so the intent row alone rebuilds the
     * same Job on a reconciled re-create; clockJob splits it back out, so the
     * program args the pod sees stay clean (client, window).
     */
    public void launchClock(Flow flow, Stage stage, String runKey, java.util.List<String> args,
                            java.util.Map<String, String> env) {
        String name = clockJobName(flow, stage, runKey);
        String namespace = flowNamespaces.namespaceOf(flow);
        java.util.List<String> durable = withEnvArgs(env, args);
        java.util.Optional<UUID> intent =
                intentRepo.insertClockIntent(stage, runKey, name, String.join("\n", durable), namespace);
        if (intent.isEmpty()) {
            return;
        }
        createFromSpec(intent.get(), clockJob(name, namespace, stage, stageImages.required(stage), durable));
    }

    /** Prepend env vars as reserved durable args ahead of the program args. */
    private static java.util.List<String> withEnvArgs(java.util.Map<String, String> env,
                                                      java.util.List<String> args) {
        if (env.isEmpty()) {
            return args;
        }
        java.util.List<String> out = new java.util.ArrayList<>(env.size() + args.size());
        env.forEach((k, v) -> out.add(ENV_ARG_PREFIX + k + "=" + v));
        out.addAll(args);
        return out;
    }

    /**
     * Arrival-scoped launch for a PRG IMMEDIATE report (SCRUM-90). Same
     * deterministic clock name (clockJobName) and durable report args as the old
     * launchClock path, so an in-flight FAILED PRG JobInstance resumes and the
     * PSR file stays a single idempotent output; but the intent carries the
     * parent's arrival_id, so the M12 orphan / stale-heartbeat sweeps recover a
     * SIGKILLed one-shot report (a clock intent's NULL arrival_id excluded it
     * from both sweeps, stranding a killed report forever). The Job carries
     * JOB_NAME (clockJob), so the prg pod's HeartbeatWriter updates this intent's
     * heartbeat_at/owner_pod and the wedged-alive path is armed too. Recreate
     * rebuilds from durable report args (createJob), never arrival stage args.
     */
    public void launchArrivalReport(final Flow flow, final Stage stage, final UUID arrivalId,
                                    final String runKey, final java.util.List<String> args) {
        String name = clockJobName(flow, stage, runKey);
        String namespace = flowNamespaces.namespaceOf(flow);
        java.util.Optional<UUID> intent =
                intentRepo.insertReportIntent(arrivalId, stage, runKey, name, String.join("\n", args), namespace);
        if (intent.isEmpty()) {
            return; // this report is already intended (idempotent report-due scan)
        }
        createFromSpec(intent.get(), clockJob(name, namespace, stage, stageImages.required(stage), args));
    }

    /** Partition durable clock args: ENV_ARG_PREFIX entries become pod env vars,
     *  everything else stays a Spring Batch program arg. */
    private static void splitEnvArgs(java.util.List<String> args,
                                     java.util.List<io.fabric8.kubernetes.api.model.EnvVar> envOut,
                                     java.util.List<String> argsOut) {
        for (String arg : args) {
            if (arg.startsWith(ENV_ARG_PREFIX)) {
                int eq = arg.indexOf('=', ENV_ARG_PREFIX.length());
                envOut.add(new io.fabric8.kubernetes.api.model.EnvVar(
                        arg.substring(ENV_ARG_PREFIX.length(), eq), arg.substring(eq + 1), null));
            } else {
                argsOut.add(arg);
            }
        }
    }

    private Job clockJob(String name, String namespace, Stage stage, String image, java.util.List<String> args) {
        // Same fail-closed family cross-check as serviceJob: a CRG window minted
        // into dcre-pay, or a PRG window into dcre-col, is the report-generator
        // half of the inversion this rename created and must not be creatable.
        stageDatabases.requireSameFamily(stage, flowNamespaces.flowForNamespace(namespace), namespace);
        java.util.List<io.fabric8.kubernetes.api.model.EnvVar> extraEnv = new java.util.ArrayList<>();
        java.util.List<String> programArgs = new java.util.ArrayList<>();
        splitEnvArgs(args, extraEnv, programArgs);
        return new JobBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .addToLabels(Map.of(LABEL_MANAGED_BY, "agt", LABEL_STAGE, stage.name()))
                .endMetadata()
                .withNewSpec()
                    .withBackoffLimit(0)
                    .withActiveDeadlineSeconds(config.stageDeadlineSeconds())
                    .withTtlSecondsAfterFinished(300)
                    .withNewTemplate()
                        .withNewMetadata().addToLabels(LABEL_MANAGED_BY, "agt").endMetadata()
                        .withNewSpec()
                            .withRestartPolicy("Never")
                            .addNewVolume().withName("exchange")
                                .withNewPersistentVolumeClaim("dcre-exchange", false).endVolume()
                            .addNewContainer()
                                .withName("stage")
                                .withImage(image)
                                .withImagePullPolicy("IfNotPresent")
                                .withArgs(programArgs.toArray(String[]::new))
                                .addNewEnv().withName("JOB_NAME").withValue(name).endEnv()
                                .addNewEnv().withName("DCRE_DB_URL").withValue(dbUrlFor(stage)).endEnv()
                                .addNewEnv().withName("DCRE_EXCHANGE_ROOT").withValue("/exchange").endEnv()
                                .addNewEnv().withName("DCRE_AGTOPS_DB_URL").withValue(config.agtopsDbUrl()).endEnv()
                                .addNewEnv().withName("DCRE_AGTOPS_DB_USER").withValue(config.agtopsDbUser()).endEnv()
                                .addAllToEnv(extraEnv)
                                .addNewVolumeMount().withName("exchange").withMountPath("/exchange").endVolumeMount()
                                .withNewResources()
                                    .addToRequests("cpu", new io.fabric8.kubernetes.api.model.Quantity("250m"))
                                    .addToRequests("memory", new io.fabric8.kubernetes.api.model.Quantity(config.stageMemoryRequest()))
                                    .addToLimits("memory", new io.fabric8.kubernetes.api.model.Quantity(config.stageMemoryLimit()))
                                .endResources()
                            .endContainer()
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();
    }

    /**
     * Program args for a service Job (arrival.id identifying per R-16; everything
     * else non-identifying). The responders (CIR; M10 MIR) carry the arrival
     * identity, with route.id carrying the route dimension of the R-16 arrival
     * identity (A-45: CIR 2.0.1 requires it to keep cross-route twin arrivals
     * from colliding on the response file), and the rejecting validator's
     * verdict so they can NACK a headerless spine (R-41/A-42). SCRUM-91: the
     * mandate leg readers take input.file and original.name like every other
     * boundary reader and nothing else, because each owns exactly one reply
     * type, so neither the old MAR reply.type arg nor the MSR response.file arg
     * has anything left to select. All
     * inputs come from durable rows (file_arrival, stage_outcome), so a
     * reconciled re-create rebuilds identical args; nothing lives only in memory
     * (same durability property clock intents get from persisted launch args).
     */
    public static java.util.List<String> serviceArgs(Stage stage, FileArrival arrival,
                                                    Map<Stage, Outcome> outcomes) {
        java.util.List<String> args = new java.util.ArrayList<>(java.util.List.of(
                "arrival.id=" + arrival.id()));
        if (BOUNDARY_READERS.contains(stage)) {
            args.add("input.file=" + arrival.claimedPath() + ",java.lang.String,false");
            args.add("original.name=" + arrival.physicalFilename() + ",java.lang.String,false");
        }
        // v1 topology: the `flow=PAY` arg is GONE. It existed because ONE reader
        // (CRR) served both families and had to be told which one it was reading
        // for, writing tx_header.flow into a shared dcre_col. Collections and
        // payments now have their own readers (CRR, PRR), their own schemas and
        // their own databases, so the family is a property of the SERVICE rather
        // than a parameter handed to it, and PRR takes no `flow` job parameter at
        // all. Re-adding one would reintroduce the two-homes hazard the split removed.
        if (RESPONDERS.contains(stage)) {
            args.add("route.id=" + responderIdentity(arrival.routeId(), "route.id", arrival) + ",java.lang.String,false");
            args.add("client.token=" + responderIdentity(arrival.clientToken(), "client.token", arrival) + ",java.lang.String,false");
            args.add("msg.id=" + responderIdentity(arrival.msgIdToken(), "msg.id", arrival) + ",java.lang.String,false");
            rejectionHint(outcomes).ifPresent(o ->
                    args.add("outcome.hint=" + o.name() + ",java.lang.String,false"));
        }
        return args;
    }

    /**
     * Fail-closed guard for the responder identity params (A-45): a null field
     * would otherwise render as the literal "null", pass the responder's
     * has-text check, and re-create the cross-route collision class under the
     * token "null". Today only the DB NOT NULL constraints prevent that; the
     * launcher must not depend on them.
     */
    private static String responderIdentity(String value, String field, FileArrival arrival) {
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("arrival " + arrival.id() + " has no " + field
                    + ": responder identity params are fail-closed (A-45)");
        }
        return value;
    }

    /**
     * The verdict that routed this arrival to its family's responder: the rejecting
     * validator stage's BUSINESS_FILE_REJECTED/BUSINESS_FILE_FATAL, whichever stage
     * it came from (on payments that can be PTV or PAI; never hardcode one). Boundary readers
     * are excluded: their fatals mean no spine/verdicts exist and CIR NACKs
     * from fatal.reason instead. Empty on the ACK path (ACCEPTED/PARTIAL only).
     */
    static Optional<Outcome> rejectionHint(Map<Stage, Outcome> outcomes) {
        Outcome hint = null;
        for (Map.Entry<Stage, Outcome> entry : outcomes.entrySet()) {
            if (BOUNDARY_READERS.contains(entry.getKey())) {
                continue;
            }
            if (entry.getValue() == Outcome.BUSINESS_FILE_REJECTED) {
                return Optional.of(entry.getValue()); // R-41 policy rejection wins
            }
            if (entry.getValue() == Outcome.BUSINESS_FILE_FATAL) {
                hint = entry.getValue();
            }
        }
        return Optional.ofNullable(hint);
    }

    /**
     * Stage-keyed extra pod env for a DAG stage Job; only CTV has any. It carries
     * the two halves of the mandate gate: the dcre_man url of CTV's SECOND,
     * read-only projection datasource (CTV_MANDATES_DB_URL_ENV), reusing the same
     * manServiceDbUrl knob every man stage pod gets rather than a second URL knob to
     * keep in step, and WHICH store the gate reads (CTV_MANDATE_SOURCE_ENV) from
     * agt.ctv-mandate-source. Pointing CTV at dcre_man was never enough on its own:
     * without the source token the pod stayed on ctv's `legacy` default and never
     * opened that datasource at all.
     *
     * <p>v1 topology: the {@code DCRE_FLOW_DC=false} arm is GONE. It existed because
     * ENDO reused the DC CTV image and had to switch the DC flow off inside a shared
     * service. Payments validation is PTV now, a different service that never runs
     * the DC flow, and whose VerdictChain made the former {@code dcre.flow-dc=false}
     * behaviour unconditional. CTV is collections-only, so the arm was unreachable.
     */
    private java.util.List<EnvVar> stageEnv(Stage stage) {
        if (stage != Stage.CTV) {
            return java.util.List.of();
        }
        return java.util.List.of(
                new EnvVar(CTV_MANDATES_DB_URL_ENV, config.manServiceDbUrl(), null),
                new EnvVar(CTV_MANDATE_SOURCE_ENV, config.ctvMandateSource(), null));
    }

    /** M2 real-service Job: Spring Batch app; program args become JobParameters. */
    private Job serviceJob(String name, String namespace, Stage stage, FileArrival arrival, String image) {
        Map<Stage, Outcome> outcomes = RESPONDERS.contains(stage)
                ? outcomeRepo.outcomesForArrival(arrival.id())
                : Map.of();
        // Fail closed if the namespace and the stage disagree about the family. The
        // namespace comes from the durable intent row, so this also catches a
        // reconciled re-create that would rebuild a pod into the wrong family.
        stageDatabases.requireSameFamily(stage, flowNamespaces.flowForNamespace(namespace), namespace);
        java.util.List<String> args = serviceArgs(stage, arrival, outcomes);
        java.util.List<EnvVar> extraEnv = stageEnv(stage);
        return new JobBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .addToLabels(Map.of(LABEL_MANAGED_BY, "agt",
                            LABEL_STAGE, stage.name(),
                            LABEL_ARRIVAL, arrival.id().toString()))
                .endMetadata()
                .withNewSpec()
                    .withBackoffLimit(0)
                    .withActiveDeadlineSeconds(config.stageDeadlineSeconds())
                    .withTtlSecondsAfterFinished(300)
                    .withNewTemplate()
                        .withNewMetadata().addToLabels(LABEL_MANAGED_BY, "agt").endMetadata()
                        .withNewSpec()
                            .withRestartPolicy("Never")
                            .addNewVolume()
                                .withName("exchange")
                                .withNewPersistentVolumeClaim("dcre-exchange", false)
                            .endVolume()
                            .addNewContainer()
                                .withName("stage")
                                .withImage(image)
                                .withImagePullPolicy("IfNotPresent")
                                .withArgs(args.toArray(String[]::new))
                                .addNewEnv().withName("JOB_NAME").withValue(name).endEnv()
                                .addNewEnv().withName("DCRE_DB_URL").withValue(dbUrlFor(stage)).endEnv()
                                .addNewEnv().withName("DCRE_EXCHANGE_ROOT").withValue("/exchange").endEnv()
                                .addNewEnv().withName("DCRE_AGTOPS_DB_URL").withValue(config.agtopsDbUrl()).endEnv()
                                .addNewEnv().withName("DCRE_AGTOPS_DB_USER").withValue(config.agtopsDbUser()).endEnv()
                                .addAllToEnv(extraEnv)
                                .addNewVolumeMount().withName("exchange").withMountPath("/exchange").endVolumeMount()
                                .withNewResources()
                                    .addToRequests("cpu", new io.fabric8.kubernetes.api.model.Quantity("250m"))
                                    .addToRequests("memory", new io.fabric8.kubernetes.api.model.Quantity(config.stageMemoryRequest()))
                                    .addToLimits("memory", new io.fabric8.kubernetes.api.model.Quantity(config.stageMemoryLimit()))
                                .endResources()
                            .endContainer()
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();
    }
}
