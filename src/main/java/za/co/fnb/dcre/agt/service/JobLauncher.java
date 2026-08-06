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
     *  so the url belongs to the stage exactly as DCRE_DB_URL does. CTV stays
     *  OUT of MAN_STAGES: its PRIMARY datasource is still dcre_col. Absent, the
     *  pod falls back to ctv's localhost dev default and the projection gate
     *  cannot reach dcre_man at all (found 2026-07-26). */
    public static final String CTV_MANDATES_DB_URL_ENV = "DCRE_CTV_MANDATES_DB_URL";

    /** Env var selecting WHICH mandate store CTV's DC-flow gate reads
     *  (legacy|projection, ctv MandateSource). STAGE-keyed for the same reason as
     *  CTV_MANDATES_DB_URL_ENV: every CTV pod runs the gate and there is no
     *  per-launch env seam. Absent, the pod is frozen on ctv's yml default
     *  `legacy`, which reads `FROM mandate` in dcre_col - a table only
     *  env-reset.sh --seed creates - so the projection gate was unreachable
     *  in-cluster no matter what dcre_man held (SCRUM-91 Task 11 Step 8).
     *  The token is carried verbatim from agt.ctv-mandate-source and never
     *  interpreted here; ctv owns the vocabulary. */
    public static final String CTV_MANDATE_SOURCE_ENV = "DCRE_CTV_MANDATE_SOURCE";

    /** Reserved durable-arg prefix carrying a pod env var rather than a Spring
     *  Batch program arg (SCRUM-78). Encoding sweep env into the durable launch
     *  args means the intent row alone rebuilds the same Job on a reconciled
     *  re-create (createJob -> clockJob), exactly as serviceJob derives
     *  DCRE_FLOW_DC from the durable arrival route; the program args the pod
     *  receives stay clean (client, window). No real Batch arg starts with this token. */
    static final String ENV_ARG_PREFIX = "env:";

    /** Boundary stages that read the claimed payload file (CRR; M4 fint-resp
     *  readers; M10 MRR instruction-book reader; SCRUM-91 the three pain.012 leg
     *  readers that replaced the merged MAR one). */
    static final java.util.Set<Stage> BOUNDARY_READERS = java.util.EnumSet.of(
            Stage.CRR, Stage.IXR, Stage.SXR, Stage.PXR, Stage.MRR,
            Stage.MIX, Stage.MSX, Stage.MPX);

    /** Stages AGT may launch. MAR, MSR (A-75), MIS (renamed MIT) and MAF (renamed
     *  MAS), both SCRUM-107,
     *  are retained-deprecated: Stage still parses them for historic
     *  agt_ops.stage_outcome rows, but they are in no DAG, have no image config
     *  and no serviceArgs branch, so a launch attempt is a bug rather than a
     *  fallback (serviceImage throws). */
    static final java.util.Set<Stage> LAUNCHABLE = java.util.Collections.unmodifiableSet(
            java.util.EnumSet.complementOf(java.util.EnumSet.of(Stage.MAR, Stage.MSR, Stage.MIS, Stage.MAF)));

    /** Whole-file responder stages: carry the A-45 arrival identity params and
     *  the rejecting validator's outcome.hint (CIR; M10 man responder MIR). */
    static final java.util.Set<Stage> RESPONDERS =
            java.util.EnumSet.of(Stage.CIR, Stage.MIR);

    /** M10 mandates stages persist in dcre_man (B2, SCRUM-79 review): the
     *  DCRE_DB_URL env follows the stage's flow family. Stage-keyed so a
     *  reconciled re-create (which has only the intent row) resolves the same
     *  URL; COL/PAY stages keep dcre_col unchanged. */
    static final java.util.Set<Stage> MAN_STAGES = java.util.EnumSet.of(
            Stage.MRR, Stage.MRV, Stage.MAS, Stage.MIT, Stage.MIR,
            Stage.MRW, Stage.MIX, Stage.MSX, Stage.MPX, Stage.MRG);

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

    /** Full 128-bit arrival identity in the name (Fugu F2). SCRUM-70: the
     *  resolved flow prefix (col-/pay-/man-, 4 chars) replaces the dcre-
     *  literal: 40 chars, DNS-1123 safe, still well under the 63-char limit. */
    public static String jobName(Flow flow, Stage stage, UUID arrivalId) {
        return flow.jobPrefix() + stage.name().toLowerCase() + "-" + arrivalId.toString().replace("-", "");
    }

    /** Launch stage for arrival; no-op when an intent already exists (non-overlap).
     *  Single arrival fetch (m3): flow resolution and the Job spec share it. */
    public void launch(UUID arrivalId, Stage stage) {
        FileArrival arrival = arrivalOf(arrivalId, stage.name());
        Flow flow = flowNamespaces.flowFor(arrival);
        String name = jobName(flow, stage, arrivalId);
        String namespace = flowNamespaces.namespaceOf(flow);
        Optional<UUID> intent = intentRepo.insertIntent(arrivalId, stage, name, namespace);
        if (intent.isEmpty()) {
            return; // already intended/launched by us or a predecessor incarnation
        }
        createFromSpec(intent.get(), serviceJob(name, namespace, stage, arrival, serviceImage(stage)));
    }

    /** Create (or re-create after crash) the Job for an existing intent. The
     *  namespace comes from the intent row (durable), never re-resolved. Durable
     *  launch args (clock windows AND SCRUM-90 arrival-scoped IMMEDIATE reports)
     *  rebuild the SAME Job from the intent row; a DAG-stage intent has none and
     *  rebuilds from its arrival. Driven by args presence, not arrivalId nullness:
     *  a report intent carries BOTH an arrival_id and durable report args, and
     *  must recreate as the report Job, never as an arrival stage Job. */
    public void createJob(UUID intentId, UUID arrivalId, Stage stage, String name, String namespace) {
        java.util.Optional<String> durable = intentRepo.intentLaunchArgs(intentId).filter(a -> !a.isBlank());
        if (durable.isPresent()) {
            createFromSpec(intentId, clockJob(name, namespace, stage, serviceImage(stage),
                    java.util.List.of(durable.get().split("\\n"))));
            return;
        }
        if (arrivalId == null) {
            throw new IllegalStateException(
                    "intent " + intentId + " has neither durable launch args nor an arrival");
        }
        createFromSpec(intentId, serviceJob(name, namespace, stage, arrivalOf(arrivalId, name), serviceImage(stage)));
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

    /** DB URL for a stage's Job env: man stages get dcre_man, all else dcre_col (B2). */
    private String dbUrlFor(Stage stage) {
        return MAN_STAGES.contains(stage) ? config.manServiceDbUrl() : config.serviceDbUrl();
    }

    /** Image for a stage; every stage is a real service since M5 (SCRUM-33:
     *  stub deleted), so a missing image is a misconfiguration, never a fallback. */
    private String serviceImage(Stage stage) {
        java.util.Optional<String> image = switch (stage) {
            case CRR -> config.crrImage();
            case CTV -> config.ctvImage();
            case CIR -> config.cirImage();
            case CDE -> config.cdeImage();
            case CRW -> config.crwImage();
            case IXR -> config.ixrImage();
            case SXR -> config.sxrImage();
            case PXR -> config.pxrImage();
            case PRG -> config.prgImage();
            case AIS -> config.aisImage();
            case HCS -> config.hcsImage();
            case MRR -> config.mrrImage();
            case MRV -> config.mrvImage();
            case MAS -> config.masImage();
            case MIT -> config.mitImage();
            case MIR -> config.mirImage();
            case MRW -> config.mrwImage();
            case MIX -> config.mixImage();
            case MSX -> config.msxImage();
            case MPX -> config.mpxImage();
            case MRG -> config.mrgImage();
            case MAR, MSR, MIS, MAF -> throw new IllegalStateException("stage " + stage
                    + " is retired (SCRUM-91 for MAR/MSR, SCRUM-107 for MIS/MAF): parseable for"
                    + " historic outcome rows, never launched");
        };
        return image.orElseThrow(() -> new IllegalStateException(
                "no image configured for stage " + stage + ": set AGT_" + stage.name() + "_IMAGE"));
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

    /** Clock-triggered launch (R-37 CRW; R-28 PRG in M4): identity (stage, runKey). */
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
        createFromSpec(intent.get(), clockJob(name, namespace, stage, serviceImage(stage), durable));
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
        createFromSpec(intent.get(), clockJob(name, namespace, stage, serviceImage(stage), args));
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
    public static java.util.List<String> serviceArgs(Stage stage, FileArrival arrival, Map<Stage, Outcome> outcomes) {
        java.util.List<String> args = new java.util.ArrayList<>(java.util.List.of(
                "arrival.id=" + arrival.id()));
        if (BOUNDARY_READERS.contains(stage)) {
            args.add("input.file=" + arrival.claimedPath() + ",java.lang.String,false");
            args.add("original.name=" + arrival.physicalFilename() + ",java.lang.String,false");
        }
        if (stage == Stage.CRR && ArrivalService.ROUTE_ONHOST_REQ_ENDO.equals(arrival.routeId())) {
            // SCRUM-69: ENDO = Payments. CRR stamps tx_header.flow from this
            // non-identifying arg (absent = COL); the route is durable on the
            // arrival row, so a reconciled re-create rebuilds it identically.
            args.add("flow=PAY,java.lang.String,false");
        }
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
     * The verdict that routed this arrival to CIR: the rejecting validator
     * stage's BUSINESS_FILE_REJECTED/BUSINESS_FILE_FATAL, whichever stage it
     * came from (on ENDO that can be AIS; never hardcode CTV). Boundary readers
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
     * Stage-keyed extra pod env for a DAG stage Job; only CTV has any. It always
     * carries the two halves of the mandate gate: the dcre_man url of CTV's
     * SECOND, read-only projection datasource (CTV_MANDATES_DB_URL_ENV), reusing
     * the same manServiceDbUrl knob every man stage pod gets rather than a second
     * URL knob to keep in step, and WHICH store the gate reads
     * (CTV_MANDATE_SOURCE_ENV) from agt.ctv-mandate-source. Pointing CTV at
     * dcre_man was never enough on its own: without the source token the pod
     * stayed on ctv's `legacy` default and never opened that datasource at all.
     * On ENDO it also switches the DC flow off: ENDO reuses the DC CTV image
     * (M5, R-36) and only the extra env differs; DC arrivals keep the yml default
     * (flow-dc true). ENDO still gets both mandate vars: they are inert there
     * (R-20 skips the gate), and a stage-keyed seam that stayed uniform is one
     * less way for a reconciled re-create to rebuild a different pod.
     */
    private java.util.List<EnvVar> stageEnv(Stage stage, FileArrival arrival) {
        if (stage != Stage.CTV) {
            return java.util.List.of();
        }
        java.util.List<EnvVar> env = new java.util.ArrayList<>(3);
        env.add(new EnvVar(CTV_MANDATES_DB_URL_ENV, config.manServiceDbUrl(), null));
        env.add(new EnvVar(CTV_MANDATE_SOURCE_ENV, config.ctvMandateSource(), null));
        if (ArrivalService.ROUTE_ONHOST_REQ_ENDO.equals(arrival.routeId())) {
            env.add(new EnvVar("DCRE_FLOW_DC", "false", null));
        }
        return env;
    }

    /** M2 real-service Job: Spring Batch app; program args become JobParameters. */
    private Job serviceJob(String name, String namespace, Stage stage, FileArrival arrival, String image) {
        Map<Stage, Outcome> outcomes = RESPONDERS.contains(stage)
                ? outcomeRepo.outcomesForArrival(arrival.id())
                : Map.of();
        java.util.List<String> args = serviceArgs(stage, arrival, outcomes);
        java.util.List<EnvVar> extraEnv = stageEnv(stage, arrival);
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
