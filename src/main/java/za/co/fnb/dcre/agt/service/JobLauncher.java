package za.co.fnb.dcre.agt.service;

import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.domain.FileArrival;
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

    /** Boundary stages that read the claimed payload file (CRR; M4 fint-resp readers). */
    static final java.util.Set<Stage> BOUNDARY_READERS =
            java.util.EnumSet.of(Stage.CRR, Stage.IXR, Stage.SXR, Stage.PXR);

    @Inject
    IntentRepo intentRepo;

    @Inject
    ArrivalRepo arrivalRepo;

    @Inject
    OutcomeRepo outcomeRepo;

    @Inject
    AgtConfig config;

    @Inject
    KubernetesClient k8s;

    /** Full 128-bit arrival identity in the name (Fugu F2): 41 chars, DNS-1123 safe. */
    public static String jobName(Stage stage, UUID arrivalId) {
        return "dcre-" + stage.name().toLowerCase() + "-" + arrivalId.toString().replace("-", "");
    }

    /** Launch stage for arrival; no-op when an intent already exists (non-overlap). */
    public void launch(UUID arrivalId, Stage stage) {
        String name = jobName(stage, arrivalId);
        Optional<UUID> intent = intentRepo.insertIntent(arrivalId, stage, name);
        if (intent.isEmpty()) {
            return; // already intended/launched by us or a predecessor incarnation
        }
        createJob(intent.get(), arrivalId, stage, name);
    }

    /** Create (or re-create after crash) the Job for an existing intent. */
    public void createJob(UUID intentId, UUID arrivalId, Stage stage, String name) {
        // arrivalId == null is a clock intent: its launch args are durable on the
        // intent row (a reconciled recreate with empty args crashed every clock
        // job with missing identifying params; caught live 2026-07-13).
        Job job = arrivalId == null
                ? clockJob(name, stage, serviceImage(stage), clockArgs(intentId))
                : serviceJob(name, stage, arrivalId, serviceImage(stage));
        createFromSpec(intentId, job);
    }

    private java.util.List<String> clockArgs(UUID intentId) {
        return intentRepo.intentLaunchArgs(intentId)
                .filter(a -> !a.isBlank())
                .map(a -> java.util.List.of(a.split("\\n")))
                .orElseThrow(() -> new IllegalStateException(
                        "clock intent " + intentId + " has no durable launch args"));
    }

    private void createFromSpec(UUID intentId, Job job) {
        String name = job.getMetadata().getName();
        if (!config.launchEnabled()) {
            LOG.debugf("launch disabled; intent %s stays INTENDED", name);
            return;
        }
        String uid;
        try {
            Job created = k8s.batch().v1().jobs().inNamespace(config.namespace()).resource(job).create();
            uid = created.getMetadata() != null ? created.getMetadata().getUid() : null;
            LOG.infof("Launched %s (uid %s)", name, uid);
        } catch (KubernetesClientException e) {
            if (e.getCode() != 409) {
                throw e; // reconciler retries later; intent stays INTENDED
            }
            Job existing = k8s.batch().v1().jobs().inNamespace(config.namespace()).withName(name).get();
            uid = existing != null && existing.getMetadata() != null ? existing.getMetadata().getUid() : null;
            LOG.infof("Job %s already exists (409, uid %s): treating as launched", name, uid);
        }
        intentRepo.markIntentLaunched(intentId, uid);
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
        };
        return image.orElseThrow(() -> new IllegalStateException(
                "no image configured for stage " + stage + ": set AGT_" + stage.name() + "_IMAGE"));
    }

    /**
     * Clock Job name from (stage, runKey). Lowercase BEFORE sanitizing (M5: a
     * sanitize-first bug collapsed FNBRF01/FNBCC01 to one name), and a LEADING
     * stage token in the run key is stripped once (M6: HcsScheduler's runKey
     * "hcs-w<n>" produced "dcre-hcs-hcs-w<n>"): the name owns the prefix, the
     * run key never contributes it.
     */
    public static String clockJobName(Stage stage, String runKey) {
        String svc = stage.name().toLowerCase();
        String key = runKey.toLowerCase().replaceAll("[^a-z0-9-]", "-");
        if (key.startsWith(svc + "-")) {
            key = key.substring(svc.length() + 1);
        }
        return "dcre-" + svc + "-" + key;
    }

    /** Clock-triggered launch (R-37 CRW; R-28 PRG in M4): identity (stage, runKey). */
    public void launchClock(Stage stage, String runKey, java.util.List<String> args) {
        String name = clockJobName(stage, runKey);
        java.util.Optional<UUID> intent =
                intentRepo.insertClockIntent(stage, runKey, name, String.join("\n", args));
        if (intent.isEmpty()) {
            return;
        }
        createFromSpec(intent.get(), clockJob(name, stage, serviceImage(stage), args));
    }

    private Job clockJob(String name, Stage stage, String image, java.util.List<String> args) {
        return new JobBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(config.namespace())
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
                                .withArgs(args.toArray(String[]::new))
                                .addNewEnv().withName("JOB_NAME").withValue(name).endEnv()
                                .addNewEnv().withName("DCRE_DB_URL").withValue(config.serviceDbUrl()).endEnv()
                                .addNewEnv().withName("DCRE_EXCHANGE_ROOT").withValue("/exchange").endEnv()
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
     * else non-identifying). CIR carries the arrival identity, with route.id
     * carrying the route dimension of the R-16 arrival identity (A-45: CIR 2.0.1
     * requires it to keep cross-route twin arrivals from colliding on the
     * response file), and the rejecting validator's verdict so it can NACK a
     * headerless spine (R-41/A-42). All
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
        if (stage == Stage.CIR) {
            args.add("route.id=" + cirIdentity(arrival.routeId(), "route.id", arrival) + ",java.lang.String,false");
            args.add("client.token=" + cirIdentity(arrival.clientToken(), "client.token", arrival) + ",java.lang.String,false");
            args.add("msg.id=" + cirIdentity(arrival.msgIdToken(), "msg.id", arrival) + ",java.lang.String,false");
            rejectionHint(outcomes).ifPresent(o ->
                    args.add("outcome.hint=" + o.name() + ",java.lang.String,false"));
        }
        return args;
    }

    /**
     * Fail-closed guard for the CIR identity params (A-45): a null field would
     * otherwise render as the literal "null", pass CIR's has-text check, and
     * re-create the cross-route collision class under the token "null". Today
     * only the DB NOT NULL constraints prevent that; the launcher must not
     * depend on them.
     */
    private static String cirIdentity(String value, String field, FileArrival arrival) {
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("arrival " + arrival.id() + " has no " + field
                    + ": CIR identity params are fail-closed (A-45)");
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

    /** M2 real-service Job: Spring Batch app; program args become JobParameters. */
    private Job serviceJob(String name, Stage stage, UUID arrivalId, String image) {
        var arrival = arrivalRepo.arrivalById(arrivalId).orElseThrow();
        Map<Stage, Outcome> outcomes = stage == Stage.CIR
                ? outcomeRepo.outcomesForArrival(arrivalId)
                : Map.of();
        java.util.List<String> args = serviceArgs(stage, arrival, outcomes);
        // ENDO reuses the DC CTV image with the DC flow switched off (M5, R-36):
        // only the extra env differs; DC arrivals keep the yml default (flow-dc true).
        java.util.List<io.fabric8.kubernetes.api.model.EnvVar> extraEnv =
                stage == Stage.CTV && ArrivalService.ROUTE_ONHOST_REQ_ENDO.equals(arrival.routeId())
                        ? java.util.List.of(new io.fabric8.kubernetes.api.model.EnvVar("DCRE_FLOW_DC", "false", null))
                        : java.util.List.of();
        return new JobBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(config.namespace())
                    .addToLabels(Map.of(LABEL_MANAGED_BY, "agt",
                            LABEL_STAGE, stage.name(),
                            LABEL_ARRIVAL, arrivalId.toString()))
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
                                .addNewEnv().withName("DCRE_DB_URL").withValue(config.serviceDbUrl()).endEnv()
                                .addNewEnv().withName("DCRE_EXCHANGE_ROOT").withValue("/exchange").endEnv()
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
