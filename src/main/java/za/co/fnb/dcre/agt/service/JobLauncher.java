package za.co.fnb.dcre.agt.service;

import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.domain.Stage;
import za.co.fnb.dcre.agt.repo.ArrivalRepo;
import za.co.fnb.dcre.agt.repo.IntentRepo;

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
        // arrivalId == null is a clock intent (CRW/PRG): args live on the run key name.
        Job job = arrivalId == null
                ? clockJob(name, stage, serviceImage(stage), java.util.List.of())
                : serviceJob(name, stage, arrivalId, serviceImage(stage));
        createFromSpec(intentId, job);
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

    /** Clock-triggered launch (R-37 CRW; R-28 PRG in M4): identity (stage, runKey). */
    public void launchClock(Stage stage, String runKey, java.util.List<String> args) {
        // Lowercase BEFORE sanitizing: sanitize-first collapsed every uppercase
        // char to '-', so FNBRF01 and FNBCC01 mapped to the SAME Job name and one
        // client's clock job silently never ran (seen live in M5 e2e).
        String name = "dcre-" + stage.name().toLowerCase() + "-"
                + runKey.toLowerCase().replaceAll("[^a-z0-9-]", "-");
        java.util.Optional<UUID> intent = intentRepo.insertClockIntent(stage, runKey, name);
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
                    .withActiveDeadlineSeconds(900L)
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
                                    .addToRequests("memory", new io.fabric8.kubernetes.api.model.Quantity("512Mi"))
                                    .addToLimits("memory", new io.fabric8.kubernetes.api.model.Quantity("768Mi"))
                                .endResources()
                            .endContainer()
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();
    }

    /** M2 real-service Job: Spring Batch app; program args become JobParameters
     *  (arrival.id identifying per R-16; file/name non-identifying). */
    private Job serviceJob(String name, Stage stage, UUID arrivalId, String image) {
        var arrival = arrivalRepo.arrivalById(arrivalId).orElseThrow();
        java.util.List<String> args = new java.util.ArrayList<>(java.util.List.of(
                "arrival.id=" + arrivalId));
        if (BOUNDARY_READERS.contains(stage)) {
            args.add("input.file=" + arrival.claimedPath() + ",java.lang.String,false");
            args.add("original.name=" + arrival.physicalFilename() + ",java.lang.String,false");
        }
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
                    .withActiveDeadlineSeconds(900L)
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
                                    .addToRequests("memory", new io.fabric8.kubernetes.api.model.Quantity("512Mi"))
                                    .addToLimits("memory", new io.fabric8.kubernetes.api.model.Quantity("768Mi"))
                                .endResources()
                            .endContainer()
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();
    }
}
