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
import za.co.fnb.dcre.agt.repo.LedgerRepo;

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

    @Inject
    LedgerRepo repo;

    @Inject
    AgtConfig config;

    @Inject
    KubernetesClient k8s;

    public static String jobName(Stage stage, UUID arrivalId) {
        return "dcre-" + stage.name().toLowerCase() + "-" + arrivalId.toString().substring(0, 8);
    }

    /** Launch stage for arrival; no-op when an intent already exists (non-overlap). */
    public void launch(UUID arrivalId, Stage stage) {
        String name = jobName(stage, arrivalId);
        Optional<UUID> intent = repo.insertIntent(arrivalId, stage, name);
        if (intent.isEmpty()) {
            return; // already intended/launched by us or a predecessor incarnation
        }
        createJob(intent.get(), arrivalId, stage, name);
    }

    /** Create (or re-create after crash) the Job for an existing intent. */
    public void createJob(UUID intentId, UUID arrivalId, Stage stage, String name) {
        if (!config.launchEnabled()) {
            LOG.debugf("launch disabled; intent %s stays INTENDED", name);
            return;
        }
        Job job = stubJob(name, stage, arrivalId);
        try {
            k8s.batch().v1().jobs().inNamespace(config.namespace()).resource(job).create();
            LOG.infof("Launched %s", name);
        } catch (KubernetesClientException e) {
            if (e.getCode() != 409) {
                throw e; // reconciler retries later; intent stays INTENDED
            }
            LOG.infof("Job %s already exists (409): treating as launched", name);
        }
        repo.markIntentLaunched(intentId);
    }

    private Job stubJob(String name, Stage stage, UUID arrivalId) {
        // M1 stub: exercises TECH (chaos file -> exit 1) and BUSINESS outcomes
        // (outcome file seam, SYNTHETIC-CONTRACT until M2 services land).
        String script = "echo run $DCRE_STAGE for $DCRE_ARRIVAL; sleep 2; "
                + "if [ -f /exchange/chaos/fail-$DCRE_STAGE ]; then exit 1; fi; "
                + "mkdir -p /exchange/outcomes; "
                + "echo BUSINESS_ACCEPTED > /exchange/outcomes/$JOB_NAME; exit 0";
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
                        .withNewMetadata()
                            .addToLabels(LABEL_MANAGED_BY, "agt")
                        .endMetadata()
                        .withNewSpec()
                            .withRestartPolicy("Never")
                            .addNewVolume()
                                .withName("exchange")
                                .withNewPersistentVolumeClaim("dcre-exchange", false)
                            .endVolume()
                            .addNewContainer()
                                .withName("stage")
                                .withImage(config.stubImage())
                                .withCommand("sh", "-c", script)
                                .addNewEnv().withName("DCRE_STAGE").withValue(stage.name()).endEnv()
                                .addNewEnv().withName("DCRE_ARRIVAL").withValue(arrivalId.toString()).endEnv()
                                .addNewEnv().withName("JOB_NAME").withValue(name).endEnv()
                                .addNewVolumeMount()
                                    .withName("exchange").withMountPath("/exchange")
                                .endVolumeMount()
                                .withNewResources()
                                    .addToRequests("cpu", new io.fabric8.kubernetes.api.model.Quantity("50m"))
                                    .addToRequests("memory", new io.fabric8.kubernetes.api.model.Quantity("32Mi"))
                                    .addToLimits("memory", new io.fabric8.kubernetes.api.model.Quantity("64Mi"))
                                .endResources()
                            .endContainer()
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();
    }
}
