package za.co.fnb.dcre.agt.service;

import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.domain.LaunchIntent;
import za.co.fnb.dcre.agt.repo.LedgerRepo;
import io.fabric8.kubernetes.client.KubernetesClient;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Level-triggered reconciliation (SPEC-DAG 6a): intents without a live Job are
 * re-created (create is name-idempotent); managed Jobs without an intent are
 * flagged as out-of-band orphans (SPEC-DAG blind-spot policy).
 */
@ApplicationScoped
public class Reconciler {

    private static final Logger LOG = Logger.getLogger(Reconciler.class);

    @Inject
    LedgerRepo repo;

    @Inject
    AgtConfig config;

    @Inject
    LeaseService lease;

    @Inject
    JobLauncher launcher;

    @Inject
    KubernetesClient k8s;

    @Scheduled(every = "10s")
    void tick() {
        if (!lease.holdsLease() || !config.launchEnabled()) {
            return;
        }
        List<Job> jobs = k8s.batch().v1().jobs().inNamespace(config.namespace())
                .withLabel(JobLauncher.LABEL_MANAGED_BY, "agt").list().getItems();
        Set<String> liveNames = new HashSet<>();
        jobs.forEach(j -> liveNames.add(j.getMetadata().getName()));

        // Intent without outcome and without a live Job: recreate (crash between
        // intent and create, or TTL-reaped before observation was recorded).
        for (LaunchIntent intent : repo.intentsWithoutOutcome()) {
            if (!liveNames.contains(intent.jobName())) {
                LOG.warnf("Reconcile: intent %s has no live Job; recreating", intent.jobName());
                launcher.createJob(intent.id(), intent.arrivalId(), intent.stage(), intent.jobName());
            }
        }

        // Managed Jobs with no intent row: out-of-band creation (kubectl etc.).
        Set<String> known = new HashSet<>(repo.allIntentJobNames());
        for (String name : liveNames) {
            if (!known.contains(name)) {
                LOG.errorf("ORPHAN managed Job with no intent: %s (out-of-band launch)", name);
            }
        }
    }
}
