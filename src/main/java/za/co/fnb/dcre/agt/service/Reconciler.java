package za.co.fnb.dcre.agt.service;

import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.domain.LaunchIntent;
import za.co.fnb.dcre.agt.domain.Outcome;
import za.co.fnb.dcre.agt.repo.IntentRepo;
import za.co.fnb.dcre.agt.repo.OutcomeRepo;
import io.fabric8.kubernetes.client.KubernetesClient;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Level-triggered reconciliation BY INTENT STATUS (Fugu F1: a LAUNCHED intent
 * is NEVER recreated):
 *   INTENDED + no live Job  -> create (the only safe recreate: unconfirmed create)
 *   INTENDED + live Job     -> promote to LAUNCHED (crash between create and mark)
 *   LAUNCHED + no live Job  -> resolve from the durable outcome seam; absent
 *                              after the grace window -> TECH_FAILED (never rerun)
 * Managed Jobs without an intent row are flagged as out-of-band orphans.
 */
@ApplicationScoped
public class Reconciler {

    private static final Logger LOG = Logger.getLogger(Reconciler.class);
    static final Duration REAP_GRACE = Duration.ofMinutes(2);

    @Inject
    IntentRepo intentRepo;

    @Inject
    OutcomeRepo outcomeRepo;

    @Inject
    AgtConfig config;

    @Inject
    LeaseService lease;

    @Inject
    JobLauncher launcher;

    @Inject
    OutcomeWatcher outcomes;

    @Inject
    KubernetesClient k8s;

    @RunOnVirtualThread
    @Scheduled(every = "10s", concurrentExecution = io.quarkus.scheduler.Scheduled.ConcurrentExecution.SKIP)
    void tick() {
        if (!lease.holdsLease() || !config.launchEnabled()) {
            return;
        }
        Map<String, Job> live = new HashMap<>();
        for (Job j : k8s.batch().v1().jobs().inNamespace(config.namespace())
                .withLabel(JobLauncher.LABEL_MANAGED_BY, "agt").list().getItems()) {
            live.put(j.getMetadata().getName(), j);
        }

        for (LaunchIntent intent : intentRepo.intentsWithoutOutcome()) {
            try {
                reconcile(intent, live.get(intent.jobName()));
            } catch (Exception e) {
                LOG.warnf("reconcile %s failed: %s", intent.jobName(), e.getMessage());
            }
        }

        Set<String> known = new HashSet<>(intentRepo.allIntentJobNames());
        for (String name : live.keySet()) {
            if (!known.contains(name)) {
                LOG.errorf("ORPHAN managed Job with no intent: %s (out-of-band launch)", name);
            }
        }
    }

    void reconcile(LaunchIntent intent, Job liveJob) {
        boolean launched = LaunchIntent.LAUNCHED.equals(intent.status());
        if (!launched) {
            if (liveJob != null) {
                // Create happened, mark did not: promote, never re-create (F1b).
                String uid = liveJob.getMetadata() != null ? liveJob.getMetadata().getUid() : null;
                intentRepo.markIntentLaunched(intent.id(), uid);
            } else {
                LOG.warnf("Reconcile: INTENDED intent %s has no Job; creating", intent.jobName());
                launcher.createJob(intent.id(), intent.arrivalId(), intent.stage(), intent.jobName());
            }
            return;
        }
        if (liveJob != null) {
            return; // OutcomeWatcher owns live observation
        }
        // LAUNCHED and reaped/vanished: resolve from the durable seam, NEVER rerun (F1).
        Optional<Outcome> business = outcomes.readBusinessOutcome(intent.jobName());
        if (business.isPresent()) {
            if (outcomeRepo.insertOutcome(intent.id(), business.get(), null, "ReapedBeforeObservation")) {
                LOG.infof("Reconcile: recovered outcome %s = %s from seam after reap",
                        intent.jobName(), business.get());
            }
            return;
        }
        OffsetDateTime created = intentRepo.intentCreatedAt(intent.id());
        if (created.plus(REAP_GRACE).isBefore(OffsetDateTime.now())) {
            if (outcomeRepo.insertOutcome(intent.id(), Outcome.TECH_FAILED, null, "VanishedNoSeam")) {
                LOG.errorf("Reconcile: LAUNCHED %s vanished with no outcome seam after grace: TECH_FAILED "
                        + "(absence is never success)", intent.jobName());
            }
        }
    }
}
