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
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Level-triggered reconciliation BY INTENT STATUS (Fugu F1, amended by R-05 /
 * spec 2026-07-14-stuck-job-recovery-design.md: a LAUNCHED intent is recreated
 * ONLY via the OrphanRelauncher: same identity, TECH-class only, bounded attempts):
 *   INTENDED + no live Job  -> create (the safe recreate: unconfirmed create)
 *   INTENDED + live Job     -> promote to LAUNCHED (crash between create and mark)
 *   LAUNCHED + no live Job  -> resolve from the durable outcome seam; absent
 *                              after the grace window -> bounded same-identity
 *                              relaunch (OrphanRelauncher), TECH_EXHAUSTED when
 *                              the budget runs out
 * A second pass sweeps intents whose CURRENT attempt ended TECH-class.
 * Managed Jobs without an intent row are flagged as out-of-band orphans.
 */
@ApplicationScoped
public class Reconciler {

    private static final Logger LOG = Logger.getLogger(Reconciler.class);
    static final Duration REAP_GRACE = Duration.ofMinutes(2);

    /** Baseline for the self-liveness startup grace (M12/SCRUM-87): before the
     *  first tick the probe measures freshness from here so boot does not flap. */
    private final Instant startedAt = Instant.now();

    /** Wall-clock of the last reconcile tick head; null until the first tick.
     *  Read by ReconcilerLivenessCheck; volatile for cross-thread visibility
     *  (the scheduler runs tick() on a virtual thread, the probe on another). */
    private volatile Instant lastTickAt;

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
    OrphanRelauncher orphans;

    @Inject
    KubernetesClient k8s;

    @Inject
    FlowNamespaces flowNamespaces;

    @RunOnVirtualThread
    @Scheduled(every = "5s", concurrentExecution = io.quarkus.scheduler.Scheduled.ConcurrentExecution.SKIP)
    void tick() {
        // Recorded FIRST, before the lease/launch gates, so self-liveness tracks
        // "the reconcile scheduler is firing" independent of leadership (a
        // non-leader still ticks; a wedged reconciler stops ticking) (SCRUM-87).
        lastTickAt = Instant.now();
        if (!lease.holdsLease() || !config.launchEnabled()) {
            return;
        }
        Map<String, Job> live = liveManagedJobs();

        for (LaunchIntent intent : intentRepo.intentsWithoutOutcome()) {
            try {
                reconcile(intent, live.get(intent.jobName()));
            } catch (Exception e) {
                LOG.warnf("reconcile %s failed: %s", intent.jobName(), e.getMessage());
            }
        }

        // Orphan relaunches (both kinds) drain within this tick before it yields.
        // Priority ahead of new work is structural, not a same-tick barrier: new
        // arrivals launch from a SEPARATE scheduler (DirectoryWatcher @ 2s); the
        // orphan sweeps get their own frequent 5s loop, so a wedged/failed intent
        // is picked up within one tick regardless of new-arrival volume. k8s-Failed
        // sweep first, then stale-heartbeat (SCRUM-86); the shared atomic claim
        // makes an intent flagged by both relaunch exactly once.
        orphans.sweepTechOrphans(live);
        orphans.sweepStaleHeartbeat(live);

        Set<String> known = new HashSet<>(intentRepo.allIntentJobNames());
        for (String name : live.keySet()) {
            if (!known.contains(name)) {
                LOG.errorf("ORPHAN managed Job with no intent: %s (out-of-band launch)", name);
            }
        }
    }

    /** SCRUM-70: managed Jobs live in the control namespace (legacy) AND the
     *  flow namespaces; the reconciler works over their union, keyed by name
     *  (names are flow-prefixed, so cross-namespace collisions cannot occur). */
    Map<String, Job> liveManagedJobs() {
        Map<String, Job> live = new HashMap<>();
        for (String namespace : flowNamespaces.allNamespaces()) {
            for (Job j : k8s.batch().v1().jobs().inNamespace(namespace)
                    .withLabel(JobLauncher.LABEL_MANAGED_BY, "agt").list().getItems()) {
                live.put(j.getMetadata().getName(), j);
            }
        }
        return live;
    }

    void reconcile(LaunchIntent intent, Job liveJob) {
        boolean launched = LaunchIntent.LAUNCHED.equals(intent.status());
        if (!launched) {
            if (liveJob != null) {
                String uid = liveJob.getMetadata() != null ? liveJob.getMetadata().getUid() : null;
                if (LaunchIntent.ABANDONED.equals(intent.status())) {
                    // Wedged-alive crash-window (SCRUM-86): a relaunch claimed this
                    // intent (heartbeat nulled) then crashed before recreate, so the
                    // OLD Job is still live. Adopt it AND re-arm the stale-heartbeat
                    // clock, else a still-wedged pod drops from 45s detection to the
                    // 900s activeDeadlineSeconds path.
                    intentRepo.reAdoptWithHeartbeat(intent.id(), uid);
                } else {
                    // Create happened, mark did not: promote, never re-create (F1b).
                    // Heartbeat stays NULL: the fresh pod arms it on its first beat.
                    intentRepo.markIntentLaunched(intent.id(), uid);
                }
            } else {
                LOG.warnf("Reconcile: INTENDED intent %s has no Job; creating", intent.jobName());
                launcher.createJob(intent.id(), intent.arrivalId(), intent.stage(), intent.jobName(),
                        intent.namespaceOr(config.namespace()));
            }
            return;
        }
        if (liveJob != null) {
            return; // OutcomeWatcher owns live observation
        }
        // LAUNCHED and reaped/vanished: resolve from the durable seam first.
        Optional<Outcome> business = outcomes.readBusinessOutcome(intent.jobName());
        if (business.isPresent()) {
            if (outcomeRepo.insertOutcome(intent.id(), intent.attempt(), business.get(), null,
                    "ReapedBeforeObservation")) {
                LOG.infof("Reconcile: recovered outcome %s = %s from seam after reap",
                        intent.jobName(), business.get());
            }
            return;
        }
        OffsetDateTime created = intentRepo.intentCreatedAt(intent.id());
        if (created.plus(REAP_GRACE).isBefore(OffsetDateTime.now())) {
            orphans.relaunchOrExhaust(intent, null); // R-05 amendment: bounded same-identity resume
        }
    }

    /** Wall-clock of the last reconcile tick head, or null before the first tick
     *  (self-liveness probe input, SCRUM-87). */
    public Instant lastTickAt() {
        return lastTickAt;
    }

    /** Baseline for the self-liveness startup grace (SCRUM-87). */
    public Instant startedAt() {
        return startedAt;
    }
}
