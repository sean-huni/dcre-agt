package za.co.fnb.dcre.agt.service;

import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.KubernetesClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.domain.LaunchIntent;
import za.co.fnb.dcre.agt.domain.Outcome;
import za.co.fnb.dcre.agt.repo.ArrivalRepo;
import za.co.fnb.dcre.agt.repo.IntentRepo;
import za.co.fnb.dcre.agt.repo.OutcomeRepo;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Optional;

/**
 * OrphanSweeper (R-05 amendment, spec 2026-07-14-stuck-job-recovery-design.md):
 * bounded same-identity relaunch of arrival intents whose stage Job died.
 * The Job is recreated with the identical name and durable args, so Spring
 * Batch resumes the same job instance from its last committed chunk/slice.
 * BUSINESS outcomes are never relaunched; clock intents self-heal at the next
 * window boundary. When the attempt budget is exhausted, the exhaustion
 * consumes one attempt slot as its own TECH_EXHAUSTED ledger row and the
 * arrival goes DAG_FAILED (terminal, never a silent zombie).
 */
@ApplicationScoped
public class OrphanRelauncher {

    private static final Logger LOG = Logger.getLogger(OrphanRelauncher.class);

    @Inject
    IntentRepo intentRepo;

    @Inject
    OutcomeRepo outcomeRepo;

    @Inject
    ArrivalRepo arrivalRepo;

    @Inject
    AgtConfig config;

    @Inject
    JobLauncher launcher;

    @Inject
    KubernetesClient k8s;

    /** OrphanSweeper (spec 2026-07-14): current attempt ended TECH -> bounded relaunch. */
    void sweepTechOrphans(final Map<String, Job> live) {
        for (LaunchIntent intent : intentRepo.launchedArrivalIntentsWithTechCurrentAttempt()) {
            try {
                relaunchOrExhaust(intent, live.get(intent.jobName()));
            } catch (Exception e) {
                LOG.warnf("orphan sweep %s failed: %s", intent.jobName(), e.getMessage());
            }
        }
    }

    void relaunchOrExhaust(final LaunchIntent intent, final Job deadJob) {
        if (intent.arrivalId() == null) {
            return; // clock windows self-heal at the next boundary
        }
        Optional<OffsetDateTime> last = intentRepo.lastAttemptAt(intent.id());
        if (last.isPresent() && last.get()
                .plusSeconds(config.orphanBackoffSeconds()).isAfter(OffsetDateTime.now())) {
            return; // backoff window
        }
        if (intent.attempt() >= config.orphanMaxAttempts()) {
            int finalAttempt = intentRepo.beginRelaunchAttempt(intent.id());
            if (outcomeRepo.insertOutcome(intent.id(), finalAttempt, Outcome.TECH_EXHAUSTED,
                    null, "OrphanBudgetExhausted")) {
                arrivalRepo.markDagFailed(intent.arrivalId());
                LOG.errorf("Orphan %s exhausted %d attempts: TECH_EXHAUSTED, arrival DAG_FAILED",
                        intent.jobName(), intent.attempt());
            }
            return;
        }
        if (deadJob != null) {
            k8s.batch().v1().jobs().inNamespace(intent.namespaceOr(config.namespace()))
                    .withName(intent.jobName()).delete(); // clear Failed object before same-name create
        }
        int attempt = intentRepo.beginRelaunchAttempt(intent.id());
        LOG.warnf("Orphan %s: relaunch attempt %d/%d (same identity, Batch resumes from last commit)",
                intent.jobName(), attempt, config.orphanMaxAttempts());
        launcher.createJob(intent.id(), intent.arrivalId(), intent.stage(), intent.jobName(),
                intent.namespaceOr(config.namespace()));
    }
}
