package za.co.fnb.dcre.agt.service;

import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.KubernetesClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.domain.LaunchIntent;
import za.co.fnb.dcre.agt.domain.Outcome;
import za.co.fnb.dcre.agt.domain.RelaunchCandidate;
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
 * The budget is per outcome CLASS (config-plane spec, "Failure classification"):
 * TECH_FAILED keeps orphan-max-attempts, TECH_CONFIG_FAILED (pod exit 78 = a
 * pre-runner startup failure) gets infra-max-attempts, because an infrastructure
 * hiccup must not spend a defect-free arrival's 3-attempt budget. Both stay
 * bounded and both end in TECH_EXHAUSTED + DAG_FAILED.
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

    /** OrphanSweeper (spec 2026-07-14): current attempt ended TECH -> bounded relaunch.
     *  The k8s-Failed path: a Failed Job condition minted a TECH_FAILED outcome, or
     *  a TECH_CONFIG_FAILED one when the pod exited 78 (pre-runner startup failure).
     *  Both are retried; only the CEILING differs (maxAttemptsFor). */
    void sweepTechOrphans(final Map<String, Job> live) {
        sweep(intentRepo.launchedArrivalIntentsWithTechCurrentAttempt(), live, "orphan");
    }

    /** Stale-heartbeat sweep (M12/SCRUM-86, R-47): the wedged-but-alive path. A
     *  LAUNCHED intent whose heartbeat fell behind the TTL is relaunched through
     *  the SAME relaunchOrExhaust/claim machinery as the k8s-Failed path, so an
     *  intent that is both k8s-Failed and stale-heartbeat is relaunched exactly
     *  once (the atomic claim clears heartbeat_at, dropping it from this sweep). */
    void sweepStaleHeartbeat(final Map<String, Job> live) {
        sweep(intentRepo.launchedArrivalIntentsWithStaleHeartbeat(config.heartbeatTtlSeconds()),
                live, "stale-heartbeat sweep");
    }

    private void sweep(final Iterable<RelaunchCandidate> worklist, final Map<String, Job> live,
                       final String label) {
        for (RelaunchCandidate candidate : worklist) {
            LaunchIntent intent = candidate.intent();
            try {
                relaunchOrExhaust(intent, candidate.currentOutcome(), live.get(intent.jobName()));
            } catch (Exception e) {
                LOG.warnf("%s %s failed: %s", label, intent.jobName(), e.getMessage());
            }
        }
    }

    /**
     * Ceiling for this attempt, chosen by the class of the CURRENT attempt's
     * outcome. TECH_CONFIG_FAILED (pod exit 78) is an infrastructure startup
     * failure with a defect-free payload, so it gets infra-max-attempts;
     * everything else, including a null class (a wedged-but-alive or TTL-reaped
     * intent that recorded no outcome), keeps the unchanged orphan budget.
     * Absence of an exit code is not evidence of a config failure.
     */
    private int maxAttemptsFor(final Outcome currentOutcome) {
        return currentOutcome == Outcome.TECH_CONFIG_FAILED
                ? config.infraMaxAttempts() : config.orphanMaxAttempts();
    }

    void relaunchOrExhaust(final LaunchIntent intent, final Outcome currentOutcome, final Job deadJob) {
        if (intent.arrivalId() == null) {
            return; // clock windows self-heal at the next boundary
        }
        Optional<OffsetDateTime> last = intentRepo.lastAttemptAt(intent.id());
        if (last.isPresent() && last.get()
                .plusSeconds(config.orphanBackoffSeconds()).isAfter(OffsetDateTime.now())) {
            return; // backoff window (pre-check: never consume an attempt while throttled)
        }
        // Single atomic claim shared by both orphan paths: LAUNCHED -> ABANDONED,
        // consume an attempt slot, clear heartbeat. Empty => another sweep or
        // incarnation already claimed this intent this tick, so skip (no
        // double-relaunch, no double attempt bump).
        Optional<Integer> claimed = intentRepo.claimForRelaunch(intent.id());
        if (claimed.isEmpty()) {
            return;
        }
        int attempt = claimed.get();
        // Per-class ceiling; the off-by-one convention is unchanged (the check
        // reads the PRE-claim attempt, the ledger row is written at the POST-claim
        // number), so max=N still executes attempts 0..N and lands the terminal
        // row at N+1.
        int maxAttempts = maxAttemptsFor(currentOutcome);
        if (intent.attempt() >= maxAttempts) {
            // Budget exhausted: the claim consumed the final slot; record on it.
            // Name WHICH budget was spent: an operator reading stage_outcome cannot
            // otherwise tell a 3-attempt job exhaustion from a 10-attempt
            // config-plane one, and the remedies differ (fix the job vs fix cfg).
            String exhaustionCondition = currentOutcome == Outcome.TECH_CONFIG_FAILED
                    ? "InfraBudgetExhausted" : "OrphanBudgetExhausted";
            if (outcomeRepo.insertOutcome(intent.id(), attempt, Outcome.TECH_EXHAUSTED,
                    null, exhaustionCondition)) {
                if (intent.isArrivalReport()) {
                    // SCRUM-90: an IMMEDIATE report is downstream of DAG completion;
                    // its exhaustion is terminal on the REPORT intent ONLY and must
                    // never regress the parent arrival's DAG (no markDagFailed).
                    LOG.errorf("Immediate report %s exhausted %d attempts: TECH_EXHAUSTED"
                            + " (parent arrival DAG untouched)", intent.jobName(), intent.attempt());
                } else {
                    arrivalRepo.markDagFailed(intent.arrivalId());
                    LOG.errorf("Orphan %s exhausted %d attempts: TECH_EXHAUSTED, arrival DAG_FAILED",
                            intent.jobName(), intent.attempt());
                }
            }
            return; // stays ABANDONED (terminal); heartbeat cleared, so never re-swept
        }
        if (deadJob != null) {
            k8s.batch().v1().jobs().inNamespace(intent.namespaceOr(config.namespace()))
                    .withName(intent.jobName()).delete(); // clear the object before same-name create
        }
        LOG.warnf("Orphan %s: relaunch attempt %d/%d after %s (same identity, Batch resumes from last commit)",
                intent.jobName(), attempt, maxAttempts,
                currentOutcome != null ? currentOutcome : "no recorded outcome");
        launcher.createJob(intent.id(), intent.arrivalId(), intent.stage(), intent.jobName(),
                intent.namespaceOr(config.namespace())); // re-marks the intent LAUNCHED
    }
}
