package za.co.fnb.dcre.agt.service;

import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobCondition;
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

import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Level-triggered outcome observer. Termination facts are OBSERVED, never
 * fabricated (R-33, Fugu F9): terminality comes from Job conditions, the
 * recorded condition carries the real type/reason, and the Job UID is checked
 * against the intent so a same-name recreate is never mistaken for the
 * original. Observation runs even when launching is paused (Fugu F1a:
 * agt.observe-enabled is independent of agt.launch-enabled).
 */
@ApplicationScoped
public class OutcomeWatcher {

    private static final Logger LOG = Logger.getLogger(OutcomeWatcher.class);

    /**
     * Exit code platform-batch reserves for a failure BEFORE its runner phase
     * (EX_CONFIG from sysexits.h): config import, property binding, secret
     * fetch. A cross-repo wire contract, asserted as a literal like DCRE_DB_URL:
     * platform-batch's ExitCodeMain emits it, AGT classifies on it. 78 is chosen
     * because 0-7 are claimed by Boot's JobExecutionExitCodeGenerator (and 1 by
     * the JVM's uncaught-exception status), 126/127 are shell-reserved, and
     * 128+N are signal deaths (137/143 are load-bearing for the chaos gate).
     */
    public static final int CONFIG_FAILURE_EXIT_CODE = 78;

    /** Appended to the observed k8s condition so the ledger row says WHY this is
     *  an infrastructure class, without discarding the real type/reason (R-33).
     *  Worst case "Failed/BackoffLimitExceeded/InfraStartup" = 40 chars, inside
     *  stage_outcome.k8s_condition VARCHAR(64). */
    static final String INFRA_CONDITION_SUFFIX = "/InfraStartup";

    @Inject
    IntentRepo intentRepo;

    @Inject
    OutcomeRepo outcomeRepo;

    @Inject
    AgtConfig config;

    @Inject
    LeaseService lease;

    @Inject
    KubernetesClient k8s;

    @RunOnVirtualThread
    @Scheduled(every = "3s", concurrentExecution = io.quarkus.scheduler.Scheduled.ConcurrentExecution.SKIP)
    void tick() {
        if (!lease.holdsLease() || !config.observeEnabled()) {
            return;
        }
        for (LaunchIntent intent : intentRepo.intentsWithoutOutcome()) {
            try {
                observe(intent);
            } catch (Exception e) {
                LOG.warnf("observe %s failed: %s", intent.jobName(), e.getMessage()); // one bad item never wedges the tick (F10)
            }
        }
    }

    void observe(LaunchIntent intent) {
        if (!LaunchIntent.LAUNCHED.equals(intent.status())) {
            return; // reconciler's problem
        }
        // SCRUM-70: observe in the namespace the intent was WRITTEN with; the
        // control namespace is only the pre-backfill fallback, never a probe.
        String namespace = intent.namespaceOr(config.namespace());
        Job job = k8s.batch().v1().jobs().inNamespace(namespace)
                .withName(intent.jobName()).get();
        if (job == null) {
            return; // TTL-reaped or not yet visible: reconciler resolves via the durable seam (F1)
        }
        String expectedUid = intentRepo.intentJobUid(intent.id()).orElse(null);
        String actualUid = job.getMetadata() != null ? job.getMetadata().getUid() : null;
        if (expectedUid != null && actualUid != null && !expectedUid.equals(actualUid)) {
            LOG.warnf("Job %s uid mismatch (expected %s, saw %s): ignoring foreign object",
                    intent.jobName(), expectedUid, actualUid);
            return;
        }
        Optional<JobCondition> terminal = terminalCondition(job);
        if (terminal.isEmpty()) {
            return;
        }
        JobCondition cond = terminal.get();
        boolean failed = "Failed".equals(cond.getType());
        String condition = cond.getType() + (cond.getReason() != null ? "/" + cond.getReason() : "");
        Integer exitCode = podExitCode(namespace, intent.jobName());

        Outcome outcome;
        if (failed) {
            // A pre-runner startup failure is INFRASTRUCTURE, not a job outcome:
            // the pod never reached the work, so the arrival carries no defect
            // and must not burn the 3-attempt orphan budget. Observed, never
            // assumed (R-33): only the pod's real exit code 78 classifies it.
            // A null exitCode (pod already gone) is NOT evidence of a config
            // failure, so it keeps today's TECH_FAILED behaviour exactly.
            boolean infra = exitCode != null && exitCode == CONFIG_FAILURE_EXIT_CODE;
            outcome = infra ? Outcome.TECH_CONFIG_FAILED : Outcome.TECH_FAILED;
            if (infra) {
                condition = condition + INFRA_CONDITION_SUFFIX;
            }
        } else {
            Optional<Outcome> business = readBusinessOutcome(intent.jobName());
            if (business.isEmpty()) {
                return; // Complete but seam not readable yet: retry next tick (F8); reconciler owns the grace cutoff
            }
            outcome = business.get();
        }
        if (outcomeRepo.insertOutcome(intent.id(), intent.attempt(), outcome, exitCode, condition)) {
            LOG.infof("Outcome %s = %s (%s, exit=%s)", intent.jobName(), outcome, condition, exitCode);
        }
    }

    static Optional<JobCondition> terminalCondition(Job job) {
        if (job.getStatus() == null || job.getStatus().getConditions() == null) {
            return Optional.empty();
        }
        return job.getStatus().getConditions().stream()
                .filter(c -> ("Complete".equals(c.getType()) || "Failed".equals(c.getType()))
                        && "True".equals(c.getStatus()))
                .findFirst();
    }

    /** Best-effort real exit code from the Job's pod; null when the pod is already
     *  gone. Reads the FIRST pod carrying the job-name label, which is only
     *  correct while one Job produces exactly one pod: both JobLauncher builder
     *  sites pin backoffLimit 0 + restartPolicy Never (pinned by
     *  NamespaceRoutingTest.bothBuilderSitesPinBackoffLimitZeroSoOneJobStaysOnePod).
     *  Raise the backoff limit and this
     *  read becomes a lottery between attempts, silently re-masking the exit-78
     *  config class as a generic TECH_FAILED. */
    private Integer podExitCode(String namespace, String jobName) {
        try {
            var pods = k8s.pods().inNamespace(namespace)
                    .withLabel("job-name", jobName).list().getItems();
            if (pods.isEmpty()) {
                return null;
            }
            var statuses = pods.get(0).getStatus().getContainerStatuses();
            if (statuses == null || statuses.isEmpty() || statuses.get(0).getState() == null
                    || statuses.get(0).getState().getTerminated() == null) {
                return null;
            }
            return statuses.get(0).getState().getTerminated().getExitCode();
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Reads the business verdict seam. Distinguishes (F8): absent/IO-hiccup ->
     * empty (retry); present-but-invalid -> TECH_FAILED (arbiter clause, R-33).
     */
    Optional<Outcome> readBusinessOutcome(String jobName) {
        Path f = Path.of(config.exchangeRoot(), "outcomes", jobName);
        String text;
        try {
            text = Files.readString(f).strip();
        } catch (NoSuchFileException e) {
            return Optional.empty();
        } catch (Exception e) {
            LOG.debugf("outcome file %s unreadable (%s): retrying", jobName, e.getMessage());
            return Optional.empty();
        }
        try {
            return Optional.of(Outcome.valueOf(text));
        } catch (IllegalArgumentException e) {
            LOG.warnf("Job %s outcome file invalid (%s): TECH_FAILED (arbiter clause)", jobName, text);
            return Optional.of(Outcome.TECH_FAILED);
        }
    }
}
