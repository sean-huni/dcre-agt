package za.co.fnb.dcre.agt.service;

import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.domain.LaunchIntent;
import za.co.fnb.dcre.agt.domain.Outcome;
import za.co.fnb.dcre.agt.repo.LedgerRepo;
import io.fabric8.kubernetes.client.KubernetesClient;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Level-triggered outcome observer (R-33: agt_ops is the sole termination
 * authority; the outcome file is the SYNTHETIC-CONTRACT business-verdict seam
 * until M2 services land). Polls rather than informs: a poll re-derives state
 * after any missed event, which is the reconciliation posture SPEC-DAG asks for.
 */
@ApplicationScoped
public class OutcomeWatcher {

    private static final Logger LOG = Logger.getLogger(OutcomeWatcher.class);

    @Inject
    LedgerRepo repo;

    @Inject
    AgtConfig config;

    @Inject
    LeaseService lease;

    @Inject
    KubernetesClient k8s;

    @Scheduled(every = "3s")
    void tick() {
        if (!lease.holdsLease() || !config.launchEnabled()) {
            return;
        }
        List<LaunchIntent> open = repo.intentsWithoutOutcome();
        for (LaunchIntent intent : open) {
            if (!LaunchIntent.LAUNCHED.equals(intent.status())) {
                continue; // reconciler's problem
            }
            Job job = k8s.batch().v1().jobs().inNamespace(config.namespace())
                    .withName(intent.jobName()).get();
            if (job == null || job.getStatus() == null) {
                continue; // not visible yet, or TTL-reaped before we observed: reconciler handles
            }
            boolean complete = job.getStatus().getSucceeded() != null && job.getStatus().getSucceeded() > 0;
            boolean failed = job.getStatus().getFailed() != null && job.getStatus().getFailed() > 0;
            if (!complete && !failed) {
                continue;
            }
            Outcome outcome = failed ? Outcome.TECH_FAILED : readBusinessOutcome(intent.jobName());
            boolean recorded = repo.insertOutcome(intent.id(), outcome, failed ? 1 : 0,
                    failed ? "Failed" : "Complete");
            if (recorded) {
                LOG.infof("Outcome %s = %s", intent.jobName(), outcome);
            }
        }
    }

    /** Reads the service-reported business outcome; absence is NEVER success (arbiter clause, R-33). */
    Outcome readBusinessOutcome(String jobName) {
        Path f = Path.of(config.exchangeRoot(), "outcomes", jobName);
        try {
            String text = Files.readString(f).strip();
            return Outcome.valueOf(text);
        } catch (Exception e) {
            LOG.warnf("Job %s Complete but no readable outcome file: TECH_FAILED (arbiter clause)", jobName);
            return Outcome.TECH_FAILED;
        }
    }
}
