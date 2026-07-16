package za.co.fnb.dcre.agt.service;

import io.quarkus.scheduler.Scheduled;
import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.domain.Stage;
import za.co.fnb.dcre.agt.repo.CollectionsReadRepo;
import za.co.fnb.dcre.agt.repo.CollectionsReadRepo.DueParent;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;

/**
 * SCRUM-55 debounce/immediate report trigger: scans the collections-side
 * prg_report_due view and launches one PRG IMMEDIATE run PER DUE PARENT.
 * Never one launch per client with comma-joined parents: Spring Batch's
 * name=value,type,identifying notation reads token[0] as the value and
 * Class.forName(token[1]) as the type, so a comma inside the parents value
 * crash-loops the PRG launch (agt-12 review blocker). The window key embeds a
 * digest of the FULL sourceMsgId so same-client same-second parents get
 * distinct JobInstances and distinct PSR file names, and the K8s job name
 * stays under the 63-char label limit even at Max35 msgIds. Level-triggered
 * like PrgScheduler; repeated scans of a still-due parent mint distinct window
 * keys, and PRG's delivery-ledger guard turns the extra runs into no-ops.
 */
@ApplicationScoped
public class ReportTrigger {

    private static final Logger LOG = Logger.getLogger(ReportTrigger.class);

    @Inject
    AgtConfig config;

    @Inject
    LeaseService lease;

    @Inject
    CollectionsReadRepo collections;

    @Inject
    JobLauncher launcher;

    @RunOnVirtualThread
    @Scheduled(every = "{dcre.agt.report-scan-seconds}",
            concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    public void tick() {
        if (!lease.holdsLease() || !config.launchEnabled() || config.prgImage().isEmpty()) {
            return;
        }
        final List<DueParent> due = collections.reportDue();
        if (due.isEmpty()) {
            return;
        }
        final long epochSec = Instant.now().getEpochSecond();
        final Set<String> launched = new HashSet<>();
        for (final DueParent parent : due) {
            if (launched.add(parent.client() + "|" + parent.sourceMsgId())) {
                launchImmediate(parent.client(), parent.sourceMsgId(), epochSec);
            }
        }
    }

    private void launchImmediate(final String client, final String sourceMsgId, final long epochSec) {
        final String window = "imm-" + epochSec + "-" + parentDigest(sourceMsgId);
        LOG.infof("report-due scan: PRG IMMEDIATE for %s parent=%s window=%s", client, sourceMsgId, window);
        launcher.launchClock(Stage.PRG, client + "-" + window, List.of(
                "client=" + client,
                "window=" + window,
                "report.type=IMMEDIATE,java.lang.String,false",
                "parents=" + sourceMsgId + ",java.lang.String,false"));
    }

    /**
     * Deterministic 12-hex-char digest of the full sourceMsgId: every AGT
     * incarnation computes the same run key for the same (client, parent,
     * epoch) tuple, all 35 msgId chars contribute (no truncation collisions),
     * and the derived K8s job name stays DNS-1123 safe.
     */
    static String parentDigest(final String sourceMsgId) {
        try {
            final byte[] sha = MessageDigest.getInstance("SHA-256")
                    .digest(sourceMsgId.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(sha, 0, 6);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
