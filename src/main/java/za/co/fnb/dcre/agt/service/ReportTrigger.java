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
import java.util.regex.Pattern;

/**
 * SCRUM-55 debounce/immediate report trigger: scans the collections-side
 * prg_report_due view and launches one PRG IMMEDIATE run PER DUE PARENT.
 * Never one launch per client with comma-joined parents: Spring Batch's
 * name=value,type,identifying notation reads token[0] as the value and
 * Class.forName(token[1]) as the type, so a comma inside the parents value
 * crash-loops the PRG launch (agt-12 review blocker). View-sourced client and
 * sourceMsgId are additionally validated fail-closed against SAFE_TOKEN before
 * use: a violating parent is excluded with an UNSAFE_TOKEN WARN, never
 * launched. The window key embeds a
 * digest of the FULL sourceMsgId so same-client same-second parents get
 * distinct JobInstances and distinct PSR file names, and the K8s job name
 * stays under the 63-char label limit even at Max35 msgIds. Level-triggered
 * like PrgScheduler; repeated scans of a still-due parent mint distinct window
 * keys, and PRG's delivery-ledger guard turns the extra runs into no-ops.
 */
@ApplicationScoped
public class ReportTrigger {

    private static final Logger LOG = Logger.getLogger(ReportTrigger.class);

    /**
     * Fail-closed whitelist for view-sourced values embedded into Spring
     * Batch's name=value,type,identifying notation and the K8s run key: a
     * comma shifts the tokens, an equals sign splits the name, a newline
     * forges log lines. Anything outside this set is excluded, never quoted.
     */
    private static final Pattern SAFE_TOKEN = Pattern.compile("^[A-Za-z0-9_-]+$");

    @Inject
    AgtConfig config;

    @Inject
    LeaseService lease;

    @Inject
    CollectionsReadRepo collections;

    @Inject
    JobLauncher launcher;

    @Inject
    FlowNamespaces flowNamespaces;

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
            if (launched.add(parent.client() + "|" + parent.sourceMsgId())
                    && safe("client", parent.client())
                    && safe("sourceMsgId", parent.sourceMsgId())) {
                launchImmediate(parent.client(), parent.sourceMsgId(), epochSec);
            }
        }
    }

    /** Fail-closed: on violation WARN (value elided to 8 chars) and never launch. */
    private static boolean safe(final String field, final String value) {
        if (value != null && SAFE_TOKEN.matcher(value).matches()) {
            return true;
        }
        LOG.warnf("excluded stage=AGT reason=UNSAFE_TOKEN field=%s value=%s", field, elide(value));
        return false;
    }

    /** Log-safe sample: unsafe bytes never reach the log line (CWE-117), even elided. */
    private static String elide(final String value) {
        if (value == null) {
            return "null";
        }
        final String sanitized = value.replaceAll("[^A-Za-z0-9_-]", "?");
        return sanitized.length() <= 8 ? sanitized : sanitized.substring(0, 8);
    }

    private void launchImmediate(final String client, final String sourceMsgId, final long epochSec) {
        final String window = "imm-" + epochSec + "-" + parentDigest(sourceMsgId);
        LOG.infof("report-due scan: PRG IMMEDIATE for %s parent=%s window=%s", client, sourceMsgId, window);
        // SCRUM-70: the IMMEDIATE window follows the parent client's flow (R-42 interim map).
        launcher.launchClock(flowNamespaces.clientFlow(client), Stage.PRG, client + "-" + window, List.of(
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
