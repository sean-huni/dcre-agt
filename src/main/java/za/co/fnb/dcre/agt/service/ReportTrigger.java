package za.co.fnb.dcre.agt.service;

import io.quarkus.scheduler.Scheduled;
import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.domain.Flow;
import za.co.fnb.dcre.agt.domain.Stage;
import za.co.fnb.dcre.agt.repo.ArrivalRepo;
import za.co.fnb.dcre.agt.repo.FamilyReadRepo;
import za.co.fnb.dcre.agt.repo.FamilyReadRepo.DueParent;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * SCRUM-55 debounce/immediate report trigger: scans EACH family's own
 * {@code prg_report_due} view and launches one IMMEDIATE report run PER DUE PARENT,
 * CRG for collections and PRG for payments.
 *
 * <p>Never one launch per client with comma-joined parents: Spring Batch's
 * name=value,type,identifying notation reads token[0] as the value and
 * Class.forName(token[1]) as the type, so a comma inside the parents value
 * crash-loops the launch (agt-12 review blocker). View-sourced client and
 * sourceMsgId are additionally validated fail-closed against SAFE_TOKEN before
 * use: a violating parent is excluded with an UNSAFE_TOKEN WARN, never launched.
 * The window key is a deterministic digest of the FULL sourceMsgId so distinct
 * same-client parents get distinct JobInstances and distinct report file names, and
 * the K8s job name stays under the 63-char label limit even at Max35 msgIds.
 * SCRUM-90: the window carries NO launch epoch, so it is stable per (client,
 * parent): a relaunch of a killed run resumes the same FAILED JobInstance (Spring
 * Batch restart) and the {@code <client>_PSR_<window>.txt} output stays a single
 * file. Level-triggered; repeated scans of a still-due parent mint the SAME window
 * key, and the generator's delivery-ledger guard plus the already-complete
 * JobInstance turn the extra runs into no-ops.
 *
 * <p>That last property is also the failure mode this class now watches for. See
 * {@link #noteStall}.
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

    /** Which generator serves which family. CRG writes dcre_col, PRG writes dcre_pay. */
    private static final Map<Flow, Stage> GENERATORS = Map.of(
            Flow.COL, Stage.CRG,
            Flow.PAY, Stage.PRG);

    @Inject
    AgtConfig config;

    @Inject
    LeaseService lease;

    @Inject
    FamilyReadRepo families;

    @Inject
    ArrivalRepo arrivalRepo;

    @Inject
    JobLauncher launcher;

    @Inject
    StageImages stageImages;

    /**
     * How many consecutive scans have seen each (flow, client, parent) still due.
     *
     * <p>In memory on purpose: this is an observability guard, not a ledger, and an
     * AGT restart resetting the count only delays the WARN. Making it durable would
     * mean a table, and the thing it is watching for is a DATA-MODEL question that
     * belongs to the schema owners rather than to a counter here.
     */
    private final Map<String, Integer> dueScans = new ConcurrentHashMap<>();

    /** Keys already WARNed about, so a stalled parent produces one line, not one per tick. */
    private final Set<String> stallReported = ConcurrentHashMap.newKeySet();

    @RunOnVirtualThread
    @Scheduled(every = "{dcre.agt.report-scan-seconds}",
            concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    public void tick() {
        if (!lease.holdsLease() || !config.launchEnabled()) {
            return;
        }
        for (final Flow flow : FamilyReadRepo.reportingFamilies()) {
            scan(flow);
        }
    }

    /** One family's report-due scan. A family whose generator has no image is skipped
     *  (SCRUM-33 launch-disabled), and it is skipped INDEPENDENTLY: an unconfigured
     *  payments image must never suppress the collections scan. */
    private void scan(final Flow flow) {
        final Stage generator = GENERATORS.get(flow);
        if (stageImages.configured(generator).isEmpty()) {
            return;
        }
        final List<DueParent> due;
        try {
            due = families.reportDue(flow);
        } catch (RuntimeException e) {
            // Per-family isolation (orchestration loop hygiene): one family's view
            // being momentarily unreadable must never abort the tick and take the
            // other family's reports with it. They are separate databases.
            LOG.warnf(e, "report-due scan skipped for %s: prg_report_due unreadable", flow);
            return;
        }
        final Set<String> seen = new HashSet<>();
        for (final DueParent parent : due) {
            if (seen.add(parent.client() + "|" + parent.sourceMsgId())
                    && safe("client", parent.client())
                    && safe("sourceMsgId", parent.sourceMsgId())) {
                launchImmediate(flow, generator, parent.client(), parent.sourceMsgId());
            }
        }
        forgetSettled(flow, due);
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

    private void launchImmediate(final Flow flow, final Stage generator,
                                 final String client, final String sourceMsgId) {
        // SCRUM-90: resolve the parent source book's arrival so the report is
        // launched ARRIVAL-SCOPED (the M12 sweeps recover a killed one-shot
        // report; a clock intent's NULL arrival_id stranded it forever). Fail
        // closed on an unresolved parent: never launch an unscoped report.
        final Optional<UUID> arrivalId = arrivalRepo.arrivalIdForSourceMsgId(client, sourceMsgId);
        if (arrivalId.isEmpty()) {
            LOG.warnf("excluded stage=AGT reason=UNKNOWN_ARRIVAL flow=%s client=%s parent=%s"
                    + ": IMMEDIATE report not launched (no arrival scope, fail-closed)",
                    flow, client, sourceMsgId);
            return;
        }
        final String window = "imm-" + parentDigest(sourceMsgId);
        noteStall(flow, generator, client, sourceMsgId, window);
        LOG.infof("report-due scan: %s IMMEDIATE for %s parent=%s window=%s arrival=%s",
                generator, client, sourceMsgId, window, arrivalId.get());
        launcher.launchArrivalReport(flow, generator, arrivalId.get(),
                client + "-" + window, List.of(
                "client=" + client,
                "window=" + window,
                "report.type=IMMEDIATE,java.lang.String,false",
                "parents=" + sourceMsgId + ",java.lang.String,false"));
    }

    /**
     * Bounded-attempt guard: make an unsatisfiable report parent VISIBLE.
     *
     * <p>The failure it watches for is total silence. A parent that the generator
     * cannot satisfy stays in {@code prg_report_due} forever; the deterministic
     * window key means every later scan mints the same run key and
     * {@code insertReportIntent} no-ops, so nothing throws, nothing is ledgered,
     * nothing is logged, and the client never receives a terminal status. That is
     * the shape the PRG builder reproduced: {@code client} has TWO HOMES in
     * {@code dcre_pay} ({@code tx_header.client_token}, and
     * {@code prw_emission_group.client} derived from {@code initg_pty}), and when
     * they diverge the view names a parent whose rows the read path cannot select.
     * The same shape affects CRG and MRG.
     *
     * <p>This does NOT fix it, deliberately. It does not widen a column, does not
     * retry, and does not fall back, because each of those makes a silent loop
     * QUIETER rather than visible, and a fallback would reintroduce the bug for
     * exactly the records the authority does not cover. Naming one authority for
     * {@code client} in the schema is a data-model decision for the owners. All this
     * does is stop the loop being invisible while they make it.
     */
    private void noteStall(final Flow flow, final Stage generator, final String client,
                           final String parent, final String window) {
        final String key = flow + "|" + client + "|" + parent;
        final int scans = dueScans.merge(key, 1, Integer::sum);
        if (scans >= config.reportStallScans() && stallReported.add(key)) {
            LOG.warnf("report-stall stage=%s flow=%s client=%s parent=%s window=%s scans=%d"
                            + ": this parent has been due for %d consecutive scans and its report"
                            + " has not settled it. Nothing is retrying and nothing has failed;"
                            + " the report simply never satisfies the view. Check that the"
                            + " client token in the report-due view is the same client the"
                            + " generator's read path selects on.",
                    generator, flow, client, parent, window, scans, scans);
        }
    }

    /** Drop counters for parents this family no longer reports as due, so a parent
     *  that settles normally never accumulates toward the WARN across its lifetime. */
    private void forgetSettled(final Flow flow, final List<DueParent> due) {
        final Set<String> stillDue = new HashSet<>();
        due.forEach(p -> stillDue.add(flow + "|" + p.client() + "|" + p.sourceMsgId()));
        final String prefix = flow + "|";
        dueScans.keySet().removeIf(k -> k.startsWith(prefix) && !stillDue.contains(k));
        stallReported.removeIf(k -> k.startsWith(prefix) && !stillDue.contains(k));
    }

    /** Parents currently over the stall threshold; the seam the guard's test asserts on. */
    Set<String> stalledParents() {
        return Set.copyOf(stallReported);
    }

    /**
     * Deterministic 12-hex-char digest of the full sourceMsgId: every AGT
     * incarnation computes the same run key for the same (client, parent)
     * tuple, so a relaunch resumes the prior JobInstance and emits a single
     * report file (SCRUM-90: no epoch in the key). All 35 msgId chars contribute
     * (no truncation collisions), and the derived K8s job name stays DNS-1123 safe.
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
