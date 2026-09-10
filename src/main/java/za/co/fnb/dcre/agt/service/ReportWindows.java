package za.co.fnb.dcre.agt.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.domain.Flow;
import za.co.fnb.dcre.agt.domain.Stage;
import za.co.fnb.dcre.agt.repo.ArrivalRepo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.function.Predicate;

/**
 * The per-(client, window) clock-report launch, written ONCE.
 *
 * <p>Each family has a report generator on the clock: CRG for collections, PRG for
 * payments, MRG for mandates. Before the v1 topology two schedulers held
 * byte-similar copies of this loop and the split would have produced a third. The
 * only things that actually differ are the stage, the window length and which
 * clients are eligible, so those are the parameters and the rest lives here.
 *
 * <p>Level-triggered: the window counter derives from the epoch, so every AGT
 * incarnation computes the same run key for the same instant and the clock-intent
 * unique key dedupes the repeats.
 */
@ApplicationScoped
public class ReportWindows {

    private static final Logger LOG = Logger.getLogger(ReportWindows.class);

    @Inject
    AgtConfig config;

    @Inject
    LeaseService lease;

    @Inject
    ArrivalRepo arrivalRepo;

    @Inject
    JobLauncher launcher;

    @Inject
    StageImages stageImages;

    @Inject
    StageNamespaces stageNamespaces;

    /** Absent/empty image = launch-disabled (SCRUM-33: no stub fallback), and no
     *  lease means another incarnation owns the side effects. */
    boolean paused(final Stage stage) {
        return !lease.holdsLease() || !config.launchEnabled()
                || stageImages.configured(stage).isEmpty();
    }

    /**
     * One report window per eligible client, plus the on-demand chaos trigger.
     *
     * <p>The FLOW is taken from the stage's own hosting family, never from the client.
     * That is the v1 correction: the pre-split scheduler resolved
     * {@code flowNamespaces.clientFlow(client)} and launched ONE stage into whichever
     * namespace came back, so a single generator served {@code col-} and {@code pay-}
     * alike. Now the eligibility predicate decides WHICH clients a generator serves
     * and the stage decides where it runs, so the two cannot disagree.
     *
     * <p>This is the NAMESPACE question, so it reads {@link StageNamespaces}. It used
     * to read the database switch, which happened to give the same answer while the
     * two were one enum.
     */
    void launchPerClient(final Stage stage, final long intervalSeconds, final Predicate<String> eligible) {
        if (paused(stage)) {
            return;
        }
        final Flow flow = stageNamespaces.namespaceFamilyOf(stage);
        final long window = window(Instant.now().getEpochSecond(), intervalSeconds);
        for (final String client : arrivalRepo.distinctClientTokens()) {
            if (!eligible.test(client)) {
                continue;
            }
            launcher.launchClock(flow, stage, client + "-w" + window, List.of(
                    "client=" + client,
                    "window=w" + window));
            considerManualTrigger(stage, flow, client, window);
        }
    }

    /** Window arithmetic shared by every clock scheduler: identical keys across incarnations. */
    public static long window(final long epochSeconds, final long intervalSeconds) {
        return epochSeconds / intervalSeconds;
    }

    /**
     * {@code <exchange>/chaos/run-<stage>-<client>} launches an immediate manual
     * window. The trigger file name follows the STAGE, so the collections one is
     * {@code run-crg-<client>} now that the collections generator is CRG; the
     * payments {@code run-prg-<client>} keeps its name and changes meaning with it.
     */
    private void considerManualTrigger(final Stage stage, final Flow flow,
                                       final String client, final long window) {
        final String token = stage.name().toLowerCase(Locale.ROOT);
        final Path trigger = Path.of(config.exchangeRoot(), "chaos", "run-" + token + "-" + client);
        try {
            if (!Files.deleteIfExists(trigger)) {
                return;
            }
        } catch (IOException e) {
            LOG.warnf("manual %s trigger for %s failed: %s", stage, client, e.getMessage());
            return;
        }
        // Idempotent within the window: repeated triggers reuse one run key.
        // The window param carries a -manual suffix so the Batch job instance is
        // distinct from the scheduled run of the same window (identifying params
        // are the instance identity; resend alone is non-identifying).
        launcher.launchClock(flow, stage, client + "-manual-" + window, List.of(
                "client=" + client,
                "window=w" + window + "-manual",
                "resend=true,java.lang.String,false"));
    }
}
