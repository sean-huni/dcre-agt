package za.co.fnb.dcre.agt.service;

import io.quarkus.scheduler.Scheduled;
import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.domain.Flow;
import za.co.fnb.dcre.agt.domain.Stage;

/**
 * The PAYMENTS report generator on the clock, one window per (payments client,
 * window). Owner: "PRG generates all the reports for all the clients at specified
 * times (of when they prefer to receive their Payment Reports)."
 *
 * <p><b>This class changed meaning at the v1 cutover.</b> It used to launch the
 * COLLECTIONS generator under the name PRG, for every client of every flow. The
 * collections half now lives in {@link CrgScheduler} against {@code dcre_col}; this
 * one serves PAY clients only and its Jobs write {@code dcre_pay}.
 *
 * <p>Scheduling note, and it is the reason the two have separate interval knobs:
 * collections transaction lists are processed ON the collection day, payments
 * transactions IMMEDIATELY. Nothing here waits for a collection day, and nothing
 * here may acquire one by being made to share a knob or a loop with CRG. The
 * report cadence is a client PREFERENCE about when they like to receive reports,
 * which is not a processing gate.
 */
@ApplicationScoped
public class PrgScheduler {

    @Inject
    AgtConfig config;

    @Inject
    ReportWindows windows;

    @Inject
    FlowNamespaces flowNamespaces;

    @RunOnVirtualThread
    @Scheduled(every = "10s", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    void tick() {
        windows.launchPerClient(Stage.PRG, config.prgIntervalSeconds(),
                client -> flowNamespaces.clientFlow(client) == Flow.PAY);
    }

    /** Kept as the shared window arithmetic every clock scheduler cites. */
    public static long window(final long epochSeconds, final long intervalSeconds) {
        return ReportWindows.window(epochSeconds, intervalSeconds);
    }
}
