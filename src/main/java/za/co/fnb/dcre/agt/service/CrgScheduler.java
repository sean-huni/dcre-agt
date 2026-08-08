package za.co.fnb.dcre.agt.service;

import io.quarkus.scheduler.Scheduled;
import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.domain.Flow;
import za.co.fnb.dcre.agt.domain.Stage;

/**
 * R-28: the COLLECTIONS report generator on the clock, one window per
 * (collections client, window).
 *
 * <p>This is the scheduler that used to be {@code PrgScheduler} and used to launch
 * {@code Stage.PRG}. The diagrams call the collections generator CRG and give the
 * name PRG to the payments one, so the token did not move, it changed meaning.
 * {@link PrgScheduler} is now a different service on a different database.
 *
 * <p>Restricted to clients whose flow is COL. Before the split ONE loop launched a
 * single generator for every client on whatever namespace that client's flow
 * resolved to, which is exactly the shared-service coupling the family split
 * exists to remove.
 */
@ApplicationScoped
public class CrgScheduler {

    @Inject
    AgtConfig config;

    @Inject
    ReportWindows windows;

    @Inject
    FlowNamespaces flowNamespaces;

    @RunOnVirtualThread
    @Scheduled(every = "10s", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    void tick() {
        windows.launchPerClient(Stage.CRG, config.crgIntervalSeconds(),
                client -> flowNamespaces.clientFlow(client) == Flow.COL);
    }
}
