package za.co.fnb.dcre.agt.service;

import io.quarkus.scheduler.Scheduled;
import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.domain.Stage;

import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * M10/SCRUM-79: the MANDATES report generator on the clock, one window per
 * (mandate-capable client, window).
 *
 * <p>Restricted to the interim {@code agt.man-clients} set (normalized trim +
 * uppercase like pay-clients) until the R-14 client reference table lands. Unlike
 * the collections and payments generators, eligibility here is an explicit list
 * rather than a flow test: mandates are their own job family and a client is
 * mandate-capable independently of which collections/payments flow it rides.
 */
@ApplicationScoped
public class MrgScheduler {

    @Inject
    AgtConfig config;

    @Inject
    ReportWindows windows;

    @RunOnVirtualThread
    @Scheduled(every = "10s", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    void tick() {
        final Set<String> manClients = manClients();
        windows.launchPerClient(Stage.MRG, config.mrgIntervalSeconds(),
                client -> manClients.contains(normalize(client)));
    }

    /** Config values normalized on read (m4): membership never depends on
     *  whitespace or case in AGT_MAN_CLIENTS. */
    private Set<String> manClients() {
        return config.manClients().stream()
                .map(MrgScheduler::normalize)
                .collect(Collectors.toUnmodifiableSet());
    }

    private static String normalize(final String token) {
        return token.strip().toUpperCase(Locale.ROOT);
    }
}
