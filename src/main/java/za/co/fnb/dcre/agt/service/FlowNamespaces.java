package za.co.fnb.dcre.agt.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.domain.FileArrival;
import za.co.fnb.dcre.agt.domain.Flow;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * SCRUM-70 flow resolver: routes classify job families. onhost-req is
 * Collections, onhost-req-endo is Payments (SCRUM-69), fint-resp follows the
 * READING client's flow. AGT itself stays in the control namespace
 * (agt.namespace); stage Jobs land in the flow namespace.
 */
@ApplicationScoped
public class FlowNamespaces {

    @Inject
    AgtConfig config;

    /**
     * Pure route resolution. INTERIM (R-42): the pay-clients membership decides
     * the fint-resp flow until the R-14 client reference table lands; unknown
     * routes fall back to collections-primary, mirroring DagEngine's DC fallback.
     * Token comparison is normalized (trim + uppercase, m4).
     */
    public static Flow flowForRoute(String routeId, String clientToken, Collection<String> payClients) {
        return switch (routeId == null ? "" : routeId) {
            case ArrivalService.ROUTE_ONHOST_REQ_ENDO -> Flow.PAY;
            case ArrivalService.ROUTE_FINT_RESP ->
                    clientToken != null && payClients.contains(normalize(clientToken)) ? Flow.PAY : Flow.COL;
            default -> Flow.COL; // onhost-req and unknown routes: collections-primary
        };
    }

    public Flow flowFor(FileArrival arrival) {
        return flowForRoute(arrival.routeId(), arrival.clientToken(), payClients());
    }

    /** Client-keyed flow for clock jobs (PRG windows, ReportTrigger IMMEDIATE). */
    public Flow clientFlow(String clientToken) {
        return flowForRoute(ArrivalService.ROUTE_FINT_RESP, clientToken, payClients());
    }

    public String namespaceOf(Flow flow) {
        return switch (flow) {
            case COL -> config.namespaceCol();
            case PAY -> config.namespacePay();
            case MAN -> config.namespaceMan();
        };
    }

    /** Control namespace + every flow namespace (distinct), for cross-namespace scans. */
    public List<String> allNamespaces() {
        return Stream.of(config.namespace(), config.namespaceCol(),
                        config.namespacePay(), config.namespaceMan())
                .distinct()
                .toList();
    }

    /** Config values normalized on read (m4): membership never depends on
     *  whitespace or case in AGT_PAY_CLIENTS. */
    private Set<String> payClients() {
        return config.payClients().stream()
                .map(FlowNamespaces::normalize)
                .collect(Collectors.toUnmodifiableSet());
    }

    private static String normalize(String token) {
        return token.strip().toUpperCase(Locale.ROOT);
    }
}
