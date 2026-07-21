package za.co.fnb.dcre.agt.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.domain.FileArrival;
import za.co.fnb.dcre.agt.domain.Flow;

import java.util.Collection;
import java.util.List;

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
     */
    public static Flow flowForRoute(String routeId, String clientToken, Collection<String> payClients) {
        return switch (routeId == null ? "" : routeId) {
            case ArrivalService.ROUTE_ONHOST_REQ_ENDO -> Flow.PAY;
            case ArrivalService.ROUTE_FINT_RESP ->
                    clientToken != null && payClients.contains(clientToken) ? Flow.PAY : Flow.COL;
            default -> Flow.COL; // onhost-req and unknown routes: collections-primary
        };
    }

    public Flow flowFor(FileArrival arrival) {
        return flowForRoute(arrival.routeId(), arrival.clientToken(), config.payClients());
    }

    /** Client-keyed flow for clock jobs (PRG windows, ReportTrigger IMMEDIATE). */
    public Flow clientFlow(String clientToken) {
        return flowForRoute(ArrivalService.ROUTE_FINT_RESP, clientToken, config.payClients());
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
        return java.util.stream.Stream.of(config.namespace(), config.namespaceCol(),
                        config.namespacePay(), config.namespaceMan())
                .distinct()
                .toList();
    }
}
