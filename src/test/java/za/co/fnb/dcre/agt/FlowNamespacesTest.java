package za.co.fnb.dcre.agt;

import org.junit.jupiter.api.Test;
import za.co.fnb.dcre.agt.domain.Flow;
import za.co.fnb.dcre.agt.domain.Stage;
import za.co.fnb.dcre.agt.service.FlowNamespaces;
import za.co.fnb.dcre.agt.service.JobLauncher;

import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SCRUM-70 flow taxonomy (spec 2026-07-21 section 1 item 3): pure resolution
 * tests, no containers, no K8s. Routes classify job families: onhost-req is
 * Collections, onhost-req-endo is Payments, fint-resp follows the reading
 * client's flow (interim pay-clients config map per R-42, R-14 table later).
 */
class FlowNamespacesTest {

    private static final Set<String> PAY_CLIENTS = Set.of("FNBRF01");

    @Test
    void onhostReqResolvesCollections() {
        assertEquals(Flow.COL, FlowNamespaces.flowForRoute("onhost-req", "FNBCC01", PAY_CLIENTS));
        assertEquals(Flow.COL, FlowNamespaces.flowForRoute("onhost-req", "FNBRF01", PAY_CLIENTS),
                "request route wins over client membership: onhost-req is always Collections");
    }

    @Test
    void onhostReqEndoResolvesPayments() {
        assertEquals(Flow.PAY, FlowNamespaces.flowForRoute("onhost-req-endo", "FNBCC01", PAY_CLIENTS),
                "SCRUM-69/70: ENDO is Payments regardless of the client token");
    }

    @Test
    void fintRespFollowsTheReadingClientsFlow() {
        assertEquals(Flow.PAY, FlowNamespaces.flowForRoute("fint-resp", "FNBRF01", PAY_CLIENTS));
        assertEquals(Flow.COL, FlowNamespaces.flowForRoute("fint-resp", "FNBCC01", PAY_CLIENTS));
        assertEquals(Flow.COL, FlowNamespaces.flowForRoute("fint-resp", null, PAY_CLIENTS),
                "a token-less response arrival falls back to collections-primary");
    }

    @Test
    void unknownRouteFallsBackToCollections() {
        // Mirrors DagEngine's DC fallback for unknown routes: collections-primary.
        assertEquals(Flow.COL, FlowNamespaces.flowForRoute("mystery-route", "FNBRF01", PAY_CLIENTS));
    }

    @Test
    void payClientTokenMatchingIsTrimmedAndCaseInsensitive() {
        // m4: membership must not depend on config/token whitespace or case.
        assertEquals(Flow.PAY, FlowNamespaces.flowForRoute("fint-resp", " fnbrf01 ", PAY_CLIENTS));
        assertEquals(Flow.COL, FlowNamespaces.flowForRoute("fint-resp", " fnbcc01 ", PAY_CLIENTS));
    }

    @Test
    void jobPrefixesReplaceTheDcreLiteral() {
        assertEquals("col-", Flow.COL.jobPrefix());
        assertEquals("pay-", Flow.PAY.jobPrefix());
        assertEquals("man-", Flow.MAN.jobPrefix());
    }

    @Test
    void arrivalJobNameCarriesTheFlowPrefixAndStaysDns1123Safe() {
        UUID arrivalId = UUID.fromString("6a1f0a8e-0000-4000-8000-000000000042");
        String hex = arrivalId.toString().replace("-", "");
        assertEquals("pay-crr-" + hex, JobLauncher.jobName(Flow.PAY, Stage.CRR, arrivalId));
        assertEquals("col-cir-" + hex, JobLauncher.jobName(Flow.COL, Stage.CIR, arrivalId));
        String name = JobLauncher.jobName(Flow.MAN, Stage.CRR, arrivalId);
        assertTrue(name.length() <= 63, "K8s name limit: " + name + " (" + name.length() + ")");
        assertTrue(name.matches("[a-z0-9]([-a-z0-9]*[a-z0-9])?"), "DNS-1123: " + name);
    }
}
