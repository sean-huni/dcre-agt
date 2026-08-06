package za.co.fnb.dcre.agt.service;

import org.junit.jupiter.api.Test;
import za.co.fnb.dcre.agt.domain.Outcome;
import za.co.fnb.dcre.agt.domain.Stage;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SCRUM-107: "which routes exist and what they do" was encoded in FOUR
 * hand-maintained places with nothing tying them together:
 *
 * <ul>
 *   <li>{@code DirectoryWatcher.INBOUND} - which routes can ARRIVE
 *   <li>{@code RouteDags.REQUESTS} / {@code RESPONSES} - which routes have a DAG
 *   <li>{@code DagEngine.isRespRoute} - which routes are responses
 *   <li>{@code DagEngine.initialStage} - the first stage, as a ternary
 * </ul>
 *
 * <p>They agreed, so there was no live defect. What was missing was the control:
 * nothing failed when they stopped agreeing, and {@code getOrDefault(route, DC)}
 * turned drift into SILENT MISROUTING rather than a failure. A route added to
 * INBOUND but not to REQUESTS ran the COLLECTIONS DAG: CRR ingesting somebody
 * else's file into dcre_col, arrival reaching DAG_COMPLETE, nothing logged.
 *
 * <p>Verified against the running orchestrator on 2026-08-06 before this guard
 * existed: a {@code file_arrival} row on route {@code totally-unknown-route}
 * caused AGT to write a launch_intent for {@code CRR} in namespace
 * {@code dcre-col}.
 *
 * <p>This matters NOW because the payments family split adds routes. Forgetting
 * one registry would silently feed payments files to collections, which is the
 * exact coupling the split exists to remove.
 */
class RouteRegistryConsistencyTest {

    /** Every route the watcher can produce must resolve to a real DAG. */
    @Test
    void everyInboundRouteHasItsOwnDagEntry() {
        for (String route : DirectoryWatcher.inboundRoutes()) {
            assertTrue(RouteDags.REQUESTS.containsKey(route) || RouteDags.RESPONSES.containsKey(route),
                    "route '" + route + "' can arrive (DirectoryWatcher.INBOUND) but has no RouteDags"
                            + " entry, so it would silently fall back to the collections DAG");
        }
    }

    /** ...and the reverse, so a retired route cannot linger with a live DAG. */
    @Test
    void everyRoutedDagIsReachableFromAnInboundChannel() {
        final Set<String> inbound = Set.copyOf(DirectoryWatcher.inboundRoutes());
        for (String route : RouteDags.REQUESTS.keySet()) {
            assertTrue(inbound.contains(route),
                    "request route '" + route + "' has a DAG but no inbound channel");
        }
        for (String route : RouteDags.RESPONSES.keySet()) {
            assertTrue(inbound.contains(route),
                    "response route '" + route + "' has a DAG but no inbound channel");
        }
    }

    /**
     * An unrecognised route must FAIL, never quietly run collections. DagEngine.advance
     * catches per arrival (F10), so a throw isolates the poisoned arrival and surfaces
     * it in the log instead of misrouting it.
     */
    @Test
    void anUnknownRouteFailsClosedInsteadOfRunningCollections() {
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> DagEngine.computeLaunches("totally-unknown-route", "FNBRF01_MSG.txt",
                        Map.of(), EnumSet.noneOf(Stage.class)),
                "an unknown route must fail closed, not default to the collections DAG");
        assertTrue(e.getMessage().contains("totally-unknown-route"),
                "the message must name the offending route; got: " + e.getMessage());
    }

    /** The same fallback existed on the terminal-verdict path; both sites must fail closed. */
    @Test
    void anUnknownRouteAlsoFailsClosedOnTheTerminalVerdictPath() {
        assertThrows(IllegalArgumentException.class,
                () -> DagEngine.terminalState("totally-unknown-route",
                        Map.of(Stage.CRR, Outcome.BUSINESS_ACCEPTED), true),
                "terminalState must not judge an unknown route by the collections shape");
    }

    /**
     * The site that actually fires FIRST. computeLaunches only picks SUCCESSORS;
     * a CLAIMED arrival's first launch comes from initialStage, whose request
     * branch was a ternary reading "onhost-req-man ? MRR : CRR" - so every
     * unknown route started at CRR. Its javadoc claimed "empty = quarantine
     * (fail closed)", which the request branch never did.
     *
     * <p>Caught only by re-running the live probe after the first fix: the unit
     * test was green and the orchestrator still wrote a CRR intent.
     */
    @Test
    void anUnknownRouteHasNoInitialStageInsteadOfDefaultingToCrr() {
        assertTrue(DagEngine.initialStage("totally-unknown-route", "FNBRF01_MSG.txt").isEmpty(),
                "initialStage must not start an unknown route at CRR");
    }

    /**
     * EMPTY, not thrown. The caller quarantines on empty: one WARN, one terminal
     * transition. Throwing here made the arrival re-throw on every 2s tick, loud
     * forever and never terminal, which is the per-item exception that wedges a
     * level-triggered loop. Caught by watching the running orchestrator, not by a
     * test: the throwing version passed its unit test.
     */
    @Test
    void anUnknownRouteIsQuarantinableRatherThanRetriedForever() {
        assertDoesNotThrow(() -> DagEngine.initialStage("totally-unknown-route", "FNBRF01_MSG.txt"),
                "initialStage must not throw: the caller's quarantine path needs an empty Optional");
    }

    /**
     * And the namespace picker: `default -> Flow.COL` put the unknown route's Job
     * in dcre-col. onhost-req legitimately relied on that default, so it is now
     * enumerated and only genuinely unknown routes fail.
     */
    @Test
    void anUnknownRouteHasNoFlowInsteadOfDefaultingToCollections() {
        assertThrows(IllegalArgumentException.class,
                () -> FlowNamespaces.flowForRoute("totally-unknown-route", "FNBRF01", Set.of()),
                "an unknown route must not resolve to the collections namespace");
        assertThrows(IllegalArgumentException.class,
                () -> FlowNamespaces.flowForRoute(null, "FNBRF01", Set.of()),
                "a null route must not resolve to the collections namespace either");
    }

    /** Every inbound REQUEST route still resolves a first stage and a flow. */
    @Test
    void everyInboundRequestRouteResolvesAnInitialStageAndFlow() {
        for (String route : DirectoryWatcher.inboundRoutes()) {
            // EVERY inbound route needs a flow: JobLauncher.launch resolves one for
            // response routes too, and an unmapped one throws there and leaves the
            // arrival CLAIMED forever. This used to sit below the continue and so
            // covered only 3 of the 5 inbound routes.
            assertDoesNotThrow(() -> FlowNamespaces.flowForRoute(route, "FNBRF01", Set.of()),
                    "inbound route '" + route + "' must resolve a flow");
            if (!RouteDags.REQUESTS.containsKey(route)) {
                continue; // response routes are token-picked from the filename
            }
            assertTrue(DagEngine.initialStage(route, "FNBRF01_MSG.txt").isPresent(),
                    "request route '" + route + "' must resolve a first stage");
        }
    }

    /** A known route is unaffected: the guard must not change live behaviour. */
    @Test
    void knownRoutesStillResolveExactlyAsBefore() {
        assertEquals(EnumSet.of(Stage.CTV),
                DagEngine.computeLaunches("onhost-req", "FNBCC01_F.txt",
                        Map.of(Stage.CRR, Outcome.BUSINESS_ACCEPTED), EnumSet.noneOf(Stage.class)));
        assertEquals(EnumSet.of(Stage.MRV),
                DagEngine.computeLaunches("onhost-req-man", "FNBRF01_F.txt",
                        Map.of(Stage.MRR, Outcome.BUSINESS_ACCEPTED), EnumSet.noneOf(Stage.class)));
        assertEquals(EnumSet.of(Stage.AIS),
                DagEngine.computeLaunches("onhost-req-endo", "FNBRF01_F.txt",
                        Map.of(Stage.CTV, Outcome.BUSINESS_ACCEPTED), EnumSet.noneOf(Stage.class)));
    }
}
