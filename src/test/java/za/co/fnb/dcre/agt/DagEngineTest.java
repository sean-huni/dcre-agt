package za.co.fnb.dcre.agt;

import org.junit.jupiter.api.Test;
import za.co.fnb.dcre.agt.domain.ArrivalStatus;
import za.co.fnb.dcre.agt.domain.Outcome;
import za.co.fnb.dcre.agt.domain.Stage;
import za.co.fnb.dcre.agt.service.ArrivalService;
import za.co.fnb.dcre.agt.service.DagEngine;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Pure decision-logic tests: no containers, no K8s. */
class DagEngineTest {

    @Test
    void crrAcceptedLaunchesCtv() {
        Set<Stage> launches = DagEngine.computeLaunches(
                Map.of(Stage.CRR, Outcome.BUSINESS_ACCEPTED),
                EnumSet.of(Stage.CRR));
        assertEquals(EnumSet.of(Stage.CTV), launches);
    }

    @Test
    void ctvAcceptedForksCdeAndCirTogether() {
        Set<Stage> launches = DagEngine.computeLaunches(
                Map.of(Stage.CRR, Outcome.BUSINESS_ACCEPTED, Stage.CTV, Outcome.BUSINESS_ACCEPTED),
                EnumSet.of(Stage.CRR, Stage.CTV));
        assertEquals(EnumSet.of(Stage.CDE, Stage.CIR), launches);
    }

    @Test
    void partialForksCdeAndCirLikeAccepted() {
        // R-41: acceptance mode moved into CTV; PARTIAL here means ACK-with-partials,
        // so PASS rows continue (old A-16 suppression retired).
        Set<Stage> launches = DagEngine.computeLaunches(
                Map.of(Stage.CRR, Outcome.BUSINESS_ACCEPTED, Stage.CTV, Outcome.BUSINESS_PARTIAL),
                EnumSet.of(Stage.CRR, Stage.CTV));
        assertEquals(EnumSet.of(Stage.CDE, Stage.CIR), launches);
    }

    @Test
    void fileRejectedRoutesToCirOnly() {
        Map<Stage, Outcome> outcomes = Map.of(Stage.CTV, Outcome.BUSINESS_FILE_REJECTED);
        Set<Stage> launches = DagEngine.computeLaunches(outcomes, Set.of());
        assertEquals(Set.of(Stage.CIR), launches);
    }

    @Test
    void partialNowContinuesPassRows() {
        // R-41: PARTIAL means ACK-with-partials; CDE must launch alongside CIR
        Map<Stage, Outcome> outcomes = Map.of(Stage.CTV, Outcome.BUSINESS_PARTIAL);
        Set<Stage> launches = DagEngine.computeLaunches(outcomes, Set.of());
        assertEquals(Set.of(Stage.CDE, Stage.CIR), launches);
    }

    @Test
    void seamLiteralMapsToEnum() {
        // OutcomeWatcher maps seam text via Outcome.valueOf: the CTV exit string
        // BUSINESS_FILE_REJECTED must resolve, never hit the arbiter clause.
        assertEquals(Outcome.BUSINESS_FILE_REJECTED, Outcome.valueOf("BUSINESS_FILE_REJECTED"));
    }

    @Test
    void techFailedResponderBlocksTerminalVerdict() {
        // Fugu F6: a NACK that never left OnHost must keep the arrival open.
        assertTrue(DagEngine.terminalState(Map.of(
                Stage.CRR, Outcome.BUSINESS_ACCEPTED, Stage.CTV, Outcome.BUSINESS_FILE_FATAL,
                Stage.CIR, Outcome.TECH_FAILED)).isEmpty());
    }

    @Test
    void partialCompletesOnlyWhenAllTerminalsReport() {
        // R-41: PARTIAL continues PASS rows, so CDE is in flight; CIR alone no
        // longer completes the arrival.
        assertTrue(DagEngine.terminalState(Map.of(
                Stage.CRR, Outcome.BUSINESS_ACCEPTED, Stage.CTV, Outcome.BUSINESS_PARTIAL,
                Stage.CIR, Outcome.BUSINESS_ACCEPTED)).isEmpty());
        assertEquals(ArrivalStatus.DAG_COMPLETE, DagEngine.terminalState(Map.of(
                Stage.CRR, Outcome.BUSINESS_ACCEPTED, Stage.CTV, Outcome.BUSINESS_PARTIAL,
                Stage.CDE, Outcome.BUSINESS_ACCEPTED,
                Stage.CIR, Outcome.BUSINESS_ACCEPTED)).orElseThrow());
    }

    @Test
    void fileRejectedTerminalMirrorsFatal() {
        // R-41: whole-file policy rejection terminates like FILE_FATAL once the
        // responder has reported; a tech-failed responder keeps the arrival open.
        assertEquals(ArrivalStatus.DAG_FAILED, DagEngine.terminalState(Map.of(
                Stage.CRR, Outcome.BUSINESS_ACCEPTED, Stage.CTV, Outcome.BUSINESS_FILE_REJECTED,
                Stage.CIR, Outcome.BUSINESS_ACCEPTED)).orElseThrow());
        assertTrue(DagEngine.terminalState(Map.of(
                Stage.CRR, Outcome.BUSINESS_ACCEPTED, Stage.CTV, Outcome.BUSINESS_FILE_REJECTED,
                Stage.CIR, Outcome.TECH_FAILED)).isEmpty());
        assertTrue(DagEngine.terminalState(Map.of(
                        Stage.CRR, Outcome.BUSINESS_ACCEPTED, Stage.CTV, Outcome.BUSINESS_FILE_REJECTED))
                        .isEmpty(),
                "mid-flight rejection (no CIR row yet) has no terminal state");
    }

    @Test
    void fileFatalRoutesToCirOnly() {
        Set<Stage> launches = DagEngine.computeLaunches(
                Map.of(Stage.CRR, Outcome.BUSINESS_ACCEPTED, Stage.CTV, Outcome.BUSINESS_FILE_FATAL),
                EnumSet.of(Stage.CRR, Stage.CTV));
        assertEquals(EnumSet.of(Stage.CIR), launches, "whole-file NACK: CIR only, never CDE/CRW");
    }

    @Test
    void techFailureLaunchesNothing() {
        Set<Stage> launches = DagEngine.computeLaunches(
                Map.of(Stage.CRR, Outcome.TECH_FAILED),
                EnumSet.of(Stage.CRR));
        assertTrue(launches.isEmpty(), "process death is not a business verdict (R-33)");
    }

    @Test
    void alreadyIntendedStagesAreNeverRelaunched() {
        Set<Stage> launches = DagEngine.computeLaunches(
                Map.of(Stage.CTV, Outcome.BUSINESS_ACCEPTED),
                EnumSet.of(Stage.CRR, Stage.CTV, Stage.CDE, Stage.CIR));
        assertTrue(launches.isEmpty(), "non-overlap: existing intents suppress relaunch");
    }

    @Test
    void fintRespTokenPicksSingleReaderStage() {
        assertEquals(Stage.IXR, DagEngine.fintRespStage("FNBRF01_ISR_20260712.txt").orElseThrow());
        assertEquals(Stage.SXR, DagEngine.fintRespStage("FNBRF01_SBSR_20260712.txt").orElseThrow());
        assertEquals(Stage.PXR, DagEngine.fintRespStage("FNBRF01_PBSR_20260712.txt").orElseThrow());
        assertTrue(DagEngine.fintRespStage("FNBRF01_XXXX_20260712.txt").isEmpty(),
                "unknown token fails closed");
    }

    @Test
    void fintRespLaunchesOnlyTheTokenStage() {
        Set<Stage> launches = DagEngine.computeLaunches(ArrivalService.ROUTE_FINT_RESP,
                "FNBRF01_PBSR_20260712.txt", Map.of(), EnumSet.noneOf(Stage.class));
        assertEquals(EnumSet.of(Stage.PXR), launches);
        assertTrue(DagEngine.computeLaunches(ArrivalService.ROUTE_FINT_RESP,
                        "FNBRF01_PBSR_20260712.txt", Map.of(), EnumSet.of(Stage.PXR)).isEmpty(),
                "non-overlap: existing intent suppresses relaunch");
    }

    @Test
    void fintRespCompletesOnReaderAcceptanceOnly() {
        assertEquals(ArrivalStatus.DAG_COMPLETE, DagEngine.terminalState(ArrivalService.ROUTE_FINT_RESP,
                Map.of(Stage.PXR, Outcome.BUSINESS_ACCEPTED)).orElseThrow());
        assertTrue(DagEngine.terminalState(ArrivalService.ROUTE_FINT_RESP,
                        Map.of(Stage.PXR, Outcome.TECH_FAILED)).isEmpty(),
                "tech failure keeps the arrival open for the reconciler");
    }

    @Test
    void onhostRouteDispatchKeepsTheStaticDag() {
        assertEquals(EnumSet.of(Stage.CTV), DagEngine.computeLaunches(ArrivalService.ROUTE_ONHOST_REQ,
                "FNBRF01_MSG1.txt", Map.of(Stage.CRR, Outcome.BUSINESS_ACCEPTED), EnumSet.of(Stage.CRR)));
    }

    @Test
    void endoCrrAcceptedLaunchesCtv() {
        Set<Stage> launches = DagEngine.computeLaunches(ArrivalService.ROUTE_ONHOST_REQ_ENDO,
                "FNBRF01_MSG1.txt", Map.of(Stage.CRR, Outcome.BUSINESS_ACCEPTED), EnumSet.of(Stage.CRR));
        assertEquals(EnumSet.of(Stage.CTV), launches);
    }

    @Test
    void endoCtvAcceptedLaunchesAisOnly() {
        Set<Stage> launches = DagEngine.computeLaunches(ArrivalService.ROUTE_ONHOST_REQ_ENDO,
                "FNBRF01_MSG1.txt",
                Map.of(Stage.CRR, Outcome.BUSINESS_ACCEPTED, Stage.CTV, Outcome.BUSINESS_ACCEPTED),
                EnumSet.of(Stage.CRR, Stage.CTV));
        assertEquals(EnumSet.of(Stage.AIS), launches, "ENDO inserts AIS between CTV and the fork");
    }

    @Test
    void endoAisAcceptedLaunchesCirOnly() {
        // SCRUM-69: ENDO = Payments, immediate; CDE never runs on the pay flow
        // (CRW picks pay rows up by tx_header.flow from ingest day).
        Set<Stage> launches = DagEngine.computeLaunches(ArrivalService.ROUTE_ONHOST_REQ_ENDO,
                "FNBRF01_MSG1.txt",
                Map.of(Stage.CRR, Outcome.BUSINESS_ACCEPTED, Stage.CTV, Outcome.BUSINESS_ACCEPTED,
                        Stage.AIS, Outcome.BUSINESS_ACCEPTED),
                EnumSet.of(Stage.CRR, Stage.CTV, Stage.AIS));
        assertEquals(EnumSet.of(Stage.CIR), launches, "pay flow: AIS acceptance launches CIR only, never CDE");
    }

    @Test
    void endoCompletesOnCirAlone() {
        // SCRUM-69 terminal set is {CIR}: the responder's acceptance completes
        // the pay-flow DAG without any CDE outcome.
        assertEquals(ArrivalStatus.DAG_COMPLETE, DagEngine.terminalState(ArrivalService.ROUTE_ONHOST_REQ_ENDO,
                Map.of(Stage.CRR, Outcome.BUSINESS_ACCEPTED, Stage.CTV, Outcome.BUSINESS_ACCEPTED,
                        Stage.AIS, Outcome.BUSINESS_ACCEPTED,
                        Stage.CIR, Outcome.BUSINESS_ACCEPTED)).orElseThrow());
        assertTrue(DagEngine.terminalState(ArrivalService.ROUTE_ONHOST_REQ_ENDO,
                        Map.of(Stage.CRR, Outcome.BUSINESS_ACCEPTED, Stage.CTV, Outcome.BUSINESS_ACCEPTED,
                                Stage.AIS, Outcome.BUSINESS_ACCEPTED)).isEmpty(),
                "mid-flight ENDO arrival has no terminal state");
    }

    @Test
    void endoPartialContinuesToAis() {
        // R-41: PARTIAL fans out to successors like ACCEPTED; on ENDO the CTV
        // successor is AIS (old A-16 CIR-only suppression retired).
        Set<Stage> launches = DagEngine.computeLaunches(ArrivalService.ROUTE_ONHOST_REQ_ENDO,
                "FNBRF01_MSG1.txt",
                Map.of(Stage.CRR, Outcome.BUSINESS_ACCEPTED, Stage.CTV, Outcome.BUSINESS_PARTIAL),
                EnumSet.of(Stage.CRR, Stage.CTV));
        assertEquals(EnumSet.of(Stage.AIS), launches, "ENDO partial continues to AIS");
    }

    @Test
    void endoFileRejectedRoutesToCirOnly() {
        Set<Stage> launches = DagEngine.computeLaunches(ArrivalService.ROUTE_ONHOST_REQ_ENDO,
                "FNBRF01_MSG1.txt",
                Map.of(Stage.CRR, Outcome.BUSINESS_ACCEPTED, Stage.CTV, Outcome.BUSINESS_FILE_REJECTED),
                EnumSet.of(Stage.CRR, Stage.CTV));
        assertEquals(EnumSet.of(Stage.CIR), launches, "policy rejection: CIR only, never AIS/CDE");
    }

    @Test
    void terminalStates() {
        assertEquals(ArrivalStatus.DAG_COMPLETE, DagEngine.terminalState(Map.of(
                Stage.CRR, Outcome.BUSINESS_ACCEPTED, Stage.CTV, Outcome.BUSINESS_ACCEPTED,
                Stage.CDE, Outcome.BUSINESS_ACCEPTED, Stage.CRW, Outcome.BUSINESS_ACCEPTED,
                Stage.CIR, Outcome.BUSINESS_ACCEPTED)).orElseThrow());
        assertEquals(ArrivalStatus.DAG_FAILED, DagEngine.terminalState(Map.of(
                Stage.CRR, Outcome.BUSINESS_ACCEPTED, Stage.CTV, Outcome.BUSINESS_FILE_FATAL,
                Stage.CIR, Outcome.BUSINESS_ACCEPTED)).orElseThrow());
        assertTrue(DagEngine.terminalState(Map.of(
                Stage.CRR, Outcome.BUSINESS_ACCEPTED)).isEmpty(), "mid-flight has no terminal state");
    }
}
