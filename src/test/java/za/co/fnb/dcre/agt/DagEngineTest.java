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
    void partialSuppressesCdeAndCrwFailClosed() {
        // A-16 fail-closed default (Fugu F12): partial acceptance never debits.
        Set<Stage> launches = DagEngine.computeLaunches(
                Map.of(Stage.CRR, Outcome.BUSINESS_ACCEPTED, Stage.CTV, Outcome.BUSINESS_PARTIAL),
                EnumSet.of(Stage.CRR, Stage.CTV));
        assertEquals(EnumSet.of(Stage.CIR), launches);
    }

    @Test
    void techFailedResponderBlocksTerminalVerdict() {
        // Fugu F6: a NACK that never left OnHost must keep the arrival open.
        assertTrue(DagEngine.terminalState(Map.of(
                Stage.CRR, Outcome.BUSINESS_ACCEPTED, Stage.CTV, Outcome.BUSINESS_FILE_FATAL,
                Stage.CIR, Outcome.TECH_FAILED)).isEmpty());
    }

    @Test
    void partialCompletesOnceResponderReports() {
        assertEquals(ArrivalStatus.DAG_COMPLETE, DagEngine.terminalState(Map.of(
                Stage.CRR, Outcome.BUSINESS_ACCEPTED, Stage.CTV, Outcome.BUSINESS_PARTIAL,
                Stage.CIR, Outcome.BUSINESS_ACCEPTED)).orElseThrow());
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
    void endoAisAcceptedForksCdeAndCirTogether() {
        Set<Stage> launches = DagEngine.computeLaunches(ArrivalService.ROUTE_ONHOST_REQ_ENDO,
                "FNBRF01_MSG1.txt",
                Map.of(Stage.CRR, Outcome.BUSINESS_ACCEPTED, Stage.CTV, Outcome.BUSINESS_ACCEPTED,
                        Stage.AIS, Outcome.BUSINESS_ACCEPTED),
                EnumSet.of(Stage.CRR, Stage.CTV, Stage.AIS));
        assertEquals(EnumSet.of(Stage.CDE, Stage.CIR), launches);
    }

    @Test
    void endoCompletesWhenBothTerminalsAreBusinessDone() {
        assertEquals(ArrivalStatus.DAG_COMPLETE, DagEngine.terminalState(ArrivalService.ROUTE_ONHOST_REQ_ENDO,
                Map.of(Stage.CRR, Outcome.BUSINESS_ACCEPTED, Stage.CTV, Outcome.BUSINESS_ACCEPTED,
                        Stage.AIS, Outcome.BUSINESS_ACCEPTED, Stage.CDE, Outcome.BUSINESS_ACCEPTED,
                        Stage.CIR, Outcome.BUSINESS_ACCEPTED)).orElseThrow());
        assertTrue(DagEngine.terminalState(ArrivalService.ROUTE_ONHOST_REQ_ENDO,
                        Map.of(Stage.CRR, Outcome.BUSINESS_ACCEPTED, Stage.CTV, Outcome.BUSINESS_ACCEPTED,
                                Stage.AIS, Outcome.BUSINESS_ACCEPTED)).isEmpty(),
                "mid-flight ENDO arrival has no terminal state");
    }

    @Test
    void endoPartialRoutesToCirOnlySkippingAisAndCde() {
        // Same A-16 fail-closed rule as DC (F12): partial acceptance never debits.
        Set<Stage> launches = DagEngine.computeLaunches(ArrivalService.ROUTE_ONHOST_REQ_ENDO,
                "FNBRF01_MSG1.txt",
                Map.of(Stage.CRR, Outcome.BUSINESS_ACCEPTED, Stage.CTV, Outcome.BUSINESS_PARTIAL),
                EnumSet.of(Stage.CRR, Stage.CTV));
        assertEquals(EnumSet.of(Stage.CIR), launches, "ENDO partial: CIR only, never AIS/CDE");
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
