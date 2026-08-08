package za.co.fnb.dcre.agt;

import org.junit.jupiter.api.Test;
import za.co.fnb.dcre.agt.domain.ArrivalStatus;
import za.co.fnb.dcre.agt.domain.Flow;
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

    // ---------- collections (DC): CRR -> CTV -> {CDE, CIR} ----------

    @Test
    void crrAcceptedLaunchesCtv() {
        Set<Stage> launches = DagEngine.computeLaunches(ArrivalService.ROUTE_ONHOST_REQ, Flow.COL, "FNBCC01_F.txt",
                Map.of(Stage.CRR, Outcome.BUSINESS_ACCEPTED),
                EnumSet.of(Stage.CRR));
        assertEquals(EnumSet.of(Stage.CTV), launches);
    }

    @Test
    void ctvAcceptedForksCdeAndCirTogether() {
        Set<Stage> launches = DagEngine.computeLaunches(ArrivalService.ROUTE_ONHOST_REQ, Flow.COL, "FNBCC01_F.txt",
                Map.of(Stage.CRR, Outcome.BUSINESS_ACCEPTED, Stage.CTV, Outcome.BUSINESS_ACCEPTED),
                EnumSet.of(Stage.CRR, Stage.CTV));
        assertEquals(EnumSet.of(Stage.CDE, Stage.CIR), launches);
    }

    @Test
    void partialForksCdeAndCirLikeAccepted() {
        // R-41: acceptance mode moved into CTV; PARTIAL here means ACK-with-partials,
        // so PASS rows continue (old A-16 suppression retired).
        Set<Stage> launches = DagEngine.computeLaunches(ArrivalService.ROUTE_ONHOST_REQ, Flow.COL, "FNBCC01_F.txt",
                Map.of(Stage.CRR, Outcome.BUSINESS_ACCEPTED, Stage.CTV, Outcome.BUSINESS_PARTIAL),
                EnumSet.of(Stage.CRR, Stage.CTV));
        assertEquals(EnumSet.of(Stage.CDE, Stage.CIR), launches);
    }

    @Test
    void fileRejectedRoutesToCirOnly() {
        Map<Stage, Outcome> outcomes = Map.of(Stage.CTV, Outcome.BUSINESS_FILE_REJECTED);
        Set<Stage> launches = DagEngine.computeLaunches(ArrivalService.ROUTE_ONHOST_REQ, Flow.COL,
                "FNBCC01_F.txt", outcomes, Set.of());
        assertEquals(Set.of(Stage.CIR), launches);
    }

    @Test
    void partialNowContinuesPassRows() {
        // R-41: PARTIAL means ACK-with-partials; CDE must launch alongside CIR
        Map<Stage, Outcome> outcomes = Map.of(Stage.CTV, Outcome.BUSINESS_PARTIAL);
        Set<Stage> launches = DagEngine.computeLaunches(ArrivalService.ROUTE_ONHOST_REQ, Flow.COL,
                "FNBCC01_F.txt", outcomes, Set.of());
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
        assertTrue(DagEngine.terminalState(ArrivalService.ROUTE_ONHOST_REQ, Flow.COL, Map.of(
                Stage.CRR, Outcome.BUSINESS_ACCEPTED, Stage.CTV, Outcome.BUSINESS_FILE_FATAL,
                Stage.CIR, Outcome.TECH_FAILED), () -> false).isEmpty());
    }

    @Test
    void partialCompletesOnlyWhenAllTerminalsReport() {
        // R-41: PARTIAL continues PASS rows, so CDE is in flight; CIR alone no
        // longer completes the arrival.
        assertTrue(DagEngine.terminalState(ArrivalService.ROUTE_ONHOST_REQ, Flow.COL, Map.of(
                Stage.CRR, Outcome.BUSINESS_ACCEPTED, Stage.CTV, Outcome.BUSINESS_PARTIAL,
                Stage.CIR, Outcome.BUSINESS_ACCEPTED), () -> false).isEmpty());
        assertEquals(ArrivalStatus.DAG_COMPLETE, DagEngine.terminalState(ArrivalService.ROUTE_ONHOST_REQ, Flow.COL, Map.of(
                Stage.CRR, Outcome.BUSINESS_ACCEPTED, Stage.CTV, Outcome.BUSINESS_PARTIAL,
                Stage.CDE, Outcome.BUSINESS_ACCEPTED,
                Stage.CIR, Outcome.BUSINESS_ACCEPTED), () -> false).orElseThrow());
    }

    @Test
    void fileRejectedTerminalMirrorsFatal() {
        // R-41: whole-file policy rejection terminates like FILE_FATAL once the
        // responder has reported; a tech-failed responder keeps the arrival open.
        assertEquals(ArrivalStatus.DAG_FAILED, DagEngine.terminalState(ArrivalService.ROUTE_ONHOST_REQ, Flow.COL, Map.of(
                Stage.CRR, Outcome.BUSINESS_ACCEPTED, Stage.CTV, Outcome.BUSINESS_FILE_REJECTED,
                Stage.CIR, Outcome.BUSINESS_ACCEPTED), () -> false).orElseThrow());
        assertTrue(DagEngine.terminalState(ArrivalService.ROUTE_ONHOST_REQ, Flow.COL, Map.of(
                Stage.CRR, Outcome.BUSINESS_ACCEPTED, Stage.CTV, Outcome.BUSINESS_FILE_REJECTED,
                Stage.CIR, Outcome.TECH_FAILED), () -> false).isEmpty());
        assertTrue(DagEngine.terminalState(ArrivalService.ROUTE_ONHOST_REQ, Flow.COL, Map.of(
                        Stage.CRR, Outcome.BUSINESS_ACCEPTED, Stage.CTV, Outcome.BUSINESS_FILE_REJECTED), () -> false)
                        .isEmpty(),
                "mid-flight rejection (no CIR row yet) has no terminal state");
    }

    @Test
    void fileFatalRoutesToCirOnly() {
        Set<Stage> launches = DagEngine.computeLaunches(ArrivalService.ROUTE_ONHOST_REQ, Flow.COL, "FNBCC01_F.txt",
                Map.of(Stage.CRR, Outcome.BUSINESS_ACCEPTED, Stage.CTV, Outcome.BUSINESS_FILE_FATAL),
                EnumSet.of(Stage.CRR, Stage.CTV));
        assertEquals(EnumSet.of(Stage.CIR), launches, "whole-file NACK: CIR only, never CDE/CRW");
    }

    @Test
    void techFailureLaunchesNothing() {
        Set<Stage> launches = DagEngine.computeLaunches(ArrivalService.ROUTE_ONHOST_REQ, Flow.COL, "FNBCC01_F.txt",
                Map.of(Stage.CRR, Outcome.TECH_FAILED),
                EnumSet.of(Stage.CRR));
        assertTrue(launches.isEmpty(), "process death is not a business verdict (R-33)");
    }

    @Test
    void alreadyIntendedStagesAreNeverRelaunched() {
        Set<Stage> launches = DagEngine.computeLaunches(ArrivalService.ROUTE_ONHOST_REQ, Flow.COL, "FNBCC01_F.txt",
                Map.of(Stage.CTV, Outcome.BUSINESS_ACCEPTED),
                EnumSet.of(Stage.CRR, Stage.CTV, Stage.CDE, Stage.CIR));
        assertTrue(launches.isEmpty(), "non-overlap: existing intents suppress relaunch");
    }

    @Test
    void terminalStates() {
        assertEquals(ArrivalStatus.DAG_COMPLETE, DagEngine.terminalState(ArrivalService.ROUTE_ONHOST_REQ, Flow.COL, Map.of(
                Stage.CRR, Outcome.BUSINESS_ACCEPTED, Stage.CTV, Outcome.BUSINESS_ACCEPTED,
                Stage.CDE, Outcome.BUSINESS_ACCEPTED, Stage.CRW, Outcome.BUSINESS_ACCEPTED,
                Stage.CIR, Outcome.BUSINESS_ACCEPTED), () -> false).orElseThrow());
        assertEquals(ArrivalStatus.DAG_FAILED, DagEngine.terminalState(ArrivalService.ROUTE_ONHOST_REQ, Flow.COL, Map.of(
                Stage.CRR, Outcome.BUSINESS_ACCEPTED, Stage.CTV, Outcome.BUSINESS_FILE_FATAL,
                Stage.CIR, Outcome.BUSINESS_ACCEPTED), () -> false).orElseThrow());
        assertTrue(DagEngine.terminalState(ArrivalService.ROUTE_ONHOST_REQ, Flow.COL, Map.of(
                Stage.CRR, Outcome.BUSINESS_ACCEPTED), () -> false).isEmpty(), "mid-flight has no terminal state");
    }

    @Test
    void onhostRouteDispatchKeepsTheStaticDag() {
        assertEquals(EnumSet.of(Stage.CTV), DagEngine.computeLaunches(ArrivalService.ROUTE_ONHOST_REQ, Flow.COL,
                "FNBRF01_MSG1.txt", Map.of(Stage.CRR, Outcome.BUSINESS_ACCEPTED), EnumSet.of(Stage.CRR)));
    }

    // ---------- payments (ENDO): PRR -> PTV -> PAI -> {PRW, PIR} ----------

    @Test
    void endoStartsAtPrrNotCrr() {
        // The payments sheet's own reader. Before the v1 split this route entered at
        // CRR, so a payments file was ingested by the collections service.
        assertEquals(Stage.PRR, DagEngine.initialStage(ArrivalService.ROUTE_ONHOST_REQ_ENDO,
                Flow.PAY, "FNBRF01_MSG1.txt").orElseThrow());
        assertEquals(Stage.CRR, DagEngine.initialStage(ArrivalService.ROUTE_ONHOST_REQ,
                Flow.COL, "FNBCC01_F.txt").orElseThrow(),
                "collections is unaffected: it keeps its own reader");
    }

    @Test
    void endoPrrAcceptedLaunchesPtv() {
        Set<Stage> launches = DagEngine.computeLaunches(ArrivalService.ROUTE_ONHOST_REQ_ENDO, Flow.PAY,
                "FNBRF01_MSG1.txt", Map.of(Stage.PRR, Outcome.BUSINESS_ACCEPTED), EnumSet.of(Stage.PRR));
        assertEquals(EnumSet.of(Stage.PTV), launches);
    }

    @Test
    void endoPtvAcceptedLaunchesPaiOnly() {
        Set<Stage> launches = DagEngine.computeLaunches(ArrivalService.ROUTE_ONHOST_REQ_ENDO, Flow.PAY,
                "FNBRF01_MSG1.txt",
                Map.of(Stage.PRR, Outcome.BUSINESS_ACCEPTED, Stage.PTV, Outcome.BUSINESS_ACCEPTED),
                EnumSet.of(Stage.PRR, Stage.PTV));
        assertEquals(EnumSet.of(Stage.PAI), launches, "PAI sits between PTV and the fork");
    }

    @Test
    void endoPaiAcceptedForksPrwAndPirTogether() {
        // The payments REQ sheet forks PARALLEL after PAI: PRW writes the Fintegrate
        // request and PIR answers OnHost. Both, at once, immediately.
        Set<Stage> launches = DagEngine.computeLaunches(ArrivalService.ROUTE_ONHOST_REQ_ENDO, Flow.PAY,
                "FNBRF01_MSG1.txt",
                Map.of(Stage.PRR, Outcome.BUSINESS_ACCEPTED, Stage.PTV, Outcome.BUSINESS_ACCEPTED,
                        Stage.PAI, Outcome.BUSINESS_ACCEPTED),
                EnumSet.of(Stage.PRR, Stage.PTV, Stage.PAI));
        assertEquals(EnumSet.of(Stage.PRW, Stage.PIR), launches);
    }

    @Test
    void endoNeverLaunchesAnyCollectionsStage() {
        // The whole point of the split. Walk the payments DAG from every acceptance
        // point and assert nothing collections-side is ever picked, which is the
        // failure the pre-split shape produced silently.
        final Set<Stage> collections = EnumSet.of(Stage.CRR, Stage.CTV, Stage.CDE, Stage.CRW, Stage.CIR,
                Stage.CIX, Stage.CSX, Stage.CPX, Stage.CRG);
        final Map<Stage, Outcome> accepted = Map.of(
                Stage.PRR, Outcome.BUSINESS_ACCEPTED, Stage.PTV, Outcome.BUSINESS_ACCEPTED,
                Stage.PAI, Outcome.BUSINESS_ACCEPTED);
        for (final Stage done : accepted.keySet()) {
            final Set<Stage> launches = DagEngine.computeLaunches(ArrivalService.ROUTE_ONHOST_REQ_ENDO, Flow.PAY,
                    "FNBRF01_MSG1.txt", Map.of(done, Outcome.BUSINESS_ACCEPTED), EnumSet.noneOf(Stage.class));
            launches.forEach(s -> assertTrue(!collections.contains(s),
                    "payments DAG launched the collections stage " + s + " after " + done));
        }
    }

    @Test
    void endoPartialContinuesToPai() {
        // R-41: PARTIAL fans out to successors like ACCEPTED.
        Set<Stage> launches = DagEngine.computeLaunches(ArrivalService.ROUTE_ONHOST_REQ_ENDO, Flow.PAY,
                "FNBRF01_MSG1.txt",
                Map.of(Stage.PRR, Outcome.BUSINESS_ACCEPTED, Stage.PTV, Outcome.BUSINESS_PARTIAL),
                EnumSet.of(Stage.PRR, Stage.PTV));
        assertEquals(EnumSet.of(Stage.PAI), launches, "ENDO partial continues to PAI");
    }

    @Test
    void endoFileRejectedRoutesToPirOnly() {
        Set<Stage> launches = DagEngine.computeLaunches(ArrivalService.ROUTE_ONHOST_REQ_ENDO, Flow.PAY,
                "FNBRF01_MSG1.txt",
                Map.of(Stage.PRR, Outcome.BUSINESS_ACCEPTED, Stage.PTV, Outcome.BUSINESS_FILE_REJECTED),
                EnumSet.of(Stage.PRR, Stage.PTV));
        assertEquals(EnumSet.of(Stage.PIR), launches, "policy rejection: the payments responder only");
    }

    @Test
    void endoCompletesWhenBothForkArmsReportAndNeverWaitsForAnEmission() {
        // THE TIMING RULE. Payments transactions are processed IMMEDIATELY, so a
        // payments arrival is complete when PRW and PIR are done and nothing else.
        // The emissionOwed supplier below returns TRUE, which would keep a
        // collections arrival open forever; if payments ever inherited
        // Emission.REQUIRED this assertion goes red.
        assertEquals(ArrivalStatus.DAG_COMPLETE,
                DagEngine.terminalState(ArrivalService.ROUTE_ONHOST_REQ_ENDO, Flow.PAY, Map.of(
                        Stage.PRR, Outcome.BUSINESS_ACCEPTED, Stage.PTV, Outcome.BUSINESS_ACCEPTED,
                        Stage.PAI, Outcome.BUSINESS_ACCEPTED, Stage.PRW, Outcome.BUSINESS_ACCEPTED,
                        Stage.PIR, Outcome.BUSINESS_ACCEPTED), () -> true).orElseThrow(),
                "a payments arrival must never wait on a CRW emission: that is the collection-day"
                        + " wait, and payments does not have one");
        assertTrue(DagEngine.terminalState(ArrivalService.ROUTE_ONHOST_REQ_ENDO, Flow.PAY, Map.of(
                        Stage.PRR, Outcome.BUSINESS_ACCEPTED, Stage.PTV, Outcome.BUSINESS_ACCEPTED,
                        Stage.PAI, Outcome.BUSINESS_ACCEPTED,
                        Stage.PIR, Outcome.BUSINESS_ACCEPTED), () -> false).isEmpty(),
                "PIR alone does not complete it: PRW is a real DAG stage on this sheet");
    }

    @Test
    void collectionsStillWaitsForItsEmission() {
        // The other half of the same rule, so the payments assertion above cannot
        // pass by the gate having been removed for everyone.
        assertTrue(DagEngine.terminalState(ArrivalService.ROUTE_ONHOST_REQ, Flow.COL, Map.of(
                        Stage.CRR, Outcome.BUSINESS_ACCEPTED, Stage.CTV, Outcome.BUSINESS_ACCEPTED,
                        Stage.CDE, Outcome.BUSINESS_ACCEPTED,
                        Stage.CIR, Outcome.BUSINESS_ACCEPTED), () -> true).isEmpty(),
                "collections stays DAG_RUNNING while CRW still owes an emission");
    }

    // ---------- response routes: the leg reader follows the FLOW ----------

    @Test
    void fintRespTokenPicksSingleReaderStagePerFamily() {
        assertEquals(Stage.CIX, DagEngine.fintRespStage(ArrivalService.ROUTE_FINT_RESP, Flow.COL,
                "FNBCC01_ISR_20260712.txt").orElseThrow());
        assertEquals(Stage.CSX, DagEngine.fintRespStage(ArrivalService.ROUTE_FINT_RESP, Flow.COL,
                "FNBCC01_SBSR_20260712.txt").orElseThrow());
        assertEquals(Stage.CPX, DagEngine.fintRespStage(ArrivalService.ROUTE_FINT_RESP, Flow.COL,
                "FNBCC01_PBSR_20260712.txt").orElseThrow());

        // Same channel, same filename shape, PAY client: the payments readers.
        assertEquals(Stage.PIX, DagEngine.fintRespStage(ArrivalService.ROUTE_FINT_RESP, Flow.PAY,
                "FNBRF01_ISR_20260712.txt").orElseThrow());
        assertEquals(Stage.PSX, DagEngine.fintRespStage(ArrivalService.ROUTE_FINT_RESP, Flow.PAY,
                "FNBRF01_SBSR_20260712.txt").orElseThrow());
        assertEquals(Stage.PPX, DagEngine.fintRespStage(ArrivalService.ROUTE_FINT_RESP, Flow.PAY,
                "FNBRF01_PBSR_20260712.txt").orElseThrow());

        assertEquals(Stage.MIX, DagEngine.fintRespStage(ArrivalService.ROUTE_FINT_RESP_MAN, Flow.MAN,
                "FNBCC01_ISR_20260712.txt").orElseThrow());

        assertTrue(DagEngine.fintRespStage(ArrivalService.ROUTE_FINT_RESP, Flow.COL,
                        "FNBRF01_XXXX_20260712.txt").isEmpty(),
                "unknown token fails closed");
    }

    @Test
    void fintRespLaunchesOnlyTheTokenStage() {
        Set<Stage> launches = DagEngine.computeLaunches(ArrivalService.ROUTE_FINT_RESP, Flow.COL,
                "FNBCC01_PBSR_20260712.txt", Map.of(), EnumSet.noneOf(Stage.class));
        assertEquals(EnumSet.of(Stage.CPX), launches);
        assertTrue(DagEngine.computeLaunches(ArrivalService.ROUTE_FINT_RESP, Flow.COL,
                        "FNBCC01_PBSR_20260712.txt", Map.of(), EnumSet.of(Stage.CPX)).isEmpty(),
                "non-overlap: existing intent suppresses relaunch");
        assertEquals(EnumSet.of(Stage.PPX), DagEngine.computeLaunches(ArrivalService.ROUTE_FINT_RESP, Flow.PAY,
                        "FNBRF01_PBSR_20260712.txt", Map.of(), EnumSet.noneOf(Stage.class)),
                "the same reply on the pay flow launches the payments reader");
    }

    @Test
    void fintRespCompletesOnReaderAcceptanceOnly() {
        assertEquals(ArrivalStatus.DAG_COMPLETE, DagEngine.terminalState(ArrivalService.ROUTE_FINT_RESP,
                Flow.COL, Map.of(Stage.CPX, Outcome.BUSINESS_ACCEPTED), () -> false).orElseThrow());
        assertTrue(DagEngine.terminalState(ArrivalService.ROUTE_FINT_RESP, Flow.COL,
                        Map.of(Stage.CPX, Outcome.TECH_FAILED), () -> false).isEmpty(),
                "tech failure keeps the arrival open for the reconciler");
        assertEquals(ArrivalStatus.DAG_COMPLETE, DagEngine.terminalState(ArrivalService.ROUTE_FINT_RESP,
                Flow.PAY, Map.of(Stage.PPX, Outcome.BUSINESS_ACCEPTED), () -> false).orElseThrow());
    }

    @Test
    void aCollectionsReaderNeverCompletesAPaymentsResponseArrival() {
        // The any-of terminal test must be scoped to the family's OWN readers. A
        // union of all six would let a collections outcome close a payments arrival.
        assertTrue(DagEngine.terminalState(ArrivalService.ROUTE_FINT_RESP, Flow.PAY,
                        Map.of(Stage.CPX, Outcome.BUSINESS_ACCEPTED), () -> false).isEmpty(),
                "a CPX outcome must not complete a payments fint-resp arrival");
        assertTrue(DagEngine.terminalState(ArrivalService.ROUTE_FINT_RESP, Flow.COL,
                        Map.of(Stage.PPX, Outcome.BUSINESS_ACCEPTED), () -> false).isEmpty(),
                "and the converse");
    }
}
