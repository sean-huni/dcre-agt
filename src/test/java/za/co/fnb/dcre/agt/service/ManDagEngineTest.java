package za.co.fnb.dcre.agt.service;

import org.junit.jupiter.api.Test;
import za.co.fnb.dcre.agt.domain.ArrivalStatus;
import za.co.fnb.dcre.agt.domain.Outcome;
import za.co.fnb.dcre.agt.domain.Stage;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * M10/SCRUM-79 pure decision-logic tests for the mandates routes: the
 * onhost-req-man request DAG (MRR -> MRV -> MAF -> MIS -> fork {MIR, MRW},
 * responder MIR) and the fint-resp-man response DAG (token-picked MAR -> MSR,
 * no responder: failures stay open for the reconciler). No containers, no K8s.
 */
class ManDagEngineTest {

    private static final String MAN_REQ = ArrivalService.ROUTE_ONHOST_REQ_MAN;
    private static final String MAN_RESP = ArrivalService.ROUTE_FINT_RESP_MAN;
    private static final String BOOK = "FNBCC01_MANB1.txt";

    // --- entry stages -----------------------------------------------------

    @Test
    void manRequestRouteEntersAtMrr() {
        assertEquals(Stage.MRR, DagEngine.initialStage(MAN_REQ, BOOK).orElseThrow(),
                "mandate instruction books enter at MRR, never CRR");
        assertEquals(Stage.CRR, DagEngine.initialStage(ArrivalService.ROUTE_ONHOST_REQ, BOOK).orElseThrow(),
                "collections entry unchanged");
        assertEquals(Stage.CRR, DagEngine.initialStage(ArrivalService.ROUTE_ONHOST_REQ_ENDO, BOOK).orElseThrow(),
                "endo entry unchanged");
    }

    @Test
    void manRespRouteEntersAtMarForEveryReplyToken() {
        assertEquals(Stage.MAR, DagEngine.initialStage(MAN_RESP, "FNBCC01_OUT1_ISR.xml").orElseThrow());
        assertEquals(Stage.MAR, DagEngine.initialStage(MAN_RESP, "FNBCC01_OUT1_SBSR.xml").orElseThrow());
        assertEquals(Stage.MAR, DagEngine.initialStage(MAN_RESP, "FNBCC01_OUT1_PBSR.xml").orElseThrow());
        assertTrue(DagEngine.initialStage(MAN_RESP, "FNBCC01_OUT1_XXXX.xml").isEmpty(),
                "unknown token on the man resp route stays fail-closed quarantine");
    }

    // --- route-aware token mapping ----------------------------------------

    @Test
    void sameTokenDifferentRoutePicksDifferentStage() {
        assertEquals(Stage.IXR, DagEngine.fintRespStage(ArrivalService.ROUTE_FINT_RESP,
                "FNBRF01_OUT1_ISR.xml").orElseThrow());
        assertEquals(Stage.MAR, DagEngine.fintRespStage(MAN_RESP,
                "FNBRF01_OUT1_ISR.xml").orElseThrow(),
                "ONE MAR service owns all three pain.012 legs (canon singular)");
        assertEquals(Stage.MAR, DagEngine.fintRespStage(MAN_RESP, "FNBRF01_OUT1_SBSR.xml").orElseThrow());
        assertEquals(Stage.MAR, DagEngine.fintRespStage(MAN_RESP, "FNBRF01_OUT1_PBSR.xml").orElseThrow());
        assertTrue(DagEngine.fintRespStage(MAN_RESP, "FNBRF01_OUT1_XXXX.xml").isEmpty(),
                "unknown token fails closed on the man route too");
    }

    // --- MAN request DAG shape ---------------------------------------------

    @Test
    void manDagChainsMrrMrvMafMis() {
        assertEquals(EnumSet.of(Stage.MRV), DagEngine.computeLaunches(MAN_REQ, BOOK,
                Map.of(Stage.MRR, Outcome.BUSINESS_ACCEPTED), EnumSet.of(Stage.MRR)));
        assertEquals(EnumSet.of(Stage.MAF), DagEngine.computeLaunches(MAN_REQ, BOOK,
                Map.of(Stage.MRR, Outcome.BUSINESS_ACCEPTED, Stage.MRV, Outcome.BUSINESS_ACCEPTED),
                EnumSet.of(Stage.MRR, Stage.MRV)));
        assertEquals(EnumSet.of(Stage.MIS), DagEngine.computeLaunches(MAN_REQ, BOOK,
                Map.of(Stage.MRR, Outcome.BUSINESS_ACCEPTED, Stage.MRV, Outcome.BUSINESS_ACCEPTED,
                        Stage.MAF, Outcome.BUSINESS_ACCEPTED),
                EnumSet.of(Stage.MRR, Stage.MRV, Stage.MAF)));
    }

    @Test
    void misAcceptedForksMirAndMrwTogether() {
        Set<Stage> launches = DagEngine.computeLaunches(MAN_REQ, BOOK,
                Map.of(Stage.MRR, Outcome.BUSINESS_ACCEPTED, Stage.MRV, Outcome.BUSINESS_ACCEPTED,
                        Stage.MAF, Outcome.BUSINESS_ACCEPTED, Stage.MIS, Outcome.BUSINESS_ACCEPTED),
                EnumSet.of(Stage.MRR, Stage.MRV, Stage.MAF, Stage.MIS));
        assertEquals(EnumSet.of(Stage.MIR, Stage.MRW), launches,
                "MIS fans out to the responder AND the pain writer");
    }

    @Test
    void manPartialProceedsLikeAccepted() {
        // R-41 semantics carry over: PARTIAL continues PASS rows.
        assertEquals(EnumSet.of(Stage.MAF), DagEngine.computeLaunches(MAN_REQ, BOOK,
                Map.of(Stage.MRR, Outcome.BUSINESS_ACCEPTED, Stage.MRV, Outcome.BUSINESS_PARTIAL),
                EnumSet.of(Stage.MRR, Stage.MRV)));
        assertEquals(EnumSet.of(Stage.MIR, Stage.MRW), DagEngine.computeLaunches(MAN_REQ, BOOK,
                Map.of(Stage.MRR, Outcome.BUSINESS_ACCEPTED, Stage.MRV, Outcome.BUSINESS_PARTIAL,
                        Stage.MAF, Outcome.BUSINESS_ACCEPTED, Stage.MIS, Outcome.BUSINESS_PARTIAL),
                EnumSet.of(Stage.MRR, Stage.MRV, Stage.MAF, Stage.MIS)));
    }

    @Test
    void manFileRejectedRoutesToMirOnly() {
        // R-41 switch-case extension: the man responder is MIR, never CIR.
        assertEquals(EnumSet.of(Stage.MIR), DagEngine.computeLaunches(MAN_REQ, BOOK,
                Map.of(Stage.MRR, Outcome.BUSINESS_ACCEPTED, Stage.MRV, Outcome.BUSINESS_FILE_REJECTED),
                EnumSet.of(Stage.MRR, Stage.MRV)),
                "whole-file policy rejection: MIR only, never MAF/MIS/MRW");
        assertEquals(EnumSet.of(Stage.MIR), DagEngine.computeLaunches(MAN_REQ, BOOK,
                Map.of(Stage.MRR, Outcome.BUSINESS_ACCEPTED, Stage.MAF, Outcome.BUSINESS_FILE_FATAL),
                EnumSet.of(Stage.MRR, Stage.MRV, Stage.MAF)),
                "whole-file fatal: MIR only");
    }

    @Test
    void manTechFailureLaunchesNothing() {
        assertTrue(DagEngine.computeLaunches(MAN_REQ, BOOK,
                        Map.of(Stage.MRR, Outcome.TECH_FAILED), EnumSet.of(Stage.MRR)).isEmpty(),
                "process death is not a business verdict (R-33)");
    }

    // --- MAN request terminal states ---------------------------------------

    @Test
    void manCompletesOnlyWhenMirAndMrwBothReport() {
        assertTrue(DagEngine.terminalState(MAN_REQ, Map.of(
                        Stage.MRR, Outcome.BUSINESS_ACCEPTED, Stage.MRV, Outcome.BUSINESS_ACCEPTED,
                        Stage.MAF, Outcome.BUSINESS_ACCEPTED, Stage.MIS, Outcome.BUSINESS_ACCEPTED,
                        Stage.MIR, Outcome.BUSINESS_ACCEPTED)).isEmpty(),
                "MRW still in flight: no terminal verdict");
        assertEquals(ArrivalStatus.DAG_COMPLETE, DagEngine.terminalState(MAN_REQ, Map.of(
                Stage.MRR, Outcome.BUSINESS_ACCEPTED, Stage.MRV, Outcome.BUSINESS_ACCEPTED,
                Stage.MAF, Outcome.BUSINESS_ACCEPTED, Stage.MIS, Outcome.BUSINESS_ACCEPTED,
                Stage.MIR, Outcome.BUSINESS_ACCEPTED, Stage.MRW, Outcome.BUSINESS_ACCEPTED)).orElseThrow());
    }

    @Test
    void manRejectedTerminalRequiresTheMirNack() {
        assertEquals(ArrivalStatus.DAG_FAILED, DagEngine.terminalState(MAN_REQ, Map.of(
                Stage.MRR, Outcome.BUSINESS_ACCEPTED, Stage.MRV, Outcome.BUSINESS_FILE_REJECTED,
                Stage.MIR, Outcome.BUSINESS_ACCEPTED)).orElseThrow());
        assertTrue(DagEngine.terminalState(MAN_REQ, Map.of(
                        Stage.MRR, Outcome.BUSINESS_ACCEPTED, Stage.MRV, Outcome.BUSINESS_FILE_REJECTED,
                        Stage.MIR, Outcome.TECH_FAILED)).isEmpty(),
                "a NACK that never left OnHost keeps the arrival open (F6 analog)");
    }

    // --- fint-resp-man DAG --------------------------------------------------

    @Test
    void manRespSeedsMarThenChainsMsr() {
        assertEquals(EnumSet.of(Stage.MAR), DagEngine.computeLaunches(MAN_RESP,
                        "FNBCC01_OUT1_PBSR.xml", Map.of(), EnumSet.noneOf(Stage.class)),
                "level-triggered re-seed of the token-picked entry reader");
        assertTrue(DagEngine.computeLaunches(MAN_RESP, "FNBCC01_OUT1_PBSR.xml",
                        Map.of(), EnumSet.of(Stage.MAR)).isEmpty(),
                "non-overlap: existing MAR intent suppresses relaunch");
        assertEquals(EnumSet.of(Stage.MSR), DagEngine.computeLaunches(MAN_RESP,
                "FNBCC01_OUT1_PBSR.xml",
                Map.of(Stage.MAR, Outcome.BUSINESS_ACCEPTED), EnumSet.of(Stage.MAR)));
        assertEquals(EnumSet.of(Stage.MSR), DagEngine.computeLaunches(MAN_RESP,
                        "FNBCC01_OUT1_PBSR.xml",
                        Map.of(Stage.MAR, Outcome.BUSINESS_PARTIAL), EnumSet.of(Stage.MAR)),
                "PARTIAL (excluded unattributable rows) still advances to MSR");
    }

    @Test
    void manRespCompletesOnMsrOnly() {
        assertTrue(DagEngine.terminalState(MAN_RESP,
                        Map.of(Stage.MAR, Outcome.BUSINESS_ACCEPTED)).isEmpty(),
                "MAR alone never completes the response DAG");
        assertEquals(ArrivalStatus.DAG_COMPLETE, DagEngine.terminalState(MAN_RESP,
                Map.of(Stage.MAR, Outcome.BUSINESS_ACCEPTED,
                        Stage.MSR, Outcome.BUSINESS_ACCEPTED)).orElseThrow());
    }

    @Test
    void manRespFailuresStayOpenForTheReconciler() {
        assertTrue(DagEngine.computeLaunches(MAN_RESP, "FNBCC01_OUT1_PBSR.xml",
                        Map.of(Stage.MAR, Outcome.BUSINESS_FILE_FATAL), EnumSet.of(Stage.MAR)).isEmpty(),
                "no responder on the response route: a fatal MAR launches nothing");
        assertTrue(DagEngine.terminalState(MAN_RESP,
                        Map.of(Stage.MAR, Outcome.BUSINESS_FILE_FATAL)).isEmpty(),
                "fail closed: the arrival stays open, mirroring collections fint-resp");
        assertTrue(DagEngine.terminalState(MAN_RESP,
                        Map.of(Stage.MAR, Outcome.TECH_FAILED)).isEmpty());
    }
}
