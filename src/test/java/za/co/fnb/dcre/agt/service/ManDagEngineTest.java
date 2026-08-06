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
 * onhost-req-man request DAG (MRR -> MRV -> MAS -> MIT -> fork {MIR, MRW},
 * responder MIR) and the fint-resp-man response DAG (SCRUM-91: one token-picked
 * leg reader of MIX/MSX/MPX, no chain and no responder, so failures stay open
 * for the reconciler). No containers, no K8s.
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
    void manRespRouteEntersAtTheLegReaderForItsReplyToken() {
        assertEquals(Stage.MIX, DagEngine.initialStage(MAN_RESP, "FNBCC01_OUT1_ISR.xml").orElseThrow());
        assertEquals(Stage.MSX, DagEngine.initialStage(MAN_RESP, "FNBCC01_OUT1_SBSR.xml").orElseThrow());
        assertEquals(Stage.MPX, DagEngine.initialStage(MAN_RESP, "FNBCC01_OUT1_PBSR.xml").orElseThrow());
        assertTrue(DagEngine.initialStage(MAN_RESP, "FNBCC01_OUT1_XXXX.xml").isEmpty(),
                "unknown token on the man resp route stays fail-closed quarantine");
    }

    // --- route-aware token mapping ----------------------------------------

    @Test
    void sameTokenDifferentRoutePicksDifferentStage() {
        assertEquals(Stage.IXR, DagEngine.fintRespStage(ArrivalService.ROUTE_FINT_RESP,
                "FNBRF01_OUT1_ISR.xml").orElseThrow());
        assertEquals(Stage.MIX, DagEngine.fintRespStage(MAN_RESP,
                "FNBRF01_OUT1_ISR.xml").orElseThrow(),
                "same token, different route: the mandates ISR reader is MIX, not IXR");
        assertEquals(Stage.MSX, DagEngine.fintRespStage(MAN_RESP, "FNBRF01_OUT1_SBSR.xml").orElseThrow());
        assertEquals(Stage.MPX, DagEngine.fintRespStage(MAN_RESP, "FNBRF01_OUT1_PBSR.xml").orElseThrow());
        assertTrue(DagEngine.fintRespStage(MAN_RESP, "FNBRF01_OUT1_XXXX.xml").isEmpty(),
                "unknown token fails closed on the man route too");
    }

    // --- MAN request DAG shape ---------------------------------------------

    @Test
    void manDagChainsMrrMrvMafMis() {
        assertEquals(EnumSet.of(Stage.MRV), DagEngine.computeLaunches(MAN_REQ, BOOK,
                Map.of(Stage.MRR, Outcome.BUSINESS_ACCEPTED), EnumSet.of(Stage.MRR)));
        assertEquals(EnumSet.of(Stage.MAS), DagEngine.computeLaunches(MAN_REQ, BOOK,
                Map.of(Stage.MRR, Outcome.BUSINESS_ACCEPTED, Stage.MRV, Outcome.BUSINESS_ACCEPTED),
                EnumSet.of(Stage.MRR, Stage.MRV)));
        assertEquals(EnumSet.of(Stage.MIT), DagEngine.computeLaunches(MAN_REQ, BOOK,
                Map.of(Stage.MRR, Outcome.BUSINESS_ACCEPTED, Stage.MRV, Outcome.BUSINESS_ACCEPTED,
                        Stage.MAS, Outcome.BUSINESS_ACCEPTED),
                EnumSet.of(Stage.MRR, Stage.MRV, Stage.MAS)));
    }

    @Test
    void misAcceptedForksMirAndMrwTogether() {
        Set<Stage> launches = DagEngine.computeLaunches(MAN_REQ, BOOK,
                Map.of(Stage.MRR, Outcome.BUSINESS_ACCEPTED, Stage.MRV, Outcome.BUSINESS_ACCEPTED,
                        Stage.MAS, Outcome.BUSINESS_ACCEPTED, Stage.MIT, Outcome.BUSINESS_ACCEPTED),
                EnumSet.of(Stage.MRR, Stage.MRV, Stage.MAS, Stage.MIT));
        assertEquals(EnumSet.of(Stage.MIR, Stage.MRW), launches,
                "MIT fans out to the responder AND the pain writer");
    }

    @Test
    void manPartialProceedsLikeAccepted() {
        // R-41 semantics carry over: PARTIAL continues PASS rows.
        assertEquals(EnumSet.of(Stage.MAS), DagEngine.computeLaunches(MAN_REQ, BOOK,
                Map.of(Stage.MRR, Outcome.BUSINESS_ACCEPTED, Stage.MRV, Outcome.BUSINESS_PARTIAL),
                EnumSet.of(Stage.MRR, Stage.MRV)));
        assertEquals(EnumSet.of(Stage.MIR, Stage.MRW), DagEngine.computeLaunches(MAN_REQ, BOOK,
                Map.of(Stage.MRR, Outcome.BUSINESS_ACCEPTED, Stage.MRV, Outcome.BUSINESS_PARTIAL,
                        Stage.MAS, Outcome.BUSINESS_ACCEPTED, Stage.MIT, Outcome.BUSINESS_PARTIAL),
                EnumSet.of(Stage.MRR, Stage.MRV, Stage.MAS, Stage.MIT)));
    }

    @Test
    void manFileRejectedRoutesToMirOnly() {
        // R-41 switch-case extension: the man responder is MIR, never CIR.
        assertEquals(EnumSet.of(Stage.MIR), DagEngine.computeLaunches(MAN_REQ, BOOK,
                Map.of(Stage.MRR, Outcome.BUSINESS_ACCEPTED, Stage.MRV, Outcome.BUSINESS_FILE_REJECTED),
                EnumSet.of(Stage.MRR, Stage.MRV)),
                "whole-file policy rejection: MIR only, never MAS/MIT/MRW");
        assertEquals(EnumSet.of(Stage.MIR), DagEngine.computeLaunches(MAN_REQ, BOOK,
                Map.of(Stage.MRR, Outcome.BUSINESS_ACCEPTED, Stage.MAS, Outcome.BUSINESS_FILE_FATAL),
                EnumSet.of(Stage.MRR, Stage.MRV, Stage.MAS)),
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
                        Stage.MAS, Outcome.BUSINESS_ACCEPTED, Stage.MIT, Outcome.BUSINESS_ACCEPTED,
                        Stage.MIR, Outcome.BUSINESS_ACCEPTED), true).isEmpty(),
                "MRW still in flight: no terminal verdict");
        assertEquals(ArrivalStatus.DAG_COMPLETE, DagEngine.terminalState(MAN_REQ, Map.of(
                Stage.MRR, Outcome.BUSINESS_ACCEPTED, Stage.MRV, Outcome.BUSINESS_ACCEPTED,
                Stage.MAS, Outcome.BUSINESS_ACCEPTED, Stage.MIT, Outcome.BUSINESS_ACCEPTED,
                Stage.MIR, Outcome.BUSINESS_ACCEPTED, Stage.MRW, Outcome.BUSINESS_ACCEPTED), true).orElseThrow());
    }

    @Test
    void manRejectedTerminalRequiresTheMirNack() {
        assertEquals(ArrivalStatus.DAG_FAILED, DagEngine.terminalState(MAN_REQ, Map.of(
                Stage.MRR, Outcome.BUSINESS_ACCEPTED, Stage.MRV, Outcome.BUSINESS_FILE_REJECTED,
                Stage.MIR, Outcome.BUSINESS_ACCEPTED), true).orElseThrow());
        assertTrue(DagEngine.terminalState(MAN_REQ, Map.of(
                        Stage.MRR, Outcome.BUSINESS_ACCEPTED, Stage.MRV, Outcome.BUSINESS_FILE_REJECTED,
                        Stage.MIR, Outcome.TECH_FAILED), true).isEmpty(),
                "a NACK that never left OnHost keeps the arrival open (F6 analog)");
    }

    // --- fint-resp-man DAG --------------------------------------------------

    @Test
    void manRespSeedsItsOneLegReaderAndNothingElse() {
        assertEquals(EnumSet.of(Stage.MPX), DagEngine.computeLaunches(MAN_RESP,
                        "FNBCC01_OUT1_PBSR.xml", Map.of(), EnumSet.noneOf(Stage.class)),
                "level-triggered re-seed of the token-picked entry reader");
        assertTrue(DagEngine.computeLaunches(MAN_RESP, "FNBCC01_OUT1_PBSR.xml",
                        Map.of(), EnumSet.of(Stage.MPX)).isEmpty(),
                "non-overlap: an existing MPX intent suppresses relaunch");
        assertTrue(DagEngine.computeLaunches(MAN_RESP, "FNBCC01_OUT1_PBSR.xml",
                        Map.of(Stage.MPX, Outcome.BUSINESS_ACCEPTED), EnumSet.of(Stage.MPX)).isEmpty(),
                "SCRUM-91: no successor edge, an accepted leg reader chains into nothing");
        assertTrue(DagEngine.computeLaunches(MAN_RESP, "FNBCC01_OUT1_PBSR.xml",
                        Map.of(Stage.MPX, Outcome.BUSINESS_PARTIAL), EnumSet.of(Stage.MPX)).isEmpty(),
                "and neither does a partial one");
    }

    @Test
    void manRespCompletesOnItsOneLegReader() {
        // The terminal set lists the three LEGAL entries, of which exactly one
        // runs per arrival, so completion is any-of and never all-of: an all-of
        // test would leave every response arrival permanently DAG_RUNNING.
        assertEquals(ArrivalStatus.DAG_COMPLETE, DagEngine.terminalState(MAN_RESP,
                        Map.of(Stage.MPX, Outcome.BUSINESS_ACCEPTED), true).orElseThrow(),
                "the PBSR leg reader alone completes a PBSR arrival");
        assertEquals(ArrivalStatus.DAG_COMPLETE, DagEngine.terminalState(MAN_RESP,
                Map.of(Stage.MIX, Outcome.BUSINESS_ACCEPTED), true).orElseThrow());
        assertEquals(ArrivalStatus.DAG_COMPLETE, DagEngine.terminalState(MAN_RESP,
                Map.of(Stage.MSX, Outcome.BUSINESS_ACCEPTED), true).orElseThrow());
        assertTrue(DagEngine.terminalState(MAN_RESP,
                        Map.of(Stage.MPX, Outcome.BUSINESS_PARTIAL), true).isEmpty(),
                "partial is not acceptance: stays open, exactly as on collections fint-resp");
    }

    @Test
    void manRespFailuresStayOpenForTheReconciler() {
        assertTrue(DagEngine.computeLaunches(MAN_RESP, "FNBCC01_OUT1_PBSR.xml",
                        Map.of(Stage.MPX, Outcome.BUSINESS_FILE_FATAL), EnumSet.of(Stage.MPX)).isEmpty(),
                "no responder on the response route: a fatal leg reader launches nothing");
        assertTrue(DagEngine.terminalState(MAN_RESP,
                        Map.of(Stage.MPX, Outcome.BUSINESS_FILE_FATAL), true).isEmpty(),
                "fail closed: the arrival stays open, mirroring collections fint-resp");
        assertTrue(DagEngine.terminalState(MAN_RESP,
                        Map.of(Stage.MPX, Outcome.TECH_FAILED), true).isEmpty());
    }

    @Test
    void retiredStagesNeverCompleteAResponseArrival() {
        // A-75: historic MAR/MSR outcome rows stay parseable, and SCRUM-107 adds
        // MIS after the rename to MIT, but they are in no DAG, so a legacy row can
        // never close a post-cutover arrival.
        assertTrue(DagEngine.terminalState(MAN_RESP,
                        Map.of(Stage.MAR, Outcome.BUSINESS_ACCEPTED,
                                Stage.MSR, Outcome.BUSINESS_ACCEPTED), true).isEmpty(),
                "the retired chain is not a terminal state any more");
        assertTrue(DagEngine.computeLaunches(MAN_REQ, BOOK,
                        Map.of(Stage.MIS, Outcome.BUSINESS_ACCEPTED),
                        EnumSet.of(Stage.MIS)).isEmpty(),
                "SCRUM-107: a historic MIS outcome row launches nothing after the MIT rename");
    }
}
