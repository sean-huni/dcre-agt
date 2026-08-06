package za.co.fnb.dcre.agt.service;

import org.junit.jupiter.api.Test;
import za.co.fnb.dcre.agt.domain.ArrivalStatus;
import za.co.fnb.dcre.agt.domain.Outcome;
import za.co.fnb.dcre.agt.domain.Stage;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SCRUM-107 (Sean, 2026-08-06): DAG_COMPLETE fired on {CDE, CIR} alone, so a
 * collections arrival reported COMPLETE before CRW had written anything to
 * Fintegrate. Sean's words: "completing the DAG without the CRW
 * executing+completing concludes to reporting fake news."
 *
 * <p>Verified against the running orchestrator on 2026-08-06 before this change:
 * a DC arrival reached DAG_COMPLETE with exactly {@code CRR -> CTV -> CDE -> CIR}
 * and CRW never launched.
 *
 * <p>R-37 (Sean-directed 2026-07-12) is NOT reversed by this: CRW stays a
 * clock-driven Process-Date Executor keyed on (date, window), and DCRE still
 * warehouses futured instructions. What changes is only the terminal CLAIM. A
 * warehoused arrival stays DAG_RUNNING, which is honest, until its process_date
 * arrives and CRW makes an emission VISIBLE for it. R-37's terminal set is
 * AMENDED to add that condition, not replaced.
 *
 * <p>The signal is R-37's own: "once an emission is VISIBLE for (arrival,
 * run_date)". A per-arrival CRW stage outcome cannot serve, because CRW's job
 * identity is (date, window) and one run serves many arrivals.
 */
class EmissionGatedTerminalTest {

    private static final Map<Stage, Outcome> DC_STAGES_DONE = Map.of(
            Stage.CRR, Outcome.BUSINESS_ACCEPTED,
            Stage.CTV, Outcome.BUSINESS_ACCEPTED,
            Stage.CDE, Outcome.BUSINESS_ACCEPTED,
            Stage.CIR, Outcome.BUSINESS_ACCEPTED);

    @Test
    void aCollectionsArrivalIsNotCompleteUntilCrwHasEmittedForIt() {
        assertTrue(DagEngine.terminalState("onhost-req", DC_STAGES_DONE, false).isEmpty(),
                "CDE and CIR done but nothing emitted: the arrival is warehoused,"
                        + " not complete. Reporting DAG_COMPLETE here is the fake news.");
    }

    @Test
    void itCompletesOnceTheEmissionIsVisible() {
        assertEquals(java.util.Optional.of(ArrivalStatus.DAG_COMPLETE),
                DagEngine.terminalState("onhost-req", DC_STAGES_DONE, true),
                "process_date reached and CRW emitted: now it is genuinely complete");
    }

    @Test
    void theEndoPayArmIsGatedTheSameWay() {
        final Map<Stage, Outcome> endoDone = Map.of(
                Stage.CRR, Outcome.BUSINESS_ACCEPTED,
                Stage.CTV, Outcome.BUSINESS_ACCEPTED,
                Stage.AIS, Outcome.BUSINESS_ACCEPTED,
                Stage.CIR, Outcome.BUSINESS_ACCEPTED);
        assertTrue(DagEngine.terminalState("onhost-req-endo", endoDone, false).isEmpty(),
                "the pay arm emits through the same CRW window job, so it gates the same");
        assertEquals(java.util.Optional.of(ArrivalStatus.DAG_COMPLETE),
                DagEngine.terminalState("onhost-req-endo", endoDone, true));
    }

    /**
     * Mandates is NOT gated: MRW is a real DAG stage in the MAN route, so the
     * writer's completion is already part of the terminal set. Gating it on a
     * collections emission would strand every mandate arrival forever.
     */
    @Test
    void mandatesIsUnaffectedBecauseMrwIsAlreadyInItsDag() {
        final Map<Stage, Outcome> manDone = Map.of(
                Stage.MRR, Outcome.BUSINESS_ACCEPTED,
                Stage.MRV, Outcome.BUSINESS_ACCEPTED,
                Stage.MAS, Outcome.BUSINESS_ACCEPTED,
                Stage.MIT, Outcome.BUSINESS_ACCEPTED,
                Stage.MIR, Outcome.BUSINESS_ACCEPTED,
                Stage.MRW, Outcome.BUSINESS_ACCEPTED);
        assertEquals(java.util.Optional.of(ArrivalStatus.DAG_COMPLETE),
                DagEngine.terminalState("onhost-req-man", manDone, false),
                "mandates completes on its own stages, with no emission gate");
    }

    /** Response routes have no emission leg either. */
    @Test
    void responseRoutesAreUnaffected() {
        assertEquals(java.util.Optional.of(ArrivalStatus.DAG_COMPLETE),
                DagEngine.terminalState("fint-resp-man",
                        Map.of(Stage.MIX, Outcome.BUSINESS_ACCEPTED), false),
                "a token-picked reader accepting completes the response arrival");
    }

    /** A rejected or failed collections file must still terminate without an emission. */
    @Test
    void aWholeFileRejectionStillTerminatesWithoutAnEmission() {
        final java.util.Optional<ArrivalStatus> s = DagEngine.terminalState("onhost-req",
                Map.of(Stage.CRR, Outcome.BUSINESS_FILE_REJECTED,
                        Stage.CIR, Outcome.BUSINESS_ACCEPTED), false);
        assertTrue(s.isPresent(),
                "a NACKed file emits nothing by design: gating it on an emission that will"
                        + " never exist would strand every rejected arrival in DAG_RUNNING forever");
    }
}
