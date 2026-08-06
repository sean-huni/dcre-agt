package za.co.fnb.dcre.agt.service;

import org.junit.jupiter.api.Test;
import za.co.fnb.dcre.agt.domain.ArrivalStatus;
import za.co.fnb.dcre.agt.domain.Outcome;
import za.co.fnb.dcre.agt.domain.Stage;

import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
 * <p>The predicate is OWED, not EMITTED, and the distinction is load-bearing.
 * "An emission exists" completes a 3-batch arrival after batch 1, completes a
 * multi-process-date arrival on day 1 with the futured remainder unsent, and
 * strands forever an arrival whose rows all failed validation and which will
 * therefore never emit at all. CRW publishes the answer as crw_emission_owed.
 * A per-arrival CRW stage outcome cannot serve as the signal either, because
 * CRW's job identity is (date, window) and one run serves many arrivals.
 */
class EmissionGatedTerminalTest {

    private static final Map<Stage, Outcome> DC_STAGES_DONE = Map.of(
            Stage.CRR, Outcome.BUSINESS_ACCEPTED,
            Stage.CTV, Outcome.BUSINESS_ACCEPTED,
            Stage.CDE, Outcome.BUSINESS_ACCEPTED,
            Stage.CIR, Outcome.BUSINESS_ACCEPTED);

    @Test
    void aCollectionsArrivalIsNotCompleteUntilCrwHasEmittedForIt() {
        assertTrue(DagEngine.terminalState(ArrivalService.ROUTE_ONHOST_REQ, DC_STAGES_DONE, () -> true).isEmpty(),
                "CDE and CIR done but nothing emitted: the arrival is warehoused,"
                        + " not complete. Reporting DAG_COMPLETE here is the fake news.");
    }

    @Test
    void itCompletesOnceTheEmissionIsVisible() {
        assertEquals(Optional.of(ArrivalStatus.DAG_COMPLETE),
                DagEngine.terminalState(ArrivalService.ROUTE_ONHOST_REQ, DC_STAGES_DONE, () -> false),
                "process_date reached and CRW emitted: now it is genuinely complete");
    }

    @Test
    void theEndoPayArmIsGatedTheSameWay() {
        final Map<Stage, Outcome> endoDone = Map.of(
                Stage.CRR, Outcome.BUSINESS_ACCEPTED,
                Stage.CTV, Outcome.BUSINESS_ACCEPTED,
                Stage.AIS, Outcome.BUSINESS_ACCEPTED,
                Stage.CIR, Outcome.BUSINESS_ACCEPTED);
        assertTrue(DagEngine.terminalState(ArrivalService.ROUTE_ONHOST_REQ_ENDO, endoDone, () -> true).isEmpty(),
                "the pay arm emits through the same CRW window job, so it gates the same");
        assertEquals(Optional.of(ArrivalStatus.DAG_COMPLETE),
                DagEngine.terminalState(ArrivalService.ROUTE_ONHOST_REQ_ENDO, endoDone, () -> false));
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
        assertEquals(Optional.of(ArrivalStatus.DAG_COMPLETE),
                DagEngine.terminalState(ArrivalService.ROUTE_ONHOST_REQ_MAN, manDone, () -> true),
                "mandates completes on its own stages, with no emission gate");
    }

    /** Response routes have no emission leg either. */
    @Test
    void responseRoutesAreUnaffected() {
        assertEquals(Optional.of(ArrivalStatus.DAG_COMPLETE),
                DagEngine.terminalState(ArrivalService.ROUTE_FINT_RESP_MAN,
                        Map.of(Stage.MIX, Outcome.BUSINESS_ACCEPTED), () -> true),
                "a token-picked reader accepting completes the response arrival");
    }

    /**
     * A NACKed file emits nothing by design, and must terminate as FAILED. Asserting
     * only isPresent() would pass on DAG_COMPLETE, and a rejected file reported
     * COMPLETE is a worse claim than the one this change exists to fix.
     */
    @Test
    void aWholeFileRejectionStillTerminatesAsFailedWithoutAnEmission() {
        assertEquals(Optional.of(ArrivalStatus.DAG_FAILED),
                DagEngine.terminalState(ArrivalService.ROUTE_ONHOST_REQ,
                        Map.of(Stage.CRR, Outcome.BUSINESS_FILE_REJECTED,
                                Stage.CIR, Outcome.BUSINESS_ACCEPTED), () -> true),
                "the fatal branch returns before the gate, so a rejected arrival can never"
                        + " be stranded waiting for an emission that will never exist");
    }

    /**
     * The stranding case the OWED predicate exists for. A PARTIAL-mode client whose
     * file has ZERO passing rows exits CTV as BUSINESS_PARTIAL, not FILE_REJECTED, so
     * it does NOT take the fatal branch. CRW plans empty for it forever. Under an
     * "emission exists" predicate it would sit in DAG_RUNNING permanently; under OWED
     * the view reports nothing owed and it terminates.
     */
    @Test
    void anArrivalThatCanNeverEmitStillTerminates() {
        assertEquals(Optional.of(ArrivalStatus.DAG_COMPLETE),
                DagEngine.terminalState(ArrivalService.ROUTE_ONHOST_REQ,
                        Map.of(Stage.CRR, Outcome.BUSINESS_ACCEPTED,
                                Stage.CTV, Outcome.BUSINESS_PARTIAL,
                                Stage.CDE, Outcome.BUSINESS_ACCEPTED,
                                Stage.CIR, Outcome.BUSINESS_ACCEPTED), () -> false),
                "zero PASS rows means nothing is owed, so the arrival must not hang");
    }

    /** The gate must never short-circuit stage completeness: an unfinished stage wins. */
    @Test
    void theGateCannotCompleteAnArrivalWhoseStagesAreUnfinished() {
        assertTrue(DagEngine.terminalState(ArrivalService.ROUTE_ONHOST_REQ,
                        Map.of(Stage.CRR, Outcome.BUSINESS_ACCEPTED), () -> false).isEmpty(),
                "nothing owed does not make a mid-flight arrival complete");
        assertTrue(DagEngine.terminalState(ArrivalService.ROUTE_ONHOST_REQ,
                        Map.of(Stage.CRR, Outcome.BUSINESS_ACCEPTED,
                                Stage.CTV, Outcome.BUSINESS_ACCEPTED,
                                Stage.CDE, Outcome.TECH_FAILED,
                                Stage.CIR, Outcome.BUSINESS_ACCEPTED), () -> false).isEmpty(),
                "a TECH_FAILED terminal stage is not business-done, gate or no gate");
    }

    /** requiresEmission is a property of the route, and only the emitting ones carry it. */
    @Test
    void onlyTheCrwEmittingRoutesRequireAnEmission() {
        assertTrue(RouteDags.REQUESTS.get(ArrivalService.ROUTE_ONHOST_REQ).requiresEmission());
        assertTrue(RouteDags.REQUESTS.get(ArrivalService.ROUTE_ONHOST_REQ_ENDO).requiresEmission());
        assertFalse(RouteDags.REQUESTS.get(ArrivalService.ROUTE_ONHOST_REQ_MAN).requiresEmission(),
                "mandates writes through MRW, a real DAG stage, and must never query dcre_col");
        RouteDags.RESPONSES.values().forEach(d -> assertFalse(d.requiresEmission(),
                "response routes have no emission leg"));
    }
}
