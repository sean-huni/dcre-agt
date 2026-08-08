package za.co.fnb.dcre.agt.service;

import org.junit.jupiter.api.Test;
import za.co.fnb.dcre.agt.domain.Flow;
import za.co.fnb.dcre.agt.domain.Stage;

import java.util.EnumSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SCRUM-91: fint-resp-man mirrors collections fint-resp exactly. Three token-picked
 * entries, NO successor edges (the MAR -> MSR chain was the deviation), no responder
 * (nothing answers OnHost on a response route, so a whole-file failure launches nothing
 * and the arrival stays open for the reconciler: fail closed).
 */
class ManRespDagTest {

    @Test
    void theManResponseDagHasNoSuccessorEdges() {
        assertTrue(RouteDags.FINT_RESP_MAN.edges().isEmpty(),
                "the MAR -> MSR chain was the deviation: no successor edges remain");
    }

    @Test
    void allThreeLegReadersAreTerminal() {
        assertEquals(EnumSet.of(Stage.MIX, Stage.MSX, Stage.MPX),
                RouteDags.FINT_RESP_MAN.terminal());
    }

    @Test
    void theResponseDagHasNoResponder() {
        assertTrue(RouteDags.FINT_RESP_MAN.responder().isEmpty(),
                "nothing answers OnHost on a response route: fail closed");
    }

    @Test
    void eachReplyTokenPicksItsOwnLegReader() {
        assertEquals(Stage.MIX, DagEngine.entryFor(Flow.MAN, "ISR"));
        assertEquals(Stage.MSX, DagEngine.entryFor(Flow.MAN, "SBSR"));
        assertEquals(Stage.MPX, DagEngine.entryFor(Flow.MAN, "PBSR"));
    }

    /**
     * The v1 cutover DELETED the retained-deprecated constants (MAR, MSR, MIS, MAF)
     * rather than keeping them parseable. A-75 kept them only because
     * {@code agt_ops.stage_outcome.stage} is parsed with {@code Stage.valueOf} and
     * historic rows would otherwise break the reconciler; the owner directive of
     * 2026-08-08 dropped every DCRE database, so there are no historic rows and the
     * constraint is gone.
     *
     * <p>Asserted rather than assumed, because "the enum no longer has it" is
     * exactly the kind of claim that survives on the strength of somebody having
     * meant to do it.
     */
    @Test
    void thePreCutoverStageNamesAreGoneEntirely() {
        for (final String retired : new String[] {"MAR", "MSR", "MIS", "MAF", "IXR", "SXR", "PXR", "AIS"}) {
            assertThrows(IllegalArgumentException.class, () -> Stage.valueOf(retired),
                    "Stage." + retired + " must not exist in the v1 roster");
        }
        assertEquals(Stage.MIT, Stage.valueOf("MIT"), "its replacement is present");
        assertEquals(Stage.MAS, Stage.valueOf("MAS"));
    }
}
