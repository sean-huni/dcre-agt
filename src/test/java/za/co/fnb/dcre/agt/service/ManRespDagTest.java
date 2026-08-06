package za.co.fnb.dcre.agt.service;

import org.junit.jupiter.api.Test;
import za.co.fnb.dcre.agt.domain.Stage;

import java.util.EnumSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
        assertEquals(Stage.MIX, DagEngine.manEntryFor("ISR"));
        assertEquals(Stage.MSX, DagEngine.manEntryFor("SBSR"));
        assertEquals(Stage.MPX, DagEngine.manEntryFor("PBSR"));
    }

    @Test
    void retiredStagesStayParseableForHistoricOutcomeRows() {
        assertEquals(Stage.MAR, Stage.valueOf("MAR"));
        assertEquals(Stage.MSR, Stage.valueOf("MSR"));
    }

    @Test
    void retiredStagesAppearInNoDagAndAreNeverLaunchable() {
        assertFalse(RouteDags.FINT_RESP_MAN.terminal().contains(Stage.MAR));
        assertFalse(RouteDags.FINT_RESP_MAN.terminal().contains(Stage.MSR));
        assertFalse(JobLauncher.LAUNCHABLE.contains(Stage.MAR));
        assertFalse(JobLauncher.LAUNCHABLE.contains(Stage.MSR));
        // SCRUM-107: MIS was renamed to MIT and is retained only so historic
        // stage_outcome rows parse; it must be as unlaunchable as MAR/MSR.
        assertFalse(JobLauncher.LAUNCHABLE.contains(Stage.MIS));
        assertTrue(JobLauncher.LAUNCHABLE.contains(Stage.MIT));
    }
}
