package za.co.fnb.dcre.agt;

import org.junit.jupiter.api.Test;
import za.co.fnb.dcre.agt.domain.ArrivalStatus;
import za.co.fnb.dcre.agt.domain.Outcome;
import za.co.fnb.dcre.agt.domain.Stage;
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
                Map.of(Stage.CRR, Outcome.BUSINESS_ACCEPTED, Stage.CTV, Outcome.BUSINESS_PARTIAL),
                EnumSet.of(Stage.CRR, Stage.CTV));
        assertEquals(EnumSet.of(Stage.CDE, Stage.CIR), launches);
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
