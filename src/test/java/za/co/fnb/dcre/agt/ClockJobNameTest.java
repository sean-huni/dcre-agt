package za.co.fnb.dcre.agt;

import org.junit.jupiter.api.Test;
import za.co.fnb.dcre.agt.domain.Flow;
import za.co.fnb.dcre.agt.domain.Stage;
import za.co.fnb.dcre.agt.service.JobLauncher;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Clock Job names are built by launchClock alone; run keys never leak a
 * stage prefix into the name (bit live in M6: HcsScheduler's runKey
 * "hcs-w82587" produced Job "dcre-hcs-hcs-w82587"). SCRUM-70: the resolved
 * flow prefix (col-/pay-/man-) replaces the old dcre- literal.
 */
class ClockJobNameTest {

    @Test
    void redundantStagePrefixInTheRunKeyIsStrippedOnce() {
        assertEquals("col-hcs-w82587", JobLauncher.clockJobName(Flow.COL, Stage.HCS, "hcs-w82587"));
        assertEquals("col-hcs-w82587", JobLauncher.clockJobName(Flow.COL, Stage.HCS, "w82587"),
                "prefixed and unprefixed run keys map to the SAME job name");
    }

    @Test
    void ordinaryRunKeysAreUntouched() {
        assertEquals("col-crw-2026-07-12-w42", JobLauncher.clockJobName(Flow.COL, Stage.CRW, "2026-07-12-w42"));
        assertEquals("col-crg-fnbcc01-w5", JobLauncher.clockJobName(Flow.COL, Stage.CRG, "FNBCC01-w5"),
                "lowercase before sanitize still holds");
    }

    @Test
    void payFlowClockJobsCarryThePayPrefix() {
        // SCRUM-70: PRG windows for a pay-flow client (R-42 interim map) run as
        // pay-prg-* in dcre-pay; the flow prefix is the name's only flow marker.
        assertEquals("pay-prg-fnbrf01-w5", JobLauncher.clockJobName(Flow.PAY, Stage.PRG, "FNBRF01-w5"));
    }

    @Test
    void manFlowClockJobsCarryTheManPrefix() {
        // M10/SCRUM-79: MRG windows always ride Flow.MAN (client-independent).
        assertEquals("man-mrg-fnbcc01-w5", JobLauncher.clockJobName(Flow.MAN, Stage.MRG, "FNBCC01-w5"));
    }

    @Test
    void stageTokenInsideTheKeyIsNotStripped() {
        assertEquals("col-crg-client-prg-w1", JobLauncher.clockJobName(Flow.COL, Stage.CRG, "client-prg-w1"),
                "only a LEADING stage token is redundant");
    }
}
