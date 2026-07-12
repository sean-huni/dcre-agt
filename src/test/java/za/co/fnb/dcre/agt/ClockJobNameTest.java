package za.co.fnb.dcre.agt;

import org.junit.jupiter.api.Test;
import za.co.fnb.dcre.agt.domain.Stage;
import za.co.fnb.dcre.agt.service.JobLauncher;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Clock Job names are built by launchClock alone; run keys never leak a
 * stage prefix into the name (bit live in M6: HcsScheduler's runKey
 * "hcs-w82587" produced Job "dcre-hcs-hcs-w82587").
 */
class ClockJobNameTest {

    @Test
    void redundantStagePrefixInTheRunKeyIsStrippedOnce() {
        assertEquals("dcre-hcs-w82587", JobLauncher.clockJobName(Stage.HCS, "hcs-w82587"));
        assertEquals("dcre-hcs-w82587", JobLauncher.clockJobName(Stage.HCS, "w82587"),
                "prefixed and unprefixed run keys map to the SAME job name");
    }

    @Test
    void ordinaryRunKeysAreUntouched() {
        assertEquals("dcre-crw-2026-07-12-w42", JobLauncher.clockJobName(Stage.CRW, "2026-07-12-w42"));
        assertEquals("dcre-prg-fnbrf01-w5", JobLauncher.clockJobName(Stage.PRG, "FNBRF01-w5"),
                "lowercase before sanitize still holds");
    }

    @Test
    void stageTokenInsideTheKeyIsNotStripped() {
        assertEquals("dcre-prg-client-prg-w1", JobLauncher.clockJobName(Stage.PRG, "client-prg-w1"),
                "only a LEADING stage token is redundant");
    }
}
