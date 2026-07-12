package za.co.fnb.dcre.agt;

import org.junit.jupiter.api.Test;
import za.co.fnb.dcre.agt.service.PrgScheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/** Pure window-arithmetic tests: no containers, no K8s. */
class PrgSchedulerTest {

    @Test
    void windowIsStableWithinTheInterval() {
        assertEquals(2, PrgScheduler.window(120, 60));
        assertEquals(PrgScheduler.window(120, 60), PrgScheduler.window(179, 60),
                "every incarnation computes the same window inside one interval");
    }

    @Test
    void windowAdvancesAcrossTheIntervalBoundary() {
        assertEquals(3, PrgScheduler.window(180, 60));
        assertNotEquals(PrgScheduler.window(179, 60), PrgScheduler.window(180, 60));
    }
}
