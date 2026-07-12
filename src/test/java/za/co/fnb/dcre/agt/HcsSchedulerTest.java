package za.co.fnb.dcre.agt;

import org.junit.jupiter.api.Test;
import za.co.fnb.dcre.agt.service.HcsScheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/** Pure window-arithmetic tests: no containers, no K8s. */
class HcsSchedulerTest {

    @Test
    void windowIsStableWithinTheSixHourBlock() {
        assertEquals(0, HcsScheduler.window(0, 6));
        assertEquals(0, HcsScheduler.window(6 * 3600 - 1, 6),
                "every incarnation computes the same window inside one 6h block");
        assertEquals(HcsScheduler.window(0, 6), HcsScheduler.window(6 * 3600 - 1, 6));
    }

    @Test
    void windowAdvancesAcrossTheSixHourBoundary() {
        assertEquals(1, HcsScheduler.window(6 * 3600, 6));
        assertNotEquals(HcsScheduler.window(6 * 3600 - 1, 6), HcsScheduler.window(6 * 3600, 6));
    }
}
