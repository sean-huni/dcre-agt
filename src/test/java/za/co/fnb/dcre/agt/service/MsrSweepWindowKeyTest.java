package za.co.fnb.dcre.agt.service;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Pure determinism tests for the MSR sweep's {@code sweep.instant} window key:
 * no containers, no K8s. The key must be stable within a window (so every AGT
 * incarnation and every kill-resume in the same window re-runs MSR's
 * SWEEP-&lt;kind&gt;-&lt;instant&gt; idempotently) and distinct across windows.
 */
class MsrSweepWindowKeyTest {

    private static final long INTERVAL = 3600;

    @Test
    void sweepInstantIsStableWithinTheWindow() {
        long windowStart = 100 * INTERVAL;
        assertEquals(MsrSweep.sweepInstant(windowStart, INTERVAL),
                MsrSweep.sweepInstant(windowStart + INTERVAL - 1, INTERVAL),
                "every incarnation inside one window computes the same sweep.instant");
    }

    @Test
    void sweepInstantChangesAcrossWindows() {
        long windowStart = 100 * INTERVAL;
        assertNotEquals(MsrSweep.sweepInstant(windowStart, INTERVAL),
                MsrSweep.sweepInstant(windowStart + INTERVAL, INTERVAL),
                "the next window yields a distinct sweep.instant");
    }

    @Test
    void sweepInstantIsTheDeterministicWindowStartInstant() {
        long epochSeconds = 123_456_789L;
        long expectedStart = (epochSeconds / INTERVAL) * INTERVAL;
        assertEquals(Instant.ofEpochSecond(expectedStart).toString(),
                MsrSweep.sweepInstant(epochSeconds, INTERVAL),
                "sweep.instant is the window-start instant, never now()");
    }
}
