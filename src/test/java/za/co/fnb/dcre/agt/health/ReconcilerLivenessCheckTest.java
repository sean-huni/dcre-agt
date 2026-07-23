package za.co.fnb.dcre.agt.health;

import org.eclipse.microprofile.health.HealthCheckResponse;
import org.junit.jupiter.api.Test;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.service.Reconciler;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * M12/SCRUM-87 (R-47): the reconciler self-liveness decision. Plain JUnit +
 * Mockito over the two collaborators (no Quarkus boot) so the DOWN / UP /
 * startup-grace branches are pinned directly on call().
 */
class ReconcilerLivenessCheckTest {

    @Test
    void downWhenLastTickStale() {
        HealthCheckResponse r = check(Instant.now().minusSeconds(100),
                Instant.now().minusSeconds(300), 15).call();
        assertEquals(HealthCheckResponse.Status.DOWN, r.getStatus(),
                "no reconcile tick within the TTL => DOWN (k8s restarts the wedged pod)");
    }

    @Test
    void upWhenTickFresh() {
        HealthCheckResponse r = check(Instant.now().minusSeconds(3),
                Instant.now().minusSeconds(300), 15).call();
        assertEquals(HealthCheckResponse.Status.UP, r.getStatus(), "a recent tick => UP");
    }

    @Test
    void upAtStartupBeforeFirstTick() {
        // lastTickAt null, pod just started: the startup grace keeps it UP so
        // boot does not flap the probe.
        HealthCheckResponse r = check(null, Instant.now(), 15).call();
        assertEquals(HealthCheckResponse.Status.UP, r.getStatus(),
                "never-ticked-yet within the grace => UP");
    }

    @Test
    void downWhenNeverTickedPastGrace() {
        // Booted but the scheduler never fired for a whole TTL: genuinely wedged.
        HealthCheckResponse r = check(null, Instant.now().minusSeconds(100), 15).call();
        assertEquals(HealthCheckResponse.Status.DOWN, r.getStatus(),
                "never ticking past the startup grace => DOWN");
    }

    private static ReconcilerLivenessCheck check(final Instant lastTick, final Instant startedAt,
                                                 final long ttlSeconds) {
        Reconciler reconciler = mock(Reconciler.class);
        when(reconciler.lastTickAt()).thenReturn(lastTick);
        when(reconciler.startedAt()).thenReturn(startedAt);
        AgtConfig config = mock(AgtConfig.class);
        when(config.selfLivenessTtlSeconds()).thenReturn(ttlSeconds);
        ReconcilerLivenessCheck c = new ReconcilerLivenessCheck();
        c.reconciler = reconciler;
        c.config = config;
        return c;
    }
}
