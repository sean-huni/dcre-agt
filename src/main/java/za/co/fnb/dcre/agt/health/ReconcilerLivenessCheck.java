package za.co.fnb.dcre.agt.health;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Liveness;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.service.Reconciler;

import java.time.Duration;
import java.time.Instant;

/**
 * AGT self-liveness (M12/SCRUM-87, R-47): the reconciler is AGT's brain, and a
 * wedged reconciler (scheduler thread stuck, deadlocked, or livelocked) leaves
 * dead jobs undetected while the pod still passes readiness. This @Liveness
 * check reports DOWN when the reconcile scheduler has not ticked within
 * dcre.agt.self-liveness-ttl-seconds; the k8s livenessProbe then restarts the
 * pod, and AGT's lease-CAS re-acquires the single-writer role on the new
 * incarnation. Before the first tick a startup grace measured from the pod's
 * start keeps boot from flapping the probe.
 */
@Liveness
@ApplicationScoped
public class ReconcilerLivenessCheck implements HealthCheck {

    static final String NAME = "reconciler-liveness";

    @Inject
    Reconciler reconciler;

    @Inject
    AgtConfig config;

    @Override
    public HealthCheckResponse call() {
        Instant last = reconciler.lastTickAt();
        boolean fresh = fresh(last, reconciler.startedAt(), config.selfLivenessTtlSeconds(), Instant.now());
        return HealthCheckResponse.named(NAME)
                .status(fresh)
                .withData("lastTickAt", last != null ? last.toString() : "never")
                .withData("ttlSeconds", config.selfLivenessTtlSeconds())
                .build();
    }

    /**
     * UP while the reconciler ticked within the TTL. Before the first tick
     * (lastTickAt null) the pod start is the reference, so a booting AGT stays
     * UP for one TTL window; an AGT whose scheduler never fires goes DOWN once
     * that window lapses.
     */
    static boolean fresh(final Instant lastTickAt, final Instant startedAt,
                         final long ttlSeconds, final Instant now) {
        Instant reference = lastTickAt != null ? lastTickAt : startedAt;
        return Duration.between(reference, now).getSeconds() <= ttlSeconds;
    }
}
