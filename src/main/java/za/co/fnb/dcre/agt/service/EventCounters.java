package za.co.fnb.dcre.agt.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import za.co.fnb.dcre.agt.domain.ArrivalStatus;
import za.co.fnb.dcre.agt.domain.Outcome;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * Monotonic event counters the control loops increment at the write. One job:
 * hold the counters. It needs no datasource and no lease, which is why it is not
 * part of {@link MetricsService}: that class scans the ledger every 10s and
 * republishes row counts, and scan-and-republish is a different job from
 * counting events other classes report.
 *
 * <p><b>Why these exist beside the gauges.</b> {@code MetricsService.count}
 * registers each gauge lazily, keyed on a tag value a {@code GROUP BY} returned,
 * so a value with no rows has NO SERIES at all and a panel reads "No data"
 * rather than 0. And the refresh loop only calls {@code set} on the tags that
 * tick returned, so a value that leaves the table keeps its last non-zero
 * reading for ever, indistinguishable from a live one. A rate panel or an alert
 * can work with neither.
 *
 * <p><b>The gauges are kept.</b> Three of the five alert rules provisioned by
 * {@code infra/dcre-infra/scripts/grafana-alerts.sh} query them by name
 * ({@code dcre-dag-failed} and {@code dcre-telemetry-silent} on
 * {@code agt_file_arrivals_total}, {@code dcre-tech-failed} on
 * {@code agt_stage_outcomes_total}); the other two name the SLA gauges. Checked
 * 2026-09-11 in that script, which itself asserts a rule count of 5. Renaming a
 * name three live rules select on buys breakage for a cosmetic gain.
 *
 * <p><b>Increment only where the write happened.</b> Every caller increments
 * inside the branch where its write actually landed, never on "the call
 * returned": {@code insertOutcome} is {@code ON CONFLICT DO NOTHING} and the
 * arrival writes are CAS updates, so a no-op is the common case rather than the
 * rare one.
 */
@ApplicationScoped
public class EventCounters {

    private static final Logger LOG = Logger.getLogger(EventCounters.class);

    /** Monotonic count of stage_outcome rows actually written, by outcome class. */
    static final String OUTCOME_EVENTS = "agt_stage_outcome_events_total";

    /** Monotonic count of file_arrival status TRANSITIONS that actually fired, by target status. */
    static final String ARRIVAL_TRANSITIONS = "agt_arrival_transitions_total";

    /**
     * The arrival statuses a TRANSITION can actually produce, which is NOT
     * {@code ArrivalStatus.values()}.
     *
     * <p>{@code file_arrival.status} has THREE writers, not two: the two CAS
     * updates {@code ArrivalRepo.transitionArrival} and
     * {@code ArrivalRepo.markDagFailed}, and {@code ArrivalRepo.insertArrival},
     * which binds a status directly at INSERT and which a search for
     * {@code UPDATE file_arrival SET status} cannot see.
     *
     * <p>{@code CLAIMED} is minted ONLY by {@code insertArrival}
     * ({@code ArrivalService:135}) and is never a transition target, so
     * pre-registering it would publish a series structurally guaranteed to sit at
     * zero for the life of the process, which an operator cannot tell from a
     * broken increment. Pre-registering a value the code cannot produce is the
     * same defect as failing to publish one it can.
     *
     * <p>Deriving this set from {@code values()} is therefore wrong, and so is
     * any future edit that adds a constant here without a transition that reaches
     * it. {@code EventCountersTest} asserts CLAIMED has no series.
     */
    static final Set<ArrivalStatus> TRANSITION_TARGETS = Collections.unmodifiableSet(EnumSet.of(
            ArrivalStatus.QUARANTINED,
            ArrivalStatus.DAG_RUNNING,
            ArrivalStatus.DAG_COMPLETE,
            ArrivalStatus.DAG_FAILED));

    @Inject
    MeterRegistry registry;

    /**
     * Registers every producible tag value at zero. A counter that first appears
     * on its first increment has exactly the "No data versus 0" hole these
     * counters exist to close, so the label space is published up front: all
     * seven {@link Outcome} constants (the outcome seam does
     * {@code Outcome.valueOf}, so every one of them is reachable) and the four
     * transition targets above.
     */
    @PostConstruct
    void init() {
        guarded(() -> {
            for (Outcome outcome : Outcome.values()) {
                outcomeCounter(outcome);
            }
            for (ArrivalStatus status : TRANSITION_TARGETS) {
                arrivalCounter(status);
            }
        });
    }

    /** @param outcome the class of the stage_outcome row that was just written. */
    public void recordOutcomeEvent(final Outcome outcome) {
        guarded(() -> outcomeCounter(outcome).increment());
    }

    /**
     * @param status the TARGET status of a transition that actually fired. An
     *     arrival minted directly at {@code QUARANTINED} by
     *     {@code ArrivalService.quarantine()} is an insert, not a transition, and
     *     is deliberately not counted here: see the class note on
     *     {@code ARRIVAL_TRANSITIONS} and the README's Metrics table.
     */
    public void recordArrivalTransition(final ArrivalStatus status) {
        guarded(() -> arrivalCounter(status).increment());
    }

    private Counter outcomeCounter(final Outcome outcome) {
        return registry.counter(OUTCOME_EVENTS, "outcome", outcome.name());
    }

    private Counter arrivalCounter(final ArrivalStatus status) {
        return registry.counter(ARRIVAL_TRANSITIONS, "status", status.name());
    }

    /**
     * Metrics are best-effort and never disturb a control loop. This is the same
     * invariant {@code MetricsService.count} declares for the scanning half, and
     * it has to be restated here because these calls sit INSIDE the DAG,
     * reconcile and orphan loops: an unguarded registry throw at the DagEngine
     * site is caught by that loop's own handler and logged as "advance DAG
     * failed", which misattributes a metrics fault to the work.
     */
    private void guarded(final Runnable work) {
        try {
            work.run();
        } catch (RuntimeException e) {
            LOG.debugf("metric update skipped: %s", e.toString());
        }
    }
}
