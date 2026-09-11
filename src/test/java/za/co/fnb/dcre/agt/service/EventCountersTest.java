package za.co.fnb.dcre.agt.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Statistic;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import za.co.fnb.dcre.agt.domain.ArrivalStatus;
import za.co.fnb.dcre.agt.domain.Outcome;
import za.co.fnb.dcre.agt.repo.ArrivalRepo;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link EventCounters} is a DIFFERENT instrument from the {@link MetricsService}
 * gauges over the same tables, and these tests assert the properties that differ,
 * by reading both out of one registry the way an exporter walks it, never by
 * reading back a field the test just set.
 *
 * <p>Nothing here boots Quarkus or touches a database: the gauge path is driven
 * against a fake {@link DataSource} whose rows the test can purge, which is the
 * only way to measure "the table emptied and the series did not" in a unit test.
 */
class EventCountersTest {

    private static final String OUTCOME_GAUGE = "agt_stage_outcomes_total";
    private static final String OUTCOME_COUNTER = "agt_stage_outcome_events_total";
    private static final String ARRIVAL_COUNTER = "agt_arrival_transitions_total";

    /** The one query whose rows this fake serves; every other query answers empty. */
    private static final String OUTCOME_SQL = "SELECT outcome, count(*) FROM stage_outcome GROUP BY 1";

    // ---------------------------------------------------------------- counters

    @Test
    void countsEveryOutcomeEventEvenWhenTheSameValueRepeats() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        EventCounters counters = countersOn(registry);

        counters.recordOutcomeEvent(Outcome.BUSINESS_ACCEPTED);
        counters.recordOutcomeEvent(Outcome.BUSINESS_ACCEPTED);
        counters.recordOutcomeEvent(Outcome.TECH_FAILED);

        assertEquals(2.0, scrapeCounter(registry, OUTCOME_COUNTER, "outcome", "BUSINESS_ACCEPTED"));
        assertEquals(1.0, scrapeCounter(registry, OUTCOME_COUNTER, "outcome", "TECH_FAILED"));
    }

    @Test
    void countsEveryArrivalTransitionUnderItsOwnStatus() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        EventCounters counters = countersOn(registry);

        counters.recordArrivalTransition(ArrivalStatus.DAG_RUNNING);
        counters.recordArrivalTransition(ArrivalStatus.DAG_COMPLETE);
        counters.recordArrivalTransition(ArrivalStatus.DAG_COMPLETE);

        assertEquals(1.0, scrapeCounter(registry, ARRIVAL_COUNTER, "status", "DAG_RUNNING"));
        assertEquals(2.0, scrapeCounter(registry, ARRIVAL_COUNTER, "status", "DAG_COMPLETE"));
    }

    // -------------------------------------------- the pre-registered label space

    /**
     * Defect half one. A gauge built by a GROUP BY has no series for a value the
     * table has never held, so a panel reads "No data" and a value that has never
     * occurred is indistinguishable from a broken query. Every PRODUCIBLE counter
     * series exists at zero from startup, so zero reads as zero.
     */
    @Test
    void everyProducibleValueHasASeriesAtZeroBeforeAnythingHappens() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        EventCounters counters = countersOn(registry);
        counters.init();

        // Every Outcome is reachable: the seam does Outcome.valueOf(text).
        for (Outcome outcome : Outcome.values()) {
            assertEquals(0.0, scrapeCounter(registry, OUTCOME_COUNTER, "outcome", outcome.name()),
                    "no counter series for outcome " + outcome);
        }
        for (ArrivalStatus status : EventCounters.TRANSITION_TARGETS) {
            assertEquals(0.0, scrapeCounter(registry, ARRIVAL_COUNTER, "status", status.name()),
                    "no counter series for transition target " + status);
        }
        assertEquals(Outcome.values().length + EventCounters.TRANSITION_TARGETS.size(),
                registry.getMeters().stream().filter(m -> m instanceof Counter).count(),
                "startup must register exactly one counter series per producible value");
    }

    /**
     * The hazard this rule exists to stop, and it is not hypothetical: deriving
     * the label space from {@code ArrivalStatus.values()} publishes
     * {@code status="CLAIMED"}, which NO transition can ever produce, because
     * CLAIMED is minted only by {@code ArrivalRepo.insertArrival}. A series
     * structurally pinned at zero is indistinguishable from a broken increment,
     * which is the same unreadable number the gauges already give.
     */
    @Test
    void claimedIsNotPublishedBecauseNoTransitionCanEverProduceIt() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        EventCounters counters = countersOn(registry);
        counters.init();

        assertTrue(EventCounters.TRANSITION_TARGETS.stream()
                        .noneMatch(s -> s == ArrivalStatus.CLAIMED),
                "CLAIMED must not be a declared transition target");
        assertNull(registry.find(ARRIVAL_COUNTER).tag("status", "CLAIMED").counter(),
                "CLAIMED is minted by insertArrival, never by a transition: publishing it would"
                        + " create a series guaranteed to read zero for the life of the process");
        assertEquals(ArrivalStatus.values().length - 1, EventCounters.TRANSITION_TARGETS.size(),
                "exactly one ArrivalStatus (CLAIMED) is insert-only");
    }

    // ------------------------------------------------- the gauge/counter contrast

    /**
     * Defect half two, and the property that makes a counter a counter. The gauge
     * loop only calls set on the tags THIS tick returned, so a value that leaves
     * the table keeps its last non-zero reading for ever. The counter counts
     * events, so purging the table cannot move it down.
     */
    @Test
    void theCounterHoldsWhileTheGaugeGoesStaleWhenTheTableIsPurged() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        List<Object[]> table = new ArrayList<>();
        List<String> unexpectedJdbc = new CopyOnWriteArrayList<>();
        EventCounters counters = countersOn(registry);
        MetricsService gauges = gaugesOn(registry, table, unexpectedJdbc);
        counters.init();

        table.add(new Object[]{"BUSINESS_ACCEPTED", 3L});
        gauges.refresh();
        counters.recordOutcomeEvent(Outcome.BUSINESS_ACCEPTED);

        // Positive control: MetricsService.count swallows every exception, so a
        // broken fake would look like a missing gauge. This assertion is what
        // distinguishes "the gauge read the table" from "the fake threw".
        assertTrue(unexpectedJdbc.isEmpty(), "fake DataSource was asked for " + unexpectedJdbc);
        assertEquals(3.0, scrapeGauge(registry, OUTCOME_GAUGE, "outcome", "BUSINESS_ACCEPTED"),
                "the gauge must have read the fake table, else the rest proves nothing");
        assertEquals(1.0, scrapeCounter(registry, OUTCOME_COUNTER, "outcome", "BUSINESS_ACCEPTED"));

        table.clear();
        gauges.refresh();

        assertEquals(3.0, scrapeGauge(registry, OUTCOME_GAUGE, "outcome", "BUSINESS_ACCEPTED"),
                "the gauge is expected to stay stale; that is the defect being worked around");
        assertEquals(1.0, scrapeCounter(registry, OUTCOME_COUNTER, "outcome", "BUSINESS_ACCEPTED"),
                "a counter must not go down when the table it shadows is purged");

        counters.recordOutcomeEvent(Outcome.BUSINESS_ACCEPTED);
        assertEquals(2.0, scrapeCounter(registry, OUTCOME_COUNTER, "outcome", "BUSINESS_ACCEPTED"),
                "the counter must keep advancing on events after the purge");
        assertEquals(3.0, scrapeGauge(registry, OUTCOME_GAUGE, "outcome", "BUSINESS_ACCEPTED"),
                "the gauge still reports the pre-purge scan, so the two instruments now disagree");
        assertTrue(unexpectedJdbc.isEmpty(), "fake DataSource was asked for " + unexpectedJdbc);
    }

    /** The gauge half of defect one, measured rather than asserted from the source. */
    @Test
    void aGaugeValueTheTableNeverHeldHasNoSeriesAtAll() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        List<Object[]> table = new ArrayList<>();
        EventCounters counters = countersOn(registry);
        MetricsService gauges = gaugesOn(registry, table, new CopyOnWriteArrayList<>());
        counters.init();

        table.add(new Object[]{"BUSINESS_ACCEPTED", 1L});
        gauges.refresh();

        assertNotNull(registry.find(OUTCOME_GAUGE).tag("outcome", "BUSINESS_ACCEPTED").gauge());
        assertNull(registry.find(OUTCOME_GAUGE).tag("outcome", "TECH_FAILED").gauge(),
                "a GROUP BY gauge cannot report zero for a value the table never held");
        assertEquals(0.0, scrapeCounter(registry, OUTCOME_COUNTER, "outcome", "TECH_FAILED"),
                "the counter reports that same value as a real zero");
    }

    // ------------------------------------------------- increment only on success

    /**
     * The constraint that makes the number trustworthy. transitionArrival is a CAS
     * UPDATE: when the arrival is not in the expected state it matches zero rows,
     * changes nothing and throws NOTHING. Counting on "it returned" would record a
     * transition that never happened, every 2s, for every arrival another
     * incarnation had already moved.
     */
    @Test
    void anArrivalTransitionThatDidNotFireIsNotCounted() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        EventCounters counters = countersOn(registry);
        counters.init();

        DagEngine engine = engineOn(counters, false);
        engine.transition(UUID.randomUUID(), ArrivalStatus.CLAIMED, ArrivalStatus.DAG_RUNNING);

        assertEquals(0.0, scrapeCounter(registry, ARRIVAL_COUNTER, "status", "DAG_RUNNING"),
                "a CAS that matched no row must not move the counter");
    }

    @Test
    void anArrivalTransitionThatFiredIsCountedOnce() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        EventCounters counters = countersOn(registry);
        counters.init();

        DagEngine engine = engineOn(counters, true);
        engine.transition(UUID.randomUUID(), ArrivalStatus.CLAIMED, ArrivalStatus.DAG_RUNNING);

        assertEquals(1.0, scrapeCounter(registry, ARRIVAL_COUNTER, "status", "DAG_RUNNING"));
        assertNull(registry.find(ARRIVAL_COUNTER).tag("status", "CLAIMED").counter(),
                "the counter is tagged with the TARGET status, never the source");
    }

    // ------------------------------------------------------- best-effort metrics

    /**
     * These increments sit INSIDE the DAG, reconcile and orphan control loops,
     * each of which wraps its body in a catch-all that logs the failure as work
     * having failed. An unguarded registry throw would therefore be reported as
     * "advance DAG failed" and misattribute a metrics fault to the arrival.
     * MetricsService declares this invariant for its scanning half; the counters
     * have to honour it too.
     */
    @Test
    void aRegistryFailureNeverEscapesIntoAControlLoop() {
        MeterRegistry exploding = new SimpleMeterRegistry() {
            @Override
            protected Counter newCounter(Meter.Id id) {
                throw new IllegalStateException("registry unavailable");
            }
        };
        EventCounters counters = countersOn(exploding);

        assertDoesNotThrow(counters::init, "startup pre-registration must be best-effort");
        assertDoesNotThrow(() -> counters.recordOutcomeEvent(Outcome.TECH_FAILED));
        assertDoesNotThrow(() -> counters.recordArrivalTransition(ArrivalStatus.DAG_FAILED));

        // And the guard must not swallow anything on a healthy registry.
        SimpleMeterRegistry healthy = new SimpleMeterRegistry();
        EventCounters ok = countersOn(healthy);
        ok.recordOutcomeEvent(Outcome.TECH_FAILED);
        assertEquals(1.0, scrapeCounter(healthy, OUTCOME_COUNTER, "outcome", "TECH_FAILED"),
                "the guard must not turn a working increment into a no-op");
    }

    // ------------------------------------------------------------------ helpers

    /** Reads a counter the way an exporter does: off the registry, through its measurements. */
    private static double scrapeCounter(MeterRegistry registry, String name, String tagKey, String tagValue) {
        Counter counter = registry.find(name).tag(tagKey, tagValue).counter();
        assertNotNull(counter, "no series " + name + "{" + tagKey + "=" + tagValue + "}");
        return measurement(counter.measure(), Statistic.COUNT);
    }

    private static double scrapeGauge(MeterRegistry registry, String name, String tagKey, String tagValue) {
        Gauge gauge = registry.find(name).tag(tagKey, tagValue).gauge();
        assertNotNull(gauge, "no series " + name + "{" + tagKey + "=" + tagValue + "}");
        return measurement(gauge.measure(), Statistic.VALUE);
    }

    private static double measurement(Iterable<Measurement> measurements, Statistic statistic) {
        return StreamSupport.stream(measurements.spliterator(), false)
                .filter(m -> m.getStatistic() == statistic)
                .mapToDouble(Measurement::getValue)
                .sum();
    }

    /** Both beans take their collaborators by field injection, so a unit test
     *  assembles them the way the container would. */
    private static EventCounters countersOn(MeterRegistry registry) {
        EventCounters counters = new EventCounters();
        counters.registry = registry;
        return counters;
    }

    private static MetricsService gaugesOn(MeterRegistry registry, List<Object[]> table,
                                           List<String> unexpectedJdbc) {
        MetricsService svc = new MetricsService();
        svc.registry = registry;
        svc.ds = fakeStageOutcomeTable(table, unexpectedJdbc);
        svc.lease = Mockito.mock(LeaseService.class);
        return svc;
    }

    private static DagEngine engineOn(EventCounters counters, boolean casFires) {
        ArrivalRepo repo = Mockito.mock(ArrivalRepo.class);
        Mockito.when(repo.transitionArrival(Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(casFires);
        DagEngine engine = new DagEngine();
        engine.arrivalRepo = repo;
        engine.counters = counters;
        return engine;
    }

    /**
     * A DataSource serving exactly the rows the test currently holds for the
     * stage_outcome GROUP BY, and empty for every other query MetricsService runs.
     * Only the JDBC methods that class actually calls are implemented; any other
     * call is recorded AND thrown, because MetricsService.count catches every
     * exception and a silently wrong fake would read as a clean result.
     */
    private static DataSource fakeStageOutcomeTable(List<Object[]> table, List<String> unexpected) {
        return (DataSource) proxy(DataSource.class, (self, method, args) -> switch (method.getName()) {
            case "getConnection" -> fakeConnection(table, unexpected);
            default -> common(DataSource.class, self, method, args, unexpected);
        });
    }

    private static Connection fakeConnection(List<Object[]> table, List<String> unexpected) {
        return (Connection) proxy(Connection.class, (self, method, args) -> switch (method.getName()) {
            case "createStatement" -> fakeStatement(table, unexpected);
            default -> common(Connection.class, self, method, args, unexpected);
        });
    }

    private static Statement fakeStatement(List<Object[]> table, List<String> unexpected) {
        return (Statement) proxy(Statement.class, (self, method, args) -> switch (method.getName()) {
            // Snapshot at query time, exactly as a real cursor would.
            case "executeQuery" -> fakeResultSet(
                    OUTCOME_SQL.equals(args[0]) ? List.copyOf(table) : List.<Object[]>of(), unexpected);
            default -> common(Statement.class, self, method, args, unexpected);
        });
    }

    private static ResultSet fakeResultSet(List<Object[]> rows, List<String> unexpected) {
        int[] cursor = {-1};
        return (ResultSet) proxy(ResultSet.class, (self, method, args) -> switch (method.getName()) {
            case "next" -> ++cursor[0] < rows.size();
            case "getString" -> rows.get(cursor[0])[0];
            case "getLong" -> ((Number) rows.get(cursor[0])[1]).longValue();
            default -> common(ResultSet.class, self, method, args, unexpected);
        });
    }

    private static Object common(Class<?> iface, Object self, java.lang.reflect.Method method,
                                 Object[] args, List<String> unexpected) {
        return switch (method.getName()) {
            case "close" -> null;
            case "toString" -> "fake" + iface.getSimpleName();
            case "hashCode" -> System.identityHashCode(self);
            case "equals" -> self == args[0];
            default -> {
                unexpected.add(iface.getSimpleName() + "." + method.getName());
                throw new UnsupportedOperationException(iface.getSimpleName() + "." + method.getName());
            }
        };
    }

    private static Object proxy(Class<?> iface, java.lang.reflect.InvocationHandler handler) {
        return Proxy.newProxyInstance(EventCountersTest.class.getClassLoader(),
                new Class<?>[]{iface}, handler);
    }
}
