package za.co.fnb.dcre.agt.service;

import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.scheduler.Scheduled;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Ledger-derived gauges for the dcre-agt dashboard (agt_* series). Values are
 * re-read from agt_ops so a restarted AGT reports truth, not in-memory state.
 */
@ApplicationScoped
public class MetricsService {

    @Inject
    DataSource ds;

    @Inject
    MeterRegistry registry;

    @Inject
    LeaseService lease;

    private final Map<String, AtomicLong> gauges = new ConcurrentHashMap<>();
    private final AtomicLong leaseHeld = new AtomicLong();

    @PostConstruct
    void init() {
        registry.gauge("agt_lease_held", leaseHeld);
    }

    @Scheduled(every = "10s", concurrentExecution = io.quarkus.scheduler.Scheduled.ConcurrentExecution.SKIP)
    void refresh() {
        leaseHeld.set(lease.holdsLease() ? 1 : 0);
        count("SELECT status, count(*) FROM file_arrival GROUP BY 1", "agt_file_arrivals_total", "status");
        // SCRUM-107: AGE, not just count. The emission gate makes DAG_RUNNING a
        // legitimate long-lived state for a warehoused arrival, so a COUNT can no
        // longer distinguish "warehoused for nine days" from "stranded forever".
        // Only the age of the OLDEST one can, and it is the number worth alerting on.
        count("SELECT 'oldest', COALESCE(MAX(EXTRACT(EPOCH FROM (now() - arrived_at))), 0)"
                + " FROM file_arrival WHERE status = 'DAG_RUNNING'",
                "agt_dag_running_oldest_age_seconds", "scope");
        count("SELECT status, count(*) FROM launch_intent GROUP BY 1", "agt_launch_intents_total", "status");
        count("SELECT outcome, count(*) FROM stage_outcome GROUP BY 1", "agt_stage_outcomes_total", "outcome");
    }

    private void count(String sql, String metric, String tagKey) {
        try (Connection c = ds.getConnection(); Statement s = c.createStatement();
             ResultSet r = s.executeQuery(sql)) {
            while (r.next()) {
                String tag = r.getString(1);
                long value = r.getLong(2);
                gauges.computeIfAbsent(metric + ":" + tag, k -> {
                    AtomicLong holder = new AtomicLong();
                    registry.gauge(metric, io.micrometer.core.instrument.Tags.of(tagKey, tag), holder);
                    return holder;
                }).set(value);
            }
        } catch (Exception e) {
            // metrics are best-effort; never disturb the control loops
        }
    }
}
