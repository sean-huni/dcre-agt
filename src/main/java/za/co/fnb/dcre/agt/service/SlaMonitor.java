package za.co.fnb.dcre.agt.service;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.domain.Flow;
import za.co.fnb.dcre.agt.repo.FamilyReadRepo;
import za.co.fnb.dcre.agt.repo.FamilyReadRepo.SlaPending;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * SCRUM-55 Task 13: Fintegrate SLA watchdog (20h amber / 24h red). Every tick
 * re-reads EACH reporting family's prg_sla_pending view and republishes truth:
 * per-(flow, client) gauges dcre_sla_pending_amber / dcre_sla_pending_red (stale
 * clients drop to 0, MetricsService pattern) plus one WARN per
 * (flow, client, e2e, level) per batch: the dedup key carries the FULL tuple
 * (idempotency-key rule), never a subset that lets two clients sharing an
 * e2e swallow each other's WARN. Pure observation like MetricsService, so no lease gate: the
 * escalation (email Fintegrate) stays an ops runbook action driven by the
 * Grafana alert on the red gauge (Task 14).
 *
 * <p>v1 topology: both {@code dcre_col} and {@code dcre_pay} publish a view of that
 * name, so reading one datasource left the other family's breaches invisible. The
 * gauges gained a {@code flow} tag rather than merging the two families' counts
 * under one client label, because a client can ride both flows and a merged gauge
 * cannot say which side is breaching.
 */
@ApplicationScoped
public class SlaMonitor {

    private static final Logger LOG = Logger.getLogger(SlaMonitor.class);
    private static final String AMBER_METRIC = "dcre_sla_pending_amber";
    private static final String RED_METRIC = "dcre_sla_pending_red";

    @Inject
    AgtConfig config;

    @Inject
    FamilyReadRepo families;

    @Inject
    MeterRegistry registry;

    /** metric:flow:client to its registered gauge holder (MetricsService pattern). */
    private final Map<String, AtomicLong> gauges = new ConcurrentHashMap<>();

    @RunOnVirtualThread
    @Scheduled(every = "{dcre.agt.sla-scan-seconds}",
            concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    public void tick() {
        for (final Flow flow : FamilyReadRepo.reportingFamilies()) {
            scan(flow);
        }
    }

    /** One family's SLA scan. Per family, so an unreadable view on one side never
     *  suppresses the other side's gauges (they are separate databases now). */
    private void scan(final Flow flow) {
        final List<SlaPending> pending;
        try {
            pending = families.slaCounts(flow, config.slaAmberHours());
        } catch (RuntimeException e) {
            // Best-effort observation (MetricsService precedent): a generator-owned
            // view that is momentarily absent must never kill the scheduler.
            LOG.warnf(e, "sla scan skipped for %s: prg_sla_pending unreadable", flow);
            return;
        }
        final Map<String, Long> amber = new HashMap<>();
        final Map<String, Long> red = new HashMap<>();
        final Set<String> warned = new HashSet<>();
        for (final SlaPending row : pending) {
            final boolean breach = row.ageHours() >= config.slaRedHours();
            (breach ? red : amber).merge(row.client(), 1L, Long::sum);
            final String level = breach ? "RED" : "AMBER";
            if (warned.add(row.client() + "|" + row.e2e() + "|" + level)) {
                // Locale.ROOT: the line feeds log-based alerting, so the
                // decimal separator must never follow the host locale.
                LOG.warnf("sla stage=FINT flow=%s client=%s e2e=%s ageHours=%s level=%s",
                        flow, row.client(), row.e2e(),
                        String.format(Locale.ROOT, "%.1f", row.ageHours()), level);
            }
        }
        publish(AMBER_METRIC, flow, amber);
        publish(RED_METRIC, flow, red);
    }

    /** Zero every known holder for (metric, flow), then set the fresh counts. Scoped
     *  to the flow so one family's republish never zeroes the other's gauges. */
    private void publish(final String metric, final Flow flow, final Map<String, Long> counts) {
        final String prefix = metric + ":" + flow + ":";
        gauges.forEach((key, holder) -> {
            if (key.startsWith(prefix)) {
                holder.set(0);
            }
        });
        counts.forEach((client, n) -> gauges.computeIfAbsent(prefix + client, k -> {
            final AtomicLong holder = new AtomicLong();
            registry.gauge(metric, Tags.of("flow", flow.name(), "client", client), holder);
            return holder;
        }).set(n));
    }
}
