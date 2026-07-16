package za.co.fnb.dcre.agt.service;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.repo.CollectionsReadRepo;
import za.co.fnb.dcre.agt.repo.CollectionsReadRepo.SlaPending;

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
 * re-reads the collections-side prg_sla_pending view and republishes truth:
 * per-client gauges dcre_sla_pending_amber / dcre_sla_pending_red (stale
 * clients drop to 0, MetricsService pattern) plus one WARN per (e2e, level)
 * per batch. Pure observation like MetricsService, so no lease gate: the
 * escalation (email Fintegrate) stays an ops runbook action driven by the
 * Grafana alert on the red gauge (Task 14).
 */
@ApplicationScoped
public class SlaMonitor {

    private static final Logger LOG = Logger.getLogger(SlaMonitor.class);
    private static final String AMBER_METRIC = "dcre_sla_pending_amber";
    private static final String RED_METRIC = "dcre_sla_pending_red";

    @Inject
    AgtConfig config;

    @Inject
    CollectionsReadRepo collections;

    @Inject
    MeterRegistry registry;

    /** metric:client to its registered gauge holder (MetricsService pattern). */
    private final Map<String, AtomicLong> gauges = new ConcurrentHashMap<>();

    @RunOnVirtualThread
    @Scheduled(every = "{dcre.agt.sla-scan-seconds}",
            concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    public void tick() {
        final List<SlaPending> pending;
        try {
            pending = collections.slaCounts(config.slaAmberHours());
        } catch (RuntimeException e) {
            // Best-effort observation (MetricsService precedent): a PRG-owned
            // view that is momentarily absent must never kill the scheduler.
            LOG.warnf(e, "sla scan skipped: prg_sla_pending unreadable");
            return;
        }
        final Map<String, Long> amber = new HashMap<>();
        final Map<String, Long> red = new HashMap<>();
        final Set<String> warned = new HashSet<>();
        for (final SlaPending row : pending) {
            final boolean breach = row.ageHours() >= config.slaRedHours();
            (breach ? red : amber).merge(row.client(), 1L, Long::sum);
            final String level = breach ? "RED" : "AMBER";
            if (warned.add(row.e2e() + "|" + level)) {
                // Locale.ROOT: the line feeds log-based alerting, so the
                // decimal separator must never follow the host locale.
                LOG.warnf("sla stage=FINT client=%s e2e=%s ageHours=%s level=%s",
                        row.client(), row.e2e(),
                        String.format(Locale.ROOT, "%.1f", row.ageHours()), level);
            }
        }
        publish(AMBER_METRIC, amber);
        publish(RED_METRIC, red);
    }

    /** Zero every known holder for the metric, then set the fresh counts. */
    private void publish(final String metric, final Map<String, Long> counts) {
        gauges.forEach((key, holder) -> {
            if (key.startsWith(metric + ":")) {
                holder.set(0);
            }
        });
        counts.forEach((client, n) -> gauges.computeIfAbsent(metric + ":" + client, k -> {
            final AtomicLong holder = new AtomicLong();
            registry.gauge(metric, Tags.of("client", client), holder);
            return holder;
        }).set(n));
    }
}
