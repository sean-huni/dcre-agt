package za.co.fnb.dcre.agt.service;

import io.quarkus.scheduler.Scheduled;
import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.domain.Stage;
import za.co.fnb.dcre.agt.repo.CollectionsReadRepo;
import za.co.fnb.dcre.agt.repo.CollectionsReadRepo.DueParent;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * SCRUM-55 debounce/immediate report trigger: scans the collections-side
 * prg_report_due view and launches one PRG IMMEDIATE window per client
 * carrying the due parents. Level-triggered like PrgScheduler; repeated scans
 * of a still-due parent mint distinct window keys, and PRG's delivery-ledger
 * guard turns the extra runs into no-ops.
 */
@ApplicationScoped
public class ReportTrigger {

    private static final Logger LOG = Logger.getLogger(ReportTrigger.class);

    @Inject
    AgtConfig config;

    @Inject
    LeaseService lease;

    @Inject
    CollectionsReadRepo collections;

    @Inject
    JobLauncher launcher;

    @RunOnVirtualThread
    @Scheduled(every = "{dcre.agt.report-scan-seconds}",
            concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    public void tick() {
        if (!lease.holdsLease() || !config.launchEnabled() || config.prgImage().isEmpty()) {
            return;
        }
        final List<DueParent> due = collections.reportDue();
        if (due.isEmpty()) {
            return;
        }
        final long epochSec = Instant.now().getEpochSecond();
        final Map<String, List<String>> byClient = new TreeMap<>();
        for (final DueParent parent : due) {
            byClient.computeIfAbsent(parent.client(), c -> new ArrayList<>()).add(parent.sourceMsgId());
        }
        byClient.forEach((client, parents) -> launchImmediate(client, parents, epochSec));
    }

    private void launchImmediate(final String client, final List<String> parents, final long epochSec) {
        final String joined = parents.stream().distinct().collect(Collectors.joining(","));
        LOG.infof("report-due scan: PRG IMMEDIATE for %s parents=%s", client, joined);
        launcher.launchClock(Stage.PRG, client + "-imm-" + epochSec, List.of(
                "client=" + client,
                "window=imm-" + epochSec,
                "report.type=IMMEDIATE,java.lang.String,false",
                "parents=" + joined + ",java.lang.String,false"));
    }
}
