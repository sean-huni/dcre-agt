package za.co.fnb.dcre.agt.service;

import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import za.co.fnb.dcre.agt.domain.ArrivalStatus;
import za.co.fnb.dcre.agt.domain.FileArrival;
import za.co.fnb.dcre.agt.domain.Outcome;
import za.co.fnb.dcre.agt.domain.Stage;
import za.co.fnb.dcre.agt.repo.LedgerRepo;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Level-triggered DAG engine over the ledgers. Static Collections DAG (M1):
 * CRR -> CTV -> [CDE, CIR]; CDE -> CRW. A BUSINESS_FILE_FATAL predecessor
 * routes to CIR only (whole-file NACK path, R-19/SPEC-DAG section 3).
 * Pure decision logic lives in computeLaunches() for unit testing.
 */
@ApplicationScoped
public class DagEngine {

    private static final Logger LOG = Logger.getLogger(DagEngine.class);

    static final Map<Stage, Set<Stage>> EDGES = new EnumMap<>(Map.of(
            Stage.CRR, EnumSet.of(Stage.CTV),
            Stage.CTV, EnumSet.of(Stage.CDE, Stage.CIR),
            Stage.CDE, EnumSet.of(Stage.CRW)
    ));
    static final Set<Stage> TERMINAL = EnumSet.of(Stage.CRW, Stage.CIR);

    @Inject
    LedgerRepo repo;

    @Inject
    LeaseService lease;

    @Inject
    JobLauncher launcher;

    @Scheduled(every = "2s", concurrentExecution = io.quarkus.scheduler.Scheduled.ConcurrentExecution.SKIP)
    void tick() {
        if (!lease.holdsLease()) {
            return;
        }
        for (FileArrival arrival : repo.arrivalsByStatus(ArrivalStatus.CLAIMED)) {
            try {
                if (arrival.claimedPath() == null) {
                    LOG.warnf("arrival %s CLAIMED without claimed_path: not launching (F21)", arrival.id());
                    continue;
                }
                launcher.launch(arrival.id(), Stage.CRR);
                repo.transitionArrival(arrival.id(), ArrivalStatus.CLAIMED, ArrivalStatus.DAG_RUNNING);
            } catch (Exception e) {
                LOG.warnf("start DAG for %s failed: %s", arrival.id(), e.getMessage());
            }
        }
        for (FileArrival arrival : repo.arrivalsByStatus(ArrivalStatus.DAG_RUNNING)) {
            try {
                Map<Stage, Outcome> outcomes = repo.outcomesForArrival(arrival.id());
                Set<Stage> intended = EnumSet.noneOf(Stage.class);
                repo.intentsForArrival(arrival.id()).forEach(i -> intended.add(i.stage()));

                if (!lease.holdsLease()) {
                    return; // re-check before side effects (F7)
                }
                for (Stage next : computeLaunches(outcomes, intended)) {
                    launcher.launch(arrival.id(), next);
                }
                terminalState(outcomes).ifPresent(
                        s -> repo.transitionArrival(arrival.id(), ArrivalStatus.DAG_RUNNING, s));
            } catch (Exception e) {
                LOG.warnf("advance DAG for %s failed: %s", arrival.id(), e.getMessage()); // one poisoned arrival never wedges the loop (F10)
            }
        }
    }

    /** Successor stages to launch now, given recorded outcomes and existing intents. */
    public static Set<Stage> computeLaunches(Map<Stage, Outcome> outcomes, Set<Stage> intended) {
        Set<Stage> launches = EnumSet.noneOf(Stage.class);
        for (Map.Entry<Stage, Outcome> done : outcomes.entrySet()) {
            switch (done.getValue()) {
                case BUSINESS_ACCEPTED -> {
                    for (Stage next : EDGES.getOrDefault(done.getKey(), Set.of())) {
                        if (!intended.contains(next)) {
                            launches.add(next);
                        }
                    }
                }
                case BUSINESS_PARTIAL -> {
                    // A-16 fail-closed default (ALL_OR_NOTHING): partial acceptance
                    // suppresses CDE/CRW; only the initial responder proceeds (Fugu F12).
                    if (!intended.contains(Stage.CIR)) {
                        launches.add(Stage.CIR);
                    }
                }
                case BUSINESS_FILE_FATAL -> {
                    // Whole-file NACK: the initial responder still runs; nothing else does.
                    if (!intended.contains(Stage.CIR)) {
                        launches.add(Stage.CIR);
                    }
                }
                case TECH_FAILED -> {
                    // Process death is never a business verdict (R-33): no successors;
                    // relaunch policy is the reconciler's/operator's call.
                }
            }
        }
        return launches;
    }

    /** Terminal arrival state, when reached. The responder (CIR) must itself be
     *  business-done before any terminal verdict (Fugu F6: a tech-failed CIR
     *  means the NACK never left; the arrival stays open for the reconciler). */
    public static java.util.Optional<ArrivalStatus> terminalState(Map<Stage, Outcome> outcomes) {
        boolean cirDone = isBusinessDone(outcomes.get(Stage.CIR));
        boolean anyFatal = outcomes.values().stream().anyMatch(o -> o == Outcome.BUSINESS_FILE_FATAL);
        boolean anyPartial = outcomes.values().stream().anyMatch(o -> o == Outcome.BUSINESS_PARTIAL);
        if (anyFatal) {
            return cirDone ? java.util.Optional.of(ArrivalStatus.DAG_FAILED) : java.util.Optional.empty();
        }
        if (anyPartial) {
            // Fail-closed partial (F12): CDE/CRW suppressed; CIR reporting the
            // partial verdict completes the arrival.
            return cirDone ? java.util.Optional.of(ArrivalStatus.DAG_COMPLETE) : java.util.Optional.empty();
        }
        boolean allTerminalDone = TERMINAL.stream()
                .allMatch(s -> isBusinessDone(outcomes.get(s)));
        return allTerminalDone ? java.util.Optional.of(ArrivalStatus.DAG_COMPLETE) : java.util.Optional.empty();
    }

    private static boolean isBusinessDone(Outcome o) {
        return o == Outcome.BUSINESS_ACCEPTED || o == Outcome.BUSINESS_PARTIAL;
    }
}
