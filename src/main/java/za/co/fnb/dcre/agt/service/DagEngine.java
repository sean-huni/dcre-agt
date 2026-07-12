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

    @Scheduled(every = "2s")
    void tick() {
        if (!lease.holdsLease()) {
            return;
        }
        for (FileArrival arrival : repo.arrivalsByStatus(ArrivalStatus.CLAIMED)) {
            launcher.launch(arrival.id(), Stage.CRR);
            repo.updateArrivalStatus(arrival.id(), ArrivalStatus.DAG_RUNNING);
        }
        for (FileArrival arrival : repo.arrivalsByStatus(ArrivalStatus.DAG_RUNNING)) {
            Map<Stage, Outcome> outcomes = repo.outcomesForArrival(arrival.id());
            Set<Stage> intended = EnumSet.noneOf(Stage.class);
            repo.intentsForArrival(arrival.id()).forEach(i -> intended.add(i.stage()));

            for (Stage next : computeLaunches(outcomes, intended)) {
                launcher.launch(arrival.id(), next);
            }
            terminalState(outcomes).ifPresent(s -> repo.updateArrivalStatus(arrival.id(), s));
        }
    }

    /** Successor stages to launch now, given recorded outcomes and existing intents. */
    public static Set<Stage> computeLaunches(Map<Stage, Outcome> outcomes, Set<Stage> intended) {
        Set<Stage> launches = EnumSet.noneOf(Stage.class);
        for (Map.Entry<Stage, Outcome> done : outcomes.entrySet()) {
            switch (done.getValue()) {
                case BUSINESS_ACCEPTED, BUSINESS_PARTIAL -> {
                    for (Stage next : EDGES.getOrDefault(done.getKey(), Set.of())) {
                        if (!intended.contains(next)) {
                            launches.add(next);
                        }
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

    /** Terminal arrival state, when reached. */
    public static java.util.Optional<ArrivalStatus> terminalState(Map<Stage, Outcome> outcomes) {
        if (outcomes.getOrDefault(Stage.CIR, null) != null
                && outcomes.values().stream().anyMatch(o -> o == Outcome.BUSINESS_FILE_FATAL)) {
            return java.util.Optional.of(ArrivalStatus.DAG_FAILED);
        }
        boolean allTerminalDone = TERMINAL.stream()
                .allMatch(s -> isBusinessDone(outcomes.get(s)));
        return allTerminalDone ? java.util.Optional.of(ArrivalStatus.DAG_COMPLETE) : java.util.Optional.empty();
    }

    private static boolean isBusinessDone(Outcome o) {
        return o == Outcome.BUSINESS_ACCEPTED || o == Outcome.BUSINESS_PARTIAL;
    }
}
