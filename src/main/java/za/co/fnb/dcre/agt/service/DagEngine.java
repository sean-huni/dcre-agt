package za.co.fnb.dcre.agt.service;

import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import za.co.fnb.dcre.agt.domain.ArrivalStatus;
import za.co.fnb.dcre.agt.domain.FileArrival;
import za.co.fnb.dcre.agt.domain.Outcome;
import za.co.fnb.dcre.agt.domain.Stage;
import za.co.fnb.dcre.agt.repo.ArrivalRepo;
import za.co.fnb.dcre.agt.repo.IntentRepo;
import za.co.fnb.dcre.agt.repo.OutcomeRepo;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Level-triggered DAG engine over the ledgers. Static Collections DAG (M1):
 * CRR -> CTV -> [CDE, CIR]; CDE -> CRW. A BUSINESS_FILE_FATAL predecessor
 * routes to CIR only (whole-file NACK path, R-19/SPEC-DAG section 3).
 * M4 adds the fint-resp route: a single reader stage picked by filename token.
 * Pure decision logic lives in computeLaunches() for unit testing.
 */
@ApplicationScoped
public class DagEngine {

    private static final Logger LOG = Logger.getLogger(DagEngine.class);

    // R-37: CRW is a clock-driven Process-Date Executor, not a DAG successor.
    static final Map<Stage, Set<Stage>> EDGES = new EnumMap<>(Map.of(
            Stage.CRR, EnumSet.of(Stage.CTV),
            Stage.CTV, EnumSet.of(Stage.CDE, Stage.CIR)
    ));
    static final Set<Stage> TERMINAL = EnumSet.of(Stage.CDE, Stage.CIR);

    /** fint-resp filename token -> reader stage; empty = unknown token (fail closed). */
    public static java.util.Optional<Stage> fintRespStage(String filename) {
        if (filename.contains("_ISR")) {
            return java.util.Optional.of(Stage.IXR);
        }
        if (filename.contains("_SBSR")) {
            return java.util.Optional.of(Stage.SXR);
        }
        if (filename.contains("_PBSR")) {
            return java.util.Optional.of(Stage.PXR);
        }
        return java.util.Optional.empty();
    }

    /** First stage for a CLAIMED arrival; empty = quarantine (fail closed). */
    static java.util.Optional<Stage> initialStage(String route, String filename) {
        return ArrivalService.ROUTE_FINT_RESP.equals(route)
                ? fintRespStage(filename)
                : java.util.Optional.of(Stage.CRR);
    }

    @Inject
    ArrivalRepo arrivalRepo;

    @Inject
    IntentRepo intentRepo;

    @Inject
    OutcomeRepo outcomeRepo;

    @Inject
    LeaseService lease;

    @Inject
    JobLauncher launcher;

    @Scheduled(every = "2s", concurrentExecution = io.quarkus.scheduler.Scheduled.ConcurrentExecution.SKIP)
    void tick() {
        if (!lease.holdsLease()) {
            return;
        }
        for (FileArrival arrival : arrivalRepo.arrivalsByStatus(ArrivalStatus.CLAIMED)) {
            try {
                if (arrival.claimedPath() == null) {
                    LOG.warnf("arrival %s CLAIMED without claimed_path: not launching (F21)", arrival.id());
                    continue;
                }
                java.util.Optional<Stage> first = initialStage(arrival.routeId(), arrival.physicalFilename());
                if (first.isEmpty()) {
                    // Unknown fint-resp token never launches (fail closed, F13).
                    arrivalRepo.transitionArrival(arrival.id(), ArrivalStatus.CLAIMED, ArrivalStatus.QUARANTINED);
                    LOG.warnf("QUARANTINED %s: no stage for %s on route %s",
                            arrival.id(), arrival.physicalFilename(), arrival.routeId());
                    continue;
                }
                launcher.launch(arrival.id(), first.get());
                arrivalRepo.transitionArrival(arrival.id(), ArrivalStatus.CLAIMED, ArrivalStatus.DAG_RUNNING);
            } catch (Exception e) {
                LOG.warnf("start DAG for %s failed: %s", arrival.id(), e.getMessage());
            }
        }
        for (FileArrival arrival : arrivalRepo.arrivalsByStatus(ArrivalStatus.DAG_RUNNING)) {
            try {
                Map<Stage, Outcome> outcomes = outcomeRepo.outcomesForArrival(arrival.id());
                Set<Stage> intended = EnumSet.noneOf(Stage.class);
                intentRepo.intentsForArrival(arrival.id()).forEach(i -> intended.add(i.stage()));

                if (!lease.holdsLease()) {
                    return; // re-check before side effects (F7)
                }
                for (Stage next : computeLaunches(arrival.routeId(), arrival.physicalFilename(), outcomes, intended)) {
                    launcher.launch(arrival.id(), next);
                }
                terminalState(arrival.routeId(), outcomes).ifPresent(
                        s -> arrivalRepo.transitionArrival(arrival.id(), ArrivalStatus.DAG_RUNNING, s));
            } catch (Exception e) {
                LOG.warnf("advance DAG for %s failed: %s", arrival.id(), e.getMessage()); // one poisoned arrival never wedges the loop (F10)
            }
        }
    }

    /** Route dispatch: onhost-req keeps the static M1 DAG; fint-resp is the
     *  single token-picked reader (re-seeded level-triggered, intents dedupe). */
    public static Set<Stage> computeLaunches(String route, String filename,
                                             Map<Stage, Outcome> outcomes, Set<Stage> intended) {
        if (!ArrivalService.ROUTE_FINT_RESP.equals(route)) {
            return computeLaunches(outcomes, intended);
        }
        Set<Stage> launches = EnumSet.noneOf(Stage.class);
        fintRespStage(filename)
                .filter(stage -> !intended.contains(stage) && !outcomes.containsKey(stage))
                .ifPresent(launches::add);
        return launches;
    }

    /** Successor stages to launch now, given recorded outcomes and existing intents (onhost-req). */
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

    /** fint-resp terminal: the single reader stage BUSINESS_ACCEPTED completes
     *  the DAG; anything else stays open for the reconciler (fail closed). */
    public static java.util.Optional<ArrivalStatus> terminalState(String route, Map<Stage, Outcome> outcomes) {
        if (!ArrivalService.ROUTE_FINT_RESP.equals(route)) {
            return terminalState(outcomes);
        }
        boolean readerAccepted = outcomes.values().stream().anyMatch(o -> o == Outcome.BUSINESS_ACCEPTED);
        return readerAccepted ? java.util.Optional.of(ArrivalStatus.DAG_COMPLETE) : java.util.Optional.empty();
    }

    /** Terminal arrival state, when reached (onhost-req). The responder (CIR) must itself be
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
