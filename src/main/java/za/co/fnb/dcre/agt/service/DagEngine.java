package za.co.fnb.dcre.agt.service;

import io.quarkus.scheduler.Scheduled;
import io.smallrye.common.annotation.RunOnVirtualThread;
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
 * Level-triggered DAG engine over the ledgers. DC route (M1):
 * CRR -> CTV -> [CDE, CIR]. A BUSINESS_FILE_FATAL or BUSINESS_FILE_REJECTED
 * predecessor routes to CIR only (whole-file NACK path, R-19/R-41/SPEC-DAG
 * section 3); BUSINESS_PARTIAL fans out like ACCEPTED (R-41: PASS rows continue).
 * M4 adds the fint-resp route: a single reader stage picked by filename token.
 * M5 adds the ENDO route: CRR -> CTV -> AIS -> [CDE, CIR].
 * Pure decision logic lives in computeLaunches() for unit testing.
 */
@ApplicationScoped
public class DagEngine {

    private static final Logger LOG = Logger.getLogger(DagEngine.class);

    /** A request route's DAG shape: successor edges plus the terminal fork. */
    record RouteDag(Map<Stage, Set<Stage>> edges, Set<Stage> terminal) { }

    // R-37: CRW is a clock-driven Process-Date Executor, not a DAG successor.
    static final RouteDag DC_DAG = new RouteDag(
            new EnumMap<>(Map.of(
                    Stage.CRR, EnumSet.of(Stage.CTV),
                    Stage.CTV, EnumSet.of(Stage.CDE, Stage.CIR))),
            EnumSet.of(Stage.CDE, Stage.CIR));

    static final RouteDag ENDO_DAG = new RouteDag(
            new EnumMap<>(Map.of(
                    Stage.CRR, EnumSet.of(Stage.CTV),
                    Stage.CTV, EnumSet.of(Stage.AIS),
                    Stage.AIS, EnumSet.of(Stage.CDE, Stage.CIR))),
            EnumSet.of(Stage.CDE, Stage.CIR));

    /** R-36 route-based DAG registry: the route -> shape mapping is data, not
     *  code; new request routes add an entry here, never a new code path. */
    static final Map<String, RouteDag> DAGS = Map.of(
            ArrivalService.ROUTE_ONHOST_REQ, DC_DAG,
            ArrivalService.ROUTE_ONHOST_REQ_ENDO, ENDO_DAG);

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

    @RunOnVirtualThread
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

    /** Route dispatch: request routes resolve their DAG from the R-36 registry;
     *  fint-resp is the single token-picked reader (re-seeded level-triggered,
     *  intents dedupe). Unknown routes fall back to the DC shape (as before). */
    public static Set<Stage> computeLaunches(String route, String filename,
                                             Map<Stage, Outcome> outcomes, Set<Stage> intended) {
        if (!ArrivalService.ROUTE_FINT_RESP.equals(route)) {
            return computeLaunches(DAGS.getOrDefault(route, DC_DAG), outcomes, intended);
        }
        Set<Stage> launches = EnumSet.noneOf(Stage.class);
        fintRespStage(filename)
                .filter(stage -> !intended.contains(stage) && !outcomes.containsKey(stage))
                .ifPresent(launches::add);
        return launches;
    }

    /** Successor stages to launch now, given recorded outcomes and existing intents (onhost-req). */
    public static Set<Stage> computeLaunches(Map<Stage, Outcome> outcomes, Set<Stage> intended) {
        return computeLaunches(DC_DAG, outcomes, intended);
    }

    private static Set<Stage> computeLaunches(RouteDag dag, Map<Stage, Outcome> outcomes, Set<Stage> intended) {
        Set<Stage> launches = EnumSet.noneOf(Stage.class);
        for (Map.Entry<Stage, Outcome> done : outcomes.entrySet()) {
            switch (done.getValue()) {
                case BUSINESS_PARTIAL, BUSINESS_ACCEPTED -> {
                    // R-41: PARTIAL continues PASS rows (acceptance mode is CTV's
                    // call now); both fan out to all successors.
                    for (Stage next : dag.edges().getOrDefault(done.getKey(), Set.of())) {
                        if (!intended.contains(next)) {
                            launches.add(next);
                        }
                    }
                }
                case BUSINESS_FILE_REJECTED, BUSINESS_FILE_FATAL -> {
                    // Whole-file NACK (fatal or R-41 policy rejection): the initial
                    // responder still runs; nothing else does.
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
            return terminalState(DAGS.getOrDefault(route, DC_DAG), outcomes);
        }
        boolean readerAccepted = outcomes.values().stream().anyMatch(o -> o == Outcome.BUSINESS_ACCEPTED);
        return readerAccepted ? java.util.Optional.of(ArrivalStatus.DAG_COMPLETE) : java.util.Optional.empty();
    }

    /** Terminal arrival state, when reached (onhost-req). The responder (CIR) must itself be
     *  business-done before any terminal verdict (Fugu F6: a tech-failed CIR
     *  means the NACK never left; the arrival stays open for the reconciler). */
    public static java.util.Optional<ArrivalStatus> terminalState(Map<Stage, Outcome> outcomes) {
        return terminalState(DC_DAG, outcomes);
    }

    private static java.util.Optional<ArrivalStatus> terminalState(RouteDag dag, Map<Stage, Outcome> outcomes) {
        boolean cirDone = isBusinessDone(outcomes.get(Stage.CIR));
        // R-41: BUSINESS_FILE_REJECTED terminates exactly like BUSINESS_FILE_FATAL
        // (CIR acceptance closes the DAG; whole file never debits).
        boolean anyFatal = outcomes.values().stream()
                .anyMatch(o -> o == Outcome.BUSINESS_FILE_FATAL || o == Outcome.BUSINESS_FILE_REJECTED);
        if (anyFatal) {
            return cirDone ? java.util.Optional.of(ArrivalStatus.DAG_FAILED) : java.util.Optional.empty();
        }
        // R-41: PARTIAL continues PASS rows, so it completes like ACCEPTED: all
        // terminal stages business-done (isBusinessDone admits PARTIAL).
        boolean allTerminalDone = dag.terminal().stream()
                .allMatch(s -> isBusinessDone(outcomes.get(s)));
        return allTerminalDone ? java.util.Optional.of(ArrivalStatus.DAG_COMPLETE) : java.util.Optional.empty();
    }

    private static boolean isBusinessDone(Outcome o) {
        return o == Outcome.BUSINESS_ACCEPTED || o == Outcome.BUSINESS_PARTIAL;
    }
}
