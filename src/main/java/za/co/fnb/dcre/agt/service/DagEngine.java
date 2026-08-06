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
import za.co.fnb.dcre.agt.service.RouteDags.RouteDag;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * Level-triggered DAG engine over the ledgers. Route shapes live in
 * {@link RouteDags} (R-36 data, not code): DC Collections
 * CRR -> CTV -> [CDE, CIR]; ENDO Payments CRR -> CTV -> AIS -> [CIR]
 * (SCRUM-69: CDE never runs on the pay flow); M10 Mandates (SCRUM-79)
 * MRR -> MRV -> MAF -> MIT -> [MIR, MRW]. A BUSINESS_FILE_FATAL or
 * BUSINESS_FILE_REJECTED predecessor routes to the route family's responder
 * only (CIR, or MIR on the man route: whole-file NACK path, R-19/R-41/SPEC-DAG
 * section 3); BUSINESS_PARTIAL fans out like ACCEPTED (R-41: PASS rows
 * continue). Response routes are token-picked and share ONE code path: the
 * filename token selects the single leg reader, IXR/SXR/PXR on fint-resp (M4)
 * and MIX/MSX/MPX on fint-resp-man (SCRUM-91, replacing the merged MAR reader
 * and its MSR chain).
 * Pure decision logic lives in computeLaunches() for unit testing.
 */
@ApplicationScoped
public class DagEngine {

    private static final Logger LOG = Logger.getLogger(DagEngine.class);

    /** pain.012/fint-resp reply token from the filename; empty = unknown
     *  (fail closed). The token selects the leg reader, nothing else: since
     *  SCRUM-91 each reader owns exactly one leg, so no launch arg carries it. */
    public static java.util.Optional<String> replyToken(String filename) {
        if (filename.contains("_ISR")) {
            return java.util.Optional.of("ISR");
        }
        if (filename.contains("_SBSR")) {
            return java.util.Optional.of("SBSR");
        }
        if (filename.contains("_PBSR")) {
            return java.util.Optional.of("PBSR");
        }
        return java.util.Optional.empty();
    }

    /** Response-route filename token -> leg reader (route-aware): each route has
     *  one reader per reply type, so the route picks the family and the token
     *  picks the leg. Empty = unknown token (fail closed). */
    public static java.util.Optional<Stage> fintRespStage(String route, String filename) {
        boolean man = ArrivalService.ROUTE_FINT_RESP_MAN.equals(route);
        return replyToken(filename).map(token -> man ? manEntryFor(token) : colEntryFor(token));
    }

    /** SCRUM-91: fint-resp-man token-picks one leg reader per reply type, exactly as
     *  fint-resp picks IXR/SXR/PXR. An unknown token is a misrouted file and fails
     *  closed (never a guessed default). */
    static Stage manEntryFor(final String token) {
        return switch (token) {
            case "ISR" -> Stage.MIX;
            case "SBSR" -> Stage.MSX;
            case "PBSR" -> Stage.MPX;
            default -> throw new IllegalArgumentException("unknown mandate reply token: " + token);
        };
    }

    /** M4 collections leg readers, the shape SCRUM-91 made the mandates route copy. */
    static Stage colEntryFor(final String token) {
        return switch (token) {
            case "ISR" -> Stage.IXR;
            case "SBSR" -> Stage.SXR;
            case "PBSR" -> Stage.PXR;
            default -> throw new IllegalArgumentException("unknown collections reply token: " + token);
        };
    }

    private static boolean isRespRoute(String route) {
        return ArrivalService.ROUTE_FINT_RESP.equals(route)
                || ArrivalService.ROUTE_FINT_RESP_MAN.equals(route);
    }

    /** First stage for a CLAIMED arrival; empty = quarantine (fail closed). */
    static java.util.Optional<Stage> initialStage(String route, String filename) {
        if (isRespRoute(route)) {
            return fintRespStage(route, filename);
        }
        return java.util.Optional.of(
                ArrivalService.ROUTE_ONHOST_REQ_MAN.equals(route) ? Stage.MRR : Stage.CRR);
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
     *  response routes seed the token-picked leg reader and nothing else
     *  (re-seeded level-triggered, intents dedupe). SCRUM-91: both response
     *  routes take this one path now, since neither has a successor edge.
     *  Unknown routes fall back to the DC shape (as before). */
    public static Set<Stage> computeLaunches(String route, String filename,
                                             Map<Stage, Outcome> outcomes, Set<Stage> intended) {
        if (!isRespRoute(route)) {
            return computeLaunches(RouteDags.REQUESTS.getOrDefault(route, RouteDags.DC), outcomes, intended);
        }
        Set<Stage> launches = EnumSet.noneOf(Stage.class);
        fintRespStage(route, filename)
                .filter(stage -> !intended.contains(stage) && !outcomes.containsKey(stage))
                .ifPresent(launches::add);
        return launches;
    }

    /** Successor stages to launch now, given recorded outcomes and existing intents (onhost-req). */
    public static Set<Stage> computeLaunches(Map<Stage, Outcome> outcomes, Set<Stage> intended) {
        return computeLaunches(RouteDags.DC, outcomes, intended);
    }

    private static Set<Stage> computeLaunches(RouteDag dag, Map<Stage, Outcome> outcomes, Set<Stage> intended) {
        Set<Stage> launches = EnumSet.noneOf(Stage.class);
        for (Map.Entry<Stage, Outcome> done : outcomes.entrySet()) {
            switch (done.getValue()) {
                case BUSINESS_PARTIAL, BUSINESS_ACCEPTED -> {
                    // R-41: PARTIAL continues PASS rows (acceptance mode is the
                    // validator's call now); both fan out to all successors.
                    for (Stage next : dag.edges().getOrDefault(done.getKey(), Set.of())) {
                        if (!intended.contains(next)) {
                            launches.add(next);
                        }
                    }
                }
                case BUSINESS_FILE_REJECTED, BUSINESS_FILE_FATAL ->
                    // Whole-file NACK (fatal or R-41 policy rejection): the route
                    // family's responder still runs; nothing else does. Response
                    // routes have no responder: fail closed, reconciler's call.
                    dag.responder()
                            .filter(responder -> !intended.contains(responder))
                            .ifPresent(launches::add);
                case TECH_FAILED, TECH_CONFIG_FAILED -> {
                    // Process death is never a business verdict (R-33): no successors;
                    // relaunch policy is the reconciler's/operator's call. A
                    // pre-runner config failure (exit 78) is even less of a verdict:
                    // the stage never ran, so it fans out to nothing either.
                }
            }
        }
        return launches;
    }

    /** Terminal verdict by route: response routes use respTerminalState, request
     *  routes their R-36 DAG shape. */
    public static java.util.Optional<ArrivalStatus> terminalState(String route, Map<Stage, Outcome> outcomes) {
        if (isRespRoute(route)) {
            return respTerminalState(RouteDags.RESPONSES.get(route), outcomes);
        }
        return terminalState(RouteDags.REQUESTS.getOrDefault(route, RouteDags.DC), outcomes);
    }

    /**
     * Response-route terminal: the ONE token-picked leg reader for this arrival
     * reporting BUSINESS_ACCEPTED completes the DAG. The dag's terminal set lists
     * the legal entries (IXR/SXR/PXR, or MIX/MSX/MPX since SCRUM-91), of which
     * exactly one ever runs, so this is an any-of test and never the all-of test
     * a request fork gets: requiring all three would leave every response arrival
     * permanently DAG_RUNNING. Anything short of acceptance (fatal, partial, tech
     * failure) stays open for the reconciler: fail closed, no responder to run.
     */
    private static java.util.Optional<ArrivalStatus> respTerminalState(RouteDag dag, Map<Stage, Outcome> outcomes) {
        boolean readerAccepted = dag.terminal().stream()
                .anyMatch(stage -> outcomes.get(stage) == Outcome.BUSINESS_ACCEPTED);
        return readerAccepted ? java.util.Optional.of(ArrivalStatus.DAG_COMPLETE) : java.util.Optional.empty();
    }

    /** Terminal arrival state, when reached (onhost-req). The responder must itself be
     *  business-done before any terminal verdict (Fugu F6: a tech-failed responder
     *  means the NACK never left; the arrival stays open for the reconciler). */
    public static java.util.Optional<ArrivalStatus> terminalState(Map<Stage, Outcome> outcomes) {
        return terminalState(RouteDags.DC, outcomes);
    }

    private static java.util.Optional<ArrivalStatus> terminalState(RouteDag dag, Map<Stage, Outcome> outcomes) {
        // R-41: BUSINESS_FILE_REJECTED terminates exactly like BUSINESS_FILE_FATAL
        // (responder acceptance closes the DAG; the whole file never debits).
        boolean anyFatal = outcomes.values().stream()
                .anyMatch(o -> o == Outcome.BUSINESS_FILE_FATAL || o == Outcome.BUSINESS_FILE_REJECTED);
        if (anyFatal) {
            // No responder on a response route: never a terminal verdict (fail closed).
            return dag.responder()
                    .filter(responder -> isBusinessDone(outcomes.get(responder)))
                    .map(responder -> ArrivalStatus.DAG_FAILED);
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
