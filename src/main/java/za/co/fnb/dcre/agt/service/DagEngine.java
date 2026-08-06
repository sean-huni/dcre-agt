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
import za.co.fnb.dcre.agt.repo.CollectionsReadRepo;
import za.co.fnb.dcre.agt.repo.IntentRepo;
import za.co.fnb.dcre.agt.repo.OutcomeRepo;
import za.co.fnb.dcre.agt.service.RouteDags.RouteDag;

import java.util.EnumSet;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.Set;

/**
 * Level-triggered DAG engine over the ledgers. Route shapes live in
 * {@link RouteDags} (R-36 data, not code): DC Collections
 * CRR -> CTV -> [CDE, CIR]; ENDO Payments CRR -> CTV -> AIS -> [CIR]
 * (SCRUM-69: CDE never runs on the pay flow); M10 Mandates (SCRUM-79)
 * MRR -> MRV -> MAS -> MIT -> [MIR, MRW]. A BUSINESS_FILE_FATAL or
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

    /** Whether this route's terminal verdict depends on a CRW emission at all, so a
     *  route that never emits does not pay for the query. */
    private static boolean emissionRequired(String route) {
        final RouteDags.RouteDag dag = RouteDags.REQUESTS.get(route);
        return dag != null && dag.requiresEmission();
    }

    /** SCRUM-107: reads the registry rather than repeating the route list. This
     *  was a FIFTH encoding of "which routes exist": a response route added to
     *  INBOUND and RESPONSES but missed here took the REQUEST branch, found no
     *  REQUESTS entry, and silently QUARANTINED every file on that route. Derived,
     *  so the drift is now impossible rather than merely tested for. */
    private static boolean isRespRoute(String route) {
        return RouteDags.RESPONSES.containsKey(route);
    }

    /**
     * First stage for a CLAIMED arrival; empty = quarantine (fail closed).
     *
     * <p>SCRUM-107: the request branch used to be the ternary
     * {@code ROUTE_ONHOST_REQ_MAN.equals(route) ? MRR : CRR}, so EVERY unrecognised
     * route silently started at CRR, against dcre_col, while this javadoc already
     * claimed the method failed closed. It now reads the entry off the route's own
     * DAG, so there is one registry rather than a second encoding that can drift.
     *
     * <p>This is the site that fires FIRST: computeLaunches only picks successors,
     * so fixing that one alone left the orchestrator still misrouting.
     *
     * <p>It returns EMPTY for an unknown route rather than throwing, so the caller's
     * existing quarantine path handles it: one WARN, one terminal transition. An
     * earlier revision threw here and the arrival re-threw on every 2s tick, which
     * is loud but never terminal, and is precisely the per-item exception that
     * wedges a level-triggered loop. Observed spinning before this correction.
     */
    static java.util.Optional<Stage> initialStage(String route, String filename) {
        if (isRespRoute(route)) {
            return fintRespStage(route, filename);
        }
        final RouteDags.RouteDag dag = RouteDags.REQUESTS.get(route);
        return dag == null ? java.util.Optional.empty() : dag.entry();
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

    @Inject
    CollectionsReadRepo collectionsRead;

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
            } catch (IllegalArgumentException e) {
                // SCRUM-107: same reasoning as the DAG_RUNNING loop below. An
                // unresolvable route or namespace never becomes resolvable by
                // waiting, so retrying it every 2s is a permanent WARN loop.
                LOG.errorf("QUARANTINED %s: unresolvable route %s: %s",
                        arrival.id(), arrival.routeId(), e.getMessage());
                arrivalRepo.transitionArrival(arrival.id(), ArrivalStatus.CLAIMED, ArrivalStatus.QUARANTINED);
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
                // SCRUM-107: a SUPPLIER, so the cross-database read happens only if every
                // stage is already done. Passing a boolean evaluated it eagerly, which
                // charged one connection + round trip per DAG_RUNNING arrival per 2s
                // tick for the arrival's whole life, and this change deliberately keeps
                // warehoused arrivals in DAG_RUNNING for days, so it grew its own N.
                terminalState(arrival.routeId(), outcomes,
                        () -> collectionsRead.emissionOwedFor(arrival.id()))
                        .ifPresent(
                        s -> arrivalRepo.transitionArrival(arrival.id(), ArrivalStatus.DAG_RUNNING, s));
            } catch (IllegalArgumentException e) {
                // SCRUM-107: an unresolvable route/namespace is not transient, so a
                // WARN-and-retry would re-throw every 2s forever: loud, never
                // terminal, ~43k log lines a day and no alert. Quarantine makes it
                // terminal and alertable, exactly as initialStage's empty does on
                // the CLAIMED loop.
                LOG.errorf("QUARANTINED %s: unresolvable route %s: %s",
                        arrival.id(), arrival.routeId(), e.getMessage());
                arrivalRepo.transitionArrival(arrival.id(), ArrivalStatus.DAG_RUNNING, ArrivalStatus.QUARANTINED);
            } catch (Exception e) {
                LOG.warnf("advance DAG for %s failed: %s", arrival.id(), e.getMessage()); // one poisoned arrival never wedges the loop (F10)
            }
        }
    }

    /** Route dispatch: request routes resolve their DAG from the R-36 registry;
     *  response routes seed the token-picked leg reader and nothing else
     *  (re-seeded level-triggered, intents dedupe). SCRUM-91: both response
     *  routes take this one path now, since neither has a successor edge.
     *
     *  <p>SCRUM-107: an unknown route FAILS CLOSED. It used to fall back to the DC
     *  shape, so a route added to DirectoryWatcher.INBOUND but not to the RouteDags
     *  registry silently ran the COLLECTIONS DAG: CRR ingesting another family's
     *  file into dcre_col, the arrival reaching DAG_COMPLETE, nothing logged.
     *  Observed against the running orchestrator before the fix. advance() catches
     *  per arrival (F10), so throwing isolates that arrival and surfaces it. */
    /** The request DAG for a route, or a named failure. Fails closed rather than
     *  defaulting to DC (SCRUM-107). */
    private static RouteDag requestDag(final String route) {
        final RouteDag dag = RouteDags.REQUESTS.get(route);
        if (dag == null) {
            throw new IllegalArgumentException("unknown request route '" + route
                    + "': no RouteDags entry, so no DAG shape applies. Add it to"
                    + " RouteDags.REQUESTS (and DirectoryWatcher.INBOUND) rather than"
                    + " letting it run the collections DAG.");
        }
        return dag;
    }

    public static Set<Stage> computeLaunches(String route, String filename,
                                             Map<Stage, Outcome> outcomes, Set<Stage> intended) {
        if (!isRespRoute(route)) {
            return computeLaunches(requestDag(route), outcomes, intended);
        }
        Set<Stage> launches = EnumSet.noneOf(Stage.class);
        fintRespStage(route, filename)
                .filter(stage -> !intended.contains(stage) && !outcomes.containsKey(stage))
                .ifPresent(launches::add);
        return launches;
    }

    /** Successor stages to launch now, given recorded outcomes and existing intents (onhost-req). */
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
    public static java.util.Optional<ArrivalStatus> terminalState(String route, Map<Stage, Outcome> outcomes,
                                                                  BooleanSupplier emissionOwed) {
        if (isRespRoute(route)) {
            return respTerminalState(RouteDags.RESPONSES.get(route), outcomes);
        }
        return terminalState(requestDag(route), outcomes, emissionOwed);
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
    private static java.util.Optional<ArrivalStatus> terminalState(RouteDag dag, Map<Stage, Outcome> outcomes,
                                                                   BooleanSupplier emissionOwed) {
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
        if (!allTerminalDone) {
            return java.util.Optional.empty();
        }
        // SCRUM-107 (Sean, 2026-08-06): every stage is done, but on a CRW-emitting
        // route the request has not reached Fintegrate while an emission is still
        // OWED. Claiming DAG_COMPLETE then is a status the system cannot back up.
        // R-37's warehousing is untouched: the arrival stays DAG_RUNNING, correctly,
        // until its process_date comes round.
        //
        // OWED, not EMITTED. "An emission exists" completes a 3-batch arrival after
        // batch 1, completes a multi-process-date arrival on day 1 with the futured
        // remainder unsent, and strands forever an arrival whose rows all failed
        // validation and which will therefore never emit at all. The view answers
        // all three. A NACKed file terminates through the anyFatal branch above and
        // never reaches here.
        // Evaluated LAST and only on emitting routes, so the read is charged once per
        // arrival at the moment it would otherwise complete.
        if (dag.requiresEmission() && emissionOwed.getAsBoolean()) {
            return java.util.Optional.empty();
        }
        return java.util.Optional.of(ArrivalStatus.DAG_COMPLETE);
    }

    private static boolean isBusinessDone(Outcome o) {
        return o == Outcome.BUSINESS_ACCEPTED || o == Outcome.BUSINESS_PARTIAL;
    }
}
