package za.co.fnb.dcre.agt.service;

import za.co.fnb.dcre.agt.domain.Stage;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * R-36 DAG shapes as data (SCRUM-79: extracted from DagEngine on the class-size
 * watch): successor edges, the terminal fork, and the route family's whole-file
 * NACK responder. The responder is empty on response DAGs: nothing answers
 * OnHost there, so a whole-file failure launches nothing and the arrival stays
 * open for the reconciler (fail closed, mirroring collections fint-resp).
 */
final class RouteDags {

    /** A route's DAG shape: successor edges, terminal fork, optional responder. */
    /** SCRUM-107: {@code entry} is the DAG's own first stage, so the route ->
     *  first-stage mapping lives HERE with the rest of the shape instead of in a
     *  ternary in DagEngine. Response DAGs have no fixed entry (the leg reader is
     *  token-picked from the filename), so theirs is empty. */
    /** Whether a route's completion depends on a CRW emission. A named pair rather
     *  than a bare trailing boolean: the factories exist so a same-typed argument
     *  cannot be transposed silently, and an unlabelled boolean is the next instance
     *  of exactly that hazard. */
    enum Emission { REQUIRED, NONE }

    record RouteDag(Optional<Stage> entry, Map<Stage, Set<Stage>> edges,
                    Set<Stage> terminal, Optional<Stage> responder, boolean requiresEmission) {

        /** A request DAG. Takes a bare entry Stage, so a request shape with no entry
         *  is unrepresentable, and names every part, so the two same-typed Optionals
         *  cannot be transposed silently (they could in the positional form). */
        static RouteDag request(final Stage entry, final Map<Stage, Set<Stage>> edges,
                                final Set<Stage> terminal, final Optional<Stage> responder,
                                final Emission emission) {
            return new RouteDag(Optional.of(entry), edges, terminal, responder,
                    emission == Emission.REQUIRED);
        }

        /** A response DAG: token-picked entries, no edges, no responder, no emission. */
        static RouteDag response(final Set<Stage> legalEntries) {
            return new RouteDag(Optional.empty(), Map.of(), legalEntries, Optional.empty(), false);
        }
    }

    // R-37: CRW is a clock-driven Process-Date Executor, not a DAG successor.
    static final RouteDag DC = RouteDag.request(
            Stage.CRR,
            new EnumMap<>(Map.of(
                    Stage.CRR, EnumSet.of(Stage.CTV),
                    Stage.CTV, EnumSet.of(Stage.CDE, Stage.CIR))),
            EnumSet.of(Stage.CDE, Stage.CIR),
            Optional.of(Stage.CIR),
            // SCRUM-107: CRW is clock-driven (R-37) and is NOT a DAG stage, but the
            // arrival is not COMPLETE until it has emitted. Terminal now also requires
            // a VISIBLE crw_emission for the arrival, so a warehoused instruction stays
            // DAG_RUNNING honestly instead of claiming completion before Fintegrate saw it.
            Emission.REQUIRED);

    static final RouteDag ENDO = RouteDag.request(
            Stage.CRR,
            new EnumMap<>(Map.of(
                    Stage.CRR, EnumSet.of(Stage.CTV),
                    Stage.CTV, EnumSet.of(Stage.AIS),
                    Stage.AIS, EnumSet.of(Stage.CIR))),
            EnumSet.of(Stage.CIR),
            Optional.of(Stage.CIR),
            Emission.REQUIRED); // pay arm emits through the same CRW window job

    /** M10 (SCRUM-79): MRR -> MRV -> MAS -> MIT -> fork {MIR, MRW}; the man
     *  responder is MIR (R-41 switch-case extension: rejections never see CIR). */
    static final RouteDag MAN = RouteDag.request(
            Stage.MRR,
            new EnumMap<>(Map.of(
                    Stage.MRR, EnumSet.of(Stage.MRV),
                    Stage.MRV, EnumSet.of(Stage.MAS),
                    Stage.MAS, EnumSet.of(Stage.MIT),
                    Stage.MIT, EnumSet.of(Stage.MIR, Stage.MRW))),
            EnumSet.of(Stage.MIR, Stage.MRW),
            Optional.of(Stage.MIR),
            Emission.NONE); // MRW is a real DAG stage here, so the writer is already terminal

    /** M4 fint-resp: one token-picked leg reader per pain.002 reply, no successor
     *  edges and no responder. Exactly one of the terminal entries ever runs on a
     *  given arrival, so the terminal set is the set of LEGAL entries, not a fork
     *  that must all complete (DagEngine.respTerminalState). */
    static final RouteDag FINT_RESP = RouteDag.response(EnumSet.of(Stage.IXR, Stage.SXR, Stage.PXR));

    /** fint-resp-man: three token-picked leg readers, NO successor edges and no
     *  responder, mirroring collections fint-resp exactly (SCRUM-91: the single
     *  MAR entry chained to MSR was the deviation). Nothing answers OnHost on a
     *  response route, so a whole-file failure launches nothing and the arrival
     *  stays open for the reconciler: fail closed. */
    static final RouteDag FINT_RESP_MAN = RouteDag.response(EnumSet.of(Stage.MIX, Stage.MSX, Stage.MPX));

    /** R-36 route-based registry for REQUEST routes: the route -> shape mapping
     *  is data, not code; new request routes add an entry here, never a new
     *  code path. */
    static final Map<String, RouteDag> REQUESTS = Map.of(
            ArrivalService.ROUTE_ONHOST_REQ, DC,
            ArrivalService.ROUTE_ONHOST_REQ_ENDO, ENDO,
            ArrivalService.ROUTE_ONHOST_REQ_MAN, MAN);

    /** R-36 registry for RESPONSE routes (SCRUM-91): same data-not-code rule. The
     *  entry stage is token-picked from the filename (DagEngine), and both routes
     *  now share one code path: collections and mandates differ only by the leg
     *  readers listed here. */
    static final Map<String, RouteDag> RESPONSES = Map.of(
            ArrivalService.ROUTE_FINT_RESP, FINT_RESP,
            ArrivalService.ROUTE_FINT_RESP_MAN, FINT_RESP_MAN);

    private RouteDags() { }
}
