package za.co.fnb.dcre.agt.service;

import za.co.fnb.dcre.agt.domain.Flow;
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
 *
 * <p>THE DIAGRAMS ARE THE SPECIFICATION (R-49). Every shape below is a
 * transcription of a sheet in design-register/docs/diagrams.
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

    /**
     * Collections REQ sheet: CRR -> CTV -> fork {CDE -> CRW, CIR}.
     *
     * <p>R-37: CRW is a clock-driven Process-Date Executor, not a DAG successor, and
     * CDE is the Collection Day Estimator that decides WHEN it may run. That pair is
     * the collection-day wait, and it belongs to this family alone.
     */
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

    /**
     * Payments REQ sheet: PRR -> PTV -> PAI -> fork {PRW, PIR}.
     *
     * <p><b>THE TIMING RULE, MADE STRUCTURAL.</b> Owner, 2026-08-08: "CRW TxList are
     * processed on the collection-day, but for payments Tx's processed immediately."
     * Two things in this shape encode that, and both are deviations FROM the
     * collections sibling that are required rather than optional:
     *
     * <ul>
     *   <li>There is no CDE analogue. Collections routes through a Collection Day
     *       Estimator; the payments sheet goes straight from PAI to the fork, because
     *       there is no day to estimate.
     *   <li>PRW is a real DAG STAGE and therefore terminal, so emission is
     *       {@code NONE}. CRW is clock-driven and the DC arrival stays open until its
     *       process date comes round; PRW runs the moment PAI accepts. Copying
     *       {@code Emission.REQUIRED} here would make every payments arrival wait on
     *       {@code crw_emission_owed} in the COLLECTIONS database, which is both the
     *       wrong database and the collection-day wait applied to a family that must
     *       not have one.
     * </ul>
     *
     * <p>The trap is easy to inherit silently because PRW forks from CRW: both emit
     * pain.008, so the two writers look interchangeable and are not. Before the v1
     * topology this route ran CRR -> CTV -> AIS -> CIR with
     * {@code Emission.REQUIRED} and the comment "pay arm emits through the same CRW
     * window job", which is exactly the defect named above.
     */
    static final RouteDag ENDO = RouteDag.request(
            Stage.PRR,
            new EnumMap<>(Map.of(
                    Stage.PRR, EnumSet.of(Stage.PTV),
                    Stage.PTV, EnumSet.of(Stage.PAI),
                    Stage.PAI, EnumSet.of(Stage.PRW, Stage.PIR))),
            EnumSet.of(Stage.PRW, Stage.PIR),
            Optional.of(Stage.PIR),
            Emission.NONE); // PRW is a real DAG stage, so the writer is already terminal

    /** Mandates REQ sheet: MRR -> MRV -> MAS -> MIT -> fork {MIR, MRW}; the man
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

    /** Collections RES sheet: one token-picked leg reader per pain.002 reply, no
     *  successor edges and no responder. Exactly one of the terminal entries ever
     *  runs on a given arrival, so the terminal set is the set of LEGAL entries, not
     *  a fork that must all complete (DagEngine.respTerminalState). */
    static final RouteDag FINT_RESP_COL = RouteDag.response(EnumSet.of(Stage.CIX, Stage.CSX, Stage.CPX));

    /** Payments RES sheet: the same shape over the payments readers, each writing its
     *  own isr_resp / sbsr_resp / pbsr_resp in dcre_pay, then PRG. */
    static final RouteDag FINT_RESP_PAY = RouteDag.response(EnumSet.of(Stage.PIX, Stage.PSX, Stage.PPX));

    /** Mandates RES sheet: three token-picked leg readers, NO successor edges and no
     *  responder. Nothing answers OnHost on a response route, so a whole-file failure
     *  launches nothing and the arrival stays open for the reconciler: fail closed. */
    static final RouteDag FINT_RESP_MAN = RouteDag.response(EnumSet.of(Stage.MIX, Stage.MSX, Stage.MPX));

    /** R-36 route-based registry for REQUEST routes: the route -> shape mapping
     *  is data, not code; new request routes add an entry here, never a new
     *  code path. */
    static final Map<String, RouteDag> REQUESTS = Map.of(
            ArrivalService.ROUTE_ONHOST_REQ, DC,
            ArrivalService.ROUTE_ONHOST_REQ_ENDO, ENDO,
            ArrivalService.ROUTE_ONHOST_REQ_MAN, MAN);

    /**
     * R-36 registry for RESPONSE routes, keyed by (route, FLOW).
     *
     * <p>The extra dimension is the v1 topology's doing and it is load-bearing.
     * Fintegrate answers collections and payments on ONE {@code fint-resp} channel,
     * because both families send pain.008 and the reply carries no family marker;
     * only mandates (pain.009) has its own channel. So the route alone cannot say
     * which family's readers must run, and before this change a payments client's
     * reply was already being sent to the {@code dcre-pay} NAMESPACE while being read
     * by the COLLECTIONS readers into {@code dcre_col}: namespace isolation without
     * data isolation.
     *
     * <p>The flow comes from the same R-42 pay-clients membership that already
     * decides the namespace ({@code FlowNamespaces.flowForRoute}), so there is no
     * second encoding of "is this payments". That membership is INTERIM until the
     * R-14 client reference table lands, and it now decides a DATABASE as well as a
     * namespace, which raises what a wrong entry costs. The alternative considered
     * and rejected was a dedicated {@code fint-resp-endo} channel mirroring
     * {@code onhost-req-endo}: it would be a cleaner key, but nothing produces into
     * it, so payments replies would keep landing in {@code fint-resp} and would keep
     * being read by the collections family. That is a producer-side change, and it
     * cannot be made from AGT.
     */
    static final Map<String, Map<Flow, RouteDag>> RESPONSES = Map.of(
            ArrivalService.ROUTE_FINT_RESP, Map.of(
                    Flow.COL, FINT_RESP_COL,
                    Flow.PAY, FINT_RESP_PAY),
            ArrivalService.ROUTE_FINT_RESP_MAN, Map.of(
                    Flow.MAN, FINT_RESP_MAN));

    /** The response DAG for a (route, flow) pair, or a named failure. Fails closed:
     *  a pair with no shape never falls back to another family's readers. */
    static RouteDag response(final String route, final Flow flow) {
        final RouteDag dag = RESPONSES.getOrDefault(route, Map.of()).get(flow);
        if (dag == null) {
            throw new IllegalArgumentException("response route '" + route + "' has no DAG for flow "
                    + flow + ": add the pair to RouteDags.RESPONSES rather than letting another"
                    + " family's leg readers run against the wrong database.");
        }
        return dag;
    }

    private RouteDags() { }
}
