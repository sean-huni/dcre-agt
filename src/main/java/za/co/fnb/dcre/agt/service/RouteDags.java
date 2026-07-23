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
    record RouteDag(Map<Stage, Set<Stage>> edges, Set<Stage> terminal, Optional<Stage> responder) { }

    // R-37: CRW is a clock-driven Process-Date Executor, not a DAG successor.
    static final RouteDag DC = new RouteDag(
            new EnumMap<>(Map.of(
                    Stage.CRR, EnumSet.of(Stage.CTV),
                    Stage.CTV, EnumSet.of(Stage.CDE, Stage.CIR))),
            EnumSet.of(Stage.CDE, Stage.CIR),
            Optional.of(Stage.CIR));

    static final RouteDag ENDO = new RouteDag(
            new EnumMap<>(Map.of(
                    Stage.CRR, EnumSet.of(Stage.CTV),
                    Stage.CTV, EnumSet.of(Stage.AIS),
                    Stage.AIS, EnumSet.of(Stage.CIR))),
            EnumSet.of(Stage.CIR),
            Optional.of(Stage.CIR));

    /** M10 (SCRUM-79): MRR -> MRV -> MAF -> MIS -> fork {MIR, MRW}; the man
     *  responder is MIR (R-41 switch-case extension: rejections never see CIR). */
    static final RouteDag MAN = new RouteDag(
            new EnumMap<>(Map.of(
                    Stage.MRR, EnumSet.of(Stage.MRV),
                    Stage.MRV, EnumSet.of(Stage.MAF),
                    Stage.MAF, EnumSet.of(Stage.MIS),
                    Stage.MIS, EnumSet.of(Stage.MIR, Stage.MRW))),
            EnumSet.of(Stage.MIR, Stage.MRW),
            Optional.of(Stage.MIR));

    /** fint-resp-man: the token-picked MAR entry (all three pain.012 legs, one
     *  service) chains into the MSR projection writer; no responder. */
    static final RouteDag FINT_RESP_MAN = new RouteDag(
            new EnumMap<>(Map.of(Stage.MAR, EnumSet.of(Stage.MSR))),
            EnumSet.of(Stage.MSR),
            Optional.empty());

    /** R-36 route-based registry for REQUEST routes: the route -> shape mapping
     *  is data, not code; new request routes add an entry here, never a new
     *  code path. Response routes are token-picked (DagEngine). */
    static final Map<String, RouteDag> REQUESTS = Map.of(
            ArrivalService.ROUTE_ONHOST_REQ, DC,
            ArrivalService.ROUTE_ONHOST_REQ_ENDO, ENDO,
            ArrivalService.ROUTE_ONHOST_REQ_MAN, MAN);

    private RouteDags() { }
}
