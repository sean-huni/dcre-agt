package za.co.fnb.dcre.agt.repo;

import io.agroal.api.AgroalDataSource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The CRW emission gate: a read-only window into the collections database for the
 * one question that only collections asks.
 *
 * <p>Collections-specific by design, not by omission. CRW is a clock-driven
 * Process-Date Executor, so a DC arrival can be complete in every stage and still
 * owe Fintegrate an emission until its process date comes round. Payments has no
 * such gate: PRW is a DAG stage that runs the moment PAI accepts, so a payments
 * arrival is done when its stages are done and {@code RouteDags.ENDO} carries
 * {@code Emission.NONE}. Nothing here is consulted for a payments arrival, and
 * making it family-generic would be encoding the collection-day wait for a family
 * that must not have one.
 *
 * <p>The family-generic report views moved to {@link FamilyReadRepo} with the v1
 * split. AGT only ever reads PUBLISHED views, never base tables, so the owner can
 * evolve them.
 */
@ApplicationScoped
public class CollectionsReadRepo {

    private static final Logger LOG = Logger.getLogger(CollectionsReadRepo.class);

    /**
     * SCRUM-107: does this arrival still OWE Fintegrate an emission?
     *
     * <p>Reads the published {@code crw_emission_owed} view, not {@code crw_emission}
     * directly: CRW owns the emission semantics and the completeness rule belongs
     * next to the tables it needs. Same contract style as prg_report_due.
     *
     * <p>OWED rather than EMITTED, because "an emission exists" is the wrong question
     * in three ways that each either overclaim or strand:
     * an arrival with zero PASS rows never emits and must still terminate; a
     * multi-batch arrival owes until EVERY planned batch is VISIBLE, not the first;
     * and a multi-process-date arrival owes until the futured remainder emits on a
     * later run_date.
     *
     * <p>Absent row means no tx_header, so nothing is owed.
     */
    private static final String EMISSION_OWED_SQL = """
            SELECT owed FROM crw_emission_owed WHERE arrival_id = ? LIMIT 1""";

    @Inject
    @io.quarkus.agroal.DataSource("collections")
    AgroalDataSource collectionsDs;

    /** Structural SQL failures (missing view, missing grant, wrong column) that must not
     *  masquerade as "not emitted yet": they never resolve by waiting. */
    private static final Set<String> STRUCTURAL = Set.of(
            "42P01", "42703", "42501", "3D000", "42883");

    /** Count of structural emission-gate failures, so a missing view or grant is
     *  distinguishable on a dashboard from an arrival that is legitimately warehoused. */
    private final AtomicLong gateFailures = new AtomicLong();

    public long emissionGateFailures() {
        return gateFailures.get();
    }

    /**
     * True while CRW still owes an emission for this arrival (SCRUM-107).
     *
     * <p>Fails CLOSED: on any error this returns TRUE (still owed), which keeps the
     * arrival open rather than letting a failed read declare it complete. A false
     * completion cannot be walked back; staying open costs another tick.
     *
     * <p>A structural error is logged at ERROR and counted, because "the view is
     * missing" and "this arrival is warehoused for nine days" are indistinguishable
     * from the arrival's status alone, and only one of them needs an operator.
     */
    public boolean emissionOwedFor(final UUID arrivalId) {
        try (Connection c = collectionsDs.getConnection();
             PreparedStatement p = c.prepareStatement(EMISSION_OWED_SQL)) {
            p.setObject(1, arrivalId);
            try (ResultSet r = p.executeQuery()) {
                return r.next() && r.getBoolean(1);
            }
        } catch (SQLException e) {
            if (STRUCTURAL.contains(e.getSQLState())) {
                gateFailures.incrementAndGet();
                LOG.errorf("emission-gate STRUCTURAL failure (SQLState %s) for %s: %s."
                                + " Every collections arrival will stay open until this is fixed.",
                        e.getSQLState(), arrivalId, e.getMessage());
            } else {
                LOG.warnf("emission-gate read failed for %s (treating as still owed): %s",
                        arrivalId, e.getMessage());
            }
            return true;
        }
    }
}
