package za.co.fnb.dcre.agt.repo;

import io.agroal.api.AgroalDataSource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Set;

/**
 * Read-only window into the collections database (SCRUM-55 Task 12). The
 * collections lane owns the schema; AGT only ever reads PUBLISHED views, never
 * base tables, so the owner can evolve them: prg_report_due and prg_sla_pending
 * (PRG), and crw_emission_owed (CRW, SCRUM-107). Bounded and ordered so one scan
 * never drags the whole backlog and the per-client join order is deterministic.
 */
@ApplicationScoped
public class CollectionsReadRepo {

    private static final Logger LOG = Logger.getLogger(CollectionsReadRepo.class);

    /** One parent whose immediate report is due. */
    public record DueParent(String client, String sourceMsgId, String reason) {
    }

    /** One Fintegrate-visible transaction still without a terminal status. */
    public record SlaPending(String client, String e2e, double ageHours) {
    }

    private static final String REPORT_DUE_SQL = """
            SELECT client, source_msg_id, reason
            FROM prg_report_due
            ORDER BY client, source_msg_id
            LIMIT 50""";

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

    private static final String SLA_PENDING_SQL = """
            SELECT client, e2e, age_hours
            FROM prg_sla_pending
            WHERE age_hours >= ?
            ORDER BY age_hours DESC
            LIMIT 500""";

    @Inject
    @io.quarkus.agroal.DataSource("collections")
    AgroalDataSource collectionsDs;

    /**
     * SLA scan input (SCRUM-55 Task 13): Fintegrate-visible transactions with
     * no terminal status, oldest first, already filtered to the amber
     * threshold. Bounded to 500 rows so a pathological backlog never turns
     * one tick into a full-view drag; the oldest (worst) rows always make
     * the cut because of the DESC order.
     */
    public List<SlaPending> slaCounts(final int amberHours) {
        final List<SlaPending> rows = new ArrayList<>();
        try (Connection c = collectionsDs.getConnection();
             PreparedStatement p = c.prepareStatement(SLA_PENDING_SQL)) {
            p.setInt(1, amberHours);
            try (ResultSet r = p.executeQuery()) {
                while (r.next()) {
                    rows.add(new SlaPending(
                            r.getString("client"), r.getString("e2e"), r.getDouble("age_hours")));
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("prg_sla_pending scan failed", e);
        }
        return rows;
    }

    public List<DueParent> reportDue() {
        final List<DueParent> due = new ArrayList<>();
        try (Connection c = collectionsDs.getConnection();
             PreparedStatement p = c.prepareStatement(REPORT_DUE_SQL);
             ResultSet r = p.executeQuery()) {
            while (r.next()) {
                due.add(new DueParent(
                        r.getString("client"), r.getString("source_msg_id"), r.getString("reason")));
            }
        } catch (SQLException e) {
            throw new IllegalStateException("prg_report_due scan failed", e);
        }
        return due;
    }

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
