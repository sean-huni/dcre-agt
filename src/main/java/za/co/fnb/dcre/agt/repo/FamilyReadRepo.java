package za.co.fnb.dcre.agt.repo;

import io.agroal.api.AgroalDataSource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import za.co.fnb.dcre.agt.domain.Flow;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Read-only window into a FAMILY's own database, for the two report-side views the
 * generators publish: {@code prg_report_due} and {@code prg_sla_pending}.
 *
 * <p><b>This class exists because a rename would not have been enough.</b> Before
 * the v1 split, AGT read {@code prg_report_due} from ONE datasource pointed at
 * {@code dcre_col}, for both flows. Both {@code dcre_col} and {@code dcre_pay} now
 * publish views with those exact names, owned by CRG and PRG respectively, so
 * pointing the single reader at either database leaves the other family's IMMEDIATE
 * reports permanently undiscovered. That fails SILENTLY: no exception, no log line,
 * simply a report that never fires. A second READ SEAM was needed, not a rename.
 *
 * <p>AGT only ever reads PUBLISHED views here, never base tables, so each family
 * can evolve its own schema. Bounded and ordered so one scan never drags a whole
 * backlog and the per-client join order is deterministic.
 */
@ApplicationScoped
public class FamilyReadRepo {

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

    private static final String SLA_PENDING_SQL = """
            SELECT client, e2e, age_hours
            FROM prg_sla_pending
            WHERE age_hours >= ?
            ORDER BY age_hours DESC
            LIMIT 500""";

    @Inject
    @io.quarkus.agroal.DataSource("collections")
    AgroalDataSource collectionsDs;

    @Inject
    @io.quarkus.agroal.DataSource("payments")
    AgroalDataSource paymentsDs;

    /** The families that publish a report-due view, so callers never hardcode the pair. */
    public static List<Flow> reportingFamilies() {
        return List.of(Flow.COL, Flow.PAY);
    }

    /**
     * The datasource for a family. Fails CLOSED on MAN rather than falling back:
     * mandates reports are purely clock-scheduled (MrgScheduler) and dcre_man
     * publishes no report-due view, so a MAN read here is a bug and a silent
     * substitution would answer it with another family's rows.
     */
    private AgroalDataSource dsFor(final Flow flow) {
        return switch (flow) {
            case COL -> collectionsDs;
            case PAY -> paymentsDs;
            case MAN -> throw new IllegalArgumentException(
                    "dcre_man publishes no report-due or sla view: mandates reports are"
                            + " clock-scheduled only, so there is nothing to read here for MAN");
        };
    }

    /** Parents whose IMMEDIATE report is due in this family's database. */
    public List<DueParent> reportDue(final Flow flow) {
        final List<DueParent> due = new ArrayList<>();
        try (Connection c = dsFor(flow).getConnection();
             PreparedStatement p = c.prepareStatement(REPORT_DUE_SQL);
             ResultSet r = p.executeQuery()) {
            while (r.next()) {
                due.add(new DueParent(
                        r.getString("client"), r.getString("source_msg_id"), r.getString("reason")));
            }
        } catch (SQLException e) {
            throw new IllegalStateException(flow + " prg_report_due scan failed", e);
        }
        return due;
    }

    /**
     * SLA scan input (SCRUM-55 Task 13): Fintegrate-visible transactions with no
     * terminal status, oldest first, already filtered to the amber threshold.
     * Bounded to 500 rows so a pathological backlog never turns one tick into a
     * full-view drag; the oldest (worst) rows always make the cut because of the
     * DESC order.
     */
    public List<SlaPending> slaCounts(final Flow flow, final int amberHours) {
        final List<SlaPending> rows = new ArrayList<>();
        try (Connection c = dsFor(flow).getConnection();
             PreparedStatement p = c.prepareStatement(SLA_PENDING_SQL)) {
            p.setInt(1, amberHours);
            try (ResultSet r = p.executeQuery()) {
                while (r.next()) {
                    rows.add(new SlaPending(
                            r.getString("client"), r.getString("e2e"), r.getDouble("age_hours")));
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException(flow + " prg_sla_pending scan failed", e);
        }
        return rows;
    }
}
