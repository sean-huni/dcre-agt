package za.co.fnb.dcre.agt.repo;

import io.agroal.api.AgroalDataSource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Read-only window into the collections database (SCRUM-55 Task 12). The PRG
 * lane owns the schema; AGT only scans the prg_report_due view (Task 9
 * contract: client, source_msg_id, reason in COMPLETE/IDLE). Bounded and
 * ordered so one scan never drags the whole backlog and the per-client join
 * order is deterministic.
 */
@ApplicationScoped
public class CollectionsReadRepo {

    /** One parent whose immediate report is due. */
    public record DueParent(String client, String sourceMsgId, String reason) {
    }

    private static final String REPORT_DUE_SQL = """
            SELECT client, source_msg_id, reason
            FROM prg_report_due
            ORDER BY client, source_msg_id
            LIMIT 50""";

    @Inject
    @io.quarkus.agroal.DataSource("collections")
    AgroalDataSource collectionsDs;

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
}
