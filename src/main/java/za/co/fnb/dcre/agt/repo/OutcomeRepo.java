package za.co.fnb.dcre.agt.repo;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import javax.sql.DataSource;
import za.co.fnb.dcre.agt.domain.Outcome;
import za.co.fnb.dcre.agt.domain.Stage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Plain-JDBC access to the stage_outcome ledger. AGT is the single writer (R-04);
 * every mutation is an INSERT guarded by a unique constraint, so replays are
 * no-ops (R-05).
 */
@ApplicationScoped
public class OutcomeRepo {

    @Inject
    DataSource ds;

    /** Idempotent: duplicate observations for the same intent are no-ops (UNIQUE(intent_id)). */
    public boolean insertOutcome(UUID intentId, Outcome outcome, Integer exitCode, String condition) {
        String sql = """
                INSERT INTO stage_outcome (intent_id, outcome, exit_code, k8s_condition)
                VALUES (?,?,?,?)
                ON CONFLICT (intent_id) DO NOTHING
                RETURNING id""";
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(sql)) {
            p.setObject(1, intentId);
            p.setString(2, outcome.name());
            if (exitCode == null) {
                p.setNull(3, java.sql.Types.INTEGER);
            } else {
                p.setInt(3, exitCode);
            }
            p.setString(4, condition);
            try (ResultSet r = p.executeQuery()) {
                return r.next();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("insertOutcome failed", e);
        }
    }

    /** stage -> outcome for one arrival. */
    public Map<Stage, Outcome> outcomesForArrival(UUID arrivalId) {
        String sql = """
                SELECT i.stage, o.outcome FROM launch_intent i
                JOIN stage_outcome o ON o.intent_id = i.id
                WHERE i.arrival_id=?""";
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(sql)) {
            p.setObject(1, arrivalId);
            try (ResultSet r = p.executeQuery()) {
                Map<Stage, Outcome> out = new HashMap<>();
                while (r.next()) {
                    out.put(Stage.valueOf(r.getString(1)), Outcome.valueOf(r.getString(2)));
                }
                return out;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("outcomesForArrival failed", e);
        }
    }
}
