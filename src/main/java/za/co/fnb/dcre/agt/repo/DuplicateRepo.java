package za.co.fnb.dcre.agt.repo;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

/**
 * Plain-JDBC access to the duplicate_delivery ledger (SCRUM-58 file-trace, spec
 * 1.1). AGT is the single writer (R-04). Every re-delivery event is recorded
 * write-ahead BEFORE the file is sunk to the duplicates dir; the insert is
 * idempotent on claim_id (the per-delivery claim UUID that prefixes the inflight
 * file), so an orphan-sweep resume that re-parses the same claim id is a no-op
 * (chaos-audit key: count(*) == count(DISTINCT claim_id)).
 */
@ApplicationScoped
public class DuplicateRepo {

    @Inject
    DataSource ds;

    /**
     * Records one re-delivery event. Idempotent per claim: a resume re-parses the
     * inflight {@code <claimUuid>_} prefix and ON CONFLICT (claim_id) no-ops.
     *
     * @return true when this call inserted the row, false when it was a no-op.
     */
    public boolean insertDuplicate(UUID claimId, UUID originalArrivalId, String routeId,
                                   String clientToken, String physicalFilename,
                                   String payloadSha256, String sunkPath) {
        String sql = """
                INSERT INTO duplicate_delivery
                  (claim_id, original_arrival_id, route_id, client_token, physical_filename, payload_sha256, sunk_path)
                VALUES (?,?,?,?,?,?,?)
                ON CONFLICT (claim_id) DO NOTHING
                RETURNING id""";
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(sql)) {
            p.setObject(1, claimId);
            p.setObject(2, originalArrivalId);
            p.setString(3, routeId);
            p.setString(4, clientToken);
            p.setString(5, physicalFilename);
            p.setString(6, payloadSha256);
            p.setString(7, sunkPath);
            try (ResultSet r = p.executeQuery()) {
                return r.next();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("insertDuplicate failed", e);
        }
    }
}
