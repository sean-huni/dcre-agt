package za.co.fnb.dcre.agt.repo;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import javax.sql.DataSource;
import za.co.fnb.dcre.agt.domain.ArrivalStatus;
import za.co.fnb.dcre.agt.domain.FileArrival;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Plain-JDBC access to the file_arrival ledger. AGT is the single writer (R-04);
 * every mutation is either an INSERT guarded by a unique constraint or a
 * status UPDATE, so replays are no-ops (R-05).
 */
@ApplicationScoped
public class ArrivalRepo {

    @Inject
    DataSource ds;

    /** @return arrival id, or empty when a dedup index already covers this file. */
    public Optional<UUID> insertArrival(UUID id, String route, String name, String sha256,
                                        String clientToken, String msgIdToken,
                                        ArrivalStatus status, String quarantineReason,
                                        String claimedPath) {
        String sql = """
                INSERT INTO file_arrival
                  (id, route_id, physical_filename, payload_sha256, client_token, msg_id_token, status, quarantine_reason, claimed_path)
                VALUES (?,?,?,?,?,?,?,?,?)
                ON CONFLICT DO NOTHING
                RETURNING id""";
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(sql)) {
            p.setObject(1, id);
            p.setString(2, route);
            p.setString(3, name);
            p.setString(4, sha256);
            p.setString(5, clientToken);
            p.setString(6, msgIdToken);
            p.setString(7, status.name());
            p.setString(8, quarantineReason);
            p.setString(9, claimedPath);
            try (ResultSet r = p.executeQuery()) {
                return r.next() ? Optional.of(r.getObject(1, UUID.class)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("insertArrival failed", e);
        }
    }

    /** True when the same CONTENT (route + hash) already exists non-quarantined (Fugu F3). */
    public boolean sameContentExists(String route, String sha256) {
        String sql = """
                SELECT count(*) FROM file_arrival
                WHERE route_id=? AND payload_sha256=? AND status <> 'QUARANTINED'""";
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(sql)) {
            p.setString(1, route);
            p.setString(2, sha256);
            try (ResultSet r = p.executeQuery()) {
                r.next();
                return r.getLong(1) > 0;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("sameContentExists failed", e);
        }
    }

    /** True when the same logical key (route + filename tokens) exists with a DIFFERENT hash. */
    public boolean sameKeyDifferentHashExists(String route, String clientToken, String msgIdToken, String sha256) {
        if (clientToken == null || msgIdToken == null) {
            return false;
        }
        String sql = """
                SELECT count(*) FROM file_arrival
                WHERE route_id=? AND client_token=? AND msg_id_token=? AND payload_sha256<>?
                  AND status <> 'QUARANTINED'""";
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(sql)) {
            p.setString(1, route);
            p.setString(2, clientToken);
            p.setString(3, msgIdToken);
            p.setString(4, sha256);
            try (ResultSet r = p.executeQuery()) {
                r.next();
                return r.getLong(1) > 0;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("sameKeyDifferentHashExists failed", e);
        }
    }

    /** Monotonic CAS transition (Fugu F7): writes only from the expected state. */
    public void transitionArrival(UUID id, ArrivalStatus from, ArrivalStatus to) {
        JdbcSupport.exec(ds, "UPDATE file_arrival SET status=? WHERE id=? AND status=?", p -> {
            p.setString(1, to.name());
            p.setObject(2, id);
            p.setString(3, from.name());
        });
    }

    public void updateArrivalClaimedPath(UUID id, String claimedPath) {
        JdbcSupport.exec(ds, "UPDATE file_arrival SET claimed_path=? WHERE id=?", p -> {
            p.setString(1, claimedPath);
            p.setObject(2, id);
        });
    }

    public List<FileArrival> arrivalsByStatus(ArrivalStatus... statuses) {
        StringBuilder in = new StringBuilder();
        for (int i = 0; i < statuses.length; i++) {
            in.append(i == 0 ? "?" : ",?");
        }
        String sql = "SELECT id, route_id, physical_filename, payload_sha256, client_token, msg_id_token,"
                + " status, quarantine_reason, claimed_path FROM file_arrival WHERE status IN (" + in + ")";
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(sql)) {
            for (int i = 0; i < statuses.length; i++) {
                p.setString(i + 1, statuses[i].name());
            }
            try (ResultSet r = p.executeQuery()) {
                List<FileArrival> out = new ArrayList<>();
                while (r.next()) {
                    out.add(new FileArrival(r.getObject(1, UUID.class), r.getString(2), r.getString(3),
                            r.getString(4), r.getString(5), r.getString(6),
                            ArrivalStatus.valueOf(r.getString(7)), r.getString(8), r.getString(9)));
                }
                return out;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("arrivalsByStatus failed", e);
        }
    }

    /** Known clients for the PRG clock windows (R-28): tokens seen on the
     *  request routes (onhost-req; onhost-req-endo since M5). */
    public List<String> distinctClientTokens() {
        String sql = """
                SELECT DISTINCT client_token FROM file_arrival
                WHERE client_token IS NOT NULL AND route_id IN ('onhost-req','onhost-req-endo')""";
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(sql);
             ResultSet r = p.executeQuery()) {
            List<String> out = new ArrayList<>();
            while (r.next()) {
                out.add(r.getString(1));
            }
            return out;
        } catch (SQLException e) {
            throw new IllegalStateException("distinctClientTokens failed", e);
        }
    }

    public Optional<FileArrival> arrivalById(UUID id) {
        String sql = "SELECT id, route_id, physical_filename, payload_sha256, client_token, msg_id_token,"
                + " status, quarantine_reason, claimed_path FROM file_arrival WHERE id=?";
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(sql)) {
            p.setObject(1, id);
            try (ResultSet r = p.executeQuery()) {
                if (!r.next()) {
                    return Optional.empty();
                }
                return Optional.of(new FileArrival(r.getObject(1, UUID.class), r.getString(2), r.getString(3),
                        r.getString(4), r.getString(5), r.getString(6),
                        ArrivalStatus.valueOf(r.getString(7)), r.getString(8), r.getString(9)));
            }
        } catch (SQLException e) {
            throw new IllegalStateException("arrivalById failed", e);
        }
    }
}
