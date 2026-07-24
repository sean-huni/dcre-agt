package za.co.fnb.dcre.agt.service;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.kubernetes.client.WithKubernetesTestServer;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import za.co.fnb.dcre.agt.CrdbTestResource;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.repo.ArrivalRepo;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * M10/SCRUM-78 (A-71): with the msr-image unset the two GLOBAL MSR sweeps stay
 * launch-disabled (SCRUM-33 semantics, no stub fallback) even with the lease held:
 * no expiry/suspend clock intents are minted.
 */
@QuarkusTest
@QuarkusTestResource(CrdbTestResource.class)
@WithKubernetesTestServer(crud = true)
@TestProfile(MsrSweepDisabledTest.MsrImageUnsetProfile.class)
class MsrSweepDisabledTest {

    /** launch-enabled but NO msr-image: launch-disabled. */
    public static class MsrImageUnsetProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("agt.launch-enabled", "true",
                    "agt.msr-expiry-interval-seconds", "3600",
                    "agt.msr-suspend-interval-seconds", "3600");
        }
    }

    @Inject
    MsrExpiryScheduler expiryScheduler;

    @Inject
    MsrSuspendScheduler suspendScheduler;

    @Inject
    LeaseService lease;

    @Inject
    AgtConfig config;

    @Inject
    ArrivalRepo arrivalRepo;

    @Inject
    DataSource ds;

    @BeforeEach
    void holdLease() {
        exec("UPDATE agt_lease SET expires_at = now() - INTERVAL '1 second'");
        assertTrue(lease.tryAcquire(config.holderId()), "test precondition: lease held");
        assertTrue(config.msrImage().isEmpty(), "test precondition: msr-image unset");
    }

    @Test
    void sweepsMintNoWindowsWhileMsrImageUnset() {
        // The global sweeps run on a pure clock regardless of arrivals; the
        // msr-image guard, not an empty client set, is what disables them.
        expiryScheduler.tick();
        suspendScheduler.tick();

        assertEquals(0, countIntentsLike("man-msr-expiry-%"),
                "no msr-image: expiry sweep launch-disabled");
        assertEquals(0, countIntentsLike("man-msr-suspend-%"),
                "no msr-image: suspend sweep launch-disabled");
    }

    private long countIntentsLike(String pattern) {
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(
                "SELECT count(*) FROM launch_intent WHERE job_name LIKE ?")) {
            p.setString(1, pattern);
            try (ResultSet r = p.executeQuery()) {
                assertTrue(r.next());
                return r.getLong(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("test SQL failed", e);
        }
    }

    private void exec(String sql) {
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(sql)) {
            p.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("test SQL failed: " + sql, e);
        }
    }
}
