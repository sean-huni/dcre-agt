package za.co.fnb.dcre.agt;

import io.agroal.api.AgroalDataSource;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.domain.Stage;
import za.co.fnb.dcre.agt.repo.CollectionsReadRepo;
import za.co.fnb.dcre.agt.service.JobLauncher;
import za.co.fnb.dcre.agt.service.LeaseService;
import za.co.fnb.dcre.agt.service.ReportTrigger;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * ReportTrigger (SCRUM-55 Task 12): scans the collections-side prg_report_due
 * view and launches one PRG IMMEDIATE window per client. The PRG lane's
 * 003-reporting.xml is not on this branch yet, so the test creates a minimal
 * compatible view (contract: client, source_msg_id, reason) over a seed table.
 */
@QuarkusTest
@QuarkusTestResource(CrdbTestResource.class)
@TestProfile(ReportTriggerTest.ReportTriggerProfile.class)
class ReportTriggerTest {

    /** tick()'s gate needs launch-enabled; the mocked launcher keeps K8s out. */
    public static class ReportTriggerProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("agt.launch-enabled", "true");
        }
    }

    @Inject
    ReportTrigger trigger;

    @Inject
    CollectionsReadRepo collectionsRepo;

    @Inject
    LeaseService lease;

    @Inject
    AgtConfig config;

    @Inject
    DataSource opsDs;

    @Inject
    @io.quarkus.agroal.DataSource("collections")
    AgroalDataSource collectionsDs;

    @InjectMock
    JobLauncher launcher;

    @BeforeEach
    void resetViewAndLease() {
        execCollections("CREATE TABLE IF NOT EXISTS prg_report_due_seed ("
                + "client VARCHAR(16) NOT NULL, source_msg_id VARCHAR(35) NOT NULL, reason VARCHAR(16) NOT NULL)");
        execCollections("CREATE OR REPLACE VIEW prg_report_due AS "
                + "SELECT client, source_msg_id, reason FROM prg_report_due_seed");
        execCollections("DELETE FROM prg_report_due_seed");
        exec(opsDs, "UPDATE agt_lease SET expires_at = now() - INTERVAL '1 second'");
        assertTrue(lease.tryAcquire(config.holderId()), "test precondition: this instance holds the lease");
    }

    @Test
    void launchesOneImmediateWindowPerClientWithDueParentsJoined() {
        seedDue("FNBCC01", "DCRECC2026071600000001", "COMPLETE");
        seedDue("FNBCC01", "DCRECC2026071600000002", "IDLE");
        seedDue("FNBRF01", "DCRERF2026071600000003", "COMPLETE");

        final long before = Instant.now().getEpochSecond();
        trigger.tick();
        final long after = Instant.now().getEpochSecond();

        final ArgumentCaptor<String> runKeys = ArgumentCaptor.captor();
        final ArgumentCaptor<List<String>> launchArgs = ArgumentCaptor.captor();
        verify(launcher, times(2)).launchClock(eq(Stage.PRG), runKeys.capture(), launchArgs.capture());

        final Map<String, List<String>> byRunKey = new HashMap<>();
        for (int i = 0; i < runKeys.getAllValues().size(); i++) {
            byRunKey.put(runKeys.getAllValues().get(i), launchArgs.getAllValues().get(i));
        }
        final long epoch = epochOf(runKeys.getAllValues().get(0), before, after);

        final List<String> cc = byRunKey.get("FNBCC01-imm-" + epoch);
        assertNotNull(cc, "one launch per client keyed <client>-imm-<epochSec>: " + byRunKey.keySet());
        assertTrue(cc.contains("client=FNBCC01"), cc.toString());
        assertTrue(cc.contains("window=imm-" + epoch), cc.toString());
        assertTrue(cc.contains("report.type=IMMEDIATE,java.lang.String,false"), cc.toString());
        assertTrue(cc.contains("parents=DCRECC2026071600000001,DCRECC2026071600000002,java.lang.String,false"),
                "due parents comma-joined: " + cc);

        final List<String> rf = byRunKey.get("FNBRF01-imm-" + epoch);
        assertNotNull(rf, "second client launches in the same scan: " + byRunKey.keySet());
        assertTrue(rf.contains("parents=DCRERF2026071600000003,java.lang.String,false"), rf.toString());
    }

    @Test
    void reportDueReadsTheViewContract() {
        seedDue("FNBCC02", "DCRECC2026071600000009", "IDLE");
        final List<CollectionsReadRepo.DueParent> due = collectionsRepo.reportDue();
        assertEquals(1, due.size());
        assertEquals(new CollectionsReadRepo.DueParent("FNBCC02", "DCRECC2026071600000009", "IDLE"),
                due.get(0));
    }

    @Test
    void emptyViewLaunchesNothing() {
        trigger.tick();
        verifyNoInteractions(launcher);
    }

    @Test
    void noLaunchWithoutTheLease() {
        seedDue("FNBCC01", "DCRECC2026071600000004", "COMPLETE");
        exec(opsDs, "UPDATE agt_lease SET holder = 'other-agt', expires_at = now() + INTERVAL '120 seconds'");
        trigger.tick();
        verifyNoInteractions(launcher);
    }

    private long epochOf(final String runKey, final long before, final long after) {
        final long epoch = Long.parseLong(runKey.substring(runKey.lastIndexOf('-') + 1));
        assertTrue(epoch >= before && epoch <= after,
                "window key derives from the scan epoch: " + runKey + " not in [" + before + "," + after + "]");
        return epoch;
    }

    private void seedDue(final String client, final String sourceMsgId, final String reason) {
        try (Connection c = collectionsDs.getConnection();
             PreparedStatement p = c.prepareStatement(
                     "INSERT INTO prg_report_due_seed (client, source_msg_id, reason) VALUES (?, ?, ?)")) {
            p.setString(1, client);
            p.setString(2, sourceMsgId);
            p.setString(3, reason);
            p.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("seed failed", e);
        }
    }

    private void execCollections(final String sql) {
        exec(collectionsDs, sql);
    }

    private void exec(final DataSource ds, final String sql) {
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(sql)) {
            p.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("test SQL failed: " + sql, e);
        }
    }
}
