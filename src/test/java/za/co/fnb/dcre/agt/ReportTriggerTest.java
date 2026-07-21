package za.co.fnb.dcre.agt;

import io.agroal.api.AgroalDataSource;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.domain.Flow;
import za.co.fnb.dcre.agt.domain.Stage;
import za.co.fnb.dcre.agt.repo.CollectionsReadRepo;
import za.co.fnb.dcre.agt.service.JobLauncher;
import za.co.fnb.dcre.agt.service.LeaseService;
import za.co.fnb.dcre.agt.service.ReportTrigger;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * ReportTrigger (SCRUM-55 Task 12, review fix agt-12): scans the
 * collections-side prg_report_due view and launches ONE PRG IMMEDIATE run PER
 * DUE PARENT. Spring Batch's name=value,type,identifying notation cannot carry
 * commas inside a value (DefaultJobParametersConverter reads token[0] as the
 * value and Class.forName(token[1]) as the type), so parents are never joined:
 * each launch carries a single sourceMsgId and a parent-distinct window key
 * (digest of the full msgId) so JobInstances and PSR file names never collide.
 * The PRG lane's 003-reporting.xml is not on this branch yet, so the test
 * creates a minimal compatible view (contract: client, source_msg_id, reason)
 * over a seed table.
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

    private CapturingHandler warns;

    @BeforeEach
    void resetViewAndLease() {
        warns = new CapturingHandler();
        Logger.getLogger(ReportTrigger.class.getName()).addHandler(warns);
        execCollections("CREATE TABLE IF NOT EXISTS prg_report_due_seed ("
                + "client VARCHAR(16) NOT NULL, source_msg_id VARCHAR(35) NOT NULL, reason VARCHAR(16) NOT NULL)");
        execCollections("CREATE OR REPLACE VIEW prg_report_due AS "
                + "SELECT client, source_msg_id, reason FROM prg_report_due_seed");
        execCollections("DELETE FROM prg_report_due_seed");
        exec(opsDs, "UPDATE agt_lease SET expires_at = now() - INTERVAL '1 second'");
        assertTrue(lease.tryAcquire(config.holderId()), "test precondition: this instance holds the lease");
    }

    @AfterEach
    void detachLogCapture() {
        Logger.getLogger(ReportTrigger.class.getName()).removeHandler(warns);
    }

    @Test
    void launchesOnePrgImmediatePerDueParentWithParseSafeParams() {
        seedDue("FNBCC01", "DCRECC2026071600000001", "COMPLETE");
        seedDue("FNBCC01", "DCRECC2026071600000002", "IDLE");
        seedDue("FNBRF01", "DCRERF2026071600000003", "COMPLETE");

        final long before = Instant.now().getEpochSecond();
        trigger.tick();
        final long after = Instant.now().getEpochSecond();

        final ArgumentCaptor<Flow> flows = ArgumentCaptor.captor();
        final ArgumentCaptor<String> runKeys = ArgumentCaptor.captor();
        final ArgumentCaptor<List<String>> launchArgs = ArgumentCaptor.captor();
        verify(launcher, times(3)).launchClock(flows.capture(), eq(Stage.PRG), runKeys.capture(), launchArgs.capture());

        final Map<String, List<String>> byRunKey = new HashMap<>();
        final Map<String, Flow> flowByRunKey = new HashMap<>();
        for (int i = 0; i < runKeys.getAllValues().size(); i++) {
            byRunKey.put(runKeys.getAllValues().get(i), launchArgs.getAllValues().get(i));
            flowByRunKey.put(runKeys.getAllValues().get(i), flows.getAllValues().get(i));
        }
        assertEquals(3, byRunKey.size(), "run keys are distinct per parent: " + byRunKey.keySet());
        final long epoch = epochOf(runKeys.getAllValues().get(0), before, after);

        assertImmediateLaunch(byRunKey, "FNBCC01", "DCRECC2026071600000001", epoch);
        assertImmediateLaunch(byRunKey, "FNBCC01", "DCRECC2026071600000002", epoch);
        assertImmediateLaunch(byRunKey, "FNBRF01", "DCRERF2026071600000003", epoch);

        // SCRUM-70: IMMEDIATE windows resolve by the parent client's flow
        // (interim R-42 pay-clients map: FNBRF01 pay, FNBCC01 collections).
        flowByRunKey.forEach((key, flow) -> assertEquals(
                key.startsWith("FNBRF01-") ? Flow.PAY : Flow.COL, flow,
                "client flow routing for " + key));
    }

    @Test
    void duplicateDueRowsForOneParentLaunchOnce() {
        seedDue("FNBCC01", "DCRECC2026071600000007", "COMPLETE");
        seedDue("FNBCC01", "DCRECC2026071600000007", "IDLE");
        trigger.tick();
        verify(launcher, times(1)).launchClock(any(Flow.class), eq(Stage.PRG), anyString(), anyList());
    }

    @Test
    void runKeyStaysDnsSafeAtMaxFieldWidths() {
        seedDue("FNBCLIENTMAX0016", "M".repeat(35), "COMPLETE");
        trigger.tick();
        final ArgumentCaptor<String> runKeys = ArgumentCaptor.captor();
        verify(launcher, times(1)).launchClock(any(Flow.class), eq(Stage.PRG), runKeys.capture(), anyList());
        final String jobName = JobLauncher.clockJobName(Flow.COL, Stage.PRG, runKeys.getValue());
        assertTrue(jobName.length() <= 63, "K8s label limit: " + jobName + " (" + jobName.length() + ")");
        assertTrue(jobName.matches("[a-z0-9]([-a-z0-9]*[a-z0-9])?"), "DNS-1123: " + jobName);
    }

    @Test
    void maliciousSourceMsgIdWithCommaIsSkippedWithUnsafeTokenWarnAndNoLaunch() {
        // Fail-closed guard: a comma in a view-sourced value shifts the
        // name=value,type,identifying tokens, so the parent must be excluded
        // (WARN, value sanitized then elided to 8 chars: unsafe bytes never
        // reach the log line, CWE-117) and never reach the launcher.
        seedDue("FNBCC01", "EVIL,java.lang.Long", "COMPLETE");

        trigger.tick();

        verifyNoInteractions(launcher);
        assertTrue(warns.lines.stream().anyMatch(w ->
                        w.equals("excluded stage=AGT reason=UNSAFE_TOKEN field=sourceMsgId value=EVIL?jav")),
                "UNSAFE_TOKEN WARN with field name and value elided to 8 chars: " + warns.lines);
    }

    @Test
    void validParentStillLaunchesWhenAMaliciousSiblingIsSkipped() {
        seedDue("FNBCC01", "DCRECC2026071600000010", "COMPLETE");
        seedDue("FNBCC01", "BAD=EQUALS_SIGN", "IDLE");

        trigger.tick();

        final ArgumentCaptor<List<String>> launchArgs = ArgumentCaptor.captor();
        verify(launcher, times(1)).launchClock(any(Flow.class), eq(Stage.PRG), anyString(), launchArgs.capture());
        assertTrue(launchArgs.getValue().contains("parents=DCRECC2026071600000010,java.lang.String,false"),
                "only the safe parent launches: " + launchArgs.getValue());
        assertTrue(warns.lines.stream().anyMatch(w ->
                        w.contains("reason=UNSAFE_TOKEN field=sourceMsgId value=BAD?EQUA")),
                "WARN for the excluded sibling: " + warns.lines);
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

    private void assertImmediateLaunch(final Map<String, List<String>> byRunKey, final String client,
                                       final String sourceMsgId, final long epoch) {
        final String window = "imm-" + epoch + "-" + digest12(sourceMsgId);
        final List<String> args = byRunKey.get(client + "-" + window);
        assertNotNull(args, "one launch keyed <client>-imm-<epochSec>-<digest12(parent)>: " + byRunKey.keySet());
        assertTrue(args.contains("client=" + client), args.toString());
        assertTrue(args.contains("window=" + window), args.toString());
        assertTrue(args.contains("report.type=IMMEDIATE,java.lang.String,false"), args.toString());
        assertTrue(args.contains("parents=" + sourceMsgId + ",java.lang.String,false"),
                "single parent per launch, never comma-joined: " + args);
        args.forEach(ReportTriggerTest::assertBatchNotationSafe);
    }

    /**
     * The exact contract Spring Batch 6's DefaultJobParametersConverter parses:
     * a bare identifying value with no comma, or value,type,identifying where
     * token[1] must load via Class.forName. A comma inside the value shifts the
     * tokens and blows up the PRG launch (the agt-12 review blocker).
     */
    private static void assertBatchNotationSafe(final String arg) {
        final String value = arg.substring(arg.indexOf('=') + 1);
        final String[] tokens = value.split(",");
        if (tokens.length == 1) {
            return; // plain identifying value, comma-free
        }
        assertEquals(3, tokens.length, "value,type,identifying with a comma-free value: " + arg);
        try {
            Class.forName(tokens[1]);
        } catch (ClassNotFoundException e) {
            throw new AssertionError("type token must be a loadable class (got '" + tokens[1] + "') in: " + arg, e);
        }
        assertTrue("true".equals(tokens[2]) || "false".equals(tokens[2]), "identifying flag: " + arg);
    }

    /** Mirrors ReportTrigger's deterministic parent digest (frozen contract). */
    private static String digest12(final String sourceMsgId) {
        try {
            final byte[] sha = MessageDigest.getInstance("SHA-256")
                    .digest(sourceMsgId.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(sha, 0, 6);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    private long epochOf(final String runKey, final long before, final long after) {
        final int imm = runKey.indexOf("-imm-");
        assertTrue(imm > 0, "run key shape <client>-imm-<epochSec>-<digest12>: " + runKey);
        final String tail = runKey.substring(imm + "-imm-".length());
        final long epoch = Long.parseLong(tail.substring(0, tail.indexOf('-')));
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

    /**
     * Captures formatted WARN+ lines emitted by the ReportTrigger category
     * (SlaMonitorTest pattern): ReportTrigger warns via Logger.warnf (printf
     * style), so String.format over the record parameters reproduces the line.
     */
    private static final class CapturingHandler extends Handler {
        private final List<String> lines = new CopyOnWriteArrayList<>();

        @Override
        public void publish(final LogRecord record) {
            if (record.getLevel().intValue() >= Level.WARNING.intValue()) {
                final Object[] params = record.getParameters();
                lines.add(params == null || params.length == 0
                        ? record.getMessage()
                        : String.format(record.getMessage(), params));
            }
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    }
}
