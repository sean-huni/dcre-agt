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
import za.co.fnb.dcre.agt.domain.ArrivalStatus;
import za.co.fnb.dcre.agt.domain.Flow;
import za.co.fnb.dcre.agt.domain.Stage;
import za.co.fnb.dcre.agt.repo.ArrivalRepo;
import za.co.fnb.dcre.agt.repo.FamilyReadRepo;
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
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
 * SCRUM-90: the IMMEDIATE report is launched ARRIVAL-SCOPED (via
 * launchArrivalReport) so the M12 sweeps recover a killed one-shot report; the
 * trigger resolves the parent book's arrival by (client, source_msg_id) and
 * fails closed when it cannot. The PRG lane's 003-reporting.xml is not on this
 * branch yet, so the test creates a minimal compatible view (contract: client,
 * source_msg_id, reason) over a seed table.
 */
@QuarkusTest
@QuarkusTestResource(CrdbTestResource.class)
@TestProfile(ReportTriggerTest.ReportTriggerProfile.class)
class ReportTriggerTest {

    /** tick()'s gate needs launch-enabled AND a configured generator per family
     *  (empty image = launch-disabled); the mocked launcher keeps K8s out. */
    public static class ReportTriggerProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("agt.launch-enabled", "true",
                    "agt.crg-image", "dcre-crg:test",
                    "agt.prg-image", "dcre-prg:test",
                    // 2 scans is enough to reach the stall WARN inside one test.
                    "agt.report-stall-scans", "2");
        }
    }

    @Inject
    ReportTrigger trigger;

    @Inject
    FamilyReadRepo families;

    @Inject
    LeaseService lease;

    @Inject
    AgtConfig config;

    @Inject
    ArrivalRepo arrivalRepo;

    @Inject
    DataSource opsDs;

    @Inject
    @io.quarkus.agroal.DataSource("collections")
    AgroalDataSource collectionsDs;

    @Inject
    @io.quarkus.agroal.DataSource("payments")
    AgroalDataSource paymentsDs;

    @InjectMock
    JobLauncher launcher;

    private CapturingHandler warns;

    /** client|sourceMsgId -> the seeded parent arrival id, for arrival-scope assertions. */
    private final Map<String, UUID> arrivalsByKey = new HashMap<>();

    @BeforeEach
    void resetViewAndLease() {
        warns = new CapturingHandler();
        Logger.getLogger(ReportTrigger.class.getName()).addHandler(warns);
        arrivalsByKey.clear();
        // BOTH families publish a view of this name, in their OWN database. The
        // payments one is created here too, because a payments report discovered
        // from the collections database would be exactly the defect the second read
        // seam exists to prevent, and a fixture with only one database cannot show it.
        for (final AgroalDataSource ds : List.of(collectionsDs, paymentsDs)) {
            exec(ds, "CREATE TABLE IF NOT EXISTS prg_report_due_seed ("
                    + "client VARCHAR(16) NOT NULL, source_msg_id VARCHAR(35) NOT NULL,"
                    + " reason VARCHAR(16) NOT NULL)");
            exec(ds, "CREATE OR REPLACE VIEW prg_report_due AS "
                    + "SELECT client, source_msg_id, reason FROM prg_report_due_seed");
            exec(ds, "DELETE FROM prg_report_due_seed");
        }
        exec(opsDs, "DELETE FROM file_arrival");
        exec(opsDs, "UPDATE agt_lease SET expires_at = now() - INTERVAL '1 second'");
        assertTrue(lease.tryAcquire(config.holderId()), "test precondition: this instance holds the lease");
    }

    @AfterEach
    void detachLogCapture() {
        Logger.getLogger(ReportTrigger.class.getName()).removeHandler(warns);
    }

    @Test
    void launchesOnePrgImmediatePerDueParentWithParseSafeParams() {
        seedParent("FNBCC01", "DCRECC2026071600000001", "COMPLETE");
        seedParent("FNBCC01", "DCRECC2026071600000002", "IDLE");
        seedParent("FNBRF01", "DCRERF2026071600000003", "COMPLETE");

        trigger.tick();

        final ArgumentCaptor<Flow> flows = ArgumentCaptor.captor();
        final ArgumentCaptor<UUID> arrivalIds = ArgumentCaptor.captor();
        final ArgumentCaptor<String> runKeys = ArgumentCaptor.captor();
        final ArgumentCaptor<List<String>> launchArgs = ArgumentCaptor.captor();
        verify(launcher, times(3)).launchArrivalReport(
                flows.capture(), eq(Stage.CRG), arrivalIds.capture(), runKeys.capture(), launchArgs.capture());

        final Map<String, List<String>> byRunKey = new HashMap<>();
        final Map<String, Flow> flowByRunKey = new HashMap<>();
        final Map<String, UUID> arrivalByRunKey = new HashMap<>();
        for (int i = 0; i < runKeys.getAllValues().size(); i++) {
            byRunKey.put(runKeys.getAllValues().get(i), launchArgs.getAllValues().get(i));
            flowByRunKey.put(runKeys.getAllValues().get(i), flows.getAllValues().get(i));
            arrivalByRunKey.put(runKeys.getAllValues().get(i), arrivalIds.getAllValues().get(i));
        }
        assertEquals(3, byRunKey.size(), "run keys are distinct per parent: " + byRunKey.keySet());

        assertImmediateLaunch(byRunKey, arrivalByRunKey, "FNBCC01", "DCRECC2026071600000001");
        assertImmediateLaunch(byRunKey, arrivalByRunKey, "FNBCC01", "DCRECC2026071600000002");
        assertImmediateLaunch(byRunKey, arrivalByRunKey, "FNBRF01", "DCRERF2026071600000003");

        // v1 topology: the family comes from the DATABASE the due row was read
        // from, not from the client's pay-clients membership. Every row here was
        // seeded in dcre_col, so every launch is a COLLECTIONS report, including
        // FNBRF01's: a pay client can still have collections instructions, and the
        // view it appeared in is what says which generator owns it.
        flowByRunKey.forEach((key, flow) -> assertEquals(Flow.COL, flow,
                "a row read from the collections database is a collections report: " + key));
    }

    @Test
    void aPaymentsParentIsDiscoveredFromThePaymentsDatabaseAndLaunchesPrg() {
        // The seam the PRG builder flagged as most easily missed. It is not a
        // rename: with only the collections datasource wired, a payments parent is
        // never seen at all, no exception is thrown and nothing is logged. The
        // payments IMMEDIATE report simply never fires.
        seedPaymentsParent("FNBRF01", "DCRERF2026080800000101", "COMPLETE");

        trigger.tick();

        final ArgumentCaptor<Flow> flows = ArgumentCaptor.captor();
        final ArgumentCaptor<List<String>> launchArgs = ArgumentCaptor.captor();
        verify(launcher, times(1)).launchArrivalReport(
                flows.capture(), eq(Stage.PRG), any(UUID.class), anyString(), launchArgs.capture());
        assertEquals(Flow.PAY, flows.getValue(), "a payments parent launches into the pay flow");
        assertTrue(launchArgs.getValue().contains("parents=DCRERF2026080800000101,java.lang.String,false"),
                launchArgs.getValue().toString());
    }

    @Test
    void thetwoFamiliesAreScannedIndependentlyAndNeverCrossOver() {
        // Same client, one due parent in EACH database. Two launches, each on its
        // own generator. A single shared read seam would produce two CRG launches,
        // or two PRG ones, and the test would still see "two launches".
        seedParent("FNBRF01", "DCRERF2026080800000201", "COMPLETE");
        seedPaymentsParent("FNBRF01", "DCRERF2026080800000202", "COMPLETE");

        trigger.tick();

        verify(launcher, times(1)).launchArrivalReport(
                eq(Flow.COL), eq(Stage.CRG), any(UUID.class),
                eq("FNBRF01-imm-" + digest12("DCRERF2026080800000201")), anyList());
        verify(launcher, times(1)).launchArrivalReport(
                eq(Flow.PAY), eq(Stage.PRG), any(UUID.class),
                eq("FNBRF01-imm-" + digest12("DCRERF2026080800000202")), anyList());
    }

    @Test
    void aParentThatNeverSettlesEventuallyWarnsInsteadOfLoopingSilently() {
        // The escalation guard. A parent the generator cannot satisfy stays in the
        // view forever; the deterministic window key makes every later scan an
        // idempotent no-op, so nothing throws, nothing is ledgered and nothing is
        // logged. This does NOT retry, widen or fall back: it only makes the loop
        // visible, because a silent infinite retrigger must not be made quieter.
        seedParent("FNBCC01", "DCRECC2026080800000301", "COMPLETE");

        trigger.tick();
        assertTrue(warns.lines.stream().noneMatch(w -> w.startsWith("report-stall")),
                "one scan is not a stall: " + warns.lines);

        trigger.tick(); // reaches agt.report-stall-scans=2

        assertTrue(warns.lines.stream().anyMatch(w -> w.startsWith("report-stall")
                        && w.contains("stage=CRG") && w.contains("flow=COL")
                        && w.contains("client=FNBCC01")
                        && w.contains("parent=DCRECC2026080800000301")
                        && w.contains("window=imm-" + digest12("DCRECC2026080800000301"))),
                "the WARN must name the stage, flow, client, parent and window: " + warns.lines);

        trigger.tick();
        assertEquals(1, warns.lines.stream().filter(w -> w.startsWith("report-stall")).count(),
                "one line per stalled parent, not one per tick: " + warns.lines);
    }

    @Test
    void aParentThatSettlesNeverAccumulatesTowardTheStallWarn() {
        // The counter must reset when the view stops reporting the parent, or a
        // long-lived healthy client eventually trips the WARN for no reason.
        seedParent("FNBCC01", "DCRECC2026080800000401", "COMPLETE");
        trigger.tick();
        execCollections("DELETE FROM prg_report_due_seed"); // the report settled it
        trigger.tick();
        seedDue("FNBCC01", "DCRECC2026080800000401", "COMPLETE"); // due again, later
        trigger.tick();

        assertTrue(warns.lines.stream().noneMatch(w -> w.startsWith("report-stall")),
                "a settled parent starts from zero when it next becomes due: " + warns.lines);
    }

    @Test
    void immediateWindowIsDeterministicAcrossRelaunches() throws InterruptedException {
        // SCRUM-90: the IMMEDIATE window IS the Spring Batch identifying param
        // and drives the <client>_PSR_<window>.txt output name (prg_report
        // UNIQUE(file_name)). A relaunch of the SAME (client, parent) must mint
        // the SAME window so a FAILED PRG instance RESUMES instead of churning a
        // fresh instance, and the report stays a single idempotent file. Two
        // ticks straddling an epoch-second boundary must therefore yield an
        // identical run key: pre-fix the epoch-seeded window changed every launch.
        seedParent("FNBCC01", "DCRECC2026072300000001", "COMPLETE");

        trigger.tick();
        Thread.sleep(1_100L); // guarantee a wall-clock second rollover between launches
        trigger.tick();

        final ArgumentCaptor<String> runKeys = ArgumentCaptor.captor();
        verify(launcher, times(2)).launchArrivalReport(
                any(Flow.class), eq(Stage.CRG), any(UUID.class), runKeys.capture(), anyList());
        assertEquals(runKeys.getAllValues().get(0), runKeys.getAllValues().get(1),
                "relaunch must reuse an identical IMMEDIATE window (deterministic from client+parent): "
                        + runKeys.getAllValues());
        assertEquals("FNBCC01-imm-" + digest12("DCRECC2026072300000001"), runKeys.getAllValues().get(0),
                "window derives only from (client, parent), never wall-clock time");
    }

    @Test
    void duplicateDueRowsForOneParentLaunchOnce() {
        seedParent("FNBCC01", "DCRECC2026071600000007", "COMPLETE");
        seedDue("FNBCC01", "DCRECC2026071600000007", "IDLE"); // a second due row, same parent
        trigger.tick();
        verify(launcher, times(1)).launchArrivalReport(
                any(Flow.class), eq(Stage.CRG), any(UUID.class), anyString(), anyList());
    }

    @Test
    void runKeyStaysDnsSafeAtMaxFieldWidths() {
        seedParent("FNBCLIENTMAX0016", "M".repeat(35), "COMPLETE");
        trigger.tick();
        final ArgumentCaptor<String> runKeys = ArgumentCaptor.captor();
        verify(launcher, times(1)).launchArrivalReport(
                any(Flow.class), eq(Stage.CRG), any(UUID.class), runKeys.capture(), anyList());
        final String jobName = JobLauncher.clockJobName(Flow.COL, Stage.CRG, runKeys.getValue());
        assertTrue(jobName.length() <= 63, "K8s label limit: " + jobName + " (" + jobName.length() + ")");
        assertTrue(jobName.matches("[a-z0-9]([-a-z0-9]*[a-z0-9])?"), "DNS-1123: " + jobName);
    }

    @Test
    void maliciousSourceMsgIdWithCommaIsSkippedWithUnsafeTokenWarnAndNoLaunch() {
        // Fail-closed guard: a comma in a view-sourced value shifts the
        // name=value,type,identifying tokens, so the parent must be excluded
        // (WARN, value sanitized then elided to 8 chars: unsafe bytes never
        // reach the log line, CWE-117) before any arrival lookup or launch.
        seedDue("FNBCC01", "EVIL,java.lang.Long", "COMPLETE");

        trigger.tick();

        verifyNoInteractions(launcher);
        assertTrue(warns.lines.stream().anyMatch(w ->
                        w.equals("excluded stage=AGT reason=UNSAFE_TOKEN field=sourceMsgId value=EVIL?jav")),
                "UNSAFE_TOKEN WARN with field name and value elided to 8 chars: " + warns.lines);
    }

    @Test
    void unknownArrivalFailsClosedWithWarnAndNoLaunch() {
        // SCRUM-90 fail-closed: a due parent with no resolvable source-book
        // arrival must NOT launch an unscoped IMMEDIATE report (an unscoped
        // clock intent is exactly what the sweeps cannot recover). No arrival is
        // seeded for this parent.
        seedDue("FNBCC01", "DCRECC2026072300000404", "COMPLETE");

        trigger.tick();

        verifyNoInteractions(launcher);
        assertTrue(warns.lines.stream().anyMatch(w ->
                        w.contains("reason=UNKNOWN_ARRIVAL")
                                && w.contains("parent=DCRECC2026072300000404")),
                "fail-closed UNKNOWN_ARRIVAL WARN, no launch: " + warns.lines);
    }

    @Test
    void validParentStillLaunchesWhenAMaliciousSiblingIsSkipped() {
        seedParent("FNBCC01", "DCRECC2026071600000010", "COMPLETE");
        seedDue("FNBCC01", "BAD=EQUALS_SIGN", "IDLE");

        trigger.tick();

        final ArgumentCaptor<List<String>> launchArgs = ArgumentCaptor.captor();
        verify(launcher, times(1)).launchArrivalReport(
                any(Flow.class), eq(Stage.CRG), any(UUID.class), anyString(), launchArgs.capture());
        assertTrue(launchArgs.getValue().contains("parents=DCRECC2026071600000010,java.lang.String,false"),
                "only the safe parent launches: " + launchArgs.getValue());
        assertTrue(warns.lines.stream().anyMatch(w ->
                        w.contains("reason=UNSAFE_TOKEN field=sourceMsgId value=BAD?EQUA")),
                "WARN for the excluded sibling: " + warns.lines);
    }

    @Test
    void reportDueReadsTheViewContract() {
        seedDue("FNBCC02", "DCRECC2026071600000009", "IDLE");
        final List<FamilyReadRepo.DueParent> due = families.reportDue(Flow.COL);
        assertEquals(1, due.size());
        assertEquals(new FamilyReadRepo.DueParent("FNBCC02", "DCRECC2026071600000009", "IDLE"),
                due.get(0));
    }

    @Test
    void emptyViewLaunchesNothing() {
        trigger.tick();
        verifyNoInteractions(launcher);
    }

    @Test
    void noLaunchWithoutTheLease() {
        seedParent("FNBCC01", "DCRECC2026071600000004", "COMPLETE");
        exec(opsDs, "UPDATE agt_lease SET holder = 'other-agt', expires_at = now() + INTERVAL '120 seconds'");
        trigger.tick();
        verifyNoInteractions(launcher);
    }

    private void assertImmediateLaunch(final Map<String, List<String>> byRunKey,
                                       final Map<String, UUID> arrivalByRunKey, final String client,
                                       final String sourceMsgId) {
        final String window = "imm-" + digest12(sourceMsgId);
        final String runKey = client + "-" + window;
        final List<String> args = byRunKey.get(runKey);
        assertNotNull(args, "one launch keyed <client>-imm-<digest12(parent)>: " + byRunKey.keySet());
        assertEquals(arrivalsByKey.get(client + "|" + sourceMsgId), arrivalByRunKey.get(runKey),
                "the launch is arrival-scoped to the resolved parent book");
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

    /** Seed a due row AND its matching source-book arrival (client, msgId), so
     *  the SCRUM-90 arrival lookup resolves. Pay clients ride onhost-req-endo. */
    private void seedParent(final String client, final String sourceMsgId, final String reason) {
        seedDue(client, sourceMsgId, reason);
        final String route = "FNBRF01".equals(client) ? "onhost-req-endo" : "onhost-req";
        final UUID id = arrivalRepo.insertArrival(UUID.randomUUID(), route,
                client + "_" + sourceMsgId + ".txt", "sha-" + client + "-" + sourceMsgId,
                client, sourceMsgId, ArrivalStatus.DAG_COMPLETE, null,
                "/exchange/claimed/" + client + "_" + sourceMsgId + ".txt").orElseThrow();
        arrivalsByKey.put(client + "|" + sourceMsgId, id);
    }

    /** The payments twin of seedParent: the due row lands in the PAYMENTS database. */
    private void seedPaymentsParent(final String client, final String sourceMsgId, final String reason) {
        seedDue(paymentsDs, client, sourceMsgId, reason);
        final UUID id = arrivalRepo.insertArrival(UUID.randomUUID(), "onhost-req-endo",
                client + "_" + sourceMsgId + ".txt", "sha-" + client + "-" + sourceMsgId,
                client, sourceMsgId, ArrivalStatus.DAG_COMPLETE, null,
                "/exchange/claimed/" + client + "_" + sourceMsgId + ".txt").orElseThrow();
        arrivalsByKey.put(client + "|" + sourceMsgId, id);
    }

    private void seedDue(final String client, final String sourceMsgId, final String reason) {
        seedDue(collectionsDs, client, sourceMsgId, reason);
    }

    private void seedDue(final AgroalDataSource ds, final String client,
                         final String sourceMsgId, final String reason) {
        try (Connection c = ds.getConnection();
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
