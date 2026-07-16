package za.co.fnb.dcre.agt;

import io.agroal.api.AgroalDataSource;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.repo.CollectionsReadRepo;
import za.co.fnb.dcre.agt.repo.CollectionsReadRepo.SlaPending;
import za.co.fnb.dcre.agt.service.SlaMonitor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SlaMonitor (SCRUM-55 Task 13): scans the collections-side prg_sla_pending
 * view and republishes truth per tick as per-client gauges
 * dcre_sla_pending_amber / dcre_sla_pending_red (20h / 24h) plus WARN lines
 * "sla stage=FINT client={} e2e={} ageHours={} level=AMBER|RED", at most one
 * WARN per (client, e2e, level) per tick batch. The PRG lane's reporting changelog is
 * not on this branch yet, so the test creates a minimal compatible view
 * (contract: client, e2e, outbound_msg_id, visible_at, age_hours with
 * age_hours derived exactly as the ratified view derives it) over a seed
 * table, mirroring the ReportTriggerTest pattern.
 */
@QuarkusTest
@QuarkusTestResource(CrdbTestResource.class)
class SlaMonitorTest {

    @Inject
    SlaMonitor monitor;

    @Inject
    CollectionsReadRepo collectionsRepo;

    @Inject
    AgtConfig config;

    @Inject
    MeterRegistry registry;

    @Inject
    @io.quarkus.agroal.DataSource("collections")
    AgroalDataSource collectionsDs;

    private CapturingHandler warns;

    /**
     * Gauge read-back point: the app registry is the Metrics.globalRegistry
     * composite whose only child is the OTel Micrometer bridge, and bridge
     * gauges return NaN on direct value() reads (UnsupportedReadLogger). A
     * SimpleMeterRegistry attached to the composite receives every gauge
     * (existing meters forward on add) and reads the live holders.
     */
    private SimpleMeterRegistry meterReader;

    @BeforeEach
    void resetViewAndLogCapture() {
        meterReader = new SimpleMeterRegistry();
        ((CompositeMeterRegistry) registry).add(meterReader);
        execCollections("CREATE TABLE IF NOT EXISTS prg_sla_pending_seed ("
                + "client VARCHAR(16) NOT NULL, e2e VARCHAR(35) NOT NULL, "
                + "outbound_msg_id VARCHAR(35) NOT NULL, visible_at TIMESTAMPTZ NOT NULL)");
        execCollections("CREATE OR REPLACE VIEW prg_sla_pending AS "
                + "SELECT client, e2e, outbound_msg_id, visible_at, "
                + "EXTRACT(EPOCH FROM (now() - visible_at)) / 3600 AS age_hours "
                + "FROM prg_sla_pending_seed");
        execCollections("DELETE FROM prg_sla_pending_seed");
        warns = new CapturingHandler();
        Logger.getLogger(SlaMonitor.class.getName()).addHandler(warns);
    }

    @AfterEach
    void detachLogCaptureAndMeterReader() {
        Logger.getLogger(SlaMonitor.class.getName()).removeHandler(warns);
        ((CompositeMeterRegistry) registry).remove(meterReader);
        meterReader.close();
    }

    @Test
    void amberAndRedRowsSetGaugesAndWarnPerLevel() {
        seedPending("FNBCC01", "E2EAMBER00000001", 21);
        seedPending("FNBCC01", "E2ERED0000000001", 25);

        monitor.tick();

        assertEquals(1.0, gaugeValue("dcre_sla_pending_amber", "FNBCC01"), "amber = rows in [20h, 24h)");
        assertEquals(1.0, gaugeValue("dcre_sla_pending_red", "FNBCC01"), "red = rows >= 24h");
        assertWarnLine("FNBCC01", "E2EAMBER00000001", "AMBER");
        assertWarnLine("FNBCC01", "E2ERED0000000001", "RED");
    }

    @Test
    void rowsUnderAmberThresholdProduceNeitherGaugeNorWarn() {
        seedPending("FNBCC03", "E2EFRESH00000001", 19);

        monitor.tick();

        assertNull(meterReader.find("dcre_sla_pending_amber").tag("client", "FNBCC03").gauge(),
                "no amber gauge for a client with only sub-20h rows");
        assertNull(meterReader.find("dcre_sla_pending_red").tag("client", "FNBCC03").gauge(),
                "no red gauge for a client with only sub-20h rows");
        assertTrue(warns.lines.stream().noneMatch(w -> w.contains("E2EFRESH00000001")),
                "no WARN under the amber threshold: " + warns.lines);
    }

    @Test
    void clearedBacklogZeroesTheGaugeOnTheNextTick() {
        seedPending("FNBRF01", "E2ECLEAR00000001", 22);
        monitor.tick();
        assertEquals(1.0, gaugeValue("dcre_sla_pending_amber", "FNBRF01"));

        execCollections("DELETE FROM prg_sla_pending_seed");
        monitor.tick();

        assertEquals(0.0, gaugeValue("dcre_sla_pending_amber", "FNBRF01"),
                "level-triggered truth: a cleared backlog drops the gauge to 0, not stale 1");
    }

    @Test
    void warnsPerClientWhenTwoClientsShareAnE2eAtTheSameLevel() {
        // Cross-entity collision guard (idempotency-key rule): the dedup key
        // must be the FULL tuple (client, e2e, level). A subset key (e2e,
        // level) silently swallows the second client's WARN line.
        seedPending("FNBCC01", "E2ESHARED0000001", 21);
        seedPending("FNBRF01", "E2ESHARED0000001", 21);

        monitor.tick();

        assertWarnLine("FNBCC01", "E2ESHARED0000001", "AMBER");
        assertWarnLine("FNBRF01", "E2ESHARED0000001", "AMBER");
    }

    @Test
    void warnsAtMostOncePerE2eAndLevelPerTickBatch() {
        seedPending("FNBCC02", "E2EDUP0000000001", 21);
        seedPending("FNBCC02", "E2EDUP0000000001", 21);

        monitor.tick();

        assertEquals(2.0, gaugeValue("dcre_sla_pending_amber", "FNBCC02"), "gauge counts rows");
        assertEquals(1, warns.lines.stream().filter(w -> w.contains("e2e=E2EDUP0000000001 ")).count(),
                "one WARN per (e2e, level) per tick batch: " + warns.lines);
    }

    @Test
    void slaCountsReadsTheViewContractFilteredAndOrdered() {
        seedPending("FNBCC01", "E2EORDER00000001", 21);
        seedPending("FNBCC01", "E2EORDER00000002", 25);
        seedPending("FNBCC01", "E2EORDER00000003", 5);

        final List<SlaPending> rows = collectionsRepo.slaCounts(20);

        assertEquals(2, rows.size(), "WHERE age_hours >= :amber filters the 5h row");
        assertEquals("E2EORDER00000002", rows.get(0).e2e(), "ORDER BY age_hours DESC");
        assertEquals("E2EORDER00000001", rows.get(1).e2e());
        assertEquals("FNBCC01", rows.get(0).client());
        assertTrue(rows.get(0).ageHours() >= 25.0, "age_hours derived from visible_at: " + rows.get(0));
    }

    @Test
    void slaKnobsDefaultTo20Amber24Red300sScan() {
        assertEquals(20, config.slaAmberHours());
        assertEquals(24, config.slaRedHours());
        assertEquals("300",
                ConfigProvider.getConfig().getValue("dcre.agt.sla-scan-seconds", String.class));
    }

    private double gaugeValue(final String metric, final String client) {
        final Gauge gauge = meterReader.find(metric).tag("client", client).gauge();
        assertNotNull(gauge, metric + "{client=" + client + "} not registered");
        return gauge.value();
    }

    private void assertWarnLine(final String client, final String e2e, final String level) {
        final Pattern line = Pattern.compile("sla stage=FINT client=" + client
                + " e2e=" + e2e + " ageHours=\\d+(\\.\\d+)? level=" + level);
        assertTrue(warns.lines.stream().anyMatch(w -> line.matcher(w).matches()),
                level + " WARN line for " + e2e + " missing in: " + warns.lines);
    }

    private void seedPending(final String client, final String e2e, final int hoursAgo) {
        try (Connection c = collectionsDs.getConnection();
             PreparedStatement p = c.prepareStatement(
                     "INSERT INTO prg_sla_pending_seed (client, e2e, outbound_msg_id, visible_at) "
                             + "VALUES (?, ?, ?, now() - ? * INTERVAL '1 hour')")) {
            p.setString(1, client);
            p.setString(2, e2e);
            p.setString(3, "OUT" + e2e);
            p.setInt(4, hoursAgo);
            p.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("seed failed", e);
        }
    }

    private void execCollections(final String sql) {
        try (Connection c = collectionsDs.getConnection();
             PreparedStatement p = c.prepareStatement(sql)) {
            p.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("test SQL failed: " + sql, e);
        }
    }

    /**
     * Captures formatted WARN+ lines emitted by the SlaMonitor category.
     * SlaMonitor logs exclusively via Logger.warnf (printf style), so applying
     * String.format over the record parameters reproduces the emitted line.
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
