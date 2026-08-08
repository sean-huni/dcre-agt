package za.co.fnb.dcre.agt;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

/** Real CockroachDB for tests (same engine as dev/prod; parity over H2). */
public class CrdbTestResource implements QuarkusTestResourceLifecycleManager {

    /** The payments database, created inside the same container as a SEPARATE database. */
    static final String PAY_DB = "dcre_pay_test";

    private CockroachContainer crdb;

    @Override
    public Map<String, String> start() {
        crdb = new CockroachContainer(
                DockerImageName.parse("cockroachdb/cockroach:v26.2.3"));
        crdb.start();
        createPaymentsDatabase();
        return Map.of(
                "quarkus.datasource.jdbc.url", crdb.getJdbcUrl(),
                "quarkus.datasource.username", crdb.getUsername(),
                "quarkus.datasource.password", crdb.getPassword(),
                // SCRUM-55: the read-only collections datasource shares the test
                // container; ReportTriggerTest creates the prg_report_due contract.
                "quarkus.datasource.collections.jdbc.url", crdb.getJdbcUrl(),
                "quarkus.datasource.collections.username", crdb.getUsername(),
                "quarkus.datasource.collections.password", crdb.getPassword(),
                // v1 topology: the payments read seam. A SEPARATE DATABASE, not the
                // same one under a second name. Both dcre_col and dcre_pay publish a
                // view called prg_report_due, so pointing the two datasources at one
                // database would make every test see each due row twice and could
                // never show which database a report was discovered from. That is the
                // fixture-monoculture trap: a dimension with one value cannot exercise
                // what that dimension drives.
                "quarkus.datasource.payments.jdbc.url", payJdbcUrl(),
                "quarkus.datasource.payments.username", crdb.getUsername(),
                "quarkus.datasource.payments.password", crdb.getPassword());
    }

    /** JDBC url of the container with the database path swapped for the payments one. */
    static String payJdbcUrl(final String baseUrl) {
        final int query = baseUrl.indexOf('?');
        final String beforeQuery = query < 0 ? baseUrl : baseUrl.substring(0, query);
        final String suffix = query < 0 ? "" : baseUrl.substring(query);
        return beforeQuery.substring(0, beforeQuery.lastIndexOf('/') + 1) + PAY_DB + suffix;
    }

    private String payJdbcUrl() {
        return payJdbcUrl(crdb.getJdbcUrl());
    }

    private void createPaymentsDatabase() {
        try (Connection c = DriverManager.getConnection(
                crdb.getJdbcUrl(), crdb.getUsername(), crdb.getPassword());
             Statement s = c.createStatement()) {
            s.execute("CREATE DATABASE IF NOT EXISTS " + PAY_DB);
        } catch (SQLException e) {
            throw new IllegalStateException("could not create the " + PAY_DB + " test database", e);
        }
    }

    @Override
    public void stop() {
        if (crdb != null) {
            crdb.stop();
        }
    }
}
