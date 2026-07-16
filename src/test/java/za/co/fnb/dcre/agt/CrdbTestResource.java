package za.co.fnb.dcre.agt;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

/** Real CockroachDB for tests (same engine as dev/prod; parity over H2). */
public class CrdbTestResource implements QuarkusTestResourceLifecycleManager {

    private CockroachContainer crdb;

    @Override
    public Map<String, String> start() {
        crdb = new CockroachContainer(
                DockerImageName.parse("cockroachdb/cockroach:v26.2.3"));
        crdb.start();
        return Map.of(
                "quarkus.datasource.jdbc.url", crdb.getJdbcUrl(),
                "quarkus.datasource.username", crdb.getUsername(),
                "quarkus.datasource.password", crdb.getPassword(),
                // SCRUM-55: the read-only collections datasource shares the test
                // container; ReportTriggerTest creates the prg_report_due contract.
                "quarkus.datasource.collections.jdbc.url", crdb.getJdbcUrl(),
                "quarkus.datasource.collections.username", crdb.getUsername(),
                "quarkus.datasource.collections.password", crdb.getPassword());
    }

    @Override
    public void stop() {
        if (crdb != null) {
            crdb.stop();
        }
    }
}
