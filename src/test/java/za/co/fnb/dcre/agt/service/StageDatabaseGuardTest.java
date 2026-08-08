package za.co.fnb.dcre.agt.service;

import org.junit.jupiter.api.Test;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.domain.DbFamily;
import za.co.fnb.dcre.agt.domain.Stage;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The url cross-check refuses a family url pointing at another context's database,
 * and this asserts THE GUARD'S OWN WORDING, not merely that something threw.
 *
 * <p>The distinction is not pedantry, it cost the shared-reference agent a round:
 * {@code SpringApplicationBuilder.properties()} loses to {@code application.yml}, so its
 * first startup-refusal test passed for the wrong reason. The app never saw the wrong
 * database at all; it fell back to the localhost default and died with
 * "database does not exist", which resembles a refusal closely enough to be mistaken
 * for one. A test asserting only "startup failed" cannot tell a guard that fired from a
 * guard that was never reached.
 *
 * <p>So every assertion here names a substring only the guard produces, and the
 * happy-path control proves the same call succeeds when the url is right, which is what
 * distinguishes "the guard rejected this" from "this configuration cannot work at all".
 */
class StageDatabaseGuardTest {

    /** A minimal AgtConfig whose db-url methods answer from a map; everything else fails. */
    private static AgtConfig configWith(final Map<String, String> urls) {
        final InvocationHandler handler = (proxy, method, args) -> {
            final String url = urls.get(method.getName());
            if (url == null) {
                throw new UnsupportedOperationException("test config does not stub " + method.getName());
            }
            return url;
        };
        return (AgtConfig) Proxy.newProxyInstance(AgtConfig.class.getClassLoader(),
                new Class<?>[]{AgtConfig.class}, handler);
    }

    private static StageDatabases databasesWith(final Map<String, String> urls) {
        final StageDatabases databases = new StageDatabases();
        databases.config = configWith(urls);
        return databases;
    }

    private static final String CORRECT_HCS =
            "jdbc:postgresql://crdb.dcre.svc.cluster.local:26257/dcre_hcs?sslmode=disable";
    private static final String COLLECTIONS =
            "jdbc:postgresql://crdb.dcre.svc.cluster.local:26257/dcre_col?sslmode=disable";

    /**
     * The control, and it comes first on purpose: without it, the refusal below could
     * equally mean the whole call path is broken.
     */
    @Test
    void aCorrectlyPointedFamilyUrlIsHandedStraightThrough() {
        final StageDatabases databases = databasesWith(Map.of("hcsServiceDbUrl", CORRECT_HCS));

        assertEquals(CORRECT_HCS, databases.urlFor(Stage.HCS),
                "a url addressing the family's own database passes unchanged");
    }

    /**
     * The one-variable typo the guard exists for: {@code AGT_HCS_SERVICE_DB_URL} left
     * pointing at the collections database, which is where the holiday calendar used to
     * live and is therefore the exact wrong value somebody will paste.
     */
    @Test
    void aFamilyUrlPointingAtAnotherContextsDatabaseIsRefusedByName() {
        final StageDatabases databases = databasesWith(Map.of("hcsServiceDbUrl", COLLECTIONS));

        final IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> databases.urlFor(Stage.HCS));

        final String message = thrown.getMessage();
        // Both sides named, so a wrong answer can never be read as an unavailable one.
        assertTrue(message.contains("HCS family owns database 'dcre_hcs'"),
                "the guard must name the family and the database it owns, got: " + message);
        assertTrue(message.contains("addresses 'dcre_col'"),
                "the guard must name what it actually found, got: " + message);
        assertTrue(message.contains("refusing to hand a stage pod another context's database"),
                "the guard must say what it refused to do, got: " + message);
        assertTrue(message.contains(COLLECTIONS),
                "the guard must echo the offending url so it can be found in config, got: " + message);
    }

    /**
     * The same refusal for a FAMILY database, so the guard is not HCS-specific.
     *
     * <p>This arm used to point at the ACS account context, which was retired with
     * {@code shared/acs} and {@code dcre_acs} on 2026-08-09. It is repointed rather than
     * deleted: the property under test is that the cross-check reads the family from the
     * enum instead of special-casing one of them, and losing the only second family here
     * would leave that property asserted nowhere.
     */
    @Test
    void aSecondFamilyIsGuardedTheSameWay() {
        final StageDatabases databases = databasesWith(Map.of("payServiceDbUrl", COLLECTIONS));

        final IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> databases.urlFor(DbFamily.PAY));

        assertTrue(thrown.getMessage().contains("PAY family owns database 'dcre_pay'"),
                "got: " + thrown.getMessage());
    }

    /**
     * A url with no parseable database segment is refused too, rather than compared
     * against null and passed. An unparseable value is "I could not look", which must
     * never read as "it looked fine".
     */
    @Test
    void anUnparseableUrlIsRefusedRatherThanAccepted() {
        final StageDatabases databases = databasesWith(Map.of("hcsServiceDbUrl", "not-a-jdbc-url"));

        final IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> databases.urlFor(Stage.HCS));

        assertTrue(thrown.getMessage().contains("HCS family owns database 'dcre_hcs'"),
                "got: " + thrown.getMessage());
    }
}
