package za.co.fnb.dcre.agt.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.domain.DbFamily;
import za.co.fnb.dcre.agt.domain.Stage;

/**
 * The database a stage's Job writes: {@code DCRE_DB_URL}, per family.
 *
 * <p>This class exists because the previous form was the highest-severity defect
 * in the v1 topology work:
 *
 * <pre>
 * return MAN_STAGES.contains(stage) ? config.manServiceDbUrl() : config.serviceDbUrl();
 * </pre>
 *
 * Two families, a boolean, and a DEFAULT. Adding a third family that way hands
 * every payments stage the collections URL, so PRW creates {@code prw_*} tables
 * inside {@code dcre_col} and nothing errors, because the write is perfectly valid
 * against the wrong database. Namespace isolation without data isolation is the
 * exact coupling the family split exists to remove.
 *
 * <p>So: exhaustive switches with NO default arm. A stage added to {@link Stage},
 * or a family added to {@link DbFamily}, breaks THIS compilation until somebody
 * names its database. There is no "everything else" any more.
 *
 * <p><b>This class answers ONE question: which database.</b> The k8s NAMESPACE a
 * Job lands in is a different question and lives in {@link StageNamespaces}. They
 * were the same switch until 2026-08-08, and that is exactly how HCS ended up
 * writing {@code dcre_col}: "hosted in the collections namespace" is true, and the
 * same answer was then read as "writes the collections database", which is not.
 *
 * <p>STAGE-keyed rather than launch-keyed, deliberately and unchanged from B2
 * (SCRUM-79 review): the reconciled re-create path ({@code JobLauncher.createJob})
 * holds only the durable intent row, so the URL must be derivable from the stage
 * alone or a re-created pod would address a different database than the original.
 */
@ApplicationScoped
public class StageDatabases {

    @Inject
    AgtConfig config;

    /** The JDBC url handed to a stage pod as {@code DCRE_DB_URL}. */
    public String urlFor(final Stage stage) {
        return urlFor(dbFamily(stage));
    }

    /**
     * The JDBC url a family's pods address, cross-checked against the database that
     * family owns.
     *
     * <p>Exhaustive, no default arm. The cross-check is the AGT-side twin of
     * {@code shared/hcs}'s {@code FamilyGuard}: pointing one of these knobs at
     * another context's database is a one-variable typo that otherwise reads as a
     * working deployment, because the write is perfectly valid against the wrong
     * database. Here it fails loudly at Job-BUILD time, before a pod exists, and the
     * message names both sides so a wrong answer can never be mistaken for an
     * unavailable one.
     */
    public String urlFor(final DbFamily family) {
        final String url = switch (family) {
            case COL -> config.serviceDbUrl();
            case PAY -> config.payServiceDbUrl();
            case MAN -> config.manServiceDbUrl();
            case HCS -> config.hcsServiceDbUrl();
            case ACS -> config.acsServiceDbUrl();
        };
        return requireAddresses(family, url);
    }

    /**
     * The database family a stage belongs to.
     *
     * <p>Exhaustive, no default arm: this is the single place that says which
     * database owns a stage's writes, and a new constant must be assigned one before
     * anything compiles. That is not theoretical. {@code ACS} landed while this file
     * was being written, and adding the constant to {@link Stage} broke THIS switch
     * until its database was named, which is exactly the outcome a set with an
     * "everything else" arm would have denied us: it would have absorbed the new stage
     * into collections silently, the way {@code HCS} was absorbed.
     *
     * <p><b>HCS is its own family and no longer sits with collections.</b> It was
     * enumerated into {@code Flow.COL} here with the reasoning that it is the single
     * writer of {@code public_holiday} (R-04) and CDE reads that calendar. The owner
     * ruled that reasoning out on 2026-08-08: the calendar moved to {@code dcre_hcs},
     * CDE reads it across a boundary, and {@code shared/hcs} now carries a
     * {@code FamilyGuard} on {@code current_database()} that refuses to start against
     * anything else. Left as it was, every HCS pod AGT launched would die on startup.
     */
    public DbFamily dbFamily(final Stage stage) {
        return switch (stage) {
            case CRR, CTV, CDE, CRW, CIR, CIX, CSX, CPX, CRG -> DbFamily.COL;
            case PRR, PTV, PAI, PRW, PIR, PIX, PSX, PPX, PRG -> DbFamily.PAY;
            case MRR, MRV, MAS, MIT, MIR, MRW, MIX, MSX, MPX, MRG -> DbFamily.MAN;
            case HCS -> DbFamily.HCS;
            case ACS -> DbFamily.ACS;
        };
    }

    /** Fails closed on a url that addresses a database the family does not own. */
    private static String requireAddresses(final DbFamily family, final String url) {
        if (!family.database().equals(databaseOf(url))) {
            throw new IllegalStateException("the " + family + " family owns database '"
                    + family.database() + "' but its configured url addresses '" + databaseOf(url)
                    + "' (" + url + "); refusing to hand a stage pod another context's database."
                    + " Fix the AGT_*_SERVICE_DB_URL for this family rather than the guard.");
        }
        return url;
    }

    /** The database segment of a JDBC url: after the last '/', before any query. */
    private static String databaseOf(final String url) {
        if (url == null) {
            return null;
        }
        final int slash = url.lastIndexOf('/');
        if (slash < 0) {
            return null;
        }
        final String tail = url.substring(slash + 1);
        final int query = tail.indexOf('?');
        return query < 0 ? tail : tail.substring(0, query);
    }
}
