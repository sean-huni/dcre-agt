package za.co.fnb.dcre.agt.domain;

/**
 * The DATABASE a stage's Job writes, and the only one it may write.
 *
 * <p>This is a DIFFERENT question from {@link Flow}, which classifies the k8s
 * NAMESPACE a Job lands in, and the two were the same enum until 2026-08-08. That
 * conflation is what put HCS in {@code Flow.COL}: HCS is hosted in the collections
 * namespace, which is true, and the same answer was then read as "HCS writes
 * {@code dcre_col}", which is false. One enum answering two questions gives the
 * wrong answer to one of them the moment they diverge, silently, because both
 * answers are well-formed.
 *
 * <p>The owner's 2026-08-08 ruling, in his words a "Violation of the 12FactorApp"
 * (https://12factor.net/): holiday data must never live in {@code dcre_col}, and
 * cross-family reference data gets its own bounded context and its own database.
 * {@code hcs} is a context in its own right on all three tests (distinct invariants,
 * distinct rate of change, distinct failure domain), so it is a family here rather
 * than a tenant of somebody else's database.
 *
 * <pre>
 * dcre_col   collections   CRR CTV CDE CRW CIR CIX CSX CPX CRG
 * dcre_pay   payments      PRR PTV PAI PRW PIR PIX PSX PPX PRG
 * dcre_man   mandates      MRR MRV MAS MIT MIR MRW MIX MSX MPX MRG
 * dcre_hcs   hcs           public_holiday
 * </pre>
 *
 * <p><b>A shared reference database is NOT automatically a bounded context.</b>
 * {@code dcre_acs} was a fifth value here for one day and was retired on 2026-08-09:
 * it had no authoritative source, no accountable owner, no ingestion of its own and no
 * freshness contract, which made it a shared integration database wearing the costume
 * of a context. Account reference rows now travel as ONE immutable versioned artifact
 * and each consuming context materialises its OWN projection into its OWN database.
 * The three tests above are the bar for minting a value here, and passing "it is
 * reference data, so it is shared" off as passing them is what has to be refused.
 *
 * <p>{@code agt_ops} is the sixth database in the owner's table and is deliberately
 * NOT a value here: it is AGT's own, it is not sharded by family, and every stage pod
 * receives it under its own name ({@code DCRE_AGTOPS_DB_URL}) for the platform-batch
 * heartbeat writer. A family value would imply some stage's PRIMARY datasource is
 * agt_ops, and none is.
 *
 * <p><b>The database name is domain fact, not configuration.</b> It is carried here
 * so {@code StageDatabases} can refuse a configured url that addresses a different
 * database, which is the AGT-side twin of {@code shared/hcs}'s {@code FamilyGuard}
 * (it compares {@code current_database()} against the context it owns and refuses to
 * migrate on a mismatch). Deliberately the same mechanism rather than a second shape:
 * a database that can be pointed anywhere by a setting is not a boundary, it is a
 * default.
 *
 * @see <a href="https://microservices.io/patterns/data/database-per-service.html">Database per Service</a>
 */
public enum DbFamily {

    /** The collections (DC) family database. */
    COL("dcre_col"),

    /** The payments (ENDO) family database. */
    PAY("dcre_pay"),

    /** The mandates family database. */
    MAN("dcre_man"),

    /** The holiday calendar context; {@code hcs} is its only writer (R-04). */
    HCS("dcre_hcs");

    private final String database;

    DbFamily(final String database) {
        this.database = database;
    }

    /** The database this family owns, and the only one its stages' pods may address. */
    public String database() {
        return database;
    }
}
