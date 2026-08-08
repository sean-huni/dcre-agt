package za.co.fnb.dcre.agt.service;

import org.junit.jupiter.api.Test;
import za.co.fnb.dcre.agt.domain.DbFamily;
import za.co.fnb.dcre.agt.domain.Flow;
import za.co.fnb.dcre.agt.domain.Stage;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The roster IS the diagrams (design-register R-49): 28 stage services across
 * three families plus the cross-family HCS.
 *
 * <p>Every assertion here exists because the mismatch it guards reached a working,
 * reviewed, twice-chaos-gated fleet and was found by listing directories against
 * the sheets rather than by any test. So the sheets' roster is written out once,
 * literally, and everything else is compared to it.
 *
 * <p>Compare the SET, never the size. Collections held 9 of 9 required services
 * with FOUR misnamed, so a count check reported 9/9 and passed.
 */
class StageRosterTest {

    /** Transcribed from design-register/docs/diagrams, in each sheet's DAG order. */
    private static final List<String> COLLECTIONS =
            List.of("CRR", "CTV", "CDE", "CRW", "CIR", "CIX", "CSX", "CPX", "CRG");
    private static final List<String> PAYMENTS =
            List.of("PRR", "PTV", "PAI", "PRW", "PIR", "PIX", "PSX", "PPX", "PRG");
    private static final List<String> MANDATES =
            List.of("MRR", "MRV", "MAS", "MIT", "MIR", "MRW", "MIX", "MSX", "MPX", "MRG");
    /** On NO sheet by design, and that is not drift: the six sheets specify the three
     *  FAMILIES. verify-topology.sh declares acs, hcs and rpt in ALLOWED_shared and
     *  exits 0 with them present. */
    private static final List<String> CROSS = List.of("HCS", "ACS");

    @Test
    void theEnumIsExactlyTheDiagramsRoster() {
        final Set<String> required = new java.util.LinkedHashSet<>();
        required.addAll(COLLECTIONS);
        required.addAll(PAYMENTS);
        required.addAll(MANDATES);
        required.addAll(CROSS);
        // A positive control on the transcription itself: a truncated or
        // hand-edited constant would otherwise compare a short list silently.
        assertEquals(9, COLLECTIONS.size(), "collections roster");
        assertEquals(9, PAYMENTS.size(), "payments roster");
        assertEquals(10, MANDATES.size(), "mandates roster");
        assertEquals(2, CROSS.size(), "cross-family roster");
        assertEquals(30, required.size(), "28 stage services plus the two cross-family contexts");

        final Set<String> actual = new java.util.LinkedHashSet<>();
        for (final Stage stage : Stage.values()) {
            actual.add(stage.name());
        }

        final Set<String> missing = new java.util.LinkedHashSet<>(required);
        missing.removeAll(actual);
        final Set<String> unexpected = new java.util.LinkedHashSet<>(actual);
        unexpected.removeAll(required);
        assertTrue(missing.isEmpty() && unexpected.isEmpty(),
                "Stage must equal the diagrams' roster. required=" + required.size()
                        + " present=" + actual.size() + " absent=" + missing
                        + " not-on-a-sheet=" + unexpected);
    }

    /**
     * LAUNCHABLE is written out explicitly and must equal the whole enum.
     *
     * <p>It used to be {@code EnumSet.complementOf(...)}, which fails OPEN: a stage
     * added afterwards was launchable by default and nothing said so, so a renamed
     * stage silently became launchable again. This assertion is the gate: the next
     * stage added to the enum fails HERE until somebody lists it deliberately.
     */
    @Test
    void everyStageIsListedAsLaunchableDeliberately() {
        assertEquals(EnumSet.allOf(Stage.class), JobLauncher.LAUNCHABLE,
                "JobLauncher.LAUNCHABLE must name every stage explicitly; a stage added to the"
                        + " enum without a launch decision fails here rather than being absorbed"
                        + " by a complement");
    }

    /** Each family's stages resolve to that family's DATABASE, and nothing straddles two. */
    @Test
    void everyStageBelongsToExactlyOneDatabaseFamily() {
        final StageDatabases databases = new StageDatabases();
        for (final String name : COLLECTIONS) {
            assertEquals(DbFamily.COL, databases.dbFamily(Stage.valueOf(name)), name);
        }
        for (final String name : PAYMENTS) {
            assertEquals(DbFamily.PAY, databases.dbFamily(Stage.valueOf(name)), name);
        }
        for (final String name : MANDATES) {
            assertEquals(DbFamily.MAN, databases.dbFamily(Stage.valueOf(name)), name);
        }
        // Owner ruling 2026-08-08: the holiday calendar is its OWN bounded context
        // with its own database. This arm said Flow.COL, so AGT handed HCS the
        // collections url; shared/hcs's FamilyGuard compares current_database()
        // against dcre_hcs before any DDL, so every HCS pod died on startup.
        assertEquals(DbFamily.HCS, databases.dbFamily(Stage.HCS),
                "HCS writes dcre_hcs, not the collections database it used to inherit");
        assertEquals(DbFamily.ACS, databases.dbFamily(Stage.ACS),
                "ACS writes dcre_acs; it is a bounded context, not a tenant of collections");
    }

    /**
     * The NAMESPACE question, asserted separately from the database one.
     *
     * <p>These were one switch until 2026-08-08 and gave one answer to two questions.
     * HCS is the case that separates them: hosted in the collections namespace, and
     * writing its own database. Asserting both directions is what stops a future
     * reader from "simplifying" them back into one.
     */
    @Test
    void everyStageIsHostedInExactlyOneNamespaceFamily() {
        final StageNamespaces namespaces = new StageNamespaces();
        for (final String name : COLLECTIONS) {
            assertEquals(Flow.COL, namespaces.namespaceFamilyOf(Stage.valueOf(name)), name);
        }
        for (final String name : PAYMENTS) {
            assertEquals(Flow.PAY, namespaces.namespaceFamilyOf(Stage.valueOf(name)), name);
        }
        for (final String name : MANDATES) {
            assertEquals(Flow.MAN, namespaces.namespaceFamilyOf(Stage.valueOf(name)), name);
        }
        assertEquals(Flow.COL, namespaces.namespaceFamilyOf(Stage.HCS),
                "HCS has no namespace of its own: HcsScheduler launches it into dcre-col"
                        + " and its Jobs carry the col- prefix");
        assertEquals(Flow.COL, namespaces.namespaceFamilyOf(Stage.ACS),
                "ACS is hosted with HCS in dcre-col for the same reason: no namespace of"
                        + " its own has been ruled into existence, and its readers span all"
                        + " three families so no family has a better claim");

        assertNotEquals(namespaces.namespaceFamilyOf(Stage.ACS).name(),
                new StageDatabases().dbFamily(Stage.ACS).name(),
                "ACS, like HCS, is hosted by one family and owned by neither");
        assertNotEquals(namespaces.namespaceFamilyOf(Stage.HCS).name(),
                new StageDatabases().dbFamily(Stage.HCS).name(),
                "HCS is the stage whose namespace and database DISAGREE. If this ever"
                        + " passes by the two agreeing again, one of the two rulings has been"
                        + " reverted and the other question is being answered by accident");
    }

    /**
     * The wire-name contract, pinned as a literal, and the comment says why.
     *
     * <p>AGT injects the database url under ONE name for every family. Eight of the
     * nine payments services read {@code ${DCRE_PAY_DB_URL:...}} instead, and nothing
     * anywhere set that name, so in a pod they fell back to their committed localhost
     * default: the pod itself. Both sides were green. Five tests pinned the payments
     * side of the name; nothing tested AGT's, and AGT is the side whose documentation
     * claimed the recipients listen on this one.
     *
     * <p>A literal is the most this repo can assert: the consumers live in a different
     * git repository and are not on AGT's classpath, so no test here can read what they
     * declare. This assertion catches the half AGT owns (a rename on this side, or a
     * per-family name creeping in) and cannot catch a rename on theirs. The durable fix
     * is GENERATING both sides from one schema; it is out of scope today and recorded
     * as a follow-up.
     */
    @Test
    void everyStagePodReadsItsDatabaseFromOneEnvNameSharedByAllFamilies() {
        assertEquals("DCRE_DB_URL", JobLauncher.DB_URL_ENV,
                "the name the services read; changing it here silently un-configures"
                        + " every stage pod in the fleet, which then falls back to localhost");
    }

    /**
     * The rename that changed a token's MEANING, asserted in both directions.
     *
     * <p>PRG occurred 76 times in AGT and every one of them meant the COLLECTIONS
     * report generator. The diagrams give that job to CRG and give the name PRG to
     * the PAYMENTS one. A find-and-replace produces a build that compiles and is
     * semantically inverted, so the direction is pinned here rather than described.
     */
    @Test
    void prgIsThePaymentsGeneratorAndCrgIsTheCollectionsOne() {
        final StageDatabases databases = new StageDatabases();
        assertEquals(DbFamily.PAY, databases.dbFamily(Stage.PRG),
                "PRG is the PAYMENTS report generator on the payments RES sheet");
        assertEquals(DbFamily.COL, databases.dbFamily(Stage.CRG),
                "CRG is the COLLECTIONS report generator on the collections RES sheet");
    }
}
