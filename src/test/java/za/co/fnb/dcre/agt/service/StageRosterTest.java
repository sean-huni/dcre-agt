package za.co.fnb.dcre.agt.service;

import org.junit.jupiter.api.Test;
import za.co.fnb.dcre.agt.domain.Stage;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
    private static final List<String> CROSS = List.of("HCS");

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
        assertEquals(29, required.size(), "28 stage services plus HCS");

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

    /** Each family's stages resolve to that family, and nothing straddles two. */
    @Test
    void everyStageBelongsToExactlyOneFamily() {
        final StageDatabases databases = new StageDatabases();
        for (final String name : COLLECTIONS) {
            assertEquals(za.co.fnb.dcre.agt.domain.Flow.COL, databases.family(Stage.valueOf(name)), name);
        }
        for (final String name : PAYMENTS) {
            assertEquals(za.co.fnb.dcre.agt.domain.Flow.PAY, databases.family(Stage.valueOf(name)), name);
        }
        for (final String name : MANDATES) {
            assertEquals(za.co.fnb.dcre.agt.domain.Flow.MAN, databases.family(Stage.valueOf(name)), name);
        }
        // HCS is cross-family and sits with collections: it is the single writer of
        // public_holiday, which CDE reads from dcre_col.
        assertEquals(za.co.fnb.dcre.agt.domain.Flow.COL, databases.family(Stage.HCS));
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
        assertEquals(za.co.fnb.dcre.agt.domain.Flow.PAY, databases.family(Stage.PRG),
                "PRG is the PAYMENTS report generator on the payments RES sheet");
        assertEquals(za.co.fnb.dcre.agt.domain.Flow.COL, databases.family(Stage.CRG),
                "CRG is the COLLECTIONS report generator on the collections RES sheet");
    }
}
