package za.co.fnb.dcre.agt.service;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Guard on {@link ArrivalService#claimIdOf(Path)} (review M3): the helper only
 * ever runs on an already-claimed content-twin, which is always
 * {@code <uuid>_<name>} prefixed, so the malformed cases are unreachable in
 * practice. Still, a name lacking the '_' separator or carrying a non-UUID
 * prefix must fail with a clear, contextual {@link IllegalStateException}
 * (the class's move/hash error idiom) rather than a bare
 * StringIndexOutOfBoundsException / IllegalArgumentException.
 */
class ClaimIdOfTest {

    @Test
    void wellFormedInflightNameYieldsTheClaimUuid() {
        final UUID id = UUID.fromString("6a1f0a8e-0000-4000-8000-000000000042");
        assertEquals(id, ArrivalService.claimIdOf(Path.of("/x/inflight", id + "_FNBRF01_MSG1.txt")));
    }

    @Test
    void noUnderscoreSeparatorFailsWithClearError() {
        // indexOf('_') == -1 -> substring(0,-1) would raise a contextless
        // StringIndexOutOfBoundsException on the raw helper.
        final Path bad = Path.of("/x/inflight/nounderscorehere.txt");
        final IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> ArrivalService.claimIdOf(bad));
        assertTrue(e.getMessage().contains("nounderscorehere.txt"),
                "message names the offending filename; got: " + e.getMessage());
    }

    @Test
    void nonUuidPrefixFailsWithClearError() {
        // indexOf('_') > 0 but the prefix is not a UUID -> UUID.fromString would
        // raise a contextless IllegalArgumentException on the raw helper.
        final Path bad = Path.of("/x/inflight/notauuid_FNBRF01_MSG1.txt");
        final IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> ArrivalService.claimIdOf(bad));
        assertTrue(e.getMessage().contains("notauuid_FNBRF01_MSG1.txt"),
                "message names the offending filename; got: " + e.getMessage());
    }

    @Test
    void leadingUnderscoreEmptyPrefixFailsWithClearError() {
        // indexOf('_') == 0 -> empty prefix; guarded the same as no separator.
        final Path bad = Path.of("/x/inflight/_FNBRF01_MSG1.txt");
        assertThrows(IllegalStateException.class, () -> ArrivalService.claimIdOf(bad));
    }
}
