package za.co.fnb.dcre.agt;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SCRUM-107 (review C2): the AGT counterpart of ctv's ProjectionIsTheDefaultSourceTest.
 *
 * <p>AGT hands {@code DCRE_CTV_MANDATE_SOURCE} to every CTV stage pod, and the value has
 * TWO homes in this repo: {@code application.yml} and {@code AgtConfig}'s
 * {@code @WithDefault}. They disagreed for one commit, with the yml on {@code projection}
 * and the annotation still on {@code legacy}.
 *
 * <p>That is not cosmetic. ctv now fails CLOSED on {@code legacy}: {@code MandateSource.from}
 * throws during bean creation, so a stage pod handed the retired value never starts, and the
 * dcre_col.mandate table it used to select has been dropped. Whichever home wins the
 * precedence argument, the answer must not be {@code legacy}. So this asserts BOTH homes,
 * rather than asserting the resolved value and leaving the other free to rot.
 *
 * <p>Asserting on the committed sources on purpose: a {@code @QuarkusTest} reading the
 * injected value would prove what THIS test's config resolves to, not what a clean clone
 * and the cluster ship.
 */
class AgtCtvMandateSourceDefaultTest {

    private static final String YML = "src/main/resources/application.yml";
    private static final String CONFIG = "src/main/java/za/co/fnb/dcre/agt/config/AgtConfig.java";

    @Test
    void theCommittedYmlDefaultIsTheProjection() throws Exception {
        String yml = Files.readString(Path.of(YML));
        assertTrue(yml.contains("${AGT_CTV_MANDATE_SOURCE:projection}"),
                "agt hands this to every CTV stage pod; the committed default must be projection");
        assertFalse(yml.contains("${AGT_CTV_MANDATE_SOURCE:legacy}"),
                "legacy would stop every CTV pod starting: ctv throws on it at bean creation");
    }

    @Test
    void theWithDefaultAnnotationAgreesWithTheYml() throws Exception {
        String config = Files.readString(Path.of(CONFIG));
        int marker = config.indexOf("String ctvMandateSource();");
        assertTrue(marker > 0, "ctvMandateSource() must exist in AgtConfig");

        String preceding = config.substring(0, marker);
        String lastDefault = preceding.substring(preceding.lastIndexOf("@WithDefault("));
        assertTrue(lastDefault.startsWith("@WithDefault(\"projection\")"),
                "the @WithDefault on ctvMandateSource must match the application.yml default,"
                        + " found: " + lastDefault.substring(0, Math.min(40, lastDefault.length())));
    }

    @Test
    void theRetiredLegacyValueIsNotNamedAsADefaultAnywhere() throws Exception {
        assertFalse(Files.readString(Path.of(CONFIG)).contains("@WithDefault(\"legacy\")"),
                "legacy is no longer a selectable mode: ctv throws on it at bean creation");
    }
}
