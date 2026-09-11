package za.co.fnb.dcre.agt.service;

import io.fabric8.kubernetes.api.model.EnvVar;
import org.junit.jupiter.api.Test;
import za.co.fnb.dcre.agt.domain.Stage;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The telemetry block AGT hands every stage pod, asserted over EVERY {@link Stage}
 * constant rather than over a sample.
 *
 * <p>Telemetry is not a per-stage special case, so it is assembled AROUND
 * {@code stageEnv}'s per-stage switch rather than inside it. A switch is the shape that
 * invites the defect this file exists to catch: a stage added to the enum later gets no
 * arm, the default arm answers "nothing", and the pod launches with no telemetry and no
 * complaint. A test that sampled one stage would agree with that silence, which is why
 * the first case walks {@code Stage.values()}.
 *
 * <p><b>The token is the lowercase enum name, and its shape is load-bearing.</b> The
 * shared library validates {@code service.name} against {@code dcre-[a-z]+} and REFUSES
 * anything else at startup, so a token carrying an uppercase letter or a hyphen does not
 * degrade the telemetry, it stops the pod. That is asserted for all 29 constants here,
 * because the enum is where a future constant with an awkward name would arrive.
 *
 * <p>No Quarkus context, no container: the per-stage environment is a pure function of
 * {@link za.co.fnb.dcre.agt.config.AgtConfig}. The complementary assertion, that the
 * block reaches the Job specs AGT actually builds on BOTH builder paths, lives in
 * {@code NamespaceRoutingTest}, because only there is a real Job spec produced.
 */
class JobLauncherTelemetryEnvTest {

    private static final List<String> TELEMETRY_KEYS = List.of(
            "DCRE_TELEMETRY_ENABLED", "DCRE_TELEMETRY_STAGE", "OTLP_ENDPOINT", "METRICS_EXPORT_STEP");

    @Test
    void everyStageGetsTheTelemetryBlockIncludingOnesWithNoOtherExtras() {
        for (final Stage stage : Stage.values()) {
            final Map<String, String> env = envFor(stage);
            for (final String key : TELEMETRY_KEYS) {
                assertTrue(env.containsKey(key),
                        "stage " + stage + " must carry " + key + "; got " + env.keySet());
            }
        }
    }

    @Test
    void theStageTokenIsLowercaseSoItMatchesTheSeamListenerToken() {
        assertEquals("crg", envFor(Stage.CRG).get("DCRE_TELEMETRY_STAGE"));
    }

    @Test
    void stagesWithTheirOwnExtrasKeepThem() {
        assertTrue(envFor(Stage.CDE).containsKey("DCRE_CDE_HOLIDAYS_DB_URL"),
                "wrapping the switch must not drop what the switch already answered");
    }

    /**
     * The library refuses a {@code service.name} outside {@code dcre-[a-z]+} at startup,
     * so this is a build-time guard on a fleet-wide crash loop rather than on a label.
     */
    @Test
    void everyStageTokenIsAcceptedByTheLibrarysServiceNamePattern() {
        for (final Stage stage : Stage.values()) {
            final String token = envFor(stage).get("DCRE_TELEMETRY_STAGE");
            assertEquals(stage.name().toLowerCase(Locale.ROOT), token,
                    "the token is the lowercase enum name for " + stage);
            assertTrue(token.matches("[a-z]+"),
                    "stage " + stage + " yields token '" + token + "', which the library's"
                            + " dcre-[a-z]+ check on service.name refuses AT STARTUP: the pod would"
                            + " not run at all. Give the constant a name whose lowercase form is"
                            + " letters only.");
        }
    }

    /**
     * The library's export gate resolves the enabled flag in Java and FAILS CLOSED, so a
     * blank value is telemetry switched off. A present-but-empty variable therefore looks
     * exactly like a correctly wired pod and exports nothing, which is why presence alone
     * (the first case) is not sufficient.
     */
    @Test
    void noTelemetryValueIsBlankForAnyStageBecauseTheExportGateFailsClosed() {
        for (final Stage stage : Stage.values()) {
            final Map<String, String> env = envFor(stage);
            for (final String key : TELEMETRY_KEYS) {
                final String value = env.get(key);
                assertFalse(value == null || value.isBlank(),
                        "stage " + stage + " carries " + key + "='" + value + "'; the export gate"
                                + " fails closed on a blank value, so this pod would export nothing"
                                + " while looking correctly wired");
            }
        }
    }

    /**
     * The endpoint and step are CONFIG, not literals. The stub answers values that differ
     * from {@code AgtConfig}'s own defaults on purpose: asserting against the defaults
     * would pass just as well for an implementation that hardcoded them, and a hardcoded
     * {@code localhost:4318} in a cluster addresses the stage pod itself.
     */
    @Test
    void theEndpointAndStepAreCarriedFromConfigRatherThanHardcoded() {
        final Map<String, String> env = envFor(Stage.CRR);
        assertEquals(JobLauncherTestAccess.STUB_OTLP_ENDPOINT, env.get("OTLP_ENDPOINT"));
        assertEquals(JobLauncherTestAccess.STUB_METRICS_STEP, env.get("METRICS_EXPORT_STEP"));
    }

    private static Map<String, String> envFor(final Stage stage) {
        final List<EnvVar> vars = JobLauncherTestAccess.stageEnv(stage);
        final Map<String, String> byName = new java.util.LinkedHashMap<>();
        for (final EnvVar var : vars) {
            // A duplicate name is a real defect: the pod takes one of the two and nothing
            // says which, so a merging collector would hide it.
            assertFalse(byName.containsKey(var.getName()),
                    "stage " + stage + " carries " + var.getName() + " twice");
            byName.put(var.getName(), var.getValue());
        }
        return byName;
    }
}
