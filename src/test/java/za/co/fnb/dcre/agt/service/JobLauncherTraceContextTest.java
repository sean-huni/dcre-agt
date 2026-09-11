package za.co.fnb.dcre.agt.service;

import org.junit.jupiter.api.Test;
import za.co.fnb.dcre.agt.domain.Stage;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The W3C trace context AGT hands a launched stage pod, so a DAG is ONE trace rather than one trace
 * per pod.
 *
 * <p>Without it each stage starts a fresh trace and the Traces board shows hundreds of unrelated
 * single-span traces: a board that is populated, queryable and answers no question anyone has. That
 * is why asserting the variable merely EXISTS is not enough, and why the second case here compares
 * two stages of one arrival. A per-pod traceparent would satisfy the first case and fail the second,
 * which is exactly the discrimination the defect needs.
 *
 * <p>The value is derived from the AMBIENT {@code Span.current()} inside the launcher, so nothing is
 * threaded through a call signature and no overload exists. The consequence is the fourth case: a
 * launch with no active span, which is a real shape here because the clock builders launch report
 * windows that no arrival triggered. That launch must contribute NO variable rather than a malformed
 * or empty one, because a child handed a malformed traceparent starts a fresh trace anyway while
 * looking configured, and {@code stageEnv}'s {@code List.copyOf} throws on a null element.
 *
 * <p>No Quarkus context and no container: {@code Span.wrap} builds an active context from a plain
 * {@link io.opentelemetry.api.trace.SpanContext} with no SDK at all, and the per-stage environment
 * is a pure function of {@code AgtConfig}. The complementary assertion, that the block reaches the
 * Job specs on BOTH builder paths, already lives in {@code NamespaceRoutingTest}.
 */
class JobLauncherTraceContextTest {

    @Test
    void everyLaunchedPodCarriesATraceparentSoOneArrivalIsOneTrace() {
        final Map<String, String> env = JobLauncherTestAccess.stageEnvWithinSpan(Stage.CRG);
        assertTrue(env.containsKey("TRACEPARENT"),
                "no TRACEPARENT; every stage pod of one arrival would start its own trace. Got "
                        + env.keySet());
        assertTrue(env.get("TRACEPARENT").matches("00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]"),
                "W3C traceparent is version-traceid-spanid-flags; got '"
                        + env.get("TRACEPARENT") + "'");
    }

    @Test
    void twoStagesOfTheSameArrivalShareATraceId() {
        final String a = JobLauncherTestAccess.stageEnvWithinSpan(Stage.CRR).get("TRACEPARENT");
        final String b = JobLauncherTestAccess.stageEnvWithinSpan(Stage.CTV).get("TRACEPARENT");
        assertEquals(a.split("-")[1], b.split("-")[1],
                "two stages of one arrival must share a trace id, or the Traces board shows one "
                        + "single-span trace per pod instead of one DAG per arrival");
    }

    /**
     * The discriminator for the case above. Two calls agreeing is also satisfied by an
     * implementation that mints one trace id per PROCESS and ignores the arrival entirely; only
     * comparing against the ambient context distinguishes derivation from coincidence.
     */
    @Test
    void theTraceparentCarriesTheAmbientArrivalContextRatherThanAFreshOne() {
        final String[] parts =
                JobLauncherTestAccess.stageEnvWithinSpan(Stage.CRG).get("TRACEPARENT").split("-");
        assertEquals(JobLauncherTestAccess.ARRIVAL_TRACE_ID, parts[1],
                "the trace id must be the one the arrival span is running under");
        assertEquals(JobLauncherTestAccess.ARRIVAL_SPAN_ID, parts[2],
                "the parent span id must be the arrival span, so the pod's spans hang under it");
        assertEquals("01", parts[3], "the sampled flag must travel, or the child samples on its own");
    }

    /**
     * A clock window no arrival triggered runs under no span. An absent member must be SKIPPED, not
     * added with a null value: {@code stageEnv} returns {@code List.copyOf}, which rejects a null
     * ELEMENT outright, and {@code new EnvVar("TRACEPARENT", null, null)} is worse still because
     * nothing rejects it and the variable reaches the pod set to nothing.
     */
    @Test
    void aLaunchOutsideAnyArrivalSpanContributesNoTraceparentAtAll() {
        final Map<String, String> env = JobLauncherTestAccess.stageEnvAsMap(Stage.MRG);
        assertFalse(env.containsKey("TRACEPARENT"),
                "an invalid span context must contribute no variable; got '"
                        + env.get("TRACEPARENT") + "'");
        // The positive control. Without it this passes just as happily when stageEnv threw, when it
        // returned nothing, or when the whole telemetry block was lost.
        assertTrue(env.containsKey("DCRE_TELEMETRY_STAGE"),
                "the rest of the telemetry block must still be there; got " + env.keySet());
    }
}
