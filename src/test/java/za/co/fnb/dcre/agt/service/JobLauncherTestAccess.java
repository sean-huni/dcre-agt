package za.co.fnb.dcre.agt.service;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.domain.Stage;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Package-private access to {@link JobLauncher#stageEnv(Stage)} for the pod-environment
 * tests, over a {@link JobLauncher} built by hand rather than by ArC.
 *
 * <p>{@code stageEnv} reads nothing but {@code config}, so a launcher with only that
 * field set answers it completely. That is why this needs no Quarkus context, no
 * CockroachDB container and no mock API server: the whole per-stage environment
 * decision is a pure function of {@link AgtConfig}, and a test of it that boots a
 * container is testing the container.
 *
 * <p>The stub follows {@code StageDatabaseGuardTest}'s idiom and FAILS CLOSED: a knob
 * the implementation reaches for and this map does not name throws rather than
 * answering null. An implementation that silently grew a dependency on an unstubbed
 * knob would otherwise put a {@code null} env value on every stage pod, which is the
 * quiet half of this defect class.
 */
final class JobLauncherTestAccess {

    /**
     * Deliberately DISTINCT from the {@code @WithDefault} values on {@link AgtConfig},
     * so a test asserting these sees the value travel from config to the pod. Asserting
     * against the defaults would pass equally for an implementation that hardcoded the
     * literal, which is the thing worth catching.
     */
    static final String STUB_OTLP_METRICS_URL = "http://collector.stub.test:4318/v1/metrics";
    static final String STUB_METRICS_STEP = "7s";
    static final String STUB_MAN_DB_URL = "jdbc:postgresql://stub:26257/dcre_man?sslmode=disable";
    static final String STUB_HCS_DB_URL = "jdbc:postgresql://stub:26257/dcre_hcs?sslmode=disable";
    static final String STUB_CTV_MANDATE_SOURCE = "projection";

    private static final Map<String, String> STUBBED = Map.of(
            "otlpMetricsUrl", STUB_OTLP_METRICS_URL,
            "metricsExportStep", STUB_METRICS_STEP,
            "manServiceDbUrl", STUB_MAN_DB_URL,
            "hcsServiceDbUrl", STUB_HCS_DB_URL,
            "ctvMandateSource", STUB_CTV_MANDATE_SOURCE);

    private JobLauncherTestAccess() {
    }

    /**
     * The W3C example ids, used as a FIXED arrival span context so the traceparent handed to a pod
     * can be compared against a value the test knows independently. A randomly minted context would
     * still prove two stages agree, and would not distinguish "derived from the arrival" from "minted
     * once per process", which is the interesting half.
     */
    static final String ARRIVAL_TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736";

    static final String ARRIVAL_SPAN_ID = "00f067aa0ba902b7";

    /** The pod environment {@link JobLauncher} would build for this stage. */
    static List<EnvVar> stageEnv(final Stage stage) {
        return launcher().stageEnv(stage);
    }

    /** The same environment as a map, for assertions keyed on the variable name. */
    static Map<String, String> stageEnvAsMap(final Stage stage) {
        return asMap(stageEnv(stage));
    }

    /**
     * The pod environment built INSIDE one active span standing for the arrival, which is how
     * {@code DagEngine} processes one: it opens an {@code arrival} span and every stage the DAG
     * launches for that file happens under it.
     *
     * <p>{@code Span.wrap} needs no SDK, no exporter and no Quarkus context: it makes a remote-shaped
     * span context current, which is exactly what the launcher reads. Every call activates the SAME
     * context deliberately, because two launches of one arrival really do share it.
     */
    static Map<String, String> stageEnvWithinSpan(final Stage stage) {
        final SpanContext arrival = SpanContext.create(ARRIVAL_TRACE_ID, ARRIVAL_SPAN_ID,
                TraceFlags.getSampled(), TraceState.getDefault());
        try (Scope ignored = Context.current().with(Span.wrap(arrival)).makeCurrent()) {
            return stageEnvAsMap(stage);
        }
    }

    private static Map<String, String> asMap(final List<EnvVar> vars) {
        final Map<String, String> byName = new LinkedHashMap<>();
        for (final EnvVar var : vars) {
            // A duplicate name is a real defect: the pod takes one of the two and nothing says
            // which, so this fails loudly rather than letting the last one win quietly.
            if (byName.containsKey(var.getName())) {
                throw new IllegalStateException("duplicate pod env var " + var.getName());
            }
            byName.put(var.getName(), var.getValue());
        }
        return byName;
    }

    private static JobLauncher launcher() {
        final JobLauncher launcher = new JobLauncher();
        launcher.config = stubConfig();
        return launcher;
    }

    private static AgtConfig stubConfig() {
        final InvocationHandler handler = (proxy, method, args) -> {
            final String value = STUBBED.get(method.getName());
            if (value == null) {
                throw new UnsupportedOperationException(
                        "stageEnv reached AgtConfig." + method.getName() + "(), which this stub does"
                                + " not answer. Add it to JobLauncherTestAccess.STUBBED deliberately"
                                + " rather than letting a null reach a stage pod's environment.");
            }
            return value;
        };
        return (AgtConfig) Proxy.newProxyInstance(AgtConfig.class.getClassLoader(),
                new Class<?>[]{AgtConfig.class}, handler);
    }
}
