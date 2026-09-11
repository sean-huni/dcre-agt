package za.co.fnb.dcre.agt.service;

import io.fabric8.kubernetes.api.model.EnvVar;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.domain.Stage;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
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
    static final String STUB_OTLP_ENDPOINT = "http://collector.stub.test:4318";
    static final String STUB_METRICS_STEP = "7s";
    static final String STUB_MAN_DB_URL = "jdbc:postgresql://stub:26257/dcre_man?sslmode=disable";
    static final String STUB_HCS_DB_URL = "jdbc:postgresql://stub:26257/dcre_hcs?sslmode=disable";
    static final String STUB_CTV_MANDATE_SOURCE = "projection";

    private static final Map<String, String> STUBBED = Map.of(
            "otlpEndpoint", STUB_OTLP_ENDPOINT,
            "metricsExportStep", STUB_METRICS_STEP,
            "manServiceDbUrl", STUB_MAN_DB_URL,
            "hcsServiceDbUrl", STUB_HCS_DB_URL,
            "ctvMandateSource", STUB_CTV_MANDATE_SOURCE);

    private JobLauncherTestAccess() {
    }

    /** The pod environment {@link JobLauncher} would build for this stage. */
    static List<EnvVar> stageEnv(final Stage stage) {
        return launcher().stageEnv(stage);
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
