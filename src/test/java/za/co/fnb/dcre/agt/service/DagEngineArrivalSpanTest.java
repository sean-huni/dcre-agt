package za.co.fnb.dcre.agt.service;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;
import za.co.fnb.dcre.agt.domain.Outcome;
import za.co.fnb.dcre.agt.domain.ArrivalStatus;
import za.co.fnb.dcre.agt.domain.FileArrival;
import za.co.fnb.dcre.agt.domain.Flow;
import za.co.fnb.dcre.agt.domain.Stage;
import za.co.fnb.dcre.agt.repo.ArrivalRepo;
import za.co.fnb.dcre.agt.repo.CollectionsReadRepo;
import za.co.fnb.dcre.agt.repo.IntentRepo;
import za.co.fnb.dcre.agt.repo.OutcomeRepo;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The other end of the trace-context wiring, and the end that can be inert without anything saying
 * so.
 *
 * <p>{@code JobLauncher} derives the traceparent from the AMBIENT {@code Span.current()}, and
 * {@code JobLauncherTraceContextTest} proves it does that correctly by making a span current itself.
 * That test stays green for a production system in which NO span is ever current, where the launcher
 * would skip the variable on every launch, exactly as designed, and every pod would still start its
 * own trace. Only driving {@code DagEngine.tick()} shows whether the span this all depends on
 * actually exists. Measured before this task: AGT created no span around a job launch at all.
 *
 * <p>So this asserts the JOIN rather than either half: the trace id the launcher would put on the
 * pod is the trace id of the {@code arrival} span the engine recorded, read back from a real SDK
 * exporter rather than from the engine's own belief.
 */
class DagEngineArrivalSpanTest {

    private static final String ROUTE = ArrivalService.ROUTE_ONHOST_REQ;

    private static final String FILENAME = "FNBCC01_F.txt";

    /**
     * Over BOTH loops, because they are two separate wirings of the same thing and the second is the
     * bigger one: the CLAIMED loop launches an arrival's FIRST stage, the DAG_RUNNING loop launches
     * every stage after it. A test that stubbed the second loop to empty would stay green with the
     * span wiring deleted from it, which is the same mutation shape this class exists to catch.
     */
    @ParameterizedTest(name = "{0} loop")
    @EnumSource(value = ArrivalStatus.class, names = {"CLAIMED", "DAG_RUNNING"})
    void everyArrivalIsProcessedInsideAnArrivalSpanWhoseContextReachesTheLaunchedPod(
            final ArrivalStatus loop) {
        final RecordingSpanExporter exported = new RecordingSpanExporter();
        final UUID arrivalId = UUID.randomUUID();
        final List<String> traceparents = new CopyOnWriteArrayList<>();

        try (SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(exported))
                .build()) {

            final DagEngine engine = new DagEngine();
            engine.tracer = tracerProvider.get("test");
            engine.lease = Mockito.mock(LeaseService.class);
            Mockito.when(engine.lease.holdsLease()).thenReturn(true);
            engine.arrivalRepo = Mockito.mock(ArrivalRepo.class);
            // Exactly one loop sees the arrival, so the span and the launch below are attributable
            // to THAT loop's wiring and to nothing else.
            Mockito.when(engine.arrivalRepo.arrivalsByStatus(Mockito.any())).thenReturn(List.of());
            Mockito.when(engine.arrivalRepo.arrivalsByStatus(loop))
                    .thenReturn(List.of(arrival(arrivalId, loop)));
            engine.outcomeRepo = Mockito.mock(OutcomeRepo.class);
            Mockito.when(engine.outcomeRepo.outcomesForArrival(Mockito.any()))
                    .thenReturn(Map.of(Stage.CRR, Outcome.BUSINESS_ACCEPTED));
            engine.intentRepo = Mockito.mock(IntentRepo.class);
            Mockito.when(engine.intentRepo.intentsForArrival(Mockito.any())).thenReturn(List.of());
            engine.collectionsRead = Mockito.mock(CollectionsReadRepo.class);
            engine.counters = Mockito.mock(EventCounters.class);
            // Mocked rather than real: the real one reads AgtConfig.payClients(), and a null
            // config there throws inside the loop, where tick() catches it and WARNs. The
            // launcher then never runs and the span assertions below still pass, which is
            // exactly why the call-count assertion exists. Measured: that is how this fixture
            // failed first.
            engine.flowNamespaces = Mockito.mock(FlowNamespaces.class);
            Mockito.when(engine.flowNamespaces.flowFor(Mockito.any())).thenReturn(Flow.COL);
            engine.launcher = Mockito.mock(JobLauncher.class);
            // The launcher is mocked because a real one needs a Kubernetes client, but the thing
            // under test is what the AMBIENT context looks like at the moment it is called. So the
            // stub asks the real launcher code what it would put on the pod, through the same
            // package-private door the sibling test uses.
            Mockito.doAnswer(invocation -> {
                traceparents.add(JobLauncherTestAccess.stageEnvAsMap(Stage.CTV).get("TRACEPARENT"));
                return null;
            }).when(engine.launcher).launch(Mockito.any(), Mockito.any());

            engine.tick();
        }

        final List<SpanData> spans = exported.spans();
        assertEquals(1, spans.size(), "one arrival, one span; got " + spans);
        final SpanData arrival = spans.getFirst();
        assertEquals("arrival", arrival.getName());
        assertEquals(arrivalId.toString(),
                arrival.getAttributes().asMap().entrySet().stream()
                        .filter(e -> "dcre.arrival.id".equals(e.getKey().getKey()))
                        .map(Map.Entry::getValue)
                        .findFirst()
                        .orElse(null),
                "the span must carry dcre.arrival.id, or a trace cannot be found from the file that "
                        + "caused it; attributes were " + arrival.getAttributes());

        assertEquals(1, traceparents.size(), "the launcher must have been called exactly once");
        final String traceparent = traceparents.getFirst();
        assertNotNull(traceparent,
                "no TRACEPARENT at launch time. The engine's span is not current on the thread that "
                        + "launches, so every stage pod would start a fresh trace while both halves "
                        + "of this wiring look correct in isolation");
        assertTrue(traceparent.startsWith("00-" + arrival.getTraceId() + "-"),
                "the pod's traceparent must carry the arrival span's trace id; span trace id was "
                        + arrival.getTraceId() + ", traceparent was " + traceparent);
        assertTrue(traceparent.contains("-" + arrival.getSpanId() + "-"),
                "the arrival span must be the PARENT, or the pod's spans hang off nothing; span id "
                        + "was " + arrival.getSpanId() + ", traceparent was " + traceparent);
    }

    /**
     * The negative control for the assertion above. A launch that happens outside the loop, with no
     * arrival span current, must contribute nothing, so "a traceparent was present" above is
     * attributable to the span and not to the launcher inventing one.
     */
    @Test
    void outsideTheLoopThereIsNoAmbientSpanAndSoNoTraceparent() {
        assertTrue(!Span.current().getSpanContext().isValid(),
                "this JVM already has an active span, so the case above proves nothing");
        assertTrue(!JobLauncherTestAccess.stageEnvAsMap(Stage.CRR).containsKey("TRACEPARENT"));
    }

    /**
     * The same arrival in whichever status the loop under test reads. DAG_RUNNING is seeded with a
     * CRR outcome of ACCEPTED so the second loop has a successor to launch: CTV. Without an outcome
     * it computes no launches, the launcher is never called, and the assertion that would have
     * caught a missing span would pass over a loop that did nothing.
     */
    private static FileArrival arrival(final UUID id, final ArrivalStatus status) {
        return new FileArrival(id, ROUTE, FILENAME, "sha", "FNBCC01", "MSG1",
                status, null, "/exchange/claimed/" + FILENAME);
    }

    /** Holds what the SDK actually ended, which is the only evidence the span existed. */
    private static final class RecordingSpanExporter implements SpanExporter {

        private final List<SpanData> spans = new CopyOnWriteArrayList<>();

        @Override
        public CompletableResultCode export(final Collection<SpanData> batch) {
            spans.addAll(batch);
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public CompletableResultCode flush() {
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public CompletableResultCode shutdown() {
            return CompletableResultCode.ofSuccess();
        }

        List<SpanData> spans() {
            return spans;
        }
    }
}
