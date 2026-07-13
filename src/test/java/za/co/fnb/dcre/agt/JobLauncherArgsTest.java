package za.co.fnb.dcre.agt;

import org.junit.jupiter.api.Test;
import za.co.fnb.dcre.agt.domain.ArrivalStatus;
import za.co.fnb.dcre.agt.domain.FileArrival;
import za.co.fnb.dcre.agt.domain.Outcome;
import za.co.fnb.dcre.agt.domain.Stage;
import za.co.fnb.dcre.agt.service.JobLauncher;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pure args-shaping tests for service Job launches (R-41): CIR carries the
 * arrival identity (route.id, client.token, msg.id per R-16/A-45) and, when a validator rejected the
 * file, that rejecting stage's verdict as outcome.hint so it can NACK without
 * a spine header (A-42 consumer contract).
 */
class JobLauncherArgsTest {

    private static final UUID ARRIVAL_ID = UUID.fromString("6a1f0a8e-0000-4000-8000-000000000042");

    private static FileArrival arrival() {
        return new FileArrival(ARRIVAL_ID, "onhost-req", "FNBRF01_MSG1.txt",
                "sha", "FNBRF01", "DCRERF2026071313500102",
                ArrivalStatus.DAG_RUNNING, null, "/exchange/claimed/FNBRF01_MSG1.txt");
    }

    @Test
    void cirCarriesClientTokenMsgIdAndOutcomeHint() {
        List<String> args = JobLauncher.serviceArgs(Stage.CIR, arrival(),
                Map.of(Stage.CRR, Outcome.BUSINESS_ACCEPTED, Stage.CTV, Outcome.BUSINESS_FILE_REJECTED));
        assertTrue(args.contains("arrival.id=" + ARRIVAL_ID));
        assertTrue(args.contains("route.id=onhost-req,java.lang.String,false"),
                "A-45: CIR 2.0.1 fails closed without the route dimension; got " + args);
        assertTrue(args.contains("client.token=FNBRF01,java.lang.String,false"));
        assertTrue(args.contains("msg.id=DCRERF2026071313500102,java.lang.String,false"));
        assertTrue(args.contains("outcome.hint=BUSINESS_FILE_REJECTED,java.lang.String,false"));
    }

    @Test
    void cirOmitsHintWhenOnlyBoundaryReaderFailed() {
        // CRR fatal: no spine/verdicts exist; CIR NACKs from fatal.reason, so no hint.
        List<String> args = JobLauncher.serviceArgs(Stage.CIR, arrival(),
                Map.of(Stage.CRR, Outcome.BUSINESS_FILE_FATAL));
        assertTrue(args.contains("client.token=FNBRF01,java.lang.String,false"));
        assertTrue(args.contains("msg.id=DCRERF2026071313500102,java.lang.String,false"));
        assertFalse(args.stream().anyMatch(a -> a.startsWith("outcome.hint=")),
                "boundary-reader fatal: hint omitted, CIR falls back to fatal.reason");
    }

    @Test
    void cirHintComesFromTheRejectingStageNotHardcodedCtv() {
        // ENDO shape: CTV accepted, AIS killed the file. The hint must carry the
        // rejecting stage's outcome, never CTV's misleading BUSINESS_ACCEPTED.
        List<String> args = JobLauncher.serviceArgs(Stage.CIR, arrival(),
                Map.of(Stage.CRR, Outcome.BUSINESS_ACCEPTED,
                        Stage.CTV, Outcome.BUSINESS_ACCEPTED,
                        Stage.AIS, Outcome.BUSINESS_FILE_FATAL));
        assertTrue(args.contains("outcome.hint=BUSINESS_FILE_FATAL,java.lang.String,false"),
                "got " + args);
    }

    @Test
    void cirOmitsHintOnPartialAck() {
        // PARTIAL is an ACK-with-partials launch: no rejecting stage, no hint.
        List<String> args = JobLauncher.serviceArgs(Stage.CIR, arrival(),
                Map.of(Stage.CRR, Outcome.BUSINESS_ACCEPTED, Stage.CTV, Outcome.BUSINESS_PARTIAL));
        assertFalse(args.stream().anyMatch(a -> a.startsWith("outcome.hint=")),
                "got " + args);
    }

    @Test
    void nonCirStagesCarryNoIdentityParams() {
        List<String> args = JobLauncher.serviceArgs(Stage.CDE, arrival(),
                Map.of(Stage.CTV, Outcome.BUSINESS_PARTIAL));
        assertEquals(List.of("arrival.id=" + ARRIVAL_ID), args);
    }

    @Test
    void cirFailsClosedOnNullOrBlankIdentityFields() {
        // FileArrival is a plain record (no constructor validation): a null
        // routeId would otherwise render as the literal "null", pass CIR's
        // has-text check, and re-create the A-45 collision class under the
        // token "null". Same latent hole for clientToken and msgIdToken.
        FileArrival nullRoute = new FileArrival(ARRIVAL_ID, null, "FNBRF01_MSG1.txt",
                "sha", "FNBRF01", "DCRERF2026071313500102",
                ArrivalStatus.DAG_RUNNING, null, "/exchange/claimed/FNBRF01_MSG1.txt");
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> JobLauncher.serviceArgs(Stage.CIR, nullRoute, Map.of()));
        assertTrue(e.getMessage().contains("route.id"), "got: " + e.getMessage());
        assertTrue(e.getMessage().contains(ARRIVAL_ID.toString()), "got: " + e.getMessage());

        FileArrival blankClient = new FileArrival(ARRIVAL_ID, "onhost-req", "FNBRF01_MSG1.txt",
                "sha", " ", "DCRERF2026071313500102",
                ArrivalStatus.DAG_RUNNING, null, "/exchange/claimed/FNBRF01_MSG1.txt");
        assertThrows(IllegalStateException.class,
                () -> JobLauncher.serviceArgs(Stage.CIR, blankClient, Map.of()));

        FileArrival nullMsgId = new FileArrival(ARRIVAL_ID, "onhost-req", "FNBRF01_MSG1.txt",
                "sha", "FNBRF01", null,
                ArrivalStatus.DAG_RUNNING, null, "/exchange/claimed/FNBRF01_MSG1.txt");
        assertThrows(IllegalStateException.class,
                () -> JobLauncher.serviceArgs(Stage.CIR, nullMsgId, Map.of()));
    }

    @Test
    void boundaryReaderArgsUnchanged() {
        List<String> args = JobLauncher.serviceArgs(Stage.CRR, arrival(), Map.of());
        assertEquals(List.of(
                "arrival.id=" + ARRIVAL_ID,
                "input.file=/exchange/claimed/FNBRF01_MSG1.txt,java.lang.String,false",
                "original.name=FNBRF01_MSG1.txt,java.lang.String,false"), args);
    }
}
