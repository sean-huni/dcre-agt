package za.co.fnb.dcre.agt;

import org.junit.jupiter.api.Test;
import za.co.fnb.dcre.agt.domain.ArrivalStatus;
import za.co.fnb.dcre.agt.domain.FileArrival;
import za.co.fnb.dcre.agt.domain.Flow;
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

    private static FileArrival endoArrival() {
        return new FileArrival(ARRIVAL_ID, "onhost-req-endo", "FNBRF01_MSG1.txt",
                "sha", "FNBRF01", "DCRERF2026071313500102",
                ArrivalStatus.DAG_RUNNING, null, "/exchange/claimed/FNBRF01_MSG1.txt");
    }

    @Test
    void endoCrrCarriesThePayFlowArg() {
        // SCRUM-69: CRR stamps tx_header.flow from the launch arg; ENDO
        // arrivals ride the pay flow. Plain non-identifying param, mirroring
        // the existing CIR identity args.
        List<String> args = JobLauncher.serviceArgs(Stage.CRR, endoArrival(), Map.of(), Flow.PAY);
        assertTrue(args.contains("flow=PAY,java.lang.String,false"), "got " + args);
    }

    @Test
    void flowArgIsEndoCrrOnly() {
        assertFalse(JobLauncher.serviceArgs(Stage.CTV, endoArrival(), Map.of(), Flow.PAY).stream()
                        .anyMatch(a -> a.startsWith("flow=")),
                "flow rides the boundary reader only; CTV keeps env-based flow switching");
        assertFalse(JobLauncher.serviceArgs(Stage.CRR, arrival(), Map.of(), Flow.COL).stream()
                        .anyMatch(a -> a.startsWith("flow=")),
                "DC arrivals carry no flow arg; CRR defaults to COL");
    }

    @Test
    void noStageCarriesAResponseFileArgAnyMore() {
        // SCRUM-91: response.file existed only to point the MSR projection at the
        // rows MAR had tagged. The projection is a view now, so the arg is gone
        // from every stage, including the leg readers that replaced MAR.
        for (Stage stage : List.of(Stage.MIX, Stage.MSX, Stage.MPX, Stage.MRV)) {
            assertFalse(JobLauncher.serviceArgs(stage, arrival(), Map.of(), Flow.COL).stream()
                            .anyMatch(a -> a.startsWith("response.file=")),
                    "no response.file on " + stage);
        }
    }

    @Test
    void cirCarriesClientTokenMsgIdAndOutcomeHint() {
        List<String> args = JobLauncher.serviceArgs(Stage.CIR, arrival(),
                Map.of(Stage.CRR, Outcome.BUSINESS_ACCEPTED, Stage.CTV, Outcome.BUSINESS_FILE_REJECTED), Flow.COL);
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
                Map.of(Stage.CRR, Outcome.BUSINESS_FILE_FATAL), Flow.COL);
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
                        Stage.AIS, Outcome.BUSINESS_FILE_FATAL), Flow.COL);
        assertTrue(args.contains("outcome.hint=BUSINESS_FILE_FATAL,java.lang.String,false"),
                "got " + args);
    }

    @Test
    void cirOmitsHintOnPartialAck() {
        // PARTIAL is an ACK-with-partials launch: no rejecting stage, no hint.
        List<String> args = JobLauncher.serviceArgs(Stage.CIR, arrival(),
                Map.of(Stage.CRR, Outcome.BUSINESS_ACCEPTED, Stage.CTV, Outcome.BUSINESS_PARTIAL), Flow.COL);
        assertFalse(args.stream().anyMatch(a -> a.startsWith("outcome.hint=")),
                "got " + args);
    }

    @Test
    void nonCirStagesCarryNoIdentityParams() {
        List<String> args = JobLauncher.serviceArgs(Stage.CDE, arrival(),
                Map.of(Stage.CTV, Outcome.BUSINESS_PARTIAL), Flow.COL);
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
                () -> JobLauncher.serviceArgs(Stage.CIR, nullRoute, Map.of(), Flow.COL));
        assertTrue(e.getMessage().contains("route.id"), "got: " + e.getMessage());
        assertTrue(e.getMessage().contains(ARRIVAL_ID.toString()), "got: " + e.getMessage());

        FileArrival blankClient = new FileArrival(ARRIVAL_ID, "onhost-req", "FNBRF01_MSG1.txt",
                "sha", " ", "DCRERF2026071313500102",
                ArrivalStatus.DAG_RUNNING, null, "/exchange/claimed/FNBRF01_MSG1.txt");
        assertThrows(IllegalStateException.class,
                () -> JobLauncher.serviceArgs(Stage.CIR, blankClient, Map.of(), Flow.COL));

        FileArrival nullMsgId = new FileArrival(ARRIVAL_ID, "onhost-req", "FNBRF01_MSG1.txt",
                "sha", "FNBRF01", null,
                ArrivalStatus.DAG_RUNNING, null, "/exchange/claimed/FNBRF01_MSG1.txt");
        assertThrows(IllegalStateException.class,
                () -> JobLauncher.serviceArgs(Stage.CIR, nullMsgId, Map.of(), Flow.COL));
    }

    @Test
    void boundaryReaderArgsUnchanged() {
        List<String> args = JobLauncher.serviceArgs(Stage.CRR, arrival(), Map.of(), Flow.COL);
        assertEquals(List.of(
                "arrival.id=" + ARRIVAL_ID,
                "input.file=/exchange/claimed/FNBRF01_MSG1.txt,java.lang.String,false",
                "original.name=FNBRF01_MSG1.txt,java.lang.String,false"), args);
    }

    // --- M10 mandates (SCRUM-79) -------------------------------------------

    private static FileArrival manArrival() {
        return new FileArrival(ARRIVAL_ID, "onhost-req-man", "FNBCC01_MANB1.txt",
                "sha", "FNBCC01", "MANB1",
                ArrivalStatus.DAG_RUNNING, null, "/exchange/claimed/FNBCC01_MANB1.txt");
    }

    private static FileArrival manRespArrival(String token) {
        String name = "FNBCC01_OUT1_" + token + ".xml";
        return new FileArrival(ARRIVAL_ID, "fint-resp-man", name,
                "sha", "FNBCC01", "OUT1_" + token,
                ArrivalStatus.DAG_RUNNING, null, "/exchange/claimed/" + name);
    }

    @Test
    void mrrIsABoundaryReader() {
        List<String> args = JobLauncher.serviceArgs(Stage.MRR, manArrival(), Map.of(), Flow.MAN);
        assertEquals(List.of(
                "arrival.id=" + ARRIVAL_ID,
                "input.file=/exchange/claimed/FNBCC01_MANB1.txt,java.lang.String,false",
                "original.name=FNBCC01_MANB1.txt,java.lang.String,false"), args,
                "MRR reads the claimed instruction book (CRR pattern)");
    }

    @Test
    void eachLegReaderIsAPlainBoundaryReader() {
        // SCRUM-91: the reply type selected MAR's target table via a reply.type
        // arg; each leg reader owns exactly one table now, so there is nothing
        // left to select and the args are the plain boundary-reader trio.
        Map<Stage, String> legs = Map.of(Stage.MIX, "ISR", Stage.MSX, "SBSR", Stage.MPX, "PBSR");
        legs.forEach((stage, token) -> assertEquals(List.of(
                "arrival.id=" + ARRIVAL_ID,
                "input.file=/exchange/claimed/FNBCC01_OUT1_" + token + ".xml,java.lang.String,false",
                "original.name=FNBCC01_OUT1_" + token + ".xml,java.lang.String,false"),
                JobLauncher.serviceArgs(stage, manRespArrival(token), Map.of(), Flow.MAN),
                stage + " reads the claimed pain.012 payload and nothing else"));
    }

    @Test
    void noLegReaderCarriesAReplyTypeArg() {
        for (Stage stage : List.of(Stage.MIX, Stage.MSX, Stage.MPX)) {
            assertFalse(JobLauncher.serviceArgs(stage, manRespArrival("ISR"), Map.of(), Flow.MAN).stream()
                            .anyMatch(a -> a.startsWith("reply.type=")),
                    "the leg is the service now, not a launch arg: " + stage);
        }
    }

    @Test
    void mirCarriesTheArrivalIdentityAndHintLikeCir() {
        // MIR is the man-route responder (CIR clone, T7): same A-45 identity
        // params and the rejecting validator's verdict as outcome.hint.
        List<String> args = JobLauncher.serviceArgs(Stage.MIR, manArrival(),
                Map.of(Stage.MRR, Outcome.BUSINESS_ACCEPTED, Stage.MRV, Outcome.BUSINESS_FILE_REJECTED), Flow.MAN);
        assertTrue(args.contains("arrival.id=" + ARRIVAL_ID));
        assertTrue(args.contains("route.id=onhost-req-man,java.lang.String,false"), "got " + args);
        assertTrue(args.contains("client.token=FNBCC01,java.lang.String,false"));
        assertTrue(args.contains("msg.id=MANB1,java.lang.String,false"));
        assertTrue(args.contains("outcome.hint=BUSINESS_FILE_REJECTED,java.lang.String,false"));
    }

    @Test
    void mirOmitsHintWhenOnlyTheBoundaryReaderFailed() {
        // MRR fatal: no spine/verdicts exist; MIR NACKs from fatal.reason.
        List<String> args = JobLauncher.serviceArgs(Stage.MIR, manArrival(),
                Map.of(Stage.MRR, Outcome.BUSINESS_FILE_FATAL), Flow.MAN);
        assertTrue(args.contains("client.token=FNBCC01,java.lang.String,false"));
        assertFalse(args.stream().anyMatch(a -> a.startsWith("outcome.hint=")), "got " + args);
    }
}
