package za.co.fnb.dcre.agt;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import za.co.fnb.dcre.agt.service.ArrivalService;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@QuarkusTestResource(CrdbTestResource.class)
class ArrivalServiceTest {

    @Inject
    ArrivalService arrivals;

    Path drop(String name, String content) throws Exception {
        Path dir = Path.of(System.getProperty("java.io.tmpdir"), "agt-test-in");
        Files.createDirectories(dir);
        Path f = dir.resolve(name);
        Files.writeString(f, content);
        return f;
    }

    @Test
    void newThenDuplicateThenQuarantine() throws Exception {
        String stem = "FNBRF01_DCRERF" + UUID.randomUUID().toString().substring(0, 8);

        var first = arrivals.register(drop(stem + ".txt", "content-A"));
        assertInstanceOf(ArrivalService.Result.NewArrival.class, first, "fresh file registers");

        var dup = arrivals.register(drop(stem + ".txt", "content-A"));
        assertInstanceOf(ArrivalService.Result.DuplicateSameHash.class, dup, "same key+hash no-ops");

        var tampered = arrivals.register(drop(stem + ".txt", "content-B"));
        assertInstanceOf(ArrivalService.Result.Quarantined.class, tampered,
                "same logical key, different content quarantines");

        UUID id = ((ArrivalService.Result.NewArrival) first).id();
        assertTrue(Files.list(arrivals.inflightDir())
                        .anyMatch(p -> p.getFileName().toString().startsWith(id.toString())),
                "claimed file moved to archive/inflight");
    }
}
