package za.co.fnb.dcre.agt.health;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;

/**
 * M12/SCRUM-87 (R-47): the @Liveness check is registered at the smallrye default
 * path the k8s livenessProbe hits (/q/health/live) and reports UP at startup.
 * A generous TTL keeps boot latency from flapping the startup-grace assertion.
 */
@QuarkusTest
@TestProfile(ReconcilerLivenessEndpointTest.Profile.class)
class ReconcilerLivenessEndpointTest {

    public static class Profile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("agt.self-liveness-ttl-seconds", "3600");
        }
    }

    @Test
    void livenessEndpointExposesTheReconcilerCheckAndIsUpAtStartup() {
        given().when().get("/q/health/live")
                .then().statusCode(200)
                .body(containsString("reconciler-liveness"));
    }
}
