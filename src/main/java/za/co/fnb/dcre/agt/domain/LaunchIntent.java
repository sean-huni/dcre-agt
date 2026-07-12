package za.co.fnb.dcre.agt.domain;

import java.util.UUID;

public record LaunchIntent(UUID id, UUID arrivalId, Stage stage, String jobName, String status, String runKey) {
    public static final String INTENDED = "INTENDED";
    public static final String LAUNCHED = "LAUNCHED";
}
