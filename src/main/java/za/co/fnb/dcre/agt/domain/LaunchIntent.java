package za.co.fnb.dcre.agt.domain;

import java.util.UUID;

/** SCRUM-70: the intent carries its target K8s namespace durably, so every
 *  recreate/observe acts on the namespace that was INTENDED. */
public record LaunchIntent(UUID id, UUID arrivalId, Stage stage, String jobName,
                           String status, String runKey, int attempt, String namespace) {
    public static final String INTENDED = "INTENDED";
    public static final String LAUNCHED = "LAUNCHED";

    /** Legacy rows (pre-SCRUM-70) keep namespace NULL: fall back to the control namespace. */
    public String namespaceOr(String fallback) {
        return namespace != null ? namespace : fallback;
    }
}
