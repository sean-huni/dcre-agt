package za.co.fnb.dcre.agt.domain;

import java.util.UUID;

/** SCRUM-70: the intent carries its target K8s namespace durably, so every
 *  recreate/observe acts on the namespace that was INTENDED. */
public record LaunchIntent(UUID id, UUID arrivalId, Stage stage, String jobName,
                           String status, String runKey, int attempt, String namespace) {
    public static final String INTENDED = "INTENDED";
    public static final String LAUNCHED = "LAUNCHED";
    /** Transient write-ahead claim marker (M12/SCRUM-86): the atomic relaunch
     *  claim flips LAUNCHED -> ABANDONED so a second sweep/incarnation cannot
     *  re-claim the same intent in the tick; the relaunching pod's createJob
     *  re-marks it LAUNCHED. A crash in that window leaves it ABANDONED with a
     *  bumped attempt and no live Job, which the reconciler recreates (its
     *  not-LAUNCHED-no-Job branch), so it is always recoverable. */
    public static final String ABANDONED = "ABANDONED";

    /** Legacy rows (pre-SCRUM-70) keep namespace NULL: fall back to the control namespace. */
    public String namespaceOr(String fallback) {
        return namespace != null ? namespace : fallback;
    }
}
