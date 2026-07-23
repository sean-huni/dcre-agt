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
     *  bumped attempt, which the reconciler recovers on the next incarnation:
     *  no live Job -> recreate; the OLD Job still live (wedged-alive) -> adopt it
     *  AND re-arm the stale-heartbeat clock (reAdoptWithHeartbeat), so the wedge
     *  is re-detected within one TTL rather than stranded to the 900s
     *  activeDeadlineSeconds path. */
    public static final String ABANDONED = "ABANDONED";

    /** Legacy rows (pre-SCRUM-70) keep namespace NULL: fall back to the control namespace. */
    public String namespaceOr(String fallback) {
        return namespace != null ? namespace : fallback;
    }
}
