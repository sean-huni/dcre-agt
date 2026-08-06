package za.co.fnb.dcre.agt.domain;

/**
 * One item of an orphan-sweep worklist: the intent plus the outcome class its
 * CURRENT attempt recorded, which is what selects the relaunch CEILING
 * (TECH_FAILED -> agt.orphan-max-attempts; TECH_CONFIG_FAILED ->
 * agt.infra-max-attempts). Carried on the worklist rather than re-queried per
 * item because both sweeps already join stage_outcome to build it.
 *
 * <p>currentOutcome is NULL when the current attempt recorded no outcome at
 * all: a wedged-but-alive intent that never produced a Job condition, or a
 * TTL-reaped one. Absence of evidence is not evidence of an infrastructure
 * failure, so a null class takes the default (orphan) ceiling.
 */
public record RelaunchCandidate(LaunchIntent intent, Outcome currentOutcome) {
}
