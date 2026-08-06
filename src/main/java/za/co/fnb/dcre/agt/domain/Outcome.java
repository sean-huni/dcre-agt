package za.co.fnb.dcre.agt.domain;

/** Stage-outcome ledger classes (SPEC-DAG section 3; Fugu F4). */
public enum Outcome {
    TECH_FAILED, BUSINESS_FILE_FATAL,
    /** R-41: ALL_OR_NOTHING whole-file business rejection; CIR-only successor. */
    BUSINESS_FILE_REJECTED,
    BUSINESS_PARTIAL, BUSINESS_ACCEPTED,
    /** OrphanSweeper: relaunch budget exhausted; arrival goes DAG_FAILED (terminal). */
    TECH_EXHAUSTED,
    /**
     * INFRASTRUCTURE startup failure, NOT a job outcome: the stage pod died
     * BEFORE its runner phase (config import, property binding, secret fetch),
     * signalled by platform-batch's reserved exit code 78 (EX_CONFIG from
     * sysexits.h). The work itself is defect-free and is still retried, but on
     * its own, larger ceiling (agt.infra-max-attempts) so a cfg restart or a
     * Vault re-seed cannot burn the 3-attempt orphan budget and turn a healthy
     * arrival into a terminal DAG_FAILED in 2-4 minutes.
     */
    TECH_CONFIG_FAILED
}
