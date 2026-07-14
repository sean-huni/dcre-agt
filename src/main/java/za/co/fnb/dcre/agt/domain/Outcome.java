package za.co.fnb.dcre.agt.domain;

/** Stage-outcome ledger classes (SPEC-DAG section 3; Fugu F4). */
public enum Outcome {
    TECH_FAILED, BUSINESS_FILE_FATAL,
    /** R-41: ALL_OR_NOTHING whole-file business rejection; CIR-only successor. */
    BUSINESS_FILE_REJECTED,
    BUSINESS_PARTIAL, BUSINESS_ACCEPTED,
    /** OrphanSweeper: relaunch budget exhausted; arrival goes DAG_FAILED (terminal). */
    TECH_EXHAUSTED
}
