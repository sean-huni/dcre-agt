package za.co.fnb.dcre.agt.domain;

/**
 * SCRUM-70 flow taxonomy (spec 2026-07-21-flow-taxonomy-mandates-design.md,
 * section 1 item 3): namespaces classify JOB FAMILIES. Collections (DC,
 * CDE-scheduled), Payments (ENDO, immediate), Mandates (M10, dormant until
 * then). The job-name prefix replaces the old dcre- literal and is shorter,
 * so the 63-char K8s name safety only improves.
 */
public enum Flow {
    COL("col-"),
    PAY("pay-"),
    MAN("man-");

    private final String jobPrefix;

    Flow(String jobPrefix) {
        this.jobPrefix = jobPrefix;
    }

    public String jobPrefix() {
        return jobPrefix;
    }
}
