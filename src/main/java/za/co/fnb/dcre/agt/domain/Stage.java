package za.co.fnb.dcre.agt.domain;

/**
 * The VERSION 1 stage roster: 28 stage services across three families plus the two
 * cross-family shared contexts, HCS and ACS. THE DIAGRAMS ARE THE SPECIFICATION
 * (design-register/docs/diagrams, R-49) for the three FAMILIES; this enum is a
 * transcription of them plus the cross-family services, which appear on no sheet by
 * design and are declared in {@code verify-topology.sh}'s {@code ALLOWED_shared}.
 *
 * <pre>
 * collections  CRR CTV CDE CRW CIR   CIX CSX CPX   CRG
 * payments     PRR PTV PAI PRW PIR   PIX PSX PPX   PRG
 * mandates     MRR MRV MAS MIT MIR MRW   MIX MSX MPX   MRG
 * cross-family HCS ACS
 * </pre>
 *
 * <p><b>PRG IS THE PAYMENTS REPORT GENERATOR.</b> Before the 2026-08-08 cutover the
 * same token named the COLLECTIONS one, which is now CRG. The token did not move,
 * it changed MEANING, so a find-and-replace over this file produces a build that
 * compiles and is semantically inverted. Every use of PRG must be read in context.
 *
 * <p>No deprecated constants. The pre-cutover names (IXR SXR PXR AIS, and MAR MSR
 * MIS MAF before them) were retained only because
 * {@code agt_ops.stage_outcome.stage} is {@code VARCHAR(16)} parsed back with
 * {@link #valueOf}, so deleting them made historic rows unparseable (A-75). The
 * owner directive of 2026-08-08 dropped every DCRE database and cut over directly
 * to a clean version 1, so there are no historic rows and the constraint that
 * justified retention no longer exists. A v1 enum holds exactly these 29 and
 * nothing else.
 */
public enum Stage {

    /** Collections (DC): the sheet's REQ chain, the three fint-resp leg readers, the report generator. */
    CRR, CTV, CDE, CRW, CIR, CIX, CSX, CPX, CRG,

    /** Payments (ENDO): PRR -> PTV -> PAI -> {PRW, PIR}, the three leg readers, and PRG. */
    PRR, PTV, PAI, PRW, PIR, PIX, PSX, PPX, PRG,

    /** Mandates: MRR -> MRV -> MAS -> MIT -> {MIR, MRW}, the three leg readers, and MRG. */
    MRR, MRV, MAS, MIT, MIR, MRW, MIX, MSX, MPX, MRG,

    /** Cross-family holiday-calendar sync (R-38): on no sheet, single writer of public_holiday. */
    HCS,

    /**
     * Cross-family account-registry census (SCRUM-107): on no sheet, single writer of
     * {@code account} / {@code account_type} in {@code dcre_acs}.
     *
     * <p><b>Absence from the sheets is not drift.</b> The six sheets specify the three
     * FAMILIES; HCS, ACS and RPT are cross-family services by design and appear on none
     * of them. {@code verify-topology.sh} declares all three in {@code ALLOWED_shared}
     * and exits 0. Read the sheets for what a family contains, never for whether a
     * cross-family service should exist.
     */
    ACS
}
