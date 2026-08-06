package za.co.fnb.dcre.agt.domain;

/** Pipeline stages of the Collections DAG (M2 replaces CRR/CTV/CIR;
 *  M4 adds the fint-resp readers IXR/SXR/PXR and the clock-driven PRG;
 *  M5 adds AIS on the ENDO route; M6 adds the clock-driven HCS holiday sync;
 *  M10/SCRUM-79 adds the Mandates family: MRR->MRV->MAS->MIT->{MIR,MRW} on
 *  onhost-req-man and the clock-driven MRG; SCRUM-91 replaces the merged
 *  MAR->MSR response chain with the three token-picked leg readers MIX/MSX/MPX,
 *  mirroring IXR/SXR/PXR).
 *
 *  <p>MAR, MSR, MIS and MAF are RETAINED-DEPRECATED (A-75): agt_ops.stage_outcome.stage is
 *  VARCHAR(16) parsed back with Stage.valueOf, so deleting them makes every
 *  historic row unparseable and breaks the reconciler on any environment with
 *  pre-cutover history. They appear in no RouteDag, no launchable set and no
 *  serviceArgs branch. Removal requires a documented history purge. */
public enum Stage {
    CRR, CTV, CDE, CRW, CIR, IXR, SXR, PXR, PRG, AIS, HCS,
    MRR, MRV, MAS, MIT, MIR, MRW, MIX, MSX, MPX, MRG,
    /** @deprecated SCRUM-91: split into MIX/MSX/MPX. Kept only for Stage.valueOf on historic rows. */
    @Deprecated MAR,
    /** @deprecated SCRUM-91: the mandate projection it wrote is replaced by derived views. */
    @Deprecated MSR,
    /** @deprecated SCRUM-107: renamed to MIT. Kept only for Stage.valueOf on historic rows. */
    @Deprecated MIS,
    /** @deprecated SCRUM-107: renamed to MAS. Kept only for Stage.valueOf on historic rows. */
    @Deprecated MAF
}
