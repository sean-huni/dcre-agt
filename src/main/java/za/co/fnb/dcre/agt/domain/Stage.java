package za.co.fnb.dcre.agt.domain;

/** Pipeline stages of the Collections DAG (M2 replaces CRR/CTV/CIR;
 *  M4 adds the fint-resp readers IXR/SXR/PXR and the clock-driven PRG;
 *  M5 adds AIS on the ENDO route; M6 adds the clock-driven HCS holiday sync;
 *  M10/SCRUM-79 adds the Mandates family: MRR->MRV->MAF->MIS->{MIR,MRW} on
 *  onhost-req-man, MAR->MSR on fint-resp-man, and the clock-driven MRG). */
public enum Stage {
    CRR, CTV, CDE, CRW, CIR, IXR, SXR, PXR, PRG, AIS, HCS,
    MRR, MRV, MAF, MIS, MIR, MRW, MAR, MSR, MRG
}
