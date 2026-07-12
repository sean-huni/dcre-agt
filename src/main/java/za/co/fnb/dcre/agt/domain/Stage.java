package za.co.fnb.dcre.agt.domain;

/** Pipeline stages of the Collections DAG (M2 replaces CRR/CTV/CIR;
 *  M4 adds the fint-resp readers IXR/SXR/PXR and the clock-driven PRG;
 *  M5 adds AIS on the ENDO route). */
public enum Stage {
    CRR, CTV, CDE, CRW, CIR, IXR, SXR, PXR, PRG, AIS
}
