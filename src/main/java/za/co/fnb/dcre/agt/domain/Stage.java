package za.co.fnb.dcre.agt.domain;

/** Pipeline stages of the Collections DAG (M1: stub jobs; M2 replaces CRR/CTV/CIR;
 *  M4 adds the fint-resp readers IXR/SXR/PXR and the clock-driven PRG). */
public enum Stage {
    CRR, CTV, CDE, CRW, CIR, IXR, SXR, PXR, PRG
}
