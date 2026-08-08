package za.co.fnb.dcre.agt.config;

import io.smallrye.config.ConfigMapping;

import java.util.Optional;

/**
 * The 29 stage-service container images, one knob per stage on the diagrams'
 * roster (28 stage services plus the cross-family HCS).
 *
 * <p>Split out of {@link AgtConfig} because it is a different responsibility and
 * because carrying 29 more accessors there put one interface far past the
 * ~100-line design trigger. It deliberately shares the {@code agt} prefix, so the
 * binding is unchanged: {@code agt.crr-image} still comes from
 * {@code AGT_CRR_IMAGE}. That naming is a CROSS-REPO CONTRACT with
 * {@code infra/dcre-infra/scripts/switch-version.sh}, which sets
 * {@code AGT_<STAGE>_IMAGE=dcre-<stage>:<version>} for every stage in the roster;
 * renaming a knob here makes that export silently inert.
 *
 * <p>Empty/absent is LAUNCH-DISABLED, never a stub fallback (SCRUM-33): a clock
 * scheduler skips its windows and a DAG launch fails fast, so a missing image is
 * a misconfiguration that surfaces rather than a silent substitution.
 *
 * <p>12FactorApp Alignment, https://12factor.net/: every value comes from the
 * environment, and a fresh clone runs with no {@code .env} at all.
 */
@ConfigMapping(prefix = "agt")
public interface AgtImages {

    // ---- collections: CRR CTV CDE CRW CIR CIX CSX CPX CRG ----

    Optional<String> crrImage();

    Optional<String> ctvImage();

    Optional<String> cdeImage();

    Optional<String> crwImage();

    Optional<String> cirImage();

    /** The three fint-resp leg readers (ISR/SBSR/PBSR), formerly IXR/SXR/PXR. */
    Optional<String> cixImage();

    Optional<String> csxImage();

    Optional<String> cpxImage();

    /** The COLLECTIONS report generator. Formerly called PRG; that token now names the payments one. */
    Optional<String> crgImage();

    // ---- payments: PRR PTV PAI PRW PIR PIX PSX PPX PRG ----

    Optional<String> prrImage();

    Optional<String> ptvImage();

    /** Account Init Service (ENDO), formerly AIS on the collections family. */
    Optional<String> paiImage();

    Optional<String> prwImage();

    Optional<String> pirImage();

    Optional<String> pixImage();

    Optional<String> psxImage();

    Optional<String> ppxImage();

    /** The PAYMENTS report generator. This knob CHANGED MEANING at the 2026-08-08 cutover. */
    Optional<String> prgImage();

    // ---- mandates: MRR MRV MAS MIT MIR MRW MIX MSX MPX MRG ----

    Optional<String> mrrImage();

    Optional<String> mrvImage();

    Optional<String> masImage();

    Optional<String> mitImage();

    Optional<String> mirImage();

    Optional<String> mrwImage();

    Optional<String> mixImage();

    Optional<String> msxImage();

    Optional<String> mpxImage();

    Optional<String> mrgImage();

    // ---- cross-family ----

    Optional<String> hcsImage();
}
