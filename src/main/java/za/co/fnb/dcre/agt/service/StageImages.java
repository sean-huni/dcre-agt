package za.co.fnb.dcre.agt.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import za.co.fnb.dcre.agt.config.AgtImages;
import za.co.fnb.dcre.agt.domain.Stage;

import java.util.Optional;

/**
 * Stage to container image, as ONE exhaustive switch.
 *
 * <p>Extracted from {@link JobLauncher} with the v1 topology: the switch grew from
 * 21 arms to 29 in a class already several times past the ~100-line design
 * trigger, and resolving an image is not the launcher's job.
 *
 * <p>Exhaustive with NO default arm on purpose. Adding a constant to {@link Stage}
 * breaks THIS compilation until somebody decides which image the new stage runs,
 * which is the fail-closed property a {@code Map} lookup or an
 * {@code EnumSet.complementOf} cannot give.
 */
@ApplicationScoped
public class StageImages {

    @Inject
    AgtImages images;

    /**
     * The configured image for a stage, or empty when the knob is unset.
     *
     * <p>Empty is LAUNCH-DISABLED (SCRUM-33), which is why the clock schedulers
     * test it before minting a window rather than discovering it at launch.
     */
    public Optional<String> configured(final Stage stage) {
        return switch (stage) {
            case CRR -> images.crrImage();
            case CTV -> images.ctvImage();
            case CDE -> images.cdeImage();
            case CRW -> images.crwImage();
            case CIR -> images.cirImage();
            case CIX -> images.cixImage();
            case CSX -> images.csxImage();
            case CPX -> images.cpxImage();
            case CRG -> images.crgImage();
            case PRR -> images.prrImage();
            case PTV -> images.ptvImage();
            case PAI -> images.paiImage();
            case PRW -> images.prwImage();
            case PIR -> images.pirImage();
            case PIX -> images.pixImage();
            case PSX -> images.psxImage();
            case PPX -> images.ppxImage();
            case PRG -> images.prgImage();
            case MRR -> images.mrrImage();
            case MRV -> images.mrvImage();
            case MAS -> images.masImage();
            case MIT -> images.mitImage();
            case MIR -> images.mirImage();
            case MRW -> images.mrwImage();
            case MIX -> images.mixImage();
            case MSX -> images.msxImage();
            case MPX -> images.mpxImage();
            case MRG -> images.mrgImage();
            case HCS -> images.hcsImage();
            case ACS -> images.acsImage();
        };
    }

    /** The image a launch requires; a missing one is a misconfiguration, never a fallback. */
    public String required(final Stage stage) {
        return configured(stage).orElseThrow(() -> new IllegalStateException(
                "no image configured for stage " + stage + ": set AGT_" + stage.name() + "_IMAGE"));
    }
}
