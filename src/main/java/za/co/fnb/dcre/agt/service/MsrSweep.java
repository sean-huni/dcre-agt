package za.co.fnb.dcre.agt.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.domain.Flow;
import za.co.fnb.dcre.agt.domain.Stage;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * M10/SCRUM-78 (A-71): shared GLOBAL clock-window mechanics for the two MSR
 * sweeps (auth-window expiry, failed-collection suspension). Both are GLOBAL,
 * NOT per-client: MSR's expiry ({@code WHERE state='PDNG'}) and suspend
 * ({@code WHERE state='ACCP'}) sweeps scan every mandate in dcre_man, so AGT
 * launches ONE sweep per interval window on a pure clock (HcsScheduler /
 * CrwScheduler pattern), never a per-client loop and independent of any arrival.
 * Both ride Flow.MAN, derive the window from the epoch (level-triggered; the
 * clock-intent unique key dedupes), select their MSR Batch job via the
 * DCRE_MSR_JOB_NAME env, and carry a deterministic {@code sweep.instant} = the
 * window START instant (never {@code now()}). That instant is MSR's
 * {@code SWEEP-<kind>-<instant>} idempotency token, so a kill-resume re-runs the
 * SAME window idempotently (SCRUM-90: never a non-deterministic key). The expiry
 * and suspend schedulers differ only in (discriminator, msr job, interval), so
 * the launch lives here once and each {@code @Scheduled} bean stays a thin tick.
 */
@ApplicationScoped
public class MsrSweep {

    private final AgtConfig config;
    private final LeaseService lease;
    private final JobLauncher launcher;

    @Inject
    MsrSweep(final AgtConfig config, final LeaseService lease, final JobLauncher launcher) {
        this.config = config;
        this.lease = lease;
        this.launcher = launcher;
    }

    /** Launch ONE global MSR sweep for the current window. {@code discriminator}
     *  (expiry/suspend) keeps the two sweeps' (stage, run_key) identities distinct
     *  though both ride Stage.MSR. The deterministic {@code sweep.instant} is the
     *  Batch job-instance identity, and the client-free run key makes the K8s Job
     *  name deterministic so a relaunch resumes the same instance. Absent/empty
     *  msr-image = launch-disabled (SCRUM-33: no stub fallback). */
    void run(final String discriminator, final String msrJob, final long intervalSeconds) {
        if (!lease.holdsLease() || !config.launchEnabled() || config.msrImage().isEmpty()) {
            return;
        }
        final long epochSeconds = Instant.now().getEpochSecond();
        final long window = PrgScheduler.window(epochSeconds, intervalSeconds);
        launcher.launchClock(Flow.MAN, Stage.MSR, discriminator + "-w" + window,
                List.of("sweep.instant=" + sweepInstant(epochSeconds, intervalSeconds)),
                Map.of(JobLauncher.MSR_JOB_ENV, msrJob));
    }

    /** Deterministic per-window sweep instant = the window START instant, stable
     *  within a window and distinct across windows. MSR reads it as the
     *  {@code sweep.instant} job parameter and scopes its {@code SWEEP-<kind>-<instant>}
     *  full-identity key on it, so re-running the same window adds no duplicate rows
     *  (SCRUM-90: never a {@code now()}-keyed token). Pure function of
     *  (epochSeconds, interval) for direct unit testing. */
    static String sweepInstant(final long epochSeconds, final long intervalSeconds) {
        final long window = PrgScheduler.window(epochSeconds, intervalSeconds);
        return Instant.ofEpochSecond(window * intervalSeconds).toString();
    }
}
