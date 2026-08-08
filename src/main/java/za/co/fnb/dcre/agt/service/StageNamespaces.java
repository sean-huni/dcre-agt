package za.co.fnb.dcre.agt.service;

import jakarta.enterprise.context.ApplicationScoped;
import za.co.fnb.dcre.agt.domain.Flow;
import za.co.fnb.dcre.agt.domain.Stage;

/**
 * The k8s NAMESPACE family a stage's Job runs in, and the fail-closed cross-check
 * that a Job never lands in another family's namespace.
 *
 * <p>Split out of {@code StageDatabases} on 2026-08-08. One switch used to answer
 * both "which namespace" and "which database", and the two are not the same
 * question: {@code HCS} is hosted in the collections namespace AND owns its own
 * database {@code dcre_hcs}. While the answers agreed, the conflation was invisible;
 * the moment they diverged the single switch had to be wrong about one of them, and
 * it was wrong about the database, silently, because a write to {@code dcre_col} is
 * perfectly valid.
 *
 * <p>Two exhaustive switches over {@link Stage} is the point, not a duplication: a
 * stage added to the enum must now be given a namespace AND a database before
 * anything compiles.
 */
@ApplicationScoped
public class StageNamespaces {

    /**
     * The flow whose namespace hosts this stage's Jobs.
     *
     * <p>Exhaustive, no default arm.
     *
     * <p><b>HCS is HOSTED by collections and OWNED by nobody but itself.</b> It has
     * no namespace of its own: nothing has ruled one into existence, {@code HcsScheduler}
     * launches it into {@code dcre-col}, and its Job names carry the {@code col-}
     * prefix. That hosting is recorded here as a decision rather than inherited from
     * its database, which is now {@code dcre_hcs}. If HCS ever gets a namespace of its
     * own, this arm is the one line that has to change, and the RBAC and manifests
     * that go with it.
     */
    public Flow namespaceFamilyOf(final Stage stage) {
        return switch (stage) {
            case CRR, CTV, CDE, CRW, CIR, CIX, CSX, CPX, CRG -> Flow.COL;
            case PRR, PTV, PAI, PRW, PIR, PIX, PSX, PPX, PRG -> Flow.PAY;
            case MRR, MRV, MAS, MIT, MIR, MRW, MIX, MSX, MPX, MRG -> Flow.MAN;
            case HCS -> Flow.COL;
        };
    }

    /**
     * Fail-closed cross-check at Job-build time: the namespace a Job is going into
     * must be the one its stage is hosted in.
     *
     * <p>This is the guard for the defect class the whole v1 split exists to remove.
     * Before it, the ENDO lane executed COLLECTIONS services in the {@code dcre-pay}
     * namespace while writing {@code dcre_col}, and nothing anywhere compared the
     * two: the namespace came from the route, the database came from a stage
     * ternary, and a disagreement between them was unobservable.
     *
     * <p>It guards the NAMESPACE half only, and that is not a weakening: the database
     * half is now guarded by {@code StageDatabases}, whose switch has no default arm
     * and whose url cross-check refuses a family url that addresses another family's
     * database. Between them a Job cannot be built into the wrong namespace, and a
     * pod cannot be handed the wrong database.
     *
     * <p>The legacy control namespace resolves to COL
     * ({@link FlowNamespaces#flowForNamespace}), which is correct: everything that
     * predates flow namespaces was collections.
     */
    public void requireCorrectNamespace(final Stage stage, final Flow namespaceFlow, final String namespace) {
        final Flow hostedIn = namespaceFamilyOf(stage);
        if (hostedIn != namespaceFlow) {
            throw new IllegalStateException("stage " + stage + " is hosted in the " + hostedIn
                    + " namespace but its Job targets namespace '" + namespace + "' (" + namespaceFlow
                    + "). Namespace isolation without data isolation is the defect the family"
                    + " split exists to remove; refusing to create the Job.");
        }
    }
}
