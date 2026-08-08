package za.co.fnb.dcre.agt.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.domain.Flow;
import za.co.fnb.dcre.agt.domain.Stage;

/**
 * The database a stage's Job writes: {@code DCRE_DB_URL}, per family.
 *
 * <p>This class exists because the previous form was the highest-severity defect
 * in the v1 topology work:
 *
 * <pre>
 * return MAN_STAGES.contains(stage) ? config.manServiceDbUrl() : config.serviceDbUrl();
 * </pre>
 *
 * Two families, a boolean, and a DEFAULT. Adding a third family that way hands
 * every payments stage the collections URL, so PRW creates {@code prw_*} tables
 * inside {@code dcre_col} and nothing errors, because the write is perfectly valid
 * against the wrong database. Namespace isolation without data isolation is the
 * exact coupling the family split exists to remove.
 *
 * <p>So: an exhaustive switch with NO default arm. A stage added to {@link Stage}
 * breaks THIS compilation until somebody names its database. There is no
 * "everything else" any more.
 *
 * <p>STAGE-keyed rather than launch-keyed, deliberately and unchanged from B2
 * (SCRUM-79 review): the reconciled re-create path ({@code JobLauncher.createJob})
 * holds only the durable intent row, so the URL must be derivable from the stage
 * alone or a re-created pod would address a different database than the original.
 */
@ApplicationScoped
public class StageDatabases {

    @Inject
    AgtConfig config;

    /**
     * The JDBC url handed to a stage pod as {@code DCRE_DB_URL}.
     *
     * <p>HCS is cross-family and sits with collections: it is the single writer of
     * {@code public_holiday} (R-04) and CDE reads that calendar from
     * {@code dcre_col}, so moving it would break the reader it exists to serve.
     * Enumerated here rather than left to a default, so the placement is a decision
     * on the record instead of a fall-through.
     */
    public String urlFor(final Stage stage) {
        return switch (family(stage)) {
            case COL -> config.serviceDbUrl();
            case PAY -> config.payServiceDbUrl();
            case MAN -> config.manServiceDbUrl();
        };
    }

    /**
     * The family a stage belongs to, straight off the diagrams.
     *
     * <p>Exhaustive, no default arm: this is the single place that says which family
     * owns a stage, and a new constant must be assigned one before anything compiles.
     *
     * <p>HCS is cross-family and sits with collections: it is the single writer of
     * {@code public_holiday} (R-04) and CDE reads that calendar from
     * {@code dcre_col}, so moving it would break the reader it exists to serve.
     * Enumerated here rather than left to a default, so the placement is a decision
     * on the record instead of a fall-through.
     */
    public Flow family(final Stage stage) {
        return switch (stage) {
            case CRR, CTV, CDE, CRW, CIR, CIX, CSX, CPX, CRG, HCS -> Flow.COL;
            case PRR, PTV, PAI, PRW, PIR, PIX, PSX, PPX, PRG -> Flow.PAY;
            case MRR, MRV, MAS, MIT, MIR, MRW, MIX, MSX, MPX, MRG -> Flow.MAN;
        };
    }

    /**
     * Fail-closed cross-check at Job-build time: the namespace a Job is going into
     * must belong to the same family as the stage it runs.
     *
     * <p>This is the guard for the defect class the whole v1 split exists to remove.
     * Before it, the ENDO lane executed COLLECTIONS services in the {@code dcre-pay}
     * namespace while writing {@code dcre_col}, and nothing anywhere compared the
     * two: the namespace came from the route, the database came from a stage
     * ternary, and a disagreement between them was unobservable. Now a Job whose
     * namespace and stage disagree is never created.
     *
     * <p>The legacy control namespace resolves to COL
     * ({@link FlowNamespaces#flowForNamespace}), which is correct: everything that
     * predates flow namespaces was collections.
     */
    public void requireSameFamily(final Stage stage, final Flow namespaceFlow, final String namespace) {
        final Flow stageFamily = family(stage);
        if (stageFamily != namespaceFlow) {
            throw new IllegalStateException("stage " + stage + " belongs to the " + stageFamily
                    + " family but its Job targets namespace '" + namespace + "' (" + namespaceFlow
                    + "). Namespace isolation without data isolation is the defect the family"
                    + " split exists to remove; refusing to create the Job.");
        }
    }
}
