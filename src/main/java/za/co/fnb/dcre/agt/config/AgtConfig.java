package za.co.fnb.dcre.agt.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

@ConfigMapping(prefix = "agt")
public interface AgtConfig {

    /** Root of the exchange directory tree (R-30 contract). */
    String exchangeRoot();

    String namespace();

    /** Unique holder id for the DB lease (pod hostname). */
    String holderId();

    /** Image used for M1 stub jobs. */
    String stubImage();

    /** Gate for K8s launches; disabled in unit tests. */
    @WithDefault("true")
    boolean launchEnabled();
}
