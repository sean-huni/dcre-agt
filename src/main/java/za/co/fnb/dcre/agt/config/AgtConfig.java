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

    /** Observation stays on even when launching is paused (Fugu F1a). */
    @WithDefault("true")
    boolean observeEnabled();

    /** M2 service images per stage; absent stages run the busybox stub. */
    java.util.Optional<String> crrImage();

    java.util.Optional<String> ctvImage();

    java.util.Optional<String> cirImage();

    java.util.Optional<String> cdeImage();

    java.util.Optional<String> crwImage();

    /** CRW Process-Date Executor window length (R-37); dev default 60s. */
    @WithDefault("60")
    long crwIntervalSeconds();

    /** JDBC url the service Jobs use for dcre_collections (in-cluster). */
    @WithDefault("jdbc:postgresql://crdb:26257/dcre_collections?sslmode=disable")
    String serviceDbUrl();
}
