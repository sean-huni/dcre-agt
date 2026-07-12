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

    /** Gate for K8s launches; disabled in unit tests. */
    @WithDefault("true")
    boolean launchEnabled();

    /** Observation stays on even when launching is paused (Fugu F1a). */
    @WithDefault("true")
    boolean observeEnabled();

    /** Service images per stage (SCRUM-33: no stub fallback; a missing image
     *  fails the launch fast). */
    java.util.Optional<String> crrImage();

    java.util.Optional<String> ctvImage();

    java.util.Optional<String> cirImage();

    java.util.Optional<String> cdeImage();

    java.util.Optional<String> crwImage();

    /** M4 fint-resp reader images (single-stage response DAGs). */
    java.util.Optional<String> ixrImage();

    java.util.Optional<String> sxrImage();

    java.util.Optional<String> pxrImage();

    /** M4 PRG clock-window executor image (R-28). */
    java.util.Optional<String> prgImage();

    /** M5 AIS endorsements stage image (ENDO route, R-36). */
    java.util.Optional<String> aisImage();

    /** CRW Process-Date Executor window length (R-37); dev default 60s. */
    @WithDefault("60")
    long crwIntervalSeconds();

    /** PRG clock-window length (R-28); dev default 60s. */
    @WithDefault("60")
    long prgIntervalSeconds();

    /** JDBC url the service Jobs use for dcre_collections (in-cluster). */
    @WithDefault("jdbc:postgresql://crdb:26257/dcre_collections?sslmode=disable")
    String serviceDbUrl();
}
