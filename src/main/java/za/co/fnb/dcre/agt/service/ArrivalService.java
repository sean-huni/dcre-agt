package za.co.fnb.dcre.agt.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import za.co.fnb.dcre.agt.domain.ArrivalStatus;
import za.co.fnb.dcre.agt.repo.ArrivalRepo;
import za.co.fnb.dcre.agt.repo.DuplicateRepo;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Optional;
import java.util.UUID;

/**
 * Registers inbound files in the file_arrival ledger (R-16; R-30 amendment,
 * SCRUM-42).
 *
 * <p>The watcher hands the path-derived {@code pathClient} (from the
 * {@code <clientbase>} directory segment) alongside the file and route. The
 * filename FNB token is parsed as {@code filenameClient} and cross-checked:
 * an unparseable name quarantines {@code UNPARSEABLE_FILENAME}; a
 * {@code filenameClient != pathClient} mismatch (a misfiled file) quarantines
 * the new reason {@code CLIENT_PATH_MISMATCH}; both fail closed.
 * {@code pathClient} is the authoritative {@code client_token} written to the
 * ledger, and the per-(client, channel) sinks are resolved from it via
 * {@link ExchangeSinks}.
 *
 * <p>Claim-first discipline (Fugu F4/F5): the ATOMIC_MOVE into
 * {@code <client>/<channel>/archive/inflight} happens BEFORE hashing and BEFORE
 * the ledger insert, so the recorded SHA-256 always describes the claimed bytes
 * and no crash window can delete the only payload copy. Dedup decision tree
 * (F3/F11/F13): same content anywhere non-quarantined -> duplicate no-op
 * (filename-independent); same logical key with different content -> quarantine;
 * else new arrival. DB partial unique indexes back every branch. The
 * {@code route_id} / {@code client_token} columns and the dedup keys
 * {@code (route, content_hash)} / {@code (route, logical_key)} are unchanged by
 * the per-client layout.
 */
@ApplicationScoped
public class ArrivalService {

    private static final Logger LOG = Logger.getLogger(ArrivalService.class);
    public static final String ROUTE_ONHOST_REQ = "onhost-req";
    public static final String ROUTE_ONHOST_REQ_ENDO = "onhost-req-endo";
    public static final String ROUTE_FINT_RESP = "fint-resp";
    /** M10 mandates routes (SCRUM-79): dedicated man exchange channels end to end. */
    public static final String ROUTE_ONHOST_REQ_MAN = "onhost-req-man";
    public static final String ROUTE_FINT_RESP_MAN = "fint-resp-man";

    @Inject
    ArrivalRepo repo;

    @Inject
    DuplicateRepo duplicates;

    @Inject
    ExchangeSinks sinks;

    public sealed interface Result {
        record NewArrival(UUID id) implements Result { }
        record DuplicateSameHash() implements Result { }
        record Quarantined(String reason) implements Result { }
    }

    /**
     * @param file       the size-stable inbound file under {@code <client>/<route>/in}
     * @param route      the AGT route id (equals the channel token)
     * @param pathClient the client owning the drop zone; the authoritative client_token
     */
    public Result register(final Path file, final String route, final String pathClient) {
        final String name = file.getFileName().toString();

        // R-31 tokens; unparseable names fail closed (F13).
        final String stem = name.contains(".") ? name.substring(0, name.lastIndexOf('.')) : name;
        final String[] tokens = stem.split("_");
        String filenameClient = null;
        String msgId = null;
        if (tokens.length >= 2 && tokens[0].startsWith("FNB")) {
            filenameClient = tokens[0];
            // Logical identity is the WHOLE stem after the client token: response
            // legs suffix the MsgId with a reply type (_ISR/_SBSR/_PBSR), and those
            // are distinct logical files, not conflicting re-sends of one key.
            msgId = stem.substring(filenameClient.length() + 1);
        }

        // Claim first: uuid-prefixed ATOMIC_MOVE into the path client's inflight.
        final UUID arrivalId = UUID.randomUUID();
        final Path claimed = sinks.inflight(pathClient, route).resolve(arrivalId + "_" + name);
        try {
            Files.move(file, claimed, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            LOG.warnf("claim of %s failed (%s); retrying next tick", name, e.getMessage());
            return new Result.Quarantined("CLAIM_RETRY");
        }
        final String sha256 = sha256(claimed);

        // pathClient is authoritative: the drop-zone directory names the client.
        if (filenameClient == null) {
            LOG.warnf("QUARANTINED %s: filename lacks R-31 tokens", name);
            return quarantine(claimed, arrivalId, name, pathClient, route, sha256, null,
                    "UNPARSEABLE_FILENAME");
        }

        if (!filenameClient.equals(pathClient)) {
            // Misfiled: the filename FNB token disagrees with the drop-zone client
            // (R-30 amendment). Fail closed; pathClient stays the ledger client_token.
            LOG.warnf("QUARANTINED %s: filename client %s != path client %s",
                    name, filenameClient, pathClient);
            return quarantine(claimed, arrivalId, name, pathClient, route, sha256, msgId,
                    "CLIENT_PATH_MISMATCH");
        }

        final Optional<UUID> twin = repo.findContentTwin(route, sha256);
        if (twin.isPresent()) {
            // Identical bytes already registered under ANY name: record the
            // re-delivery write-ahead, then no-op (F3, spec 1.1).
            recordDuplicate(claimed, twin.get(), name, pathClient, route, sha256);
            LOG.infof("Duplicate content re-delivery ignored: %s", name);
            return new Result.DuplicateSameHash();
        }

        if (repo.sameKeyDifferentHashExists(route, pathClient, msgId, sha256)) {
            LOG.warnf("QUARANTINED %s: same logical key, different hash", name);
            return quarantine(claimed, arrivalId, name, pathClient, route, sha256, msgId,
                    "SAME_KEY_DIFFERENT_HASH");
        }

        final Optional<UUID> id = repo.insertArrival(arrivalId, route, name, sha256, pathClient, msgId,
                ArrivalStatus.CLAIMED, null, claimed.toString());
        if (id.isEmpty()) {
            // Raced another writer on a dedup index: classify by what exists now.
            final Optional<UUID> raceTwin = repo.findContentTwin(route, sha256);
            if (raceTwin.isPresent()) {
                recordDuplicate(claimed, raceTwin.get(), name, pathClient, route, sha256);
                return new Result.DuplicateSameHash();
            }
            return quarantine(claimed, arrivalId, name, pathClient, route, sha256, msgId,
                    "SAME_KEY_DIFFERENT_HASH");
        }
        LOG.infof("Arrival %s claimed as %s", name, claimed.getFileName());
        return new Result.NewArrival(id.get());
    }

    /**
     * Quarantine fix (spec 2.3): the file_arrival row id IS the claim arrivalId
     * that prefixes the error-dir file, and claimed_path holds the error sink
     * path, so an error-dir {@code <uuid>_<name>} resolves to its row directly.
     * Row committed write-ahead BEFORE the move (fail path capture, Section 5.2).
     */
    private Result quarantine(final Path claimed, final UUID arrivalId, final String name,
                              final String client, final String route, final String sha256,
                              final String msgId, final String reason) {
        final Path sink = sinks.error(client, route).resolve(arrivalId + "_" + name); // unique: never clobbers evidence (F5)
        repo.insertArrival(arrivalId, route, name, sha256, client, msgId,
                ArrivalStatus.QUARANTINED, reason, sink.toString());
        move(claimed, sink);
        return new Result.Quarantined(reason);
    }

    /**
     * Records the re-delivery in duplicate_delivery write-ahead (spec 1.1) BEFORE
     * sinking the file, keyed by the claim_id parsed from the inflight
     * {@code <claimUuid>_} prefix so an orphan-sweep resume that re-parses the
     * same claim id is an ON CONFLICT no-op. sunk_path commits before the move.
     */
    private void recordDuplicate(final Path claimed, final UUID originalArrivalId, final String name,
                                 final String client, final String route, final String sha256) {
        final UUID claimId = claimIdOf(claimed);
        final Path sunk = sinks.duplicates(client, route).resolve(claimId + "_" + name); // kept, not deleted (F4)
        duplicates.insertDuplicate(claimId, originalArrivalId, route, client, name, sha256, sunk.toString());
        move(claimed, sunk);
    }

    /** The per-delivery claim UUID prefixing the inflight file ({@code <claimUuid>_<name>}).
     *  Package-private for the pure-unit malformed-name guard test (M3). Only ever
     *  reached for an already-claimed content-twin, which is always {@code <uuid>_}
     *  prefixed; the guards fail closed with context (move/hash error idiom) rather
     *  than a bare StringIndexOutOfBounds / IllegalArgumentException. */
    static UUID claimIdOf(final Path claimed) {
        final String fn = claimed.getFileName().toString();
        final int sep = fn.indexOf('_');
        if (sep <= 0) {
            throw new IllegalStateException("inflight filename lacks a '<claimUuid>_' prefix: " + fn);
        }
        try {
            return UUID.fromString(fn.substring(0, sep));
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("inflight filename prefix is not a valid claim UUID: " + fn, e);
        }
    }

    private static void move(final Path from, final Path to) {
        try {
            Files.move(from, to, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new IllegalStateException("cannot move " + from + " -> " + to, e);
        }
    }

    /** Streaming digest (F17): no whole-file buffering on the long-running AGT. */
    private static String sha256(final Path file) {
        try {
            final MessageDigest md = MessageDigest.getInstance("SHA-256");
            try (InputStream in = new DigestInputStream(Files.newInputStream(file), md)) {
                in.transferTo(OutputStreamSink.NULL);
            }
            return HexFormat.of().formatHex(md.digest());
        } catch (Exception e) {
            throw new IllegalStateException("cannot hash " + file, e);
        }
    }

    private static final class OutputStreamSink {
        static final java.io.OutputStream NULL = java.io.OutputStream.nullOutputStream();
    }
}
