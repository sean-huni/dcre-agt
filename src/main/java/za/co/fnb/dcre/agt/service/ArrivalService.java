package za.co.fnb.dcre.agt.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.domain.ArrivalStatus;
import za.co.fnb.dcre.agt.repo.ArrivalRepo;

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
 * Registers inbound files in the file_arrival ledger (R-16).
 *
 * Claim-first discipline (Fugu F4/F5): the physical ATOMIC_MOVE into
 * archive/inflight happens BEFORE hashing and BEFORE the ledger insert, so the
 * recorded SHA-256 always describes the claimed bytes and no crash window can
 * delete the only payload copy. Dedup decision tree (F3/F11/F13):
 * unparseable filename -> quarantine (fail closed); same content anywhere
 * non-quarantined -> duplicate no-op (filename-independent); same logical key
 * with different content -> quarantine; else new arrival. DB partial unique
 * indexes back every branch (uq_arrival_content, uq_arrival_logical_key).
 */
@ApplicationScoped
public class ArrivalService {

    private static final Logger LOG = Logger.getLogger(ArrivalService.class);
    public static final String ROUTE_ONHOST_REQ = "onhost-req";
    public static final String ROUTE_FINT_RESP = "fint-resp";

    @Inject
    ArrivalRepo repo;

    @Inject
    AgtConfig config;

    public sealed interface Result {
        record NewArrival(UUID id) implements Result { }
        record DuplicateSameHash() implements Result { }
        record Quarantined(String reason) implements Result { }
    }

    public Result register(Path file) {
        return register(file, ROUTE_ONHOST_REQ);
    }

    public Result register(Path file, String route) {
        String name = file.getFileName().toString();

        // R-31 tokens; unparseable names fail closed (F13).
        String stem = name.contains(".") ? name.substring(0, name.lastIndexOf('.')) : name;
        String[] tokens = stem.split("_");
        String client = null;
        String msgId = null;
        if (tokens.length >= 2 && tokens[0].startsWith("FNB")) {
            client = tokens[0];
            // Logical identity is the WHOLE stem after the client token: response
            // legs suffix the MsgId with a reply type (_ISR/_SBSR/_PBSR), and those
            // are distinct logical files, not conflicting re-sends of one key.
            msgId = stem.substring(client.length() + 1);
        }

        // Claim first: uuid-prefixed ATOMIC_MOVE into inflight (same filesystem).
        UUID arrivalId = UUID.randomUUID();
        Path claimed = inflightDir().resolve(arrivalId + "_" + name);
        try {
            Files.move(file, claimed, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            LOG.warnf("claim of %s failed (%s); retrying next tick", name, e.getMessage());
            return new Result.Quarantined("CLAIM_RETRY");
        }
        String sha256 = sha256(claimed);

        if (client == null) {
            repo.insertArrival(UUID.randomUUID(), route, name, sha256, null, null,
                    ArrivalStatus.QUARANTINED, "UNPARSEABLE_FILENAME", null);
            moveToError(claimed, arrivalId, name);
            LOG.warnf("QUARANTINED %s: filename lacks R-31 tokens", name);
            return new Result.Quarantined("UNPARSEABLE_FILENAME");
        }

        if (repo.sameContentExists(route, sha256)) {
            // Identical bytes already registered under ANY name: no-op (F3).
            moveToDuplicates(claimed, arrivalId, name);
            LOG.infof("Duplicate content re-delivery ignored: %s", name);
            return new Result.DuplicateSameHash();
        }

        if (repo.sameKeyDifferentHashExists(route, client, msgId, sha256)) {
            repo.insertArrival(UUID.randomUUID(), route, name, sha256, client, msgId,
                    ArrivalStatus.QUARANTINED, "SAME_KEY_DIFFERENT_HASH", null);
            moveToError(claimed, arrivalId, name);
            LOG.warnf("QUARANTINED %s: same logical key, different hash", name);
            return new Result.Quarantined("SAME_KEY_DIFFERENT_HASH");
        }

        Optional<UUID> id = repo.insertArrival(arrivalId, route, name, sha256, client, msgId,
                ArrivalStatus.CLAIMED, null, claimed.toString());
        if (id.isEmpty()) {
            // Raced another writer on a dedup index: classify by what exists now.
            if (repo.sameContentExists(route, sha256)) {
                moveToDuplicates(claimed, arrivalId, name);
                return new Result.DuplicateSameHash();
            }
            repo.insertArrival(UUID.randomUUID(), route, name, sha256, client, msgId,
                    ArrivalStatus.QUARANTINED, "SAME_KEY_DIFFERENT_HASH", null);
            moveToError(claimed, arrivalId, name);
            return new Result.Quarantined("SAME_KEY_DIFFERENT_HASH");
        }
        LOG.infof("Arrival %s claimed as %s", name, claimed.getFileName());
        return new Result.NewArrival(id.get());
    }

    private void moveToError(Path claimed, UUID id, String name) {
        move(claimed, errorDir().resolve(id + "_" + name)); // unique: never clobbers evidence (F5)
    }

    private void moveToDuplicates(Path claimed, UUID id, String name) {
        move(claimed, duplicatesDir().resolve(id + "_" + name)); // kept, not deleted (F4)
    }

    public Path inflightDir() {
        return ensure(Path.of(config.exchangeRoot(), "archive", "inflight"));
    }

    public Path errorDir() {
        return ensure(Path.of(config.exchangeRoot(), "error"));
    }

    public Path duplicatesDir() {
        return ensure(Path.of(config.exchangeRoot(), "archive", "duplicates"));
    }

    private static Path ensure(Path dir) {
        try {
            Files.createDirectories(dir);
            return dir;
        } catch (IOException e) {
            throw new IllegalStateException("cannot create " + dir, e);
        }
    }

    private static void move(Path from, Path to) {
        try {
            Files.move(from, to, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new IllegalStateException("cannot move " + from + " -> " + to, e);
        }
    }

    /** Streaming digest (F17): no whole-file buffering on the long-running AGT. */
    private static String sha256(Path file) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
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
