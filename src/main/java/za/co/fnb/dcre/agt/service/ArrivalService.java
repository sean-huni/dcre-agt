package za.co.fnb.dcre.agt.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.domain.ArrivalStatus;
import za.co.fnb.dcre.agt.repo.LedgerRepo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Optional;
import java.util.UUID;

/**
 * Registers inbound files in the file_arrival ledger (R-16): atomic claim by
 * rename, SHA-256 identity, R-31 filename tokens, same-key-different-hash
 * quarantine. AGT never opens file CONTENT beyond hashing (structural facts only).
 */
@ApplicationScoped
public class ArrivalService {

    private static final Logger LOG = Logger.getLogger(ArrivalService.class);
    public static final String ROUTE_ONHOST_REQ = "onhost-req";

    @Inject
    LedgerRepo repo;

    @Inject
    AgtConfig config;

    public sealed interface Result {
        record NewArrival(UUID id) implements Result { }
        record DuplicateSameHash() implements Result { }
        record Quarantined(String reason) implements Result { }
    }

    public Result register(Path file) {
        String name = file.getFileName().toString();
        String sha256 = sha256(file);
        String client = null;
        String msgId = null;
        String stem = name.contains(".") ? name.substring(0, name.lastIndexOf('.')) : name;
        String[] tokens = stem.split("_");
        if (tokens.length >= 2 && tokens[0].startsWith("FNB")) {
            client = tokens[0];
            msgId = tokens[1];
        }

        if (repo.sameKeyDifferentHashExists(ROUTE_ONHOST_REQ, client, msgId, sha256)) {
            repo.insertArrival(ROUTE_ONHOST_REQ, name, sha256, client, msgId,
                    ArrivalStatus.QUARANTINED, "SAME_KEY_DIFFERENT_HASH");
            move(file, errorDir().resolve(name));
            LOG.warnf("QUARANTINED %s: same logical key, different hash", name);
            return new Result.Quarantined("SAME_KEY_DIFFERENT_HASH");
        }

        Optional<UUID> id = repo.insertArrival(ROUTE_ONHOST_REQ, name, sha256, client, msgId,
                ArrivalStatus.CLAIMED, null);
        if (id.isEmpty()) {
            // Same (route, name, hash): re-delivery of identical content is a no-op (R-16).
            try {
                Files.deleteIfExists(file);
            } catch (IOException e) {
                LOG.warnf("could not remove duplicate file %s: %s", file, e.getMessage());
            }
            LOG.infof("Duplicate same-hash re-delivery ignored: %s", name);
            return new Result.DuplicateSameHash();
        }

        Path claimed = inflightDir().resolve(id.get() + "_" + name);
        move(file, claimed);
        repo.updateArrivalClaimedPath(id.get(), claimed.toString());
        LOG.infof("Arrival %s claimed as %s", name, claimed.getFileName());
        return new Result.NewArrival(id.get());
    }

    public Path inflightDir() {
        return ensure(Path.of(config.exchangeRoot(), "archive", "inflight"));
    }

    public Path errorDir() {
        return ensure(Path.of(config.exchangeRoot(), "error"));
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
            Files.move(from, to, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new IllegalStateException("cannot move " + from + " -> " + to, e);
        }
    }

    private static String sha256(Path file) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(md.digest(Files.readAllBytes(file)));
        } catch (Exception e) {
            throw new IllegalStateException("cannot hash " + file, e);
        }
    }
}
