package za.co.fnb.dcre.agt.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.config.AgtExchangeConfig;
import za.co.fnb.dcre.agt.config.AgtExchangeConfig.ChannelDirs;
import za.co.fnb.dcre.agt.config.AgtExchangeConfig.InboundChannels;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Resolves the per-(client, inbound-channel) AGT sink directories from
 * agt.exchange config (SCRUM-42 / R-30 amendment): the claim target and
 * duplicates live under the channel {@code archive} dir, quarantine under the
 * channel {@code error} dir, all beneath {@code <root>/<clientbase>/<route>}.
 * Fail-closed on an unconfigured client/route so a misconfigured file never
 * lands in a shared or wrong client's tree.
 */
@ApplicationScoped
public class ExchangeSinks {

    @Inject
    AgtConfig config;

    @Inject
    AgtExchangeConfig exchange;

    /** Claim target: {@code <root>/<clientbase>/<route>/archive/inflight}. */
    public Path inflight(final String client, final String route) {
        return ensure(archiveDir(client, route).resolve("inflight"));
    }

    /** Quarantine sink: {@code <root>/<clientbase>/<route>/error}. */
    public Path error(final String client, final String route) {
        return ensure(root().resolve(channel(client, route).error()));
    }

    /** Duplicate sink: {@code <root>/<clientbase>/<route>/archive/duplicates}. */
    public Path duplicates(final String client, final String route) {
        return ensure(archiveDir(client, route).resolve("duplicates"));
    }

    private Path archiveDir(final String client, final String route) {
        return root().resolve(channel(client, route).archive());
    }

    private Path root() {
        return Path.of(config.exchangeRoot());
    }

    private ChannelDirs channel(final String client, final String route) {
        final InboundChannels channels = exchange.clients().get(client);
        if (channels == null) {
            throw new IllegalArgumentException("no agt.exchange config for client " + client);
        }
        return switch (route) {
            case ArrivalService.ROUTE_ONHOST_REQ -> channels.onhostReq();
            case ArrivalService.ROUTE_ONHOST_REQ_ENDO -> channels.onhostReqEndo();
            case ArrivalService.ROUTE_FINT_RESP -> channels.fintResp();
            case ArrivalService.ROUTE_ONHOST_REQ_MAN -> channels.onhostReqMan();
            case ArrivalService.ROUTE_FINT_RESP_MAN -> channels.fintRespMan();
            default -> throw new IllegalArgumentException("not an AGT inbound route: " + route);
        };
    }

    private static Path ensure(final Path dir) {
        try {
            Files.createDirectories(dir);
            return dir;
        } catch (IOException e) {
            throw new IllegalStateException("cannot create " + dir, e);
        }
    }
}
