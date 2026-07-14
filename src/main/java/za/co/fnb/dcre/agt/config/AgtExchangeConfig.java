package za.co.fnb.dcre.agt.config;

import io.smallrye.config.ConfigMapping;

import java.util.Map;

/**
 * Per-client inbound exchange layout (SCRUM-42, R-30 amendment). AGT only owns
 * the three INBOUND channels (onhost-req, onhost-req-endo, fint-resp); each
 * relative path resolves against {@code agt.exchange-root}
 * ({@link AgtConfig#exchangeRoot()}). Kept self-contained (no platform-files
 * dependency): AGT binds this locally.
 *
 * <p>SmallRye kebab-case binding maps {@code onhostReq()} to the yml key
 * {@code onhost-req}, {@code onhostReqEndo()} to {@code onhost-req-endo}, and
 * {@code fintResp()} to {@code fint-resp}. Nested types are interfaces because
 * {@code @ConfigMapping} groups cannot be records.
 */
@ConfigMapping(prefix = "agt.exchange")
public interface AgtExchangeConfig {

    /** Client token (FNBCC01/FNBCC02/FNBRF01) -> its inbound channel dirs. */
    Map<String, InboundChannels> clients();

    interface InboundChannels {
        ChannelDirs onhostReq();

        ChannelDirs onhostReqEndo();

        ChannelDirs fintResp();
    }

    interface ChannelDirs {
        String in();

        String error();

        String archive();
    }
}
