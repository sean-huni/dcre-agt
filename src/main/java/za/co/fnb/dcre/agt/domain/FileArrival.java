package za.co.fnb.dcre.agt.domain;

import java.util.UUID;

public record FileArrival(UUID id, String routeId, String physicalFilename,
                          String payloadSha256, String clientToken, String msgIdToken,
                          ArrivalStatus status, String quarantineReason, String claimedPath) {
}
