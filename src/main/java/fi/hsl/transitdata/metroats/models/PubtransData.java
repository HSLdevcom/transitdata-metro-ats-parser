package fi.hsl.transitdata.metroats.models;

import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.common.transitdata.proto.PubtransTableProtos;

public class PubtransData {
    public PubtransTableProtos.Common common;
    public PubtransTableProtos.DOITripInfo tripInfo;
    public InternalMessages.StopEstimate.Type eventType;

    public PubtransData(InternalMessages.StopEstimate.Type eventType, PubtransTableProtos.Common common, PubtransTableProtos.DOITripInfo tripInfo) {
        this.tripInfo = tripInfo;
        this.common = common;
        this.eventType = eventType;
    }
}
