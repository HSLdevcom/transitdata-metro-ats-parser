package fi.hsl.transitdata.metroats.stopestimates;

import fi.hsl.common.transitdata.proto.InternalMessages;
import org.apache.pulsar.client.api.Message;

import java.util.List;
import java.util.Optional;

public interface IStopEstimatesFactory {
    Optional<List<InternalMessages.StopEstimate>> toStopEstimates(final Message message);
}
