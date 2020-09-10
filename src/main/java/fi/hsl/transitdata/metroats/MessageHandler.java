package fi.hsl.transitdata.metroats;

import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.common.pulsar.IMessageHandler;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.TransitdataProperties.*;
import fi.hsl.common.transitdata.TransitdataSchema;
import fi.hsl.common.transitdata.proto.MetroAtsProtos;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;


public class MessageHandler implements IMessageHandler {
    private static final Logger log = LoggerFactory.getLogger(MessageHandler.class);

    private Consumer<byte[]> consumer;
    private Producer<byte[]> cancellationProducer;
    private Producer<byte[]> stopEstimateProducer;
    private MetroCancellationFactory metroCancellationFactory;
    private MetroEstimateStopEstimatesFactory stopEstimateFactory;
    private MetroEstimatesFactory metroEstimatesFactory;

    public MessageHandler(PulsarApplicationContext context, final MetroCancellationFactory metroCancellationFactory, final MetroEstimateStopEstimatesFactory stopEstimateFactory, MetroEstimatesFactory metroEstimatesFactory) {
        consumer = context.getConsumer();
        cancellationProducer = context.getProducers().get("metro-trip-cancellation");
        stopEstimateProducer = context.getProducers().get("pubtrans-stop-estimate");
        this.metroCancellationFactory = metroCancellationFactory;
        this.stopEstimateFactory = stopEstimateFactory;
        this.metroEstimatesFactory = metroEstimatesFactory;
    }

    public void handleMessage(Message received) throws Exception {
        try {
            if (TransitdataSchema.hasProtobufSchema(received, ProtobufSchema.MqttRawMessage)) {
                final Optional<MetroAtsProtos.MetroEstimate> maybeMetroEstimate = metroEstimatesFactory.toMetroEstimate(received);
                if (maybeMetroEstimate.isPresent()) {
                final List<InternalMessages.StopEstimate> maybeStopEstimates = stopEstimateFactory.toStopEstimates(maybeMetroEstimate.get(), received.getEventTime());
                    final MessageId messageId = received.getMessageId();
                    final long timestamp = received.getEventTime();
                    final String key = received.getKey();
                    final List<InternalMessages.StopEstimate> stopEstimates = maybeStopEstimates;
                    stopEstimates.forEach(stopEstimate -> sendStopEstimatePulsarMessage(messageId, stopEstimate, timestamp, key));
                }
                final Optional<InternalMessages.TripCancellation> maybeTripCancellation = metroCancellationFactory.toTripCancellation(maybeMetroEstimate.get(), received.getEventTime());
                if (maybeTripCancellation.isPresent()) {
                    final InternalMessages.TripCancellation cancellation = maybeTripCancellation.get();
                    final MessageId messageId = received.getMessageId();
                    final long timestamp = received.getEventTime();
                    sendCancellationPulsarMessage(messageId, cancellation, timestamp, cancellation.getTripId());
                }

            }
            else {
                log.warn("Received unexpected schema, ignoring.");
            }
            ack(received.getMessageId()); //Ack so we don't receive it again
        }
        catch (Exception e) {
            log.error("Exception while handling message", e);
        }
    }

    private void ack(MessageId received) {
        consumer.acknowledgeAsync(received)
                .exceptionally(throwable -> {
                    log.error("Failed to ack Pulsar message", throwable);
                    return null;
                })
                .thenRun(() -> {});
    }

    private void sendStopEstimatePulsarMessage(MessageId received, InternalMessages.StopEstimate estimate, long timestamp, String key) {

        stopEstimateProducer.newMessage()
            .key(key)
            .eventTime(timestamp)
            .property(TransitdataProperties.KEY_PROTOBUF_SCHEMA, ProtobufSchema.InternalMessagesStopEstimate.toString())
            .property(TransitdataProperties.KEY_SCHEMA_VERSION, Integer.toString(estimate.getSchemaVersion()))
            .property(TransitdataProperties.KEY_DVJ_ID, estimate.getTripInfo().getTripId()) // TODO remove once TripUpdateProcessor won't need it anymore
            .value(estimate.toByteArray())
            .sendAsync()
            .whenComplete((MessageId id, Throwable t) -> {
                if (t != null) {
                    log.error("Failed to send Pulsar message", t);
                    //Should we abort?
                }
                else {
                    //Does this become a bottleneck? Does pulsar send more messages before we ack the previous one?
                    //If yes we need to get rid of this
                    ack(received);
                }
            });

    }

    public void sendCancellationPulsarMessage(final InternalMessages.TripCancellation cancellation, final long timestamp, final String dvjId) {
        sendCancellationPulsarMessage(null, cancellation, timestamp, dvjId);
    }

    public void sendCancellationPulsarMessage(final MessageId received, final InternalMessages.TripCancellation cancellation, final long timestamp, final String dvjId) {
        cancellationProducer.newMessage()
                .key(dvjId)
                .eventTime(timestamp)
                .property(TransitdataProperties.KEY_PROTOBUF_SCHEMA, TransitdataProperties.ProtobufSchema.InternalMessagesTripCancellation.toString())
                .property(TransitdataProperties.KEY_SCHEMA_VERSION, Integer.toString(cancellation.getSchemaVersion()))
                .property(TransitdataProperties.KEY_DVJ_ID, dvjId) // TODO remove once TripUpdateProcessor won't need it anymore
                .value(cancellation.toByteArray())
                .sendAsync()
                .whenComplete((MessageId id, Throwable t) -> {
                    if (t != null) {
                        log.error("Failed to send Pulsar message", t);
                        //Should we abort?
                    }
                    else {
                        log.info("Produced a cancellation for trip: " + cancellation.getRouteId() + "/" +
                                cancellation.getDirectionId() + "-" + cancellation.getStartTime() + "-" +
                                cancellation.getStartDate() + " with status: " + cancellation.getStatus().toString());
                    }
                });
    }
}
