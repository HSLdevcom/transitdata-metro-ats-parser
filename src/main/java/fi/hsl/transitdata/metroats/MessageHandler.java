package fi.hsl.transitdata.metroats;

import fi.hsl.common.transitdata.proto.MetroAtsProtos;
import fi.hsl.common.pulsar.IMessageHandler;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.TransitdataProperties.*;
import fi.hsl.common.transitdata.TransitdataSchema;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;


public class MessageHandler implements IMessageHandler {
    private static final Logger log = LoggerFactory.getLogger(MessageHandler.class);

    private Consumer<byte[]> consumer;
    private Producer<byte[]> producer;
    private MetroEstimatesFactory metroEstimatesFactory;

    public MessageHandler(PulsarApplicationContext context, final MetroEstimatesFactory metroEstimatesFactory) {
        consumer = context.getConsumer();
        producer = context.getProducer();
        this.metroEstimatesFactory = metroEstimatesFactory;
    }

    public void handleMessage(Message received) throws Exception {
        try {
            if (TransitdataSchema.hasProtobufSchema(received, ProtobufSchema.MqttRawMessage)) {
                final Optional<MetroAtsProtos.MetroEstimate> maybeMetroEstimate = metroEstimatesFactory.toMetroEstimate(received);

                    if (maybeMetroEstimate.isPresent()) {
                        final MessageId messageId = received.getMessageId();
                        final long timestamp = received.getEventTime();
                        final String key = received.getKey();
                        final MetroAtsProtos.MetroEstimate metroEstimate = maybeMetroEstimate.get();
                        sendPulsarMessage(messageId, metroEstimate, timestamp, key);
                    } else {
                        log.warn("Parsing MQTTRawMessage has failed, ignoring.");
                        ack(received.getMessageId()); //Ack so we don't receive it again
                    }
            }
            else {
                log.warn("Received unexpected schema, ignoring.");
                ack(received.getMessageId()); //Ack so we don't receive it again
            }
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

    private void sendPulsarMessage(MessageId received, MetroAtsProtos.MetroEstimate estimate, long timestamp, String key) {
        producer.newMessage()
                .key(key)
                .eventTime(timestamp)
                .property(TransitdataProperties.KEY_PROTOBUF_SCHEMA, ProtobufSchema.MetroAtsEstimate.toString())
                .property(TransitdataProperties.KEY_SCHEMA_VERSION, Integer.toString(estimate.getSchemaVersion()))
                .value(estimate.toByteArray())
                .sendAsync()
                .whenComplete((MessageId id, Throwable t) -> {
                    if (t != null) {
                        log.error("Failed to send Pulsar message", t);
                        // TODO:
                        //Should we abort?
                    }
                    else {
                        //Does this become a bottleneck? Does pulsar send more messages before we ack the previous one?
                        //If yes we need to get rid of this
                        ack(received);
                    }
                });

    }
}
