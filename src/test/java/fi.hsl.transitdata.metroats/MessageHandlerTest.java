package fi.hsl.transitdata.metroats;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;
import fi.hsl.common.mqtt.proto.Mqtt;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.redis.RedisUtils;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.shade.javassist.bytecode.ByteArray;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MessageHandlerTest {

    public PulsarApplicationContext mockContext;
    public Consumer mockConsumer;
    public MetroCancellationFactory metroCancellationFactory;
    public MetroEstimateStopEstimatesFactory metroEstimateStopEstimatesFactory;
    public MetroEstimatesFactory metroEstimatesFactory;
    public Producer<byte[]> mockCancellationProducer;
    public Producer<byte[]> stopEstimateProducer;

    @Before
    public void before(){
        Jedis mockJedis = mock(Jedis.class);
        mockConsumer = mock(Consumer.class);
        when(mockConsumer.acknowledgeAsync(Mockito.any(MessageId.class))).thenReturn(new CompletableFuture<Void>());
        mockContext = mock(PulsarApplicationContext.class);
        when(mockContext.getJedis()).thenReturn(mockJedis);
        when(mockContext.getConsumer()).thenReturn(mockConsumer);

        TypedMessageBuilder tripUpdateMockTypeMessageBuilder = createMock();
        TypedMessageBuilder stopEstimateMockTypeMessageBuilder = createMock();
        mockCancellationProducer = mock(Producer.class);
        when(mockCancellationProducer.newMessage()).thenReturn(tripUpdateMockTypeMessageBuilder);
        stopEstimateProducer = mock(Producer.class);
        when(stopEstimateProducer.newMessage()).thenReturn(stopEstimateMockTypeMessageBuilder);

        HashMap<String, Producer<byte[]>> producerMocks = new HashMap();
        producerMocks.put("metro-trip-cancellation", mockCancellationProducer);
        producerMocks.put("pubtrans-stop-estimate", stopEstimateProducer);
        when(mockContext.getProducers()).thenReturn(producerMocks);
        RedisUtils mockRedis = mock(RedisUtils.class);

        metroCancellationFactory = new MetroCancellationFactory(mockRedis, 30);
        metroEstimateStopEstimatesFactory = new MetroEstimateStopEstimatesFactory();
        metroEstimatesFactory = new MetroEstimatesFactory(mockContext, true);
    }

    private TypedMessageBuilder createMock(){
        TypedMessageBuilder mockTypeMessageBuilder = mock(TypedMessageBuilder.class);
        when(mockTypeMessageBuilder.key(Mockito.any())).thenReturn(mockTypeMessageBuilder);
        when(mockTypeMessageBuilder.eventTime(Mockito.anyLong())).thenReturn(mockTypeMessageBuilder);
        when(mockTypeMessageBuilder.property(Mockito.any(), Mockito.any())).thenReturn(mockTypeMessageBuilder);
        when(mockTypeMessageBuilder.value(Mockito.any())).thenReturn(mockTypeMessageBuilder);
        when(mockTypeMessageBuilder.sendAsync()).thenReturn(new CompletableFuture());
        return mockTypeMessageBuilder;
    }

    @Test
    public void handleMessageTest() throws Exception {
        MessageHandler messageHandler = new MessageHandler(mockContext, metroCancellationFactory, metroEstimateStopEstimatesFactory, metroEstimatesFactory);
        Message message = createMessage("metro.json");
        messageHandler.handleMessage(message);
        Mockito.verify(mockConsumer, Mockito.times(1)).acknowledgeAsync(Mockito.any(MessageId.class));
        Mockito.verify(stopEstimateProducer, Mockito.times(44)).newMessage();
        //No cancellations since so are not connected to redis
        Mockito.verify(mockCancellationProducer, Mockito.times(0)).newMessage();
    }

    private Message<Any> createMessage(String path ) throws IOException {
        URL url  = getClass().getClassLoader().getResource(path);
        String content = new Scanner(url.openStream(), "UTF-8").useDelimiter("\\A").next();
        String topic = "/topic/with/json/payload/#";
        byte[] payload = content.getBytes(Charset.forName("UTF-8"));
        BiFunction<String, byte[], byte[]> mapper = createMapper();
        byte[] mapped = mapper.apply(topic, payload);
        Mqtt.RawMessage mqttMessage = Mqtt.RawMessage.parseFrom(mapped);
        Message pulsarMessage = mock(Message.class);
        when(pulsarMessage.getEventTime()).thenReturn(new Date().getTime());
        when(pulsarMessage.getData()).thenReturn(mqttMessage.toByteArray());
        when(pulsarMessage.getMessageId()).thenReturn(mock(MessageId.class));
        when(pulsarMessage.getProperty("protobuf-schema")).thenReturn("mqtt-raw");
        return pulsarMessage;
    }

    private BiFunction<String, byte[], byte[]> createMapper() {
        return new BiFunction<String, byte[], byte[]>() {
            @Override
            public byte[] apply(String topic, byte[] payload) {
                Mqtt.RawMessage.Builder builder = Mqtt.RawMessage.newBuilder();
                MessageLite raw = builder
                        .setSchemaVersion(builder.getSchemaVersion())
                        .setTopic(topic)
                        .setPayload(ByteString.copyFrom(payload))
                        .build();
                return raw.toByteArray();
            }
        };
    }
}
