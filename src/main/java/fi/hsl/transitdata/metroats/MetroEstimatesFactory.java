package fi.hsl.transitdata.metroats;

import com.fasterxml.jackson.databind.ObjectMapper;
import fi.hsl.common.mqtt.proto.Mqtt;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.TransitdataSchema;
import fi.hsl.common.transitdata.proto.MetroAtsProtos;
import fi.hsl.transitdata.metroats.models.MetroEstimate;
import fi.hsl.transitdata.metroats.models.MetroProgress;
import fi.hsl.transitdata.metroats.models.MetroStopEstimate;
import fi.hsl.transitdata.metroats.models.MetroTrainType;
import org.apache.pulsar.client.api.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.*;

public class MetroEstimatesFactory {

    private static final Logger log = LoggerFactory.getLogger(MetroEstimatesFactory.class);

    private static final ObjectMapper mapper = new ObjectMapper();
    private Jedis jedis;

    public MetroEstimatesFactory(final PulsarApplicationContext context) {
        this.jedis = context.getJedis();
    }

    public Optional<MetroAtsProtos.MetroEstimate> toMetroEstimate(final Message message) {
        try {
            Optional<TransitdataSchema> maybeSchema = TransitdataSchema.parseFromPulsarMessage(message);
            if (maybeSchema.isPresent()) {
                final byte[] data = message.getData();
                final Mqtt.RawMessage mqttMessage = Mqtt.RawMessage.parseFrom(data);
                final byte[] payload = mqttMessage.getPayload().toByteArray();
                Optional<MetroEstimate> maybeMetroEstimate = parsePayload(payload);

                if (maybeMetroEstimate.isPresent()) {
                    final MetroEstimate metroEstimate = maybeMetroEstimate.get();
                    Optional<MetroAtsProtos.MetroEstimate> maybeMetroAtsEstimate = toMetroEstimate(metroEstimate);
                    return maybeMetroAtsEstimate;
                }
            }
        } catch (Exception e) {
            log.warn("Failed to produce metro schedule stop estimates.", e);
        }
        return Optional.empty();
    }

    private Optional<MetroAtsProtos.MetroEstimate> toMetroEstimate(final MetroEstimate metroEstimate) throws Exception {
        final String[] stopShortNames = metroEstimate.routeName.split("-");
        if (stopShortNames.length != 2) {
            log.warn("Failed to parse metro estimate route name {}", metroEstimate.routeName);
            return Optional.empty();
        }
        final String startStopShortName = stopShortNames[0];
        final String startDatetime = metroEstimate.beginTime;
        final String metroId = TransitdataProperties.formatMetroId(startStopShortName, startDatetime);

        MetroAtsProtos.MetroEstimate.Builder metroEstimateBuilder = MetroAtsProtos.MetroEstimate.newBuilder();

        // Set fields from mqtt-pulsar-gateway into metroEstimateBuilder
        metroEstimateBuilder.setSchemaVersion(metroEstimate.schemaVersion);
        // trainType
        Optional<MetroAtsProtos.MetroTrainType> maybeMetroTrainTypeAts = getMetroTrainTypeAts(metroEstimate.trainType);
        if (!maybeMetroTrainTypeAts.isPresent()) {
            log.warn("metroEstimate.trainType is missing: {}", metroEstimate.trainType);
            return Optional.empty();
        }
        metroEstimateBuilder.setTrainType(maybeMetroTrainTypeAts.get());
        // journeySectionprogress
        Optional<MetroAtsProtos.MetroProgress> maybeMetroAtsProgress = getMetroAtsProgress(metroEstimate.journeySectionprogress);
        if (!maybeMetroAtsProgress.isPresent()) {
            log.warn("metroEstimate.journeySectionprogress is missing: {}", metroEstimate.journeySectionprogress);
            return Optional.empty();
        }
        metroEstimateBuilder.setJourneySectionprogress(maybeMetroAtsProgress.get());
        metroEstimateBuilder.setBeginTime(metroEstimate.beginTime);
        metroEstimateBuilder.setEndTime(metroEstimate.endTime);

        // Set fields from Redis into metroEstimateBuilder
        Optional<Map<String, String>> metroJourneyData = getMetroJourneyData(metroId);
        if(!metroJourneyData.isPresent()) {
            log.warn("Couldn't read metroJourneyData from redis. Metro ID: {}, redis map: {}.", metroId, metroJourneyData);
            return Optional.empty();
        }
        metroJourneyData.ifPresent(map -> {
            if (map.containsKey(TransitdataProperties.KEY_OPERATING_DAY))
                metroEstimateBuilder.setOperatingDay(map.get(TransitdataProperties.KEY_OPERATING_DAY));
            if (map.containsKey(TransitdataProperties.KEY_START_STOP_NUMBER))
                metroEstimateBuilder.setStartStopNumber(map.get(TransitdataProperties.KEY_START_STOP_NUMBER));
            if (map.containsKey(TransitdataProperties.KEY_START_TIME))
                metroEstimateBuilder.setStartTime(map.get(TransitdataProperties.KEY_START_TIME));
            if (map.containsKey(TransitdataProperties.KEY_DVJ_ID)) {
                metroEstimateBuilder.setDvjId(map.get(TransitdataProperties.KEY_DVJ_ID));
            } if (map.containsKey(TransitdataProperties.KEY_START_STOP_SHORT_NAME))
                metroEstimateBuilder.setStartStopShortName(map.get(TransitdataProperties.KEY_START_STOP_SHORT_NAME));
            if (map.containsKey(TransitdataProperties.KEY_ROUTE_NAME))
                metroEstimateBuilder.setRouteName(map.get(TransitdataProperties.KEY_ROUTE_NAME));
            if (map.containsKey(TransitdataProperties.KEY_START_DATETIME))
                metroEstimateBuilder.setStartDatetime(map.get(TransitdataProperties.KEY_START_DATETIME));
            if (map.containsKey(TransitdataProperties.KEY_DIRECTION))
                metroEstimateBuilder.setDirection(map.get(TransitdataProperties.KEY_DIRECTION));
        });

        // routeRows
        List<MetroAtsProtos.MetroStopEstimate> metroStopEstimates = new ArrayList<>();
        for (MetroStopEstimate metroStopEstimate : metroEstimate.routeRows) {
            Optional<MetroAtsProtos.MetroStopEstimate> maybeMetroStopEstimate = toMetroStopEstimate(metroStopEstimate);
            if (!maybeMetroStopEstimate.isPresent()) {
                    return Optional.empty();
            } else {
                metroStopEstimates.add(maybeMetroStopEstimate.get());
            }
        };
        metroEstimateBuilder.addAllMetroRow(metroStopEstimates);

        return Optional.of(metroEstimateBuilder.build());
    }

    private Optional<MetroAtsProtos.MetroTrainType> getMetroTrainTypeAts(MetroTrainType metroTrainType) {
        Optional<MetroAtsProtos.MetroTrainType> maybeMetroTrainTypeAts;
        switch (metroTrainType) {
            case M:
                maybeMetroTrainTypeAts = Optional.of(MetroAtsProtos.MetroTrainType.M);
                break;
            case T:
                maybeMetroTrainTypeAts = Optional.of(MetroAtsProtos.MetroTrainType.T);
                break;
            default:
                log.warn("Unrecognized metroTrainType {}.", metroTrainType);
                maybeMetroTrainTypeAts = Optional.empty();
                break;
        }

        return maybeMetroTrainTypeAts;
    }

    private Optional<MetroAtsProtos.MetroStopEstimate> toMetroStopEstimate (MetroStopEstimate metroStopEstimate) {
        MetroAtsProtos.MetroStopEstimate.Builder metroStopEstimateBuilder = MetroAtsProtos.MetroStopEstimate.newBuilder();

        // Set fields from mqtt-pulsar-gateway into metroStopEstimateBuilder
        metroStopEstimateBuilder.setStation((metroStopEstimate.station));
        metroStopEstimateBuilder.setPlatform((metroStopEstimate.platform));

        metroStopEstimateBuilder.setArrivalTimePlanned(metroStopEstimate.arrivalTimePlanned);
        // ArrivalTimeForecast (can be "null" or "")
        if (!metroStopEstimate.arrivalTimeForecast.equals("null") && !metroStopEstimate.arrivalTimeForecast.isEmpty()) {
            metroStopEstimateBuilder.setArrivalTimeForecast(metroStopEstimate.arrivalTimeForecast);
        } else {
            metroStopEstimateBuilder.setArrivalTimeForecast("");
        }
        metroStopEstimateBuilder.setArrivalTimeMeasured((metroStopEstimate.arrivalTimeMeasured));
        metroStopEstimateBuilder.setDepartureTimePlanned(metroStopEstimate.departureTimePlanned);
        // DepartureTimeForecast (can be "null" or "")
        if (!metroStopEstimate.departureTimeForecast.equals("null") && !metroStopEstimate.departureTimeForecast.isEmpty()) {
            metroStopEstimateBuilder.setDepartureTimeForecast(metroStopEstimate.departureTimeForecast);
        } else {
            metroStopEstimateBuilder.setDepartureTimeForecast("");
        }
        metroStopEstimateBuilder.setDepartureTimeMeasured(metroStopEstimate.departureTimeMeasured);
        metroStopEstimateBuilder.setSource(metroStopEstimate.source);
        // rowProgress
        Optional<MetroAtsProtos.MetroProgress> maybeMetroAtsProgress = getMetroAtsProgress(metroStopEstimate.rowProgress);
        if (!maybeMetroAtsProgress.isPresent()) {
            return Optional.empty();
        }
        metroStopEstimateBuilder.setRowProgress(maybeMetroAtsProgress.get());

        return Optional.of(metroStopEstimateBuilder.build());
    }

    private Optional<MetroAtsProtos.MetroProgress> getMetroAtsProgress(MetroProgress metroProgress) {
        Optional<MetroAtsProtos.MetroProgress> maybeMetroAtsProgress;
        switch (metroProgress) {
            case SCHEDULED:
                maybeMetroAtsProgress = Optional.of(MetroAtsProtos.MetroProgress.SCHEDULED);
                break;
            case INPROGRESS:
                maybeMetroAtsProgress = Optional.of(MetroAtsProtos.MetroProgress.INPROGRESS);
                break;
            case COMPLETED:
                maybeMetroAtsProgress = Optional.of(MetroAtsProtos.MetroProgress.COMPLETED);
                break;
            case CANCELLED:
                maybeMetroAtsProgress = Optional.of(MetroAtsProtos.MetroProgress.CANCELLED);
                break;
            default:
                log.warn("Unrecognized metroProgress {}.", metroProgress);
                maybeMetroAtsProgress = Optional.empty();
                break;
        }
        return maybeMetroAtsProgress;
    }


    private Optional<Map<String, String>> getMetroJourneyData(final String metroId) {
        Map<String, String> redisMap = jedis.hgetAll(metroId);
        if (redisMap.isEmpty()) {
            return Optional.empty();
        }
        return Optional.ofNullable(redisMap);
    }

    private Optional<MetroEstimate> parsePayload(final byte[] payload) {
        try {
            MetroEstimate metroEstimate = mapper.readValue(payload, MetroEstimate.class);
            return Optional.of(metroEstimate);
        } catch (Exception e) {
            log.warn(String.format("Failed to parse payload %s.", new String(payload)), e);
        }
        return Optional.empty();
    }

}
