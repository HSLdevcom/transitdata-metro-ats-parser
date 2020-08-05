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
    private boolean addedTripsEnabled;

    public MetroEstimatesFactory(final PulsarApplicationContext context, boolean addedTripsEnabled) {
        this.jedis = context.getJedis();
        this.addedTripsEnabled = addedTripsEnabled;
        log.info("addedTripsEnabled set to: {}", this.addedTripsEnabled);
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
        final String endStopShortName = stopShortNames[1];

        // Create a metroKey to be used for Redis Query
        String metroKey;
        Optional<String> maybeStopNumber = MetroUtils.getStopNumber(startStopShortName, startStopShortName, endStopShortName);
        if (maybeStopNumber.isPresent()) {
            String stopNumber = maybeStopNumber.get();
            // Convert UTC datetime to local datetime because the keys in Redis have local datetime
            final Optional<String> maybeStartDatetime = MetroUtils.convertUtcDatetimeToPubtransDatetime(metroEstimate.beginTime);
            if (maybeStartDatetime.isPresent()) {
                final String startDatetime = maybeStartDatetime.get();
                metroKey = TransitdataProperties.formatMetroId(stopNumber, startDatetime);
            } else {
                log.warn("Failed to convert UTC datetime {} to local datetime", metroEstimate.beginTime);
                return Optional.empty();
            }
        } else {
            log.warn("Failed to get stop number for stop shortNames: startStopShortName: {}, endStopShortName: {}", startStopShortName, endStopShortName);
            return Optional.empty();
        }
        MetroAtsProtos.MetroEstimate.Builder metroEstimateBuilder = MetroAtsProtos.MetroEstimate.newBuilder();

        // Set fields from mqtt-pulsar-gateway into metroEstimateBuilder
        metroEstimateBuilder.setSchemaVersion(metroEstimateBuilder.getSchemaVersion());
        // trainType
        Optional<MetroAtsProtos.MetroTrainType> maybeMetroTrainTypeAts = getMetroTrainTypeAts(metroEstimate.trainType);
        if (!maybeMetroTrainTypeAts.isPresent()) {
            log.warn("metroEstimate.trainType is missing: {}", metroEstimate.trainType);
            return Optional.empty();
        }
        metroEstimateBuilder.setTrainType(maybeMetroTrainTypeAts.get());
        // journeySectionprogress
        Optional<MetroAtsProtos.MetroProgress> maybeMetroAtsProgress = getMetroAtsProgress(metroEstimate.journeySectionprogress, String.format("route name: %s, begin time: %s", metroEstimate.routeName, metroEstimate.beginTime));
        if (!maybeMetroAtsProgress.isPresent()) {
            log.warn("metroEstimate.journeySectionprogress is missing: {}", metroEstimate.journeySectionprogress);
            return Optional.empty();
        }
        metroEstimateBuilder.setJourneySectionprogress(maybeMetroAtsProgress.get());
        metroEstimateBuilder.setBeginTime(metroEstimate.beginTime);
        metroEstimateBuilder.setEndTime(metroEstimate.endTime);
        metroEstimateBuilder.setStartStopShortName(startStopShortName);

        Optional<Map<String, String>> metroJourneyData = getMetroJourneyData(metroKey);
        if(metroJourneyData.isPresent()) {
            // Set fields from Redis into metroEstimateBuilder
            Map<String, String> map = metroJourneyData.get();

            if (map.containsKey(TransitdataProperties.KEY_OPERATING_DAY))
                metroEstimateBuilder.setOperatingDay(map.get(TransitdataProperties.KEY_OPERATING_DAY));
            if (map.containsKey(TransitdataProperties.KEY_START_STOP_NUMBER))
                metroEstimateBuilder.setStartStopNumber(map.get(TransitdataProperties.KEY_START_STOP_NUMBER));
            if (map.containsKey(TransitdataProperties.KEY_START_TIME))
                metroEstimateBuilder.setStartTime(map.get(TransitdataProperties.KEY_START_TIME));
            if (map.containsKey(TransitdataProperties.KEY_DVJ_ID))
                metroEstimateBuilder.setDvjId(map.get(TransitdataProperties.KEY_DVJ_ID));
            if (map.containsKey(TransitdataProperties.KEY_ROUTE_NAME))
                metroEstimateBuilder.setRouteName(map.get(TransitdataProperties.KEY_ROUTE_NAME));
            if (map.containsKey(TransitdataProperties.KEY_START_DATETIME))
                metroEstimateBuilder.setStartDatetime(map.get(TransitdataProperties.KEY_START_DATETIME));
            if (map.containsKey(TransitdataProperties.KEY_DIRECTION))
                metroEstimateBuilder.setDirection(map.get(TransitdataProperties.KEY_DIRECTION));
        } else if (addedTripsEnabled){
            log.debug("Couldn't read metroJourneyData from redis, assuming that this metro journey is not present in the static schedule and creating added trip. Metro key: {}, redis map: {}. ", metroKey, metroJourneyData);
            MetroUtils.getRouteName(startStopShortName, endStopShortName).ifPresent(metroEstimateBuilder::setRouteName);
            MetroUtils.getJoreDirection(startStopShortName, endStopShortName).map(String::valueOf).ifPresent(metroEstimateBuilder::setDirection);
            maybeStopNumber.ifPresent(metroEstimateBuilder::setStartStopNumber);

            String startDateTime = MetroUtils.convertUtcDatetimeToPubtransDatetime(metroEstimate.beginTime).get();

            String operatingDay = startDateTime.substring(0, 10).replaceAll("-", "");
            String startTime = startDateTime.substring(11, 19);

            metroEstimateBuilder.setOperatingDay(operatingDay);
            metroEstimateBuilder.setStartTime(startTime);

            metroEstimateBuilder.setDvjId("metro-"+operatingDay+"-"+startTime+"-"+metroEstimate.routeName+"-"+metroEstimate.trainType.toString());

            metroEstimateBuilder.setScheduled(false);
        } else {
            log.debug("Couldn't read metroJourneyData from redis, ignoring this estimate. Metro key: {}, redis map: {}. ", metroKey, metroJourneyData);
            return Optional.empty();
        }

        Integer direction = metroJourneyData.map(map -> Integer.parseInt(map.get(TransitdataProperties.KEY_DIRECTION))).orElse(MetroUtils.getJoreDirection(startStopShortName, endStopShortName).orElse(null));
        if(direction == null) {
            log.warn("Couldn't read direction from metroJourneyData: {}.", direction);
            return Optional.empty();
        }

        // routeRows
        List<MetroAtsProtos.MetroStopEstimate> metroStopEstimates = new ArrayList<>();
        for (MetroStopEstimate metroStopEstimate : metroEstimate.routeRows) {
            Optional<MetroAtsProtos.MetroStopEstimate> maybeMetroStopEstimate = toMetroStopEstimate(metroStopEstimate, direction, metroEstimate.beginTime, startStopShortName);
            if (!maybeMetroStopEstimate.isPresent()) {
                    return Optional.empty();
            } else {
                metroStopEstimates.add(maybeMetroStopEstimate.get());
            }
        }

        if (!metroStopEstimates.stream().map(MetroAtsProtos.MetroStopEstimate::getStopNumber).allMatch(new HashSet<>()::add)) {
            log.warn("Metro {} (beginTime: {}, dir: {}) had multiple estimates for one stop", metroEstimate.routeName, metroEstimate.beginTime, direction);
        }

        metroEstimateBuilder.addAllMetroRows(metroStopEstimates);

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

    private Optional<MetroAtsProtos.MetroStopEstimate> toMetroStopEstimate (MetroStopEstimate metroStopEstimate, Integer direction, String beginTime, String startStopShortName) {
        MetroAtsProtos.MetroStopEstimate.Builder metroStopEstimateBuilder = MetroAtsProtos.MetroStopEstimate.newBuilder();

        // Set fields from mqtt-pulsar-gateway into metroStopEstimateBuilder
        metroStopEstimateBuilder.setStation((metroStopEstimate.station));
        metroStopEstimateBuilder.setPlatform((metroStopEstimate.platform));

        if (validateDatetime(metroStopEstimate.arrivalTimePlanned)) {
            metroStopEstimateBuilder.setArrivalTimePlanned(metroStopEstimate.arrivalTimePlanned);
        } else {
            metroStopEstimateBuilder.setArrivalTimePlanned("");
        }
        if (validateDatetime(metroStopEstimate.arrivalTimeForecast)) {
            metroStopEstimateBuilder.setArrivalTimeForecast(metroStopEstimate.arrivalTimeForecast);
        } else {
            metroStopEstimateBuilder.setArrivalTimeForecast("");
        }
        if (validateDatetime(metroStopEstimate.arrivalTimeMeasured)) {
            metroStopEstimateBuilder.setArrivalTimeMeasured(metroStopEstimate.arrivalTimeMeasured);
        } else {
            metroStopEstimateBuilder.setArrivalTimeMeasured("");
        }
        if (validateDatetime(metroStopEstimate.departureTimePlanned)) {
            metroStopEstimateBuilder.setDepartureTimePlanned(metroStopEstimate.departureTimePlanned);
        } else {
            metroStopEstimateBuilder.setDepartureTimePlanned("");
        }
        if (validateDatetime(metroStopEstimate.departureTimeForecast)) {
            metroStopEstimateBuilder.setDepartureTimeForecast(metroStopEstimate.departureTimeForecast);
        } else {
            metroStopEstimateBuilder.setDepartureTimeForecast("");
        }
        if (validateDatetime(metroStopEstimate.departureTimeMeasured)) {
            metroStopEstimateBuilder.setDepartureTimeMeasured(metroStopEstimate.departureTimeMeasured);
        } else {
            metroStopEstimateBuilder.setDepartureTimeMeasured("");
        }
        metroStopEstimateBuilder.setSource(metroStopEstimate.source);

        // stop number
        String shortName = metroStopEstimate.station;
        Optional<String> maybeStopNumber = MetroUtils.getStopNumber(shortName, direction);
        if (!maybeStopNumber.isPresent()) {
            log.warn("Couldn't find stopNumber for shortName: {} (Metro: direction {}, beginTime {}, startStopShortName: {})", shortName, direction, beginTime, startStopShortName);
            return Optional.empty();
        }
        metroStopEstimateBuilder.setStopNumber(maybeStopNumber.get());

        // rowProgress
        Optional<MetroAtsProtos.MetroProgress> maybeMetroAtsProgress = getMetroAtsProgress(metroStopEstimate.rowProgress, String.format("route departure time forecast %s:, station: %s", metroStopEstimate.departureTimeForecast, metroStopEstimate.station));
        maybeMetroAtsProgress.ifPresent(metroStopEstimateBuilder::setRowProgress);

        return Optional.of(metroStopEstimateBuilder.build());
    }

    private boolean validateDatetime(final String datetime) {
        return datetime != null && !datetime.equals("null") && !datetime.isEmpty();
    }

    private Optional<MetroAtsProtos.MetroProgress> getMetroAtsProgress(MetroProgress metroProgress, String details) {
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
                log.info("metroProgress is cancelled: details {} %s", details);
                maybeMetroAtsProgress = Optional.of(MetroAtsProtos.MetroProgress.CANCELLED);
                break;
            default:
                log.warn("Unrecognized metroProgress {}.", metroProgress);
                maybeMetroAtsProgress = Optional.empty();
                break;
        }
        return maybeMetroAtsProgress;
    }


    private Optional<Map<String, String>> getMetroJourneyData(final String metroKey) {
        synchronized (jedis) {
            try {
                Map<String, String> redisMap = jedis.hgetAll(metroKey);
                if (redisMap.isEmpty()) {
                    return Optional.empty();
                }
                return Optional.ofNullable(redisMap);
            } catch (Exception e) {
                log.error("Couldn't read metroJourneyData from redis. Metro key: {}", metroKey, e);
                return Optional.empty();
            }
        }
    }

    public static Optional<MetroEstimate> parsePayload(final byte[] payload) {
        try {
            MetroEstimate metroEstimate = mapper.readValue(payload, MetroEstimate.class);
            return Optional.of(metroEstimate);
        } catch (Exception e) {
            log.warn(String.format("Failed to parse payload %s.", new String(payload)), e);
        }
        return Optional.empty();
    }
}
